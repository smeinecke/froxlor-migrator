#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from froxlor_migrator.api import FroxlorClient
from froxlor_migrator.config import load_config
from froxlor_migrator.migrate import Migrator, Selection
from froxlor_migrator.transfer import TransferRunner
from froxlor_migrator.util import as_int, pick, slugify


def _domain_in_source_root(domain: dict, source_root: str) -> bool:
    docroot = str(pick(domain, "documentroot", default="")).strip()
    root = source_root.rstrip("/")
    return bool(docroot.startswith(root + "/") or docroot == root)


def _php_setting_map_by_name(
    source_domains: list[dict],
    source_php_settings: list[dict],
    target_php_settings: list[dict],
) -> dict[int, int]:
    source_by_id = {as_int(pick(row, "id", default=0)): row for row in source_php_settings}
    target_by_name = {str(pick(row, "description", default="")).strip().lower(): as_int(pick(row, "id", default=0)) for row in target_php_settings}
    target_ids = [as_int(pick(row, "id", default=0)) for row in target_php_settings if as_int(pick(row, "id", default=0)) > 0]
    default_target_id = target_ids[0] if target_ids else 0

    mapping: dict[int, int] = {}
    source_ids = {as_int(pick(row, "phpsettingid", default=0)) for row in source_domains if as_int(pick(row, "phpsettingid", default=0)) > 0}
    for source_id in sorted(source_ids):
        src_row = source_by_id.get(source_id)
        src_name = str(pick(src_row or {}, "description", default="")).strip().lower()
        target_id = target_by_name.get(src_name, 0)
        if target_id <= 0:
            target_id = source_id if source_id in target_ids else default_target_id
        if target_id > 0:
            mapping[source_id] = target_id
    return mapping


def _migrate_customer(config_path: str, customer_login: str, include_mail: bool) -> None:
    config = load_config(config_path)
    source = FroxlorClient(
        config.source.api_url,
        config.source.api_key,
        config.source.api_secret,
        config.source.timeout_seconds,
    )
    target = FroxlorClient(
        config.target.api_url,
        config.target.api_key,
        config.target.api_secret,
        config.target.timeout_seconds,
    )

    customer = None
    for row in source.list_customers():
        login = str(pick(row, "loginname", "login", default="")).strip().lower()
        if login == customer_login.lower():
            customer = row
            break
    if not customer:
        raise RuntimeError(f"Customer not found on source: {customer_login}")

    customer_id = as_int(pick(customer, "customerid", "id", default=0))
    login = str(pick(customer, "loginname", "login", default="")).strip()
    domains = source.list_domains(customerid=customer_id if customer_id else None, loginname=login or None)
    selected_domains = [row for row in domains if _domain_in_source_root(row, config.paths.source_web_root)]
    selected_domain_names = {str(pick(row, "domain", "domainname", default="")).strip().lower() for row in selected_domains}

    subdomains = source.list_subdomains(customerid=customer_id if customer_id else None, loginname=login or None)
    selected_subdomains = [row for row in subdomains if str(pick(row, "domain", "domainname", default="")).split(".", 1)[-1].lower() in selected_domain_names]

    databases = source.list_mysqls(customerid=customer_id if customer_id else None, loginname=login or None)
    mailboxes = source.list_emails(customerid=customer_id if customer_id else None, loginname=login or None)
    ftp_accounts = source.list_ftps(customerid=customer_id if customer_id else None, loginname=login or None)
    ssh_keys = source.list_ssh_keys(customerid=customer_id if customer_id else None, loginname=login or None)
    data_dumps = source.list_data_dumps(customerid=customer_id if customer_id else None, loginname=login or None)
    dir_protections = source.list_dir_protections(customerid=customer_id if customer_id else None, loginname=login or None)
    dir_options = source.list_dir_options(customerid=customer_id if customer_id else None, loginname=login or None)

    mailbox_names = {str(pick(row, "email_full", "email", "emailaddr", default="")).strip().lower() for row in mailboxes}
    email_forwarders = [
        row
        for row in source.list_email_forwarders(customerid=customer_id if customer_id else None, loginname=login or None)
        if str(pick(row, "email", "emailaddr", default="")).strip().lower() in mailbox_names
    ]
    email_senders = [
        row
        for row in source.list_email_senders(customerid=customer_id if customer_id else None, loginname=login or None)
        if str(pick(row, "email", "emailaddr", default="")).strip().lower() in mailbox_names
    ]

    domain_zones: list[dict] = []
    for domain_name in sorted(selected_domain_names):
        domain_zones.extend(source.list_domain_zones(domainname=domain_name))

    php_setting_map = _php_setting_map_by_name(
        selected_domains,
        source.list_php_settings(),
        target.list_php_settings(),
    )

    runner = TransferRunner(
        config=config,
        dry_run=False,
        manifest_name=slugify(f"bootstrap-apply-{login}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"),
    )
    migrator = Migrator(config=config, source=source, target=target, runner=runner)

    selection = Selection(
        customer=customer,
        domains=selected_domains,
        subdomains=selected_subdomains,
        databases=databases,
        mailboxes=mailboxes,
        email_forwarders=email_forwarders,
        email_senders=email_senders,
        ftp_accounts=ftp_accounts,
        ssh_keys=ssh_keys,
        data_dumps=data_dumps,
        dir_protections=dir_protections,
        dir_options=dir_options,
        domain_zones=domain_zones,
        include_files=True,
        include_databases=True,
        include_mail=include_mail,
        php_setting_map=php_setting_map,
        ip_mapping={},
    )
    context = migrator.execute(selection)
    print(
        f"Migrated {login}: target_customer_id={context.target_customer_id} "
        f"domains={len(selected_domains)} dbs={len(context.source_to_target_db)} mailboxes={len(mailboxes)} "
        f"manifest={runner.manifest_path}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run non-interactive apply migration for selected customers")
    parser.add_argument("--config", required=True, help="Path to config TOML")
    parser.add_argument("--customer", action="append", required=True, help="Customer login (repeatable)")
    parser.add_argument("--include-mail", action="store_true", help="Also migrate mailbox content via doveadm")
    args = parser.parse_args()

    for login in args.customer:
        _migrate_customer(args.config, login, include_mail=args.include_mail)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
