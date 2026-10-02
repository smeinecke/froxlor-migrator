"""Shared, side-effect-free migration planning helpers.

Both front-ends (the headless CLI in ``tui.py`` and the Textual wizard in
``wizard.py``) consume these helpers to turn raw Froxlor API rows into the
``Selection`` that ``Migrator.execute`` consumes. Nothing in this module prints
or prompts; API data arrives and leaves as plain dicts.
"""

from __future__ import annotations

import shlex
from collections.abc import Callable, Iterable
from typing import Any

from .api import FroxlorApiError, FroxlorClient
from .config import AppConfig
from .migration.types import ResourceRow, Selection
from .util import as_int, domain_name, ftp_username, mailbox_address, pick, resolve_subdomain_parts


def split_csv(raw: str | None) -> list[str]:
    if raw is None:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def filter_ssh_keys_for_ftps(ssh_keys: list[dict], ftp_rows: list[dict]) -> list[dict]:
    # SSH keys attach to FTP users; keys for FTP accounts that were not
    # selected cannot be migrated and would hard-fail in _ensure_ssh_keys.
    ftp_names = {ftp_username(item) for item in ftp_rows}
    return [item for item in ssh_keys if ftp_username(item) in ftp_names]


def parse_mapping_arg(raw: str | None, arg_name: str) -> dict[str, str]:
    if raw is None or not raw.strip():
        return {}
    mapping: dict[str, str] = {}
    for entry in split_csv(raw):
        if "=>" in entry:
            left, right = entry.split("=>", 1)
        elif "=" in entry:
            left, right = entry.split("=", 1)
        else:
            raise ValueError(f"Invalid {arg_name} value '{entry}': expected source=>target")
        source_token = left.strip().lower()
        target_token = right.strip().lower()
        if not source_token or not target_token:
            raise ValueError(f"Invalid {arg_name} value '{entry}': empty source/target token")
        mapping[source_token] = target_token
    return mapping


def build_value_index(rows: list[dict], value_getter: Callable[[dict], int], alias_getter: Callable[[dict], list[str]]) -> dict[str, set[int]]:
    index: dict[str, set[int]] = {}
    for row in rows:
        value = value_getter(row)
        if value <= 0:
            continue
        for alias in alias_getter(row):
            key = alias.strip().lower()
            if not key:
                continue
            index.setdefault(key, set()).add(value)
    return index


def resolve_named_mapping(
    raw_mapping: dict[str, str],
    source_rows: list[dict],
    source_value_getter: Callable[[dict], int],
    source_alias_getter: Callable[[dict], list[str]],
    target_rows: list[dict],
    target_value_getter: Callable[[dict], int],
    target_alias_getter: Callable[[dict], list[str]],
    mapping_label: str,
) -> dict[int, int]:
    if not raw_mapping:
        return {}
    source_index = build_value_index(source_rows, source_value_getter, source_alias_getter)
    target_index = build_value_index(target_rows, target_value_getter, target_alias_getter)
    resolved: dict[int, int] = {}
    for source_token, target_token in raw_mapping.items():
        source_matches = source_index.get(source_token, set())
        if not source_matches:
            raise ValueError(f"{mapping_label} contains unknown source token '{source_token}'")
        if len(source_matches) > 1:
            raise ValueError(f"{mapping_label} source token '{source_token}' is ambiguous")
        target_matches = target_index.get(target_token, set())
        if not target_matches:
            raise ValueError(f"{mapping_label} contains unknown target token '{target_token}'")
        if len(target_matches) > 1:
            raise ValueError(f"{mapping_label} target token '{target_token}' is ambiguous")
        resolved[next(iter(source_matches))] = next(iter(target_matches))
    return resolved


def php_setting_aliases(row: dict) -> list[str]:
    setting_id = as_int(pick(row, "id", default=0))
    description = str(pick(row, "description", default="")).strip()
    binary = str(pick(row, "binary", default="")).strip()
    aliases = [
        str(setting_id) if setting_id > 0 else "",
        description,
        binary,
        f"{description}|{binary}" if description or binary else "",
    ]
    return aliases


def ip_aliases(row: dict) -> list[str]:
    ip_id = as_int(pick(row, "id", default=0))
    ip = str(pick(row, "ip", default="")).strip()
    port = as_int(pick(row, "port", default=0))
    ssl = as_int(pick(row, "ssl", default=0))
    aliases = [
        str(ip_id) if ip_id > 0 else "",
        f"{ip}:{port}",
        f"{ip}:{port}:{ssl}",
    ]
    return aliases


def php_setting_token(row: dict) -> str:
    desc = str(pick(row, "description", default="")).strip()
    binary = str(pick(row, "binary", default="")).strip()
    return f"{desc}|{binary}".strip("|").lower()


def ip_token(row: dict) -> str:
    ip = str(pick(row, "ip", default="")).strip()
    if not ip:
        return ""
    port = as_int(pick(row, "port", default=0))
    ssl = as_int(pick(row, "ssl", default=0))
    return f"{ip}:{port}:{ssl}".lower()


def build_mapping_tokens(
    resolved_map: dict[int, int],
    source_rows: list[dict],
    target_rows: list[dict],
    token_getter: Callable[[dict], str],
) -> dict[str, str]:
    if not resolved_map:
        return {}
    source_by_id = {as_int(pick(row, "id", default=0)): row for row in source_rows}
    target_by_id = {as_int(pick(row, "id", default=0)): row for row in target_rows}
    tokens: dict[str, str] = {}
    for source_id, target_id in sorted(resolved_map.items()):
        source_row = source_by_id.get(source_id)
        target_row = target_by_id.get(target_id)
        if source_row is None or target_row is None:
            continue
        source_token = token_getter(source_row)
        target_token = token_getter(target_row)
        if source_token and target_token:
            tokens[source_token] = target_token
    return tokens


def build_php_mapping_tokens(resolved_map: dict[int, int], source_settings: list[dict], target_settings: list[dict]) -> dict[str, str]:
    return build_mapping_tokens(resolved_map, source_settings, target_settings, php_setting_token)


def build_ip_mapping_tokens(resolved_map: dict[int, int], source_rows: list[dict], target_rows: list[dict]) -> dict[str, str]:
    return build_mapping_tokens(resolved_map, source_rows, target_rows, ip_token)


def select_rows_by_tokens(
    rows: list[dict],
    selectors_raw: str | None,
    selector_values: Callable[[dict], list[str]],
    selector_label: str,
) -> list[dict]:
    if selectors_raw is None:
        return rows
    tokens = {token.lower() for token in split_csv(selectors_raw)}
    if not tokens:
        return rows
    if "all" in tokens:
        return rows
    if "none" in tokens:
        return []

    selected: list[dict] = []
    unresolved = set(tokens)
    for row in rows:
        values = {value.strip().lower() for value in selector_values(row) if value and value.strip()}
        if values & tokens:
            selected.append(row)
            unresolved -= values & tokens
    if unresolved:
        missing = ", ".join(sorted(unresolved))
        raise ValueError(f"Unknown {selector_label} selector(s): {missing}")
    return selected


def flag_parts(enabled: bool, flag: str) -> list[str]:
    return [flag] if enabled else []


def customer_selector_token(customer: dict[str, Any]) -> str:
    customer_id = as_int(pick(customer, "customerid", "id", default=0))
    if customer_id > 0:
        return str(customer_id)
    return str(pick(customer, "loginname", "login", default="")).strip()


def customer_selector_values(row: dict) -> list[str]:
    return [str(row.get("id", "")), str(row.get("login", "")), str(row.get("name", "")), str(row.get("email", ""))]


def names_or_none(names: Iterable[str]) -> str:
    cleaned = [name for name in names if name]
    return ",".join(cleaned) if cleaned else "none"


def mapping_parts(flag: str, tokens: dict[str, str]) -> list[str]:
    if not tokens:
        return []
    value = ",".join(f"{source}=>{target}" for source, target in sorted(tokens.items()))
    return [flag, value]


def build_replay_command(
    *,
    config_path: str,
    apply: bool,
    debug: bool,
    migrate_whole_customer: bool,
    selected_customer: dict[str, Any],
    target_customer: dict[str, Any] | None,
    selected_domains: list[dict],
    selected_subdomains: list[dict],
    selected_databases: list[dict],
    selected_mailboxes: list[dict],
    selected_ftps: list[dict],
    php_mapping_tokens: dict[str, str],
    ip_mapping_tokens: dict[str, str],
    include_files: bool,
    include_databases: bool,
    include_mail: bool,
    include_certificates: bool,
    include_domain_zones: bool,
    include_password_sync: bool,
    include_forwarders: bool,
    include_sender_aliases: bool,
    skip_subdomains: bool,
    skip_database_name_validation: bool,
) -> str:
    parts: list[str] = ["froxlor-migrator", "--config", config_path, "--non-interactive", "--yes"]
    parts += flag_parts(apply, "--apply")
    parts += flag_parts(debug, "--debug")
    parts.append("--whole-customer" if migrate_whole_customer else "--domain-only")

    parts.extend(["--source-customer", customer_selector_token(selected_customer)])
    if not migrate_whole_customer:
        target_token = "new" if target_customer is None else customer_selector_token(target_customer)
        parts.extend(["--target-customer", target_token])

    parts.extend([
        "--domains",
        names_or_none(dedupe_keep_order(domain_name(row) for row in selected_domains)),
        "--subdomains",
        names_or_none(dedupe_keep_order(domain_name(row) for row in selected_subdomains)),
        "--databases",
        names_or_none(dedupe_keep_order(str(pick(row, "databasename", "dbname", default="")).strip() for row in selected_databases)),
        "--mailboxes",
        names_or_none(dedupe_keep_order(mailbox_address(row) for row in selected_mailboxes)),
        "--ftp-accounts",
        names_or_none(dedupe_keep_order(ftp_username(row) for row in selected_ftps)),
    ])

    parts += mapping_parts("--php-map", php_mapping_tokens)
    parts += mapping_parts("--ip-map", ip_mapping_tokens)

    parts.extend(["--include-files", "yes" if include_files else "no"])
    parts.extend(["--include-databases", "yes" if include_databases else "no"])
    parts.extend(["--include-mail", "yes" if include_mail else "no"])

    parts += flag_parts(skip_subdomains, "--skip-subdomains")
    parts += flag_parts(skip_database_name_validation, "--skip-database-name-validation")
    parts += flag_parts(not include_certificates, "--skip-certificates")
    parts += flag_parts(not include_domain_zones, "--skip-dns-zones")
    parts += flag_parts(not include_password_sync, "--skip-password-sync")
    parts += flag_parts(not include_forwarders, "--skip-forwarders")
    parts += flag_parts(not include_sender_aliases, "--skip-sender-aliases")

    return " ".join(shlex.quote(part) for part in parts)


def customer_view(customers: list[dict]) -> list[dict]:
    view = []
    for item in customers:
        view.append({
            "id": as_int(pick(item, "customerid", "id", default=0)),
            "login": pick(item, "loginname", "login", default=""),
            "name": pick(item, "name", "company", default=""),
            "email": pick(item, "email", default=""),
            "_raw": item,
        })
    return view


def domain_view(domains: list[dict]) -> list[dict]:
    view = []
    for item in domains:
        view.append({
            "domain": pick(item, "domain", "domainname", default=""),
            "docroot": pick(item, "documentroot", default=""),
            "ssl": pick(item, "sslenabled", default=""),
            "php": as_int(pick(item, "phpsettingid", default=0)),
            "_raw": item,
        })
    return view


def db_view(dbs: list[dict]) -> list[dict]:
    view = []
    for item in dbs:
        view.append({
            "dbname": pick(item, "databasename", "dbname", default=""),
            "description": pick(item, "description", default=""),
            "server": pick(item, "mysql_server", "dbserver", default=""),
            "_raw": item,
        })
    return view


def subdomain_view(rows: list[dict]) -> list[dict]:
    view = []
    for item in rows:
        view.append({
            "domain": pick(item, "domain", "domainname", default=""),
            "path": pick(item, "path", "documentroot", default=""),
            "ssl": pick(item, "sslenabled", "ssl_enabled", default=""),
            "_raw": item,
        })
    return view


def ftp_view(rows: list[dict]) -> list[dict]:
    view = []
    for item in rows:
        view.append({
            "username": pick(item, "username", "ftpuser", default=""),
            "path": pick(item, "path", default=""),
            "login": pick(item, "login_enabled", default=""),
            "_raw": item,
        })
    return view


def mail_view(emails: list[dict], selected_domains: set[str]) -> list[dict]:
    view = []
    for item in emails:
        email = mailbox_address(item)
        domain = email.split("@", 1)[1] if "@" in email else ""
        if selected_domains and domain not in selected_domains:
            continue
        view.append({"email": email, "domain": domain, "_raw": item})
    return view


def php_settings_view(settings: list[dict]) -> list[dict]:
    view = []
    for item in settings:
        view.append({
            "id": as_int(pick(item, "id", default=0)),
            "description": pick(item, "description", default=""),
            "binary": pick(item, "binary", default=""),
            "_raw": item,
        })
    return view


def ip_view(ip_rows: list[dict]) -> list[dict]:
    view = []
    for item in ip_rows:
        view.append({
            "id": as_int(pick(item, "id", default=0)),
            "ip": str(pick(item, "ip", default="")),
            "port": as_int(pick(item, "port", default=0)),
            "ssl": as_int(pick(item, "ssl", default=0)),
            "_raw": item,
        })
    return view


def domain_in_source_root(domain: dict, source_root: str) -> bool:
    docroot = str(pick(domain, "documentroot", default="")).strip()
    root = source_root.rstrip("/")
    if not docroot or not docroot.startswith("/"):
        # Empty documentroot resolves to the customer homedir, and Froxlor may
        # store documentroot relative to it - both are inside source_web_root.
        return True
    return bool(docroot.startswith(root + "/") or docroot == root)


def build_clients(config: AppConfig) -> tuple[FroxlorClient, FroxlorClient]:
    source = FroxlorClient(
        api_url=config.source.api_url,
        api_key=config.source.api_key,
        api_secret=config.source.api_secret,
        timeout_seconds=config.source.timeout_seconds,
    )
    target = FroxlorClient(
        api_url=config.target.api_url,
        api_key=config.target.api_key,
        api_secret=config.target.api_secret,
        timeout_seconds=config.target.timeout_seconds,
    )
    return source, target


def discover_customer_resources(source: FroxlorClient, customer_id: int, customer_login: str) -> dict[str, list[dict]]:
    kwargs = {"customerid": customer_id if customer_id else None, "loginname": customer_login or None}
    return {
        "domains": source.list_domains(**kwargs),
        "subdomains": source.list_subdomains(**kwargs),
        "dbs": source.list_mysqls(**kwargs),
        "emails": source.list_emails(**kwargs),
        "ftps": source.list_ftps(**kwargs),
        "forwarders": source.list_email_forwarders(**kwargs),
        "sender_aliases": source.list_email_senders(**kwargs),
        "dir_protections": source.list_dir_protections(**kwargs),
        "dir_options": source.list_dir_options(**kwargs),
        "ssh_keys": source.list_ssh_keys(**kwargs),
        "data_dumps": source.list_data_dumps(**kwargs),
    }


def fetch_domain_zones(source: FroxlorClient, domain_names: Iterable[str]) -> tuple[list[dict], list[str]]:
    """Return (zone rows, per-domain error strings) — one API call per domain."""
    zones: list[dict] = []
    errors: list[str] = []
    for zone_domain in sorted(set(domain_names)):
        try:
            zones.extend(source.list_domain_zones(domainname=zone_domain))
        except FroxlorApiError as exc:
            errors.append(f"{zone_domain}: {exc}")
    return zones, errors


def narrow_subdomains(subdomains: list[dict], selected_domain_names: set[str]) -> list[dict]:
    """Keep only subdomains whose parent domain was selected."""
    return [
        item
        for item in subdomains
        if resolve_subdomain_parts(
            domain_name(item),
            str(pick(item, "parentdomain", "maindomain", default="")),
            selected_domain_names,
        )
        is not None
    ]


def derive_scoped_resources(
    resources: dict[str, list[dict]],
    selected_mailboxes: list[dict],
    selected_ftps: list[dict],
) -> dict[str, list[dict]]:
    """Resources auto-derived from explicit selections (forwarders/aliases
    follow selected mailboxes, SSH keys follow selected FTP accounts)."""
    mailbox_names = {mailbox_address(item) for item in selected_mailboxes}
    return {
        "dir_protections": resources["dir_protections"],
        "dir_options": resources["dir_options"],
        "ssh_keys": filter_ssh_keys_for_ftps(resources["ssh_keys"], selected_ftps),
        "data_dumps": resources["data_dumps"],
        "forwarders": [item for item in resources["forwarders"] if mailbox_address(item) in mailbox_names],
        "sender_aliases": [item for item in resources["sender_aliases"] if mailbox_address(item) in mailbox_names],
    }


def collect_php_mapping_candidates(
    selected_domains: list[dict],
    source_settings: list[dict],
    target_settings: list[dict],
) -> tuple[list[int], list[dict], list[dict], dict[int, int]]:
    """Return (used source ids, source rows, target view rows, default map).

    The default map keeps a source id when it exists on the target, else falls
    back to the first target setting — the same rule the headless path uses.
    """
    source_ids = sorted({as_int(pick(item, "phpsettingid", default=0)) for item in selected_domains if as_int(pick(item, "phpsettingid", default=0)) > 0})
    if not source_ids:
        return [], [], [], {}
    target_rows = php_settings_view(target_settings)
    if not target_rows:
        raise ValueError("No target PHP settings found")
    source_rows = [row for row in source_settings if as_int(pick(row, "id", default=0)) in source_ids]
    valid_target_ids = {row["id"] for row in target_rows}
    default_target_id = target_rows[0]["id"]
    default_map = {source_id: (source_id if source_id in valid_target_ids else default_target_id) for source_id in source_ids}
    return source_ids, source_rows, target_rows, default_map


def collect_ip_mapping_candidates(selected_domains: list[dict]) -> list[dict]:
    """Deduplicated source ip:port rows actually bound to selected domains."""
    source_ips: dict[int, dict] = {}
    for domain in selected_domains:
        for ip in pick(domain, "ipsandports", default=[]) or []:
            ip_id = as_int(pick(ip, "id", default=0))
            if ip_id > 0 and ip_id not in source_ips:
                source_ips[ip_id] = ip
    return [source_ips[ip_id] for ip_id in sorted(source_ips)]


def build_selection(
    *,
    customer: ResourceRow,
    target_customer: ResourceRow | None,
    domains: list[ResourceRow],
    subdomains: list[ResourceRow],
    databases: list[ResourceRow],
    mailboxes: list[ResourceRow],
    email_forwarders: list[ResourceRow],
    email_senders: list[ResourceRow],
    ftp_accounts: list[ResourceRow],
    ssh_keys: list[ResourceRow],
    data_dumps: list[ResourceRow],
    dir_protections: list[ResourceRow],
    dir_options: list[ResourceRow],
    domain_zones: list[ResourceRow],
    include_files: bool,
    include_databases: bool,
    include_mail: bool,
    include_subdomains: bool,
    validate_database_names: bool,
    php_setting_map: dict[int, int],
    ip_mapping: dict[int, int],
    include_certificates: bool,
    include_domain_zones: bool,
    include_password_sync: bool,
    include_forwarders: bool,
    include_sender_aliases: bool,
) -> Selection:
    return Selection(
        customer=customer,
        target_customer=target_customer,
        domains=domains,
        subdomains=subdomains,
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
        include_files=include_files,
        include_databases=include_databases,
        include_mail=include_mail,
        include_subdomains=include_subdomains,
        validate_database_names=validate_database_names,
        php_setting_map=php_setting_map,
        ip_mapping=ip_mapping,
        include_certificates=include_certificates,
        include_domain_zones=include_domain_zones,
        include_password_sync=include_password_sync,
        include_forwarders=include_forwarders,
        include_sender_aliases=include_sender_aliases,
    )


def plan_rows(
    *,
    selected_domains: list[dict],
    selected_subdomains: list[dict],
    selected_databases: list[dict],
    selected_mailboxes: list[dict],
    selected_forwarders: list[dict],
    selected_sender_aliases: list[dict],
    selected_ftps: list[dict],
    selected_ssh_keys: list[dict],
    selected_data_dumps: list[dict],
    selected_dir_protections: list[dict],
    selected_dir_options: list[dict],
    selected_domain_zones: list[dict],
    migrate_whole_customer: bool,
    php_setting_map: dict[int, int],
    ip_mapping: dict[int, int],
    include_files: bool,
    include_databases: bool,
    include_mail: bool,
    include_certificates: bool,
    include_domain_zones: bool,
    include_password_sync: bool,
    include_forwarders: bool,
    include_sender_aliases: bool,
    include_subdomains: bool,
    validate_database_names: bool,
    debug: bool,
    dry_run: bool,
) -> list[tuple[str, str]]:
    return [
        ("Customer", "1"),
        ("Domains", str(len(selected_domains))),
        ("Subdomains", str(len(selected_subdomains) if include_subdomains else 0)),
        ("Databases", str(len(selected_databases) if include_databases else 0)),
        ("Mailboxes", str(len(selected_mailboxes) if include_mail else 0)),
        ("Mail forwarders", str(len(selected_forwarders) if include_forwarders else 0)),
        ("Sender aliases", str(len(selected_sender_aliases) if include_sender_aliases else 0)),
        ("FTP accounts", str(len(selected_ftps))),
        ("SSH keys", str(len(selected_ssh_keys))),
        ("Data dumps", str(len(selected_data_dumps))),
        ("Dir protections", str(len(selected_dir_protections))),
        ("Dir options", str(len(selected_dir_options))),
        ("Domain zone records", str(len(selected_domain_zones))),
        ("Whole customer mode", "yes" if migrate_whole_customer else "no"),
        ("PHP mappings", str(len(php_setting_map))),
        ("Mapped IP entries", str(len(ip_mapping))),
        ("Certificates", "yes" if include_certificates else "no"),
        ("Domain zone sync", "yes" if include_domain_zones else "no"),
        ("Password hash sync", "yes" if include_password_sync else "no"),
        ("Forwarders", "yes" if include_forwarders else "no"),
        ("Sender aliases sync", "yes" if include_sender_aliases else "no"),
        ("Files transfer", "yes" if include_files else "no"),
        ("Validate DB names", "yes" if validate_database_names else "no"),
        ("Debug tracing", "yes" if debug else "no"),
        ("Dry-run", "yes" if dry_run else "no"),
    ]
