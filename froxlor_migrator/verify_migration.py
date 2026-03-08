from __future__ import annotations

import argparse
import shlex
import subprocess
from typing import Any

from .api import FroxlorApiError, FroxlorClient
from .config import load_config
from .util import as_bool, as_int, pick


def _domain_name(row: dict[str, Any]) -> str:
    return str(pick(row, "domain", "domainname", default="")).lower()


def _mail_name(row: dict[str, Any]) -> str:
    return str(pick(row, "email_full", "email", "emailaddr", default="")).lower()


def _subdomain_name(row: dict[str, Any]) -> str:
    return str(pick(row, "domain", "domainname", default="")).lower()


def _ftp_name(row: dict[str, Any]) -> str:
    return str(pick(row, "username", "ftpuser", default="")).lower()


def _dir_protection_name(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(pick(row, "path", default="")).strip().lower(),
        str(pick(row, "username", default="")).strip().lower(),
    )


def _dir_option_name(row: dict[str, Any]) -> str:
    return str(pick(row, "path", default="")).strip().lower()


def _ssh_key_name(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(pick(row, "username", "ftpuser", default="")).strip().lower(),
        str(pick(row, "ssh_pubkey", default="")).strip(),
    )


def _data_dump_key(row: dict[str, Any]) -> tuple[str, int, int, int, str]:
    return (
        str(pick(row, "path", default="")).strip(),
        as_int(pick(row, "dump_dbs", default=0)),
        as_int(pick(row, "dump_mail", default=0)),
        as_int(pick(row, "dump_web", default=0)),
        str(pick(row, "pgp_public_key", default="")).strip(),
    )


def _docroot_in_any_root(docroot: str, roots: list[str]) -> bool:
    value = docroot.strip()
    for root in roots:
        normalized = root.rstrip("/")
        if not normalized:
            continue
        if value == normalized or value.startswith(normalized + "/"):
            return True
    return False


def _expected_target_docroot(source_docroot: str, source_roots: list[str], target_root: str) -> str:
    value = source_docroot.strip()
    target_base = target_root.rstrip("/")
    for root in source_roots:
        normalized = root.rstrip("/")
        if not normalized:
            continue
        if value == normalized:
            return target_base
        if value.startswith(normalized + "/"):
            suffix = value[len(normalized) :]
            return target_base + suffix
    return value


def _normalize_customer_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        login = str(pick(row, "loginname", "login", default="")).strip().lower()
        if login:
            result[login] = row
    return result


def _normalize_php_setting_map(rows: list[dict[str, Any]]) -> dict[int, str]:
    result: dict[int, str] = {}
    for row in rows:
        setting_id = as_int(pick(row, "id", default=0))
        if setting_id <= 0:
            continue
        result[setting_id] = str(pick(row, "description", default="")).strip().lower()
    return result


def _compare_domain(
    source_row: dict[str, Any],
    target_row: dict[str, Any],
    source_php_map: dict[int, str],
    target_php_map: dict[int, str],
    source_roots: list[str],
    target_root: str,
) -> list[str]:
    errors: list[str] = []
    expected_documentroot = _expected_target_docroot(
        str(pick(source_row, "documentroot", default="")),
        source_roots,
        target_root,
    )
    checks = [
        (
            "documentroot",
            expected_documentroot,
            str(pick(target_row, "documentroot", default="")),
        ),
        (
            "phpenabled",
            as_int(pick(source_row, "phpenabled", default=0)),
            as_int(pick(target_row, "phpenabled", default=0)),
        ),
        (
            "sslenabled",
            as_int(pick(source_row, "ssl_enabled", default=0)),
            as_int(pick(target_row, "ssl_enabled", default=0)),
        ),
        (
            "letsencrypt",
            as_int(pick(source_row, "letsencrypt", default=0)),
            as_int(pick(target_row, "letsencrypt", default=0)),
        ),
        (
            "isemaildomain",
            as_int(pick(source_row, "isemaildomain", default=0)),
            as_int(pick(target_row, "isemaildomain", default=0)),
        ),
        (
            "email_only",
            as_int(pick(source_row, "email_only", default=0)),
            as_int(pick(target_row, "email_only", default=0)),
        ),
        (
            "specialsettings",
            str(pick(source_row, "specialsettings", default="")),
            str(pick(target_row, "specialsettings", default="")),
        ),
        (
            "ssl_specialsettings",
            str(pick(source_row, "ssl_specialsettings", default="")),
            str(pick(target_row, "ssl_specialsettings", default="")),
        ),
        (
            "openbasedir",
            as_int(pick(source_row, "openbasedir", default=0)),
            as_int(pick(target_row, "openbasedir", default=0)),
        ),
        (
            "openbasedir_path",
            str(pick(source_row, "openbasedir_path", default="")),
            str(pick(target_row, "openbasedir_path", default="")),
        ),
        (
            "writeaccesslog",
            as_int(pick(source_row, "writeaccesslog", default=0)),
            as_int(pick(target_row, "writeaccesslog", default=0)),
        ),
        (
            "writeerrorlog",
            as_int(pick(source_row, "writeerrorlog", default=0)),
            as_int(pick(target_row, "writeerrorlog", default=0)),
        ),
        (
            "dkim",
            as_int(pick(source_row, "dkim", default=0)),
            as_int(pick(target_row, "dkim", default=0)),
        ),
        (
            "alias",
            as_int(pick(source_row, "alias", default=0)),
            as_int(pick(target_row, "alias", default=0)),
        ),
        (
            "specialsettingsforsubdomains",
            as_int(pick(source_row, "specialsettingsforsubdomains", default=0)),
            as_int(pick(target_row, "specialsettingsforsubdomains", default=0)),
        ),
        (
            "phpsettingsforsubdomains",
            as_int(pick(source_row, "phpsettingsforsubdomains", default=0)),
            as_int(pick(target_row, "phpsettingsforsubdomains", default=0)),
        ),
        (
            "mod_fcgid_starter",
            as_int(pick(source_row, "mod_fcgid_starter", default=-1)),
            as_int(pick(target_row, "mod_fcgid_starter", default=-1)),
        ),
        (
            "mod_fcgid_maxrequests",
            as_int(pick(source_row, "mod_fcgid_maxrequests", default=-1)),
            as_int(pick(target_row, "mod_fcgid_maxrequests", default=-1)),
        ),
        (
            "deactivated",
            as_int(pick(source_row, "deactivated", default=0)),
            as_int(pick(target_row, "deactivated", default=0)),
        ),
    ]
    for field, src, dst in checks:
        if src != dst:
            errors.append(f"{field} source={src!r} target={dst!r}")

    src_php_id = as_int(pick(source_row, "phpsettingid", default=0))
    dst_php_id = as_int(pick(target_row, "phpsettingid", default=0))
    src_php_name = source_php_map.get(src_php_id, "")
    dst_php_name = target_php_map.get(dst_php_id, "")
    if src_php_name and dst_php_name and src_php_name != dst_php_name:
        errors.append(f"phpsetting source={src_php_name!r} target={dst_php_name!r}")
    elif src_php_id > 0 and dst_php_id > 0 and src_php_name == "" and dst_php_name == "" and src_php_id != dst_php_id:
        errors.append(f"phpsettingid source={src_php_id!r} target={dst_php_id!r}")

    src_dkim = str(pick(source_row, "dkim_pubkey", default=""))
    if src_dkim and src_dkim != str(pick(target_row, "dkim_pubkey", default="")):
        errors.append("dkim_pubkey mismatch")

    return errors


def _compare_mail(source_row: dict[str, Any], target_row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    fields = [
        "spam_tag_level",
        "rewrite_subject",
        "spam_kill_level",
        "bypass_spam",
        "policy_greylist",
        "iscatchall",
    ]
    for field in fields:
        src = as_int(pick(source_row, field, default=0))
        dst = as_int(pick(target_row, field, default=0))
        if src != dst:
            errors.append(f"{field} source={src} target={dst}")
    return errors


def _compare_customer(source_row: dict[str, Any], target_row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    checks = [
        (
            "diskspace_ul",
            as_int(pick(source_row, "diskspace_ul", default=0)),
            as_int(pick(target_row, "diskspace_ul", default=0)),
        ),
        (
            "traffic_ul",
            as_int(pick(source_row, "traffic_ul", default=0)),
            as_int(pick(target_row, "traffic_ul", default=0)),
        ),
        (
            "subdomains_ul",
            as_int(pick(source_row, "subdomains_ul", default=0)),
            as_int(pick(target_row, "subdomains_ul", default=0)),
        ),
        (
            "emails_ul",
            as_int(pick(source_row, "emails_ul", default=0)),
            as_int(pick(target_row, "emails_ul", default=0)),
        ),
        (
            "email_accounts_ul",
            as_int(pick(source_row, "email_accounts_ul", default=0)),
            as_int(pick(target_row, "email_accounts_ul", default=0)),
        ),
        (
            "email_forwarders_ul",
            as_int(pick(source_row, "email_forwarders_ul", default=0)),
            as_int(pick(target_row, "email_forwarders_ul", default=0)),
        ),
        (
            "email_quota_ul",
            as_int(pick(source_row, "email_quota_ul", default=0)),
            as_int(pick(target_row, "email_quota_ul", default=0)),
        ),
        (
            "ftps_ul",
            as_int(pick(source_row, "ftps_ul", default=0)),
            as_int(pick(target_row, "ftps_ul", default=0)),
        ),
        (
            "mysqls_ul",
            as_int(pick(source_row, "mysqls_ul", default=0)),
            as_int(pick(target_row, "mysqls_ul", default=0)),
        ),
        (
            "createstdsubdomain",
            as_int(pick(source_row, "createstdsubdomain", default=0)),
            as_int(pick(target_row, "createstdsubdomain", default=0)),
        ),
        (
            "store_defaultindex",
            as_int(pick(source_row, "store_defaultindex", default=0)),
            as_int(pick(target_row, "store_defaultindex", default=0)),
        ),
        (
            "deactivated",
            as_int(pick(source_row, "deactivated", default=0)),
            as_int(pick(target_row, "deactivated", default=0)),
        ),
    ]
    for field, src, dst in checks:
        if src != dst:
            errors.append(f"{field} source={src!r} target={dst!r}")
    source_theme = str(pick(source_row, "theme", default="")).strip().lower()
    target_theme = str(pick(target_row, "theme", default="")).strip().lower()
    if source_theme and source_theme != target_theme:
        errors.append(f"theme source={source_theme!r} target={target_theme!r}")
    source_password = str(pick(source_row, "password", default="")).strip()
    target_password = str(pick(target_row, "password", default="")).strip()
    if source_password and source_password != target_password:
        errors.append("password hash mismatch")
    if as_int(pick(source_row, "type_2fa", default=0)) != as_int(pick(target_row, "type_2fa", default=0)):
        errors.append(f"type_2fa source={as_int(pick(source_row, 'type_2fa', default=0))!r} target={as_int(pick(target_row, 'type_2fa', default=0))!r}")
    if str(pick(source_row, "data_2fa", default="")).strip() != str(pick(target_row, "data_2fa", default="")).strip():
        errors.append("data_2fa mismatch")
    return errors


def _compare_subdomain(
    source_row: dict[str, Any],
    target_row: dict[str, Any],
    source_php_map: dict[int, str],
    target_php_map: dict[int, str],
) -> list[str]:
    errors: list[str] = []
    checks = [
        (
            "path",
            str(pick(source_row, "path", default="")),
            str(pick(target_row, "path", default="")),
        ),
        ("url", str(pick(source_row, "url", default="")), str(pick(target_row, "url", default=""))),
        (
            "sslenabled",
            as_int(pick(source_row, "ssl_enabled", "sslenabled", default=0)),
            as_int(pick(target_row, "ssl_enabled", "sslenabled", default=0)),
        ),
        (
            "ssl_redirect",
            as_int(pick(source_row, "ssl_redirect", default=0)),
            as_int(pick(target_row, "ssl_redirect", default=0)),
        ),
        (
            "letsencrypt",
            as_int(pick(source_row, "letsencrypt", default=0)),
            as_int(pick(target_row, "letsencrypt", default=0)),
        ),
    ]
    for field, src, dst in checks:
        if str(src) != str(dst):
            errors.append(f"{field} source={src!r} target={dst!r}")
    src_php_id = as_int(pick(source_row, "phpsettingid", default=0))
    dst_php_id = as_int(pick(target_row, "phpsettingid", default=0))
    src_php_name = source_php_map.get(src_php_id, "")
    dst_php_name = target_php_map.get(dst_php_id, "")
    if src_php_name and dst_php_name and src_php_name != dst_php_name:
        errors.append(f"phpsetting source={src_php_name!r} target={dst_php_name!r}")
    elif src_php_id > 0 and dst_php_id > 0 and src_php_name == "" and dst_php_name == "" and src_php_id != dst_php_id:
        errors.append(f"phpsettingid source={src_php_id!r} target={dst_php_id!r}")
    return errors


def _compare_ftp(source_row: dict[str, Any], target_row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    checks = [
        (
            "path",
            str(pick(source_row, "path", default="")),
            str(pick(target_row, "path", default="")),
        ),
        (
            "description",
            str(pick(source_row, "description", "ftp_description", default="")),
            str(pick(target_row, "description", "ftp_description", default="")),
        ),
        (
            "shell",
            str(pick(source_row, "shell", default="")),
            str(pick(target_row, "shell", default="")),
        ),
        (
            "login_enabled",
            as_bool(pick(source_row, "login_enabled", default=1), default=True),
            as_bool(pick(target_row, "login_enabled", default=1), default=True),
        ),
        (
            "password",
            str(pick(source_row, "password", default="")).strip(),
            str(pick(target_row, "password", default="")).strip(),
        ),
    ]
    for field, src, dst in checks:
        if str(src) != str(dst):
            errors.append(f"{field} source={src!r} target={dst!r}")
    return errors


def _compare_dir_protection(source_row: dict[str, Any], target_row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    checks = [
        (
            "path",
            str(pick(source_row, "path", default="")),
            str(pick(target_row, "path", default="")),
        ),
        (
            "username",
            str(pick(source_row, "username", default="")),
            str(pick(target_row, "username", default="")),
        ),
        (
            "authname",
            str(pick(source_row, "authname", default="")),
            str(pick(target_row, "authname", default="")),
        ),
        (
            "password",
            str(pick(source_row, "password", default="")),
            str(pick(target_row, "password", default="")),
        ),
    ]
    for field, src, dst in checks:
        if str(src) != str(dst):
            errors.append(f"{field} source={src!r} target={dst!r}")
    return errors


def _compare_dir_option(source_row: dict[str, Any], target_row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    checks = [
        (
            "options_indexes",
            as_bool(pick(source_row, "options_indexes", default=0), default=False),
            as_bool(pick(target_row, "options_indexes", default=0), default=False),
        ),
        (
            "options_cgi",
            as_bool(pick(source_row, "options_cgi", default=0), default=False),
            as_bool(pick(target_row, "options_cgi", default=0), default=False),
        ),
        (
            "error404path",
            str(pick(source_row, "error404path", default="")),
            str(pick(target_row, "error404path", default="")),
        ),
        (
            "error403path",
            str(pick(source_row, "error403path", default="")),
            str(pick(target_row, "error403path", default="")),
        ),
        (
            "error500path",
            str(pick(source_row, "error500path", default="")),
            str(pick(target_row, "error500path", default="")),
        ),
        (
            "error401path",
            str(pick(source_row, "error401path", default="")),
            str(pick(target_row, "error401path", default="")),
        ),
    ]
    for field, src, dst in checks:
        if str(src) != str(dst):
            errors.append(f"{field} source={src!r} target={dst!r}")
    return errors


def _is_custom_zone_record(row: dict[str, Any]) -> bool:
    for flag in (
        "is_default",
        "isdefault",
        "is_default_record",
        "isfroxlordefault",
        "default_entry",
    ):
        if as_int(pick(row, flag, default=0)) == 1:
            return False
    record_type = str(pick(row, "type", default="")).upper()
    if record_type in {"SOA", "NS"}:
        return False
    return True


def _run_mysql_query_local(mysql_cmd: str, mysql_args: list[str], database: str, sql: str) -> list[list[str]]:
    command = [*shlex.split(mysql_cmd), *mysql_args, database, "-N", "-B", "-e", sql]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "mysql query failed")
    rows: list[list[str]] = []
    for line in completed.stdout.splitlines():
        rows.append(line.split("\t"))
    return rows


def _run_mysql_query_target_via_ssh(config, sql: str) -> list[list[str]]:
    mysql = shlex.quote(config.commands.mysql)
    target_args = " ".join(shlex.quote(arg) for arg in config.mysql.target_import_args)
    panel_db = shlex.quote(config.mysql.target_panel_database)
    ssh = config.commands.ssh
    options = []
    if not config.ssh.strict_host_key_checking:
        options.append("-o StrictHostKeyChecking=no")
        options.append("-o UserKnownHostsFile=/dev/null")
    options.append(f"-p {config.ssh.port}")
    ssh_prefix = f"{ssh} {' '.join(options)} -l {shlex.quote(config.ssh.user)} {shlex.quote(config.ssh.host)}"
    remote_cmd = f"{mysql} {target_args} {panel_db} -N -B -e {shlex.quote(sql)}"
    command = f"{ssh_prefix} {shlex.quote(remote_cmd)}"
    completed = subprocess.run(["bash", "-o", "pipefail", "-c", command], capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "target mysql query failed")
    rows: list[list[str]] = []
    for line in completed.stdout.splitlines():
        rows.append(line.split("\t"))
    return rows


def _load_redirect_map_source(config, customer_id: int) -> dict[str, tuple[str, int]]:
    sql = (
        "SELECT d.domain, a.domain, COALESCE(drc.rid, 1) "
        "FROM panel_domains d "
        "JOIN panel_domains a ON a.id=d.aliasdomain "
        "LEFT JOIN domain_redirect_codes drc ON drc.did=d.id "
        f"WHERE d.customerid={customer_id} AND d.aliasdomain IS NOT NULL"
    )
    rows = _run_mysql_query_local(
        config.commands.mysql,
        config.mysql.source_dump_args,
        config.mysql.source_panel_database,
        sql,
    )
    result: dict[str, tuple[str, int]] = {}
    for row in rows:
        if len(row) < 3:
            continue
        result[str(row[0]).strip().lower()] = (
            str(row[1]).strip().lower(),
            as_int(row[2], default=1),
        )
    return result


def _load_redirect_map_target(config, customer_id: int) -> dict[str, tuple[str, int]]:
    sql = (
        "SELECT d.domain, a.domain, COALESCE(drc.rid, 1) "
        "FROM panel_domains d "
        "JOIN panel_domains a ON a.id=d.aliasdomain "
        "LEFT JOIN domain_redirect_codes drc ON drc.did=d.id "
        f"WHERE d.customerid={customer_id} AND d.aliasdomain IS NOT NULL"
    )
    rows = _run_mysql_query_target_via_ssh(config, sql)
    result: dict[str, tuple[str, int]] = {}
    for row in rows:
        if len(row) < 3:
            continue
        result[str(row[0]).strip().lower()] = (
            str(row[1]).strip().lower(),
            as_int(row[2], default=1),
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify migrated source/target parity")
    parser.add_argument("--config", default="config.toml", help="Path to config TOML")
    parser.add_argument("--customer", action="append", default=[], help="Customer login to verify (repeatable)")
    args = parser.parse_args()

    config = load_config(args.config)
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

    try:
        source_customers = _normalize_customer_map(source.list_customers())
        target_customers = _normalize_customer_map(target.list_customers())
        source_php_map = _normalize_php_setting_map(source.list_php_settings())
        target_php_map = _normalize_php_setting_map(target.list_php_settings())
    except FroxlorApiError as exc:
        print(f"ERROR: {exc}")
        return 2

    requested = [x.strip().lower() for x in args.customer if x.strip()]
    if requested:
        logins = requested
    else:
        logins = sorted(set(source_customers) & set(target_customers))

    failures = 0
    for login in logins:
        src_customer = source_customers.get(login)
        dst_customer = target_customers.get(login)
        if not src_customer or not dst_customer:
            print(f"FAIL customer={login}: missing on {'source' if not src_customer else 'target'}")
            failures += 1
            continue

        src_id = as_int(pick(src_customer, "customerid", "id", default=0))
        dst_id = as_int(pick(dst_customer, "customerid", "id", default=0))

        customer_errs = _compare_customer(src_customer, dst_customer)
        if customer_errs:
            print(f"FAIL customer={login}: {'; '.join(customer_errs)}")
            failures += 1
            customer_failed = True
        else:
            customer_failed = False

        src_domains = {_domain_name(x): x for x in source.list_domains(customerid=src_id, loginname=login)}
        dst_domains = {_domain_name(x): x for x in target.list_domains(customerid=dst_id, loginname=login)}
        src_subdomains = {_subdomain_name(x): x for x in source.list_subdomains(customerid=src_id, loginname=login)}
        dst_subdomains = {_subdomain_name(x): x for x in target.list_subdomains(customerid=dst_id, loginname=login)}
        source_roots = [config.paths.source_web_root, config.paths.source_transfer_root]
        migratable_domain_names = {
            name
            for name, row in src_domains.items()
            if _docroot_in_any_root(str(pick(row, "documentroot", default="")), source_roots)
        }

        src_mails = {_mail_name(x): x for x in source.list_emails(customerid=src_id, loginname=login)}
        dst_mails = {_mail_name(x): x for x in target.list_emails(customerid=dst_id, loginname=login)}
        src_ftps = {_ftp_name(x): x for x in source.list_ftps(customerid=src_id, loginname=login)}
        dst_ftps = {_ftp_name(x): x for x in target.list_ftps(customerid=dst_id, loginname=login)}
        src_dir_protections = {_dir_protection_name(x): x for x in source.list_dir_protections(customerid=src_id, loginname=login)}
        dst_dir_protections = {_dir_protection_name(x): x for x in target.list_dir_protections(customerid=dst_id, loginname=login)}
        src_dir_options = {_dir_option_name(x): x for x in source.list_dir_options(customerid=src_id, loginname=login)}
        dst_dir_options = {_dir_option_name(x): x for x in target.list_dir_options(customerid=dst_id, loginname=login)}
        src_ssh_keys = {_ssh_key_name(x): x for x in source.list_ssh_keys(customerid=src_id, loginname=login)}
        dst_ssh_keys = {_ssh_key_name(x): x for x in target.list_ssh_keys(customerid=dst_id, loginname=login)}

        src_data_dumps = {_data_dump_key(x) for x in source.list_data_dumps(customerid=src_id, loginname=login)}
        dst_data_dumps = {_data_dump_key(x) for x in target.list_data_dumps(customerid=dst_id, loginname=login)}

        src_forwarders = {
            (
                str(pick(x, "email", "emailaddr", default="")).strip().lower(),
                str(pick(x, "destination", default="")).strip().lower(),
            )
            for x in source.list_email_forwarders(customerid=src_id, loginname=login)
        }
        dst_forwarders = {
            (
                str(pick(x, "email", "emailaddr", default="")).strip().lower(),
                str(pick(x, "destination", default="")).strip().lower(),
            )
            for x in target.list_email_forwarders(customerid=dst_id, loginname=login)
        }

        src_senders = {
            (
                str(pick(x, "email", "emailaddr", default="")).strip().lower(),
                str(pick(x, "allowed_sender", default="")).strip().lower(),
            )
            for x in source.list_email_senders(customerid=src_id, loginname=login)
        }
        dst_senders = {
            (
                str(pick(x, "email", "emailaddr", default="")).strip().lower(),
                str(pick(x, "allowed_sender", default="")).strip().lower(),
            )
            for x in target.list_email_senders(customerid=dst_id, loginname=login)
        }

        src_certs = {str(pick(x, "domainname", "domain", default="")).lower(): x for x in source.listing("Certificates.listing")}
        dst_certs = {str(pick(x, "domainname", "domain", default="")).lower(): x for x in target.listing("Certificates.listing")}

        try:
            src_redirects = _load_redirect_map_source(config, src_id)
            dst_redirects = _load_redirect_map_target(config, dst_id)
        except Exception as exc:
            print(f"FAIL customer={login} redirects: could not query redirect mappings ({exc})")
            failures += 1
            customer_failed = True
            src_redirects = {}
            dst_redirects = {}

        for domain in sorted(src_domains):
            source_docroot = str(pick(src_domains[domain], "documentroot", default=""))
            if source_docroot and not _docroot_in_any_root(source_docroot, source_roots):
                print(
                    f"SKIP customer={login} domain={domain}: outside source roots "
                    f"({config.paths.source_web_root}, {config.paths.source_transfer_root}) ({source_docroot})"
                )
                continue
            if domain not in dst_domains:
                print(f"FAIL customer={login} domain={domain}: missing on target")
                failures += 1
                customer_failed = True
                continue
            errs = _compare_domain(
                src_domains[domain],
                dst_domains[domain],
                source_php_map,
                target_php_map,
                source_roots,
                config.paths.target_web_root,
            )
            if errs:
                print(f"FAIL customer={login} domain={domain}: {'; '.join(errs)}")
                failures += 1
                customer_failed = True
            src_cert = src_certs.get(domain)
            if src_cert:
                dst_cert = dst_certs.get(domain)
                if not dst_cert:
                    print(f"FAIL customer={login} cert={domain}: missing on target")
                    failures += 1
                    customer_failed = True
                else:
                    for field in (
                        "ssl_cert_file",
                        "ssl_key_file",
                        "ssl_ca_file",
                        "ssl_cert_chainfile",
                    ):
                        if str(pick(src_cert, field, default="")) != str(pick(dst_cert, field, default="")):
                            print(f"FAIL customer={login} cert={domain}: {field} mismatch")
                            failures += 1
                            customer_failed = True

            src_zones = {
                (
                    str(pick(item, "record", default="")).strip().lower(),
                    str(pick(item, "type", default="")).strip().upper(),
                    as_int(pick(item, "prio", default=0)),
                    str(pick(item, "content", default="")).strip().lower(),
                    as_int(pick(item, "ttl", default=18000)),
                )
                for item in source.list_domain_zones(domainname=domain)
                if _is_custom_zone_record(item)
            }
            dst_zones = {
                (
                    str(pick(item, "record", default="")).strip().lower(),
                    str(pick(item, "type", default="")).strip().upper(),
                    as_int(pick(item, "prio", default=0)),
                    str(pick(item, "content", default="")).strip().lower(),
                    as_int(pick(item, "ttl", default=18000)),
                )
                for item in target.list_domain_zones(domainname=domain)
                if _is_custom_zone_record(item)
            }
            missing_zones = sorted(src_zones - dst_zones)
            for zone in missing_zones:
                print(f"FAIL customer={login} zone={domain}: missing custom record {zone}")
                failures += 1
                customer_failed = True

        for domain in sorted(src_subdomains):
            parent_domain = domain.split(".", 1)[1] if "." in domain else ""
            if parent_domain and parent_domain not in migratable_domain_names:
                continue
            if domain not in dst_subdomains:
                print(f"FAIL customer={login} subdomain={domain}: missing on target")
                failures += 1
                customer_failed = True
                continue
            errs = _compare_subdomain(src_subdomains[domain], dst_subdomains[domain], source_php_map, target_php_map)
            if errs:
                print(f"FAIL customer={login} subdomain={domain}: {'; '.join(errs)}")
                failures += 1
                customer_failed = True

        for mailbox in sorted(src_mails):
            if mailbox not in dst_mails:
                print(f"FAIL customer={login} mailbox={mailbox}: missing on target")
                failures += 1
                customer_failed = True
                continue
            errs = _compare_mail(src_mails[mailbox], dst_mails[mailbox])
            if errs:
                print(f"FAIL customer={login} mailbox={mailbox}: {'; '.join(errs)}")
                failures += 1
                customer_failed = True

        for ftp_user in sorted(src_ftps):
            if ftp_user not in dst_ftps:
                print(f"FAIL customer={login} ftp={ftp_user}: missing on target")
                failures += 1
                customer_failed = True
                continue
            errs = _compare_ftp(src_ftps[ftp_user], dst_ftps[ftp_user])
            if errs:
                print(f"FAIL customer={login} ftp={ftp_user}: {'; '.join(errs)}")
                failures += 1
                customer_failed = True

        for key in sorted(src_dir_protections):
            if key not in dst_dir_protections:
                print(f"FAIL customer={login} dir-protection={key[0]}:{key[1]}: missing on target")
                failures += 1
                customer_failed = True
                continue
            errs = _compare_dir_protection(src_dir_protections[key], dst_dir_protections[key])
            if errs:
                print(f"FAIL customer={login} dir-protection={key[0]}:{key[1]}: {'; '.join(errs)}")
                failures += 1
                customer_failed = True

        for path in sorted(src_dir_options):
            if path not in dst_dir_options:
                print(f"FAIL customer={login} dir-option={path}: missing on target")
                failures += 1
                customer_failed = True
                continue
            errs = _compare_dir_option(src_dir_options[path], dst_dir_options[path])
            if errs:
                print(f"FAIL customer={login} dir-option={path}: {'; '.join(errs)}")
                failures += 1
                customer_failed = True

        missing_forwarders = sorted(src_forwarders - dst_forwarders)
        for emailaddr, destination in missing_forwarders:
            print(f"FAIL customer={login} forwarder={emailaddr}->{destination}: missing on target")
            failures += 1
            customer_failed = True

        missing_senders = sorted(src_senders - dst_senders)
        for emailaddr, allowed_sender in missing_senders:
            print(f"FAIL customer={login} sender={emailaddr}->{allowed_sender}: missing on target")
            failures += 1
            customer_failed = True

        for key in sorted(src_ssh_keys):
            if key not in dst_ssh_keys:
                print(f"FAIL customer={login} ssh-key={key[0]}: missing public key on target")
                failures += 1
                customer_failed = True
                continue
            src_desc = str(pick(src_ssh_keys[key], "description", default="")).strip()
            dst_desc = str(pick(dst_ssh_keys[key], "description", default="")).strip()
            if src_desc != dst_desc:
                print(f"FAIL customer={login} ssh-key={key[0]}: description source={src_desc!r} target={dst_desc!r}")
                failures += 1
                customer_failed = True

        missing_data_dumps = sorted(src_data_dumps - dst_data_dumps)
        for item in missing_data_dumps:
            print(f"FAIL customer={login} data-dump={item}: missing on target")
            failures += 1
            customer_failed = True

        for domain_name, src_redirect in sorted(src_redirects.items()):
            if domain_name not in dst_redirects:
                print(f"FAIL customer={login} redirect={domain_name}: missing on target")
                failures += 1
                customer_failed = True
                continue
            if src_redirect != dst_redirects[domain_name]:
                print(f"FAIL customer={login} redirect={domain_name}: source={src_redirect!r} target={dst_redirects[domain_name]!r}")
                failures += 1
                customer_failed = True

        if not customer_failed:
            print(f"OK customer={login}: domains/mail/settings/certs match")

    if failures:
        print(f"Verification failed: {failures} mismatch(es)")
        return 1
    print("Verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
