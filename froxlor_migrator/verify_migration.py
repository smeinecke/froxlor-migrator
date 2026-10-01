from __future__ import annotations

import argparse
import shlex
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

from .api import FroxlorApiError, FroxlorClient
from .config import load_config
from .froxlor_mysql import (
    connect_kwargs_from_credentials,
    extract_sql_root_credentials,
    froxlor_userdata_paths,
    load_local_sql_credentials,
)
from .mysql_driver import query as mysql_query
from .mysql_tunnel import open_ssh_tunnel, open_ssh_unix_socket_tunnel
from .ssh_driver import SshDriver
from .transfer import remote_sudo_prefix
from .util import (
    as_bool,
    as_int,
    data_dump_key,
    domain_name,
    ftp_username,
    is_custom_zone_record,
    mailbox_address,
    pick,
    relative_customer_path,
    resolve_subdomain_parts,
    ssh_key_identity,
)


def _dir_protection_name(row: dict[str, Any], customer_login: str = "") -> tuple[str, str]:
    return (
        relative_customer_path(str(pick(row, "path", default="")), customer_login).lower(),
        str(pick(row, "username", default="")).strip().lower(),
    )


def _dir_option_name(row: dict[str, Any], customer_login: str = "") -> str:
    return relative_customer_path(str(pick(row, "path", default="")), customer_login).lower()


def _docroot_in_any_root(docroot: str, roots: list[str]) -> bool:
    value = docroot.strip()
    if not value or not value.startswith("/"):
        # Empty/relative docroots resolve inside the customer homedir.
        return True
    for root in roots:
        normalized = root.rstrip("/")
        if not normalized:
            continue
        if value == normalized or value.startswith(normalized + "/"):
            return True
    return False


def _expected_target_docroot(source_docroot: str, source_roots: list[str], target_root: str, customer_login: str = "") -> str:
    """Mirror the migrator's two-hop docroot mapping.

    ``source_roots`` is ``[source_web_root, source_transfer_root]``. The first
    hop resolves the API documentroot to a source filesystem path
    (:meth:`_resolve_source_docroot`), the second maps it onto the target web
    root (:meth:`_resolve_target_docroot`). Verify matches customers by login
    name, so the target login equals ``customer_login``.
    """
    value = source_docroot.strip()
    target_base = target_root.rstrip("/")
    web_root = source_roots[0].rstrip("/") if source_roots else ""
    transfer_root = source_roots[1].rstrip("/") if len(source_roots) > 1 else web_root

    # Hop 1: API documentroot -> source filesystem path.
    if value.startswith("/"):
        if web_root and value.startswith(web_root + "/"):
            source_fs = transfer_root + value[len(web_root) :]
        else:
            source_fs = value
    else:
        source_fs = f"{transfer_root}/{customer_login}/{value.lstrip('/')}"

    # Hop 2: source filesystem path -> target documentroot.
    if transfer_root and source_fs.startswith(transfer_root + "/"):
        suffix = source_fs[len(transfer_root) :].lstrip("/")
        parts = suffix.split("/", 1)
        if len(parts) == 2 and parts[0] == customer_login:
            return f"{target_base}/{customer_login}/{parts[1]}"
        return f"{target_base}/{suffix}"
    rel = value.lstrip("/")
    return f"{target_base}/{customer_login}/{rel}" if customer_login else f"{target_base}/{rel}"


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
    customer_login: str = "",
) -> list[str]:
    errors: list[str] = []
    expected_documentroot = _expected_target_docroot(
        str(pick(source_row, "documentroot", default="")),
        source_roots,
        target_root,
        customer_login,
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
        (
            "selectserveralias",
            as_int(pick(source_row, "wwwserveralias", "selectserveralias", default=0)),
            as_int(pick(target_row, "wwwserveralias", "selectserveralias", default=0)),
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


def _compare_customer(source_row: dict[str, Any], target_row: dict[str, Any], check_password: bool = True) -> list[str]:
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
    ]
    for field, src, dst in checks:
        if src != dst:
            errors.append(f"{field} source={src!r} target={dst!r}")
    if check_password:
        # type_2fa is the only auth field the API still exposes; `password`
        # and `data_2fa` are unset in listing/get rows and are compared via
        # the panel databases in main().
        if as_int(pick(source_row, "type_2fa", default=0)) != as_int(pick(target_row, "type_2fa", default=0)):
            errors.append(f"type_2fa source={as_int(pick(source_row, 'type_2fa', default=0))!r} target={as_int(pick(target_row, 'type_2fa', default=0))!r}")
    return errors


def _customer_warnings(source_row: dict[str, Any], target_row: dict[str, Any]) -> list[str]:
    """Fields that are only applied when updating an existing customer.

    The migrator intentionally strips deactivated/theme on Customers.add, so a
    mismatch for a freshly created customer is expected - report it as a
    warning instead of failing the verification.
    """
    warnings: list[str] = []
    source_deactivated = as_int(pick(source_row, "deactivated", default=0))
    target_deactivated = as_int(pick(target_row, "deactivated", default=0))
    if source_deactivated != target_deactivated:
        warnings.append(f"deactivated source={source_deactivated!r} target={target_deactivated!r}")
    source_theme = str(pick(source_row, "theme", default="")).strip().lower()
    target_theme = str(pick(target_row, "theme", default="")).strip().lower()
    if source_theme and source_theme != target_theme:
        warnings.append(f"theme source={source_theme!r} target={target_theme!r}")
    return warnings


def _compare_subdomain(
    source_row: dict[str, Any],
    target_row: dict[str, Any],
    source_php_map: dict[int, str],
    target_php_map: dict[int, str],
    customer_login: str = "",
) -> list[str]:
    errors: list[str] = []
    checks = [
        (
            # Source listings may return the stored path absolute (embedding
            # the customer login) while the target stores it relative.
            "path",
            relative_customer_path(str(pick(source_row, "path", default="")), customer_login),
            relative_customer_path(str(pick(target_row, "path", default="")), customer_login),
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


def _relative_ftp_path(row: dict[str, Any], customer_login: str) -> str:
    """Docroot-relative FTP path; '/' when the homedir is the customer root."""
    ftp_path = str(pick(row, "path", default="")).strip().strip("/")
    if not ftp_path:
        homedir = str(pick(row, "homedir", default="")).strip()
        marker = f"/{customer_login.strip('/')}/"
        if customer_login and marker in homedir:
            ftp_path = homedir.split(marker, 1)[1].strip("/")
    return ftp_path or "/"


def _expected_ftp_path(source_row: dict[str, Any], source_login: str, target_login: str) -> str:
    """Mirror the migrator's FTP path derivation for parity checks."""
    return _relative_ftp_path(source_row, source_login)


def _compare_ftp(
    source_row: dict[str, Any],
    target_row: dict[str, Any],
    source_login: str = "",
    target_login: str = "",
    check_password: bool = True,
) -> list[str]:
    errors: list[str] = []
    checks = [
        (
            "path",
            _expected_ftp_path(source_row, source_login, target_login),
            _relative_ftp_path(target_row, target_login),
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
    ]
    if check_password:
        checks.append((
            "password",
            str(pick(source_row, "password", default="")).strip(),
            str(pick(target_row, "password", default="")).strip(),
        ))
    for field, src, dst in checks:
        if str(src) != str(dst):
            errors.append(f"{field} source={src!r} target={dst!r}")
    return errors


def _compare_dir_protection(
    source_row: dict[str, Any],
    target_row: dict[str, Any],
    check_password: bool = True,
    customer_login: str = "",
) -> list[str]:
    errors: list[str] = []
    checks = [
        (
            "path",
            relative_customer_path(str(pick(source_row, "path", default="")), customer_login),
            relative_customer_path(str(pick(target_row, "path", default="")), customer_login),
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
    ]
    if check_password:
        checks.append((
            "password",
            str(pick(source_row, "password", default="")),
            str(pick(target_row, "password", default="")),
        ))
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


def _run_mysql_query_local(connect_kwargs: dict[str, Any], database: str, sql: str) -> list[list[str]]:
    return mysql_query(connect_kwargs, database, sql)


@contextmanager
def _target_connect_kwargs_via_ssh(config) -> Iterator[dict[str, Any]]:
    ssh = SshDriver(config)
    try:
        target_creds = None
        sudo = remote_sudo_prefix(config)
        for path in froxlor_userdata_paths():
            content = ""
            try:
                content = ssh.read_file(path)
            except Exception:
                # The SSH user may not be able to read userdata.inc.php via
                # SFTP; fall back to sudo cat like the migrator does.
                proc = ssh.run(f"{sudo}cat {shlex.quote(path)}")
                if proc.returncode == 0:
                    content = proc.stdout
            if not content.strip():
                continue
            target_creds = extract_sql_root_credentials(content)
            if target_creds:
                break
        if not target_creds:
            raise RuntimeError("Could not parse target sql_root credentials from froxlor userdata files via SSH")
        kwargs = connect_kwargs_from_credentials(target_creds)
        remote_socket = str(kwargs.get("unix_socket", "")).strip()
        if remote_socket:
            # Paramiko cannot open direct-streamlocal channels; ssh -L forwards
            # the remote socket to a local socket path used via unix_socket.
            tunneled = dict(kwargs)
            with open_ssh_unix_socket_tunnel(config, remote_socket) as local_socket:
                tunneled["unix_socket"] = local_socket
                yield tunneled
            return
        remote_host = str(kwargs.get("host", "localhost"))
        remote_port = int(kwargs.get("port", 3306))
        with open_ssh_tunnel(ssh.transport(), remote_host, remote_port) as (_, local_port):
            tunneled = dict(kwargs)
            tunneled["host"] = "127.0.0.1"
            tunneled["port"] = local_port
            yield tunneled
    finally:
        ssh.close()


def _run_mysql_query_target(config, sql: str) -> list[list[str]]:
    with _target_connect_kwargs_via_ssh(config) as connect_kwargs:
        return mysql_query(connect_kwargs, config.mysql.target_panel_database, sql)


@contextmanager
def _target_panel_session(config) -> Iterator[Callable[[str], list[list[str]]]]:
    """One SSH connection + tunnel shared by every per-customer target
    panel query in a verification run (redirects, FTP hashes, customer
    secrets) instead of a fresh SSH session per query."""
    with _target_connect_kwargs_via_ssh(config) as connect_kwargs:
        yield lambda sql: mysql_query(connect_kwargs, config.mysql.target_panel_database, sql)


def _load_redirect_map_source(config, customer_id: int) -> dict[str, tuple[str, int]]:
    sql = (
        "SELECT d.domain, a.domain, COALESCE(drc.rid, 1) "
        "FROM panel_domains d "
        "JOIN panel_domains a ON a.id=d.aliasdomain "
        "LEFT JOIN domain_redirect_codes drc ON drc.did=d.id "
        f"WHERE d.customerid={customer_id} AND d.aliasdomain IS NOT NULL"
    )
    rows = _run_mysql_query_local(
        connect_kwargs_from_credentials(load_local_sql_credentials(froxlor_userdata_paths())),
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


def _load_redirect_map_target(config, customer_id: int, query: Callable[[str], list[list[str]]] | None = None) -> dict[str, tuple[str, int]]:
    sql = (
        "SELECT d.domain, a.domain, COALESCE(drc.rid, 1) "
        "FROM panel_domains d "
        "JOIN panel_domains a ON a.id=d.aliasdomain "
        "LEFT JOIN domain_redirect_codes drc ON drc.did=d.id "
        f"WHERE d.customerid={customer_id} AND d.aliasdomain IS NOT NULL"
    )
    rows = (query or (lambda q: _run_mysql_query_target(config, q)))(sql)
    result: dict[str, tuple[str, int]] = {}
    for row in rows:
        if len(row) < 3:
            continue
        result[str(row[0]).strip().lower()] = (
            str(row[1]).strip().lower(),
            as_int(row[2], default=1),
        )
    return result


def _load_ftp_password_map(config, customer_id: int, target: bool, query: Callable[[str], list[list[str]]] | None = None) -> dict[str, str]:
    # Ftps.listing strips `password`, so the API compare is vacuous — the hash
    # only exists in panel DB `ftp_users`.
    sql = f"SELECT username, password FROM ftp_users WHERE customerid={int(customer_id)}"
    if target:
        rows = (query or (lambda q: _run_mysql_query_target(config, q)))(sql)
    else:
        rows = _run_mysql_query_local(
            connect_kwargs_from_credentials(load_local_sql_credentials(froxlor_userdata_paths())),
            config.mysql.source_panel_database,
            sql,
        )
    return {str(row[0]).strip().lower(): str(row[1]).strip() for row in rows if len(row) >= 2}


def _load_customer_secrets(config, customer_id: int, target: bool, query: Callable[[str], list[list[str]]] | None = None) -> tuple[str, int, str] | None:
    """(password, type_2fa, data_2fa) from panel_customers — the API strips
    password and data_2fa from customer rows, so API fields cannot be used."""
    sql = f"SELECT password, type_2fa, data_2fa FROM panel_customers WHERE customerid={int(customer_id)} LIMIT 1"
    if target:
        rows = (query or (lambda q: _run_mysql_query_target(config, q)))(sql)
    else:
        rows = _run_mysql_query_local(
            connect_kwargs_from_credentials(load_local_sql_credentials(froxlor_userdata_paths())),
            config.mysql.source_panel_database,
            sql,
        )
    if not rows or not rows[0]:
        return None
    row = rows[0]
    password = str(row[0]).strip() if len(row) > 0 and row[0] is not None else ""
    type_2fa = as_int(row[1], default=0) if len(row) > 1 else 0
    data_2fa = str(row[2]).strip() if len(row) > 2 and row[2] is not None else ""
    return (password, type_2fa, data_2fa)


def _parse_verify_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify migrated source/target parity")
    parser.add_argument("--config", default="config.toml", help="Path to config TOML")
    parser.add_argument("--customer", action="append", default=[], help="Customer login to verify (repeatable)")
    parser.add_argument("--skip-password-sync", action="store_true", help="Skip password/2FA hash comparisons")
    parser.add_argument("--skip-subdomains", action="store_true", help="Skip subdomain comparisons")
    parser.add_argument("--skip-domain-zones", action="store_true", help="Skip DNS zone record comparisons")
    parser.add_argument("--skip-certificates", action="store_true", help="Skip certificate comparisons")
    parser.add_argument("--skip-mail", action="store_true", help="Skip mailbox comparisons")
    parser.add_argument("--skip-forwarders", action="store_true", help="Skip mail forwarder comparisons")
    parser.add_argument("--skip-sender-aliases", action="store_true", help="Skip sender alias comparisons")
    parser.add_argument("--skip-ftp", action="store_true", help="Skip FTP account comparisons")
    parser.add_argument("--skip-dir-protections", action="store_true", help="Skip directory protection comparisons")
    parser.add_argument("--skip-dir-options", action="store_true", help="Skip directory option comparisons")
    parser.add_argument("--skip-ssh-keys", action="store_true", help="Skip SSH key comparisons")
    parser.add_argument("--skip-data-dumps", action="store_true", help="Skip data dump comparisons")
    parser.add_argument("--skip-redirects", action="store_true", help="Skip domain redirect comparisons")
    return parser.parse_args()


class _Report:
    """Per-customer result emitter; keeps the failure tally and the
    'any check failed' flag the OK line depends on."""

    def __init__(self, login: str) -> None:
        self.login = login
        self.failures = 0
        self.failed = False

    def fail(self, detail: str, subject: str = "") -> None:
        scope = f" {subject}" if subject else ""
        print(f"FAIL customer={self.login}{scope}: {detail}")
        self.failures += 1
        self.failed = True

    def warn(self, detail: str) -> None:
        print(f"WARN customer={self.login}: {detail}")

    def skip(self, subject: str, detail: str) -> None:
        print(f"SKIP customer={self.login} {subject}: {detail}")


def _custom_zone_records(rows: list[dict[str, Any]], domain: str) -> set[tuple[str, str, int, str, int]]:
    return {
        (
            str(pick(item, "record", default="")).strip().lower(),
            str(pick(item, "type", default="")).strip().upper(),
            as_int(pick(item, "prio", default=0)),
            str(pick(item, "content", default="")).strip(),
            as_int(pick(item, "ttl", default=18000)),
        )
        for item in rows
        if is_custom_zone_record(item, domain)
    }


def _forwarder_key(x: dict[str, Any]) -> tuple[str, str]:
    return (
        str(pick(x, "email", "emailaddr", default="")).strip().lower(),
        str(pick(x, "destination", default="")).strip().lower(),
    )


def _sender_key(x: dict[str, Any]) -> tuple[str, str]:
    return (
        str(pick(x, "email", "emailaddr", default="")).strip().lower(),
        str(pick(x, "allowed_sender", default="")).strip().lower(),
    )


def _cert_map(client: FroxlorClient) -> dict[str, dict[str, Any]]:
    return {str(pick(x, "domainname", "domain", default="")).lower(): x for x in client.listing("Certificates.listing")}


def _verify_customer_secrets(
    report: _Report,
    config,
    src_id: int,
    dst_id: int,
    panel_query: Callable[[str], list[list[str]]],
    customer_errs: list[str],
) -> None:
    try:
        src_secrets = _load_customer_secrets(config, src_id, target=False)
        dst_secrets = _load_customer_secrets(config, dst_id, target=True, query=panel_query)
    except Exception as exc:
        report.warn(f"could not query customer password/2FA state ({exc})")
        return
    if src_secrets is None or dst_secrets is None:
        report.warn(f"customer row missing in {'source' if src_secrets is None else 'target'} panel DB")
        return
    if src_secrets[0] and src_secrets[0] != dst_secrets[0]:
        customer_errs.append("password-hash mismatch")
    if src_secrets[1] != dst_secrets[1]:
        customer_errs.append(f"type_2fa source={src_secrets[1]!r} target={dst_secrets[1]!r}")
    if src_secrets[2] != dst_secrets[2]:
        customer_errs.append("data_2fa mismatch")


def _named_rows(rows_fn: Callable[[], list[dict[str, Any]]], key_fn: Callable[[dict[str, Any]], Any], skip: bool) -> dict:
    return {} if skip else {key_fn(x): x for x in rows_fn()}


def _key_set(rows_fn: Callable[[], list[dict[str, Any]]], key_fn: Callable[[dict[str, Any]], Any], skip: bool) -> set:
    return set() if skip else {key_fn(x) for x in rows_fn()}


def _load_customer_resources(
    args: argparse.Namespace,
    source: FroxlorClient,
    target: FroxlorClient,
    login: str,
    src_id: int,
    dst_id: int,
    source_roots: list[str],
) -> dict[str, Any] | None:
    """List every comparable collection for one customer pair.

    Returns None when any listing call fails; the caller records the
    failure and moves on to the next customer."""
    try:
        src_domains = {domain_name(x): x for x in source.list_domains(customerid=src_id, loginname=login)}
        return {
            "domains": src_domains,
            "dst_domains": {domain_name(x): x for x in target.list_domains(customerid=dst_id, loginname=login)},
            "subdomains": _named_rows(lambda: source.list_subdomains(customerid=src_id, loginname=login), domain_name, args.skip_subdomains),
            "dst_subdomains": _named_rows(lambda: target.list_subdomains(customerid=dst_id, loginname=login), domain_name, args.skip_subdomains),
            "migratable_domain_names": {
                name for name, row in src_domains.items() if _docroot_in_any_root(str(pick(row, "documentroot", default="")), source_roots)
            },
            "mails": _named_rows(lambda: source.list_emails(customerid=src_id, loginname=login), mailbox_address, args.skip_mail),
            "dst_mails": _named_rows(lambda: target.list_emails(customerid=dst_id, loginname=login), mailbox_address, args.skip_mail),
            "ftps": _named_rows(lambda: source.list_ftps(customerid=src_id, loginname=login), ftp_username, args.skip_ftp),
            "dst_ftps": _named_rows(lambda: target.list_ftps(customerid=dst_id, loginname=login), ftp_username, args.skip_ftp),
            "dir_protections": _named_rows(
                lambda: source.list_dir_protections(customerid=src_id, loginname=login),
                lambda x: _dir_protection_name(x, login),
                args.skip_dir_protections,
            ),
            "dst_dir_protections": _named_rows(
                lambda: target.list_dir_protections(customerid=dst_id, loginname=login),
                lambda x: _dir_protection_name(x, login),
                args.skip_dir_protections,
            ),
            "dir_options": _named_rows(
                lambda: source.list_dir_options(customerid=src_id, loginname=login), lambda x: _dir_option_name(x, login), args.skip_dir_options
            ),
            "dst_dir_options": _named_rows(
                lambda: target.list_dir_options(customerid=dst_id, loginname=login), lambda x: _dir_option_name(x, login), args.skip_dir_options
            ),
            "ssh_keys": _named_rows(lambda: source.list_ssh_keys(customerid=src_id, loginname=login), ssh_key_identity, args.skip_ssh_keys),
            "dst_ssh_keys": _named_rows(lambda: target.list_ssh_keys(customerid=dst_id, loginname=login), ssh_key_identity, args.skip_ssh_keys),
            "data_dumps": _key_set(lambda: source.list_data_dumps(customerid=src_id, loginname=login, strict=True), data_dump_key, args.skip_data_dumps),
            "dst_data_dumps": _key_set(lambda: target.list_data_dumps(customerid=dst_id, loginname=login, strict=True), data_dump_key, args.skip_data_dumps),
            "forwarders": _key_set(
                lambda: source.list_email_forwarders(customerid=src_id, loginname=login, strict=True),
                _forwarder_key,
                args.skip_forwarders,
            ),
            "dst_forwarders": _key_set(
                lambda: target.list_email_forwarders(customerid=dst_id, loginname=login, strict=True),
                _forwarder_key,
                args.skip_forwarders,
            ),
            "senders": _key_set(
                lambda: source.list_email_senders(customerid=src_id, loginname=login, strict=True),
                _sender_key,
                args.skip_sender_aliases,
            ),
            "dst_senders": _key_set(
                lambda: target.list_email_senders(customerid=dst_id, loginname=login, strict=True),
                _sender_key,
                args.skip_sender_aliases,
            ),
            "certs": {} if args.skip_certificates else _cert_map(source),
            "dst_certs": {} if args.skip_certificates else _cert_map(target),
        }
    except FroxlorApiError:
        return None


def _verify_domains(
    report: _Report,
    source: FroxlorClient,
    target: FroxlorClient,
    args: argparse.Namespace,
    config,
    login: str,
    res: dict[str, Any],
    source_php_map,
    target_php_map,
    source_roots: list[str],
) -> None:
    for domain in sorted(res["domains"]):
        source_docroot = str(pick(res["domains"][domain], "documentroot", default=""))
        if source_docroot and not _docroot_in_any_root(source_docroot, source_roots):
            report.skip(
                f"domain={domain}",
                f"outside source roots ({config.paths.source_web_root}, {config.paths.source_transfer_root}) ({source_docroot})",
            )
            continue
        if domain not in res["dst_domains"]:
            report.fail("missing on target", subject=f"domain={domain}")
            continue
        errs = _compare_domain(
            res["domains"][domain],
            res["dst_domains"][domain],
            source_php_map,
            target_php_map,
            source_roots,
            config.paths.target_web_root,
            login,
        )
        if errs:
            report.fail("; ".join(errs), subject=f"domain={domain}")
        _verify_certificate(report, domain, res["certs"], res["dst_certs"])
        if not args.skip_domain_zones:
            _verify_zone(report, source, target, login, domain)


def _verify_certificate(report: _Report, domain: str, src_certs: dict, dst_certs: dict) -> None:
    src_cert = src_certs.get(domain)
    if not src_cert:
        return
    dst_cert = dst_certs.get(domain)
    if not dst_cert:
        report.fail("missing on target", subject=f"cert={domain}")
        return
    for field in ("ssl_cert_file", "ssl_key_file", "ssl_ca_file", "ssl_cert_chainfile"):
        if str(pick(src_cert, field, default="")) != str(pick(dst_cert, field, default="")):
            report.fail(f"{field} mismatch", subject=f"cert={domain}")


def _verify_zone(report: _Report, source: FroxlorClient, target: FroxlorClient, login: str, domain: str) -> None:
    try:
        src_zone_rows = source.list_domain_zones(domainname=domain, strict=True)
        dst_zone_rows = target.list_domain_zones(domainname=domain, strict=True)
    except FroxlorApiError as exc:
        report.fail(f"could not list zone records ({exc})", subject=f"zone={domain}")
        return
    src_zones = _custom_zone_records(src_zone_rows, domain)
    dst_zones = _custom_zone_records(dst_zone_rows, domain)
    for zone in sorted(src_zones - dst_zones):
        report.fail(f"missing custom record {zone}", subject=f"zone={domain}")


def _verify_subdomains(report: _Report, login: str, res: dict[str, Any], source_php_map, target_php_map) -> None:
    for domain in sorted(res["subdomains"]):
        if resolve_subdomain_parts(domain, "", res["migratable_domain_names"]) is None:
            continue
        if domain not in res["dst_subdomains"]:
            report.fail("missing on target", subject=f"subdomain={domain}")
            continue
        errs = _compare_subdomain(
            res["subdomains"][domain],
            res["dst_subdomains"][domain],
            source_php_map,
            target_php_map,
            customer_login=login,
        )
        if errs:
            report.fail("; ".join(errs), subject=f"subdomain={domain}")


def _verify_mailboxes(report: _Report, src_mails: dict, dst_mails: dict) -> None:
    for mailbox in sorted(src_mails):
        if mailbox not in dst_mails:
            report.fail("missing on target", subject=f"mailbox={mailbox}")
            continue
        errs = _compare_mail(src_mails[mailbox], dst_mails[mailbox])
        if errs:
            report.fail("; ".join(errs), subject=f"mailbox={mailbox}")


def _verify_ftps(
    report: _Report,
    login: str,
    src_ftps: dict,
    dst_ftps: dict,
    src_ftp_hashes: dict[str, str],
    dst_ftp_hashes: dict[str, str],
    check_password: bool,
) -> None:
    for ftp_user in sorted(src_ftps):
        if ftp_user not in dst_ftps:
            report.fail("missing on target", subject=f"ftp={ftp_user}")
            continue
        errs = _compare_ftp(
            src_ftps[ftp_user],
            dst_ftps[ftp_user],
            source_login=login,
            target_login=login,
            check_password=check_password,
        )
        if check_password and src_ftp_hashes:
            src_hash = src_ftp_hashes.get(ftp_user)
            dst_hash = dst_ftp_hashes.get(ftp_user)
            if src_hash and dst_hash is not None and src_hash != dst_hash:
                errs.append("password-hash mismatch")
            elif src_hash and dst_hash is None:
                errs.append("password hash missing on target")
        if errs:
            report.fail("; ".join(errs), subject=f"ftp={ftp_user}")


def _verify_dir_protections(report: _Report, login: str, src: dict, dst: dict, check_password: bool) -> None:
    for key in sorted(src):
        if key not in dst:
            report.fail("missing on target", subject=f"dir-protection={key[0]}:{key[1]}")
            continue
        errs = _compare_dir_protection(src[key], dst[key], check_password=check_password, customer_login=login)
        if errs:
            report.fail("; ".join(errs), subject=f"dir-protection={key[0]}:{key[1]}")


def _verify_dir_options(report: _Report, src: dict, dst: dict) -> None:
    for path in sorted(src):
        if path not in dst:
            report.fail("missing on target", subject=f"dir-option={path}")
            continue
        errs = _compare_dir_option(src[path], dst[path])
        if errs:
            report.fail("; ".join(errs), subject=f"dir-option={path}")


def _verify_ssh_keys(report: _Report, src: dict, dst: dict) -> None:
    for key in sorted(src):
        if key not in dst:
            report.fail("missing public key on target", subject=f"ssh-key={key[0]}")
            continue
        src_desc = str(pick(src[key], "description", default="")).strip()
        dst_desc = str(pick(dst[key], "description", default="")).strip()
        if src_desc != dst_desc:
            report.fail(f"description source={src_desc!r} target={dst_desc!r}", subject=f"ssh-key={key[0]}")


def _verify_redirects(report: _Report, src_redirects: dict, dst_redirects: dict) -> None:
    for redirect_domain, src_redirect in sorted(src_redirects.items()):
        if redirect_domain not in dst_redirects:
            report.fail("missing on target", subject=f"redirect={redirect_domain}")
            continue
        if src_redirect != dst_redirects[redirect_domain]:
            report.fail(
                f"source={src_redirect!r} target={dst_redirects[redirect_domain]!r}",
                subject=f"redirect={redirect_domain}",
            )


def _verify_customer(
    args: argparse.Namespace,
    config,
    source: FroxlorClient,
    target: FroxlorClient,
    login: str,
    source_customers: dict,
    target_customers: dict,
    source_php_map,
    target_php_map,
    panel_query: Callable[[str], list[list[str]]],
) -> int:
    """Verify one customer pair. Returns the number of failed checks."""
    report = _Report(login)
    src_customer = source_customers.get(login)
    dst_customer = target_customers.get(login)
    if not src_customer or not dst_customer:
        report.fail(f"missing on {'source' if not src_customer else 'target'}")
        return report.failures

    src_id = as_int(pick(src_customer, "customerid", "id", default=0))
    dst_id = as_int(pick(dst_customer, "customerid", "id", default=0))

    customer_errs = _compare_customer(src_customer, dst_customer, check_password=not args.skip_password_sync)
    if not args.skip_password_sync:
        _verify_customer_secrets(report, config, src_id, dst_id, panel_query, customer_errs)
    for warning in _customer_warnings(src_customer, dst_customer):
        report.warn(warning)
    if customer_errs:
        report.fail("; ".join(customer_errs))

    source_roots = [config.paths.source_web_root, config.paths.source_transfer_root]
    res = _load_customer_resources(args, source, target, login, src_id, dst_id, source_roots)
    if res is None:
        report.fail("could not list resources")
        return report.failures

    src_redirects: dict[str, Any] = {}
    dst_redirects: dict[str, Any] = {}
    if not args.skip_redirects:
        try:
            src_redirects = _load_redirect_map_source(config, src_id)
            dst_redirects = _load_redirect_map_target(config, dst_id, panel_query)
        except Exception as exc:
            report.fail(f"redirects: could not query redirect mappings ({exc})")

    src_ftp_hashes: dict[str, str] = {}
    dst_ftp_hashes: dict[str, str] = {}
    if not args.skip_ftp and not args.skip_password_sync:
        try:
            src_ftp_hashes = _load_ftp_password_map(config, src_id, target=False)
            dst_ftp_hashes = _load_ftp_password_map(config, dst_id, target=True, query=panel_query)
        except Exception as exc:
            report.warn(f"could not query FTP password hashes ({exc})")

    _verify_domains(report, source, target, args, config, login, res, source_php_map, target_php_map, source_roots)
    _verify_subdomains(report, login, res, source_php_map, target_php_map)
    _verify_mailboxes(report, res["mails"], res["dst_mails"])
    _verify_ftps(
        report,
        login,
        res["ftps"],
        res["dst_ftps"],
        src_ftp_hashes,
        dst_ftp_hashes,
        check_password=not args.skip_password_sync,
    )
    _verify_dir_protections(report, login, res["dir_protections"], res["dst_dir_protections"], check_password=not args.skip_password_sync)
    _verify_dir_options(report, res["dir_options"], res["dst_dir_options"])

    for emailaddr, destination in sorted(res["forwarders"] - res["dst_forwarders"]):
        report.fail("missing on target", subject=f"forwarder={emailaddr}->{destination}")
    for emailaddr, allowed_sender in sorted(res["senders"] - res["dst_senders"]):
        report.fail("missing on target", subject=f"sender={emailaddr}->{allowed_sender}")

    _verify_ssh_keys(report, res["ssh_keys"], res["dst_ssh_keys"])

    for item in sorted(res["data_dumps"] - res["dst_data_dumps"]):
        report.fail("missing on target", subject=f"data-dump={item}")

    _verify_redirects(report, src_redirects, dst_redirects)

    if not report.failed:
        print(f"OK customer={login}: domains/mail/settings/certs match")
    return report.failures


def main() -> int:
    args = _parse_verify_args()
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
    logins = requested if requested else sorted(set(source_customers) & set(target_customers))

    failures = 0
    target_session_stack: ExitStack | None = None
    target_panel_query_fn: Callable[[str], list[list[str]]] | None = None

    def _target_panel_query(sql: str) -> list[list[str]]:
        nonlocal target_session_stack, target_panel_query_fn
        if target_panel_query_fn is None:
            # Lazily open one SSH session + tunnel shared by every
            # per-customer target panel query below.
            target_session_stack = ExitStack()
            target_panel_query_fn = target_session_stack.enter_context(_target_panel_session(config))
        return target_panel_query_fn(sql)

    try:
        for login in logins:
            failures += _verify_customer(
                args,
                config,
                source,
                target,
                login,
                source_customers,
                target_customers,
                source_php_map,
                target_php_map,
                _target_panel_query,
            )
    finally:
        if target_session_stack is not None:
            target_session_stack.close()

    if failures:
        print(f"Verification failed: {failures} mismatch(es)")
        return 1
    print("Verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
