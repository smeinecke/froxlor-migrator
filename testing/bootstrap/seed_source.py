from __future__ import annotations

import base64
import json
import os
import random
import secrets
import shutil
import string
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

import requests
from requests import JSONDecodeError


def safe_extract_tar(archive: tarfile.TarFile, destination: Path) -> None:
    dest = destination.resolve()
    members = []
    for member in archive.getmembers():
        member_path = (dest / member.name).resolve()
        if not str(member_path).startswith(str(dest) + os.sep) and member_path != dest:
            raise ApiError(f"Blocked unsafe archive member path: {member.name}")
        members.append(member)
    archive.extractall(path=dest, members=members)


def _pw(length: int = 20) -> str:
    alphabet = string.ascii_letters + string.digits + "-_"
    return "".join(secrets.choice(alphabet) for _ in range(length))


class ApiError(RuntimeError):
    pass


class FroxlorApi:
    def __init__(self, api_url: str, api_key: str, api_secret: str, timeout: int = 30) -> None:
        self.api_url = api_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout

    def _auth(self) -> str:
        raw = f"{self.api_key}:{self.api_secret}".encode()
        return base64.b64encode(raw).decode("ascii")

    def call(self, command: str, params: dict[str, Any] | None = None) -> Any:
        body: dict[str, Any] = {"command": command}
        if params:
            body["params"] = params

        last_error: ApiError | Exception | None = None
        for attempt in range(1, 6):
            try:
                response = requests.post(
                    self.api_url,
                    headers={
                        "Authorization": f"Basic {self._auth()}",
                        "Content-Type": "application/json",
                    },
                    json=body,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = exc
                response = None
            else:
                if response.status_code < 500:
                    if response.status_code >= 400:
                        raise ApiError(
                            f"{command} failed with HTTP {response.status_code}: {response.text[:300]}"
                        )
                    try:
                        payload = response.json()
                    except JSONDecodeError as exc:
                        raise ApiError(f"{command} returned non-JSON response: {response.text[:300]}") from exc
                    if int(payload.get("status", 200)) >= 400:
                        raise ApiError(f"{command} failed: {payload.get('status_message', 'unknown error')}")
                    return payload.get("data")
                last_error = ApiError(
                    f"{command} failed with HTTP {response.status_code}: {response.text[:300]}"
                )

            if attempt < 5:
                time.sleep(min(5, attempt))

        if isinstance(last_error, ApiError):
            raise last_error
        if last_error is not None:
            raise ApiError(f"{command} failed after retries: {last_error}")
        raise ApiError(f"{command} failed after retries with no response")

    def listing(self, command: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        merged = dict(params or {})
        merged.setdefault("sql_limit", 500)
        merged.setdefault("sql_offset", 0)

        rows: list[dict[str, Any]] = []
        while True:
            payload = self.call(command, merged)
            if isinstance(payload, dict):
                chunk = payload.get("list") or []
                total = int(payload.get("count", len(chunk)))
            else:
                chunk = payload or []
                total = len(chunk)
            rows.extend(chunk)
            if not chunk or len(rows) >= total:
                break
            merged["sql_offset"] = int(merged["sql_offset"]) + len(chunk)
        return rows


def _pick(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row[key]
    return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def ensure_customer(
    api: FroxlorApi,
    login: str,
    email: str,
    firstname: str,
    lastname: str,
    default_php_setting_id: int,
    mysql_server_id: int,
) -> dict[str, Any]:
    for row in api.listing("Customers.listing"):
        if str(_pick(row, "loginname", "login", default="")) == login:
            api.call(
                "Customers.update",
                {
                    "id": _to_int(_pick(row, "customerid", "id", default=0)),
                    "phpenabled": True,
                    "allowed_phpconfigs": [default_php_setting_id],
                    "allowed_mysqlserver": [mysql_server_id],
                    "mysqls_ul": True,
                    "diskspace_ul": True,
                    "traffic_ul": True,
                    "emails_ul": True,
                    "email_accounts_ul": True,
                    "email_forwarders_ul": True,
                    "email_quota_ul": True,
                    "ftps_ul": True,
                    "subdomains_ul": True,
                },
            )
            return row

    payload = {
        "new_loginname": login,
        "email": email,
        "firstname": firstname,
        "name": lastname,
        "new_customer_password": _pw(24),
        "sendpassword": False,
        "diskspace_ul": True,
        "traffic_ul": True,
        "subdomains_ul": True,
        "emails_ul": True,
        "email_accounts_ul": True,
        "email_forwarders_ul": True,
        "email_quota_ul": True,
        "ftps_ul": True,
        "mysqls_ul": True,
        "phpenabled": True,
        "isphpenabled": True,
        "allowed_phpconfigs": [default_php_setting_id],
        "allowed_mysqlserver": [mysql_server_id],
        "dnsenabled": True,
        "api_allowed": True,
    }
    try:
        api.call("Customers.add", payload)
    except ApiError:
        pass
    for row in api.listing("Customers.listing"):
        if str(_pick(row, "loginname", "login", default="")) == login:
            return row
    raise ApiError(f"Customer was not created: {login}")


def ensure_php_settings(api: FroxlorApi) -> list[int]:
    current = api.listing("PhpSettings.listing")
    if not current:
        raise ApiError("No base PHP setting found in Froxlor; create one in panel first")

    desired_profiles = ["php8.3", "php8.4"]
    base = current[0]
    base_ini = str(base.get("phpsettings", "memory_limit = 256M\nmax_execution_time = 60"))

    fpm_rows = api.listing("FpmDaemons.listing")
    fpm_by_desc = {str(_pick(row, "description", default="")).strip().lower(): row for row in fpm_rows}
    fpm_ids: dict[str, int] = {}
    for profile in desired_profiles:
        version = profile.replace("php", "", 1)
        fpm_payload = {
            "description": profile,
            "reload_cmd": f"service {profile}-fpm restart",
            "config_dir": f"/etc/php/{version}/fpm/pool.d/",
            "pm": "dynamic",
            "max_children": 5,
            "start_servers": 2,
            "min_spare_servers": 1,
            "max_spare_servers": 3,
            "max_requests": 0,
            "idle_timeout": 10,
            "limit_extensions": ".php",
        }
        existing_fpm = fpm_by_desc.get(profile.lower())
        if existing_fpm:
            api.call(
                "FpmDaemons.update",
                {
                    "id": _to_int(_pick(existing_fpm, "id", default=0)),
                    **fpm_payload,
                },
            )
        else:
            try:
                api.call("FpmDaemons.add", fpm_payload)
            except ApiError as exc:
                message = str(exc).lower()
                if "already exists" not in message:
                    raise
                refreshed_collision_rows = api.listing("FpmDaemons.listing")
                collision = next(
                    (
                        row
                        for row in refreshed_collision_rows
                        if str(_pick(row, "reload_cmd", default="")).strip().lower() == str(fpm_payload["reload_cmd"]).lower()
                        or str(_pick(row, "config_dir", default="")).strip().lower() == str(fpm_payload["config_dir"]).lower()
                    ),
                    None,
                )
                if not collision:
                    raise
                api.call(
                    "FpmDaemons.update",
                    {
                        "id": _to_int(_pick(collision, "id", default=0)),
                        **fpm_payload,
                    },
                )

    refreshed_fpm = api.listing("FpmDaemons.listing")
    for row in refreshed_fpm:
        name = str(_pick(row, "description", default="")).strip().lower()
        daemon_id = _to_int(_pick(row, "id", default=0))
        if name in {p.lower() for p in desired_profiles} and daemon_id > 0:
            fpm_ids[name] = daemon_id

    for profile in desired_profiles:
        if fpm_ids.get(profile.lower(), 0) <= 0:
            raise ApiError(f"No FPM daemon config found for profile '{profile}'")

    by_description = {str(_pick(row, "description", default="")).strip().lower(): row for row in current}

    for profile in desired_profiles:
        payload = {
            "description": profile,
            "phpsettings": base_ini,
            "fpmconfig": fpm_ids[profile.lower()],
        }
        existing_profile = by_description.get(profile.lower())
        if existing_profile:
            api.call(
                "PhpSettings.update",
                {
                    "id": _to_int(_pick(existing_profile, "id", default=0)),
                    **payload,
                },
            )
        else:
            api.call("PhpSettings.add", payload)

    refreshed = api.listing("PhpSettings.listing")
    refreshed_by_description = {str(_pick(row, "description", default="")).strip().lower(): row for row in refreshed}
    ids: list[int] = []
    for profile in desired_profiles:
        row = refreshed_by_description.get(profile.lower())
        if not row:
            raise ApiError(f"Could not ensure PHP profile '{profile}'")
        ids.append(_to_int(_pick(row, "id", default=0)))
    return ids


def ensure_domain(
    api: FroxlorApi,
    customer_id: int,
    domain: str,
    documentroot: str,
    phpsettingid: int,
    is_email_domain: bool = True,
    extra_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    existing: dict[str, Any] | None = None
    for row in api.listing("Domains.listing"):
        if str(_pick(row, "domain", "domainname", default="")) == domain:
            existing = row
            break

    base_settings: dict[str, Any] = {
        "documentroot": documentroot,
        "isemaildomain": is_email_domain,
        "phpenabled": True,
        "phpsettingid": phpsettingid,
        "sslenabled": False,
    }
    if extra_settings:
        base_settings.update(extra_settings)

    if not existing:
        api.call(
            "Domains.add",
            {
                "customerid": customer_id,
                "domain": domain,
                **base_settings,
            },
        )

    for row in api.listing("Domains.listing"):
        if str(_pick(row, "domain", "domainname", default="")) == domain:
            domain_id = _to_int(_pick(row, "id", default=0))
            api.call(
                "Domains.update",
                {
                    "id": domain_id,
                    "domainname": domain,
                    "customerid": customer_id,
                    **base_settings,
                },
            )
            break

    for row in api.listing("Domains.listing"):
        if str(_pick(row, "domain", "domainname", default="")) == domain:
            return row
    raise ApiError(f"Domain was not created: {domain}")


def ensure_database(
    api: FroxlorApi,
    customer_id: int,
    customer_login: str,
    custom_suffix: str,
    description: str,
    mysql_server: int,
    db_host: str,
    db_port: str,
    db_root_user: str,
    db_root_pass: str,
    panel_db_name: str,
) -> tuple[str, str]:
    expected_name = f"{customer_login}_{custom_suffix}".replace("-", "_")
    db_rows = api.listing("Mysqls.listing", {"customerid": customer_id})
    for row in db_rows:
        dbname = str(_pick(row, "databasename", "dbname", default=""))
        if dbname == expected_name or dbname.endswith(custom_suffix):
            return dbname, "(unknown-existing-password)"

    subprocess.run(
        [
            "python3",
            "-c",
            (
                "import pymysql; "
                f"conn=pymysql.connect(host={db_host!r}, port={int(db_port)!r}, user={db_root_user!r}, password={db_root_pass!r}, database={panel_db_name!r}, autocommit=True); "
                "cur=conn.cursor(); "
                "cur.execute(\"UPDATE panel_settings SET value='DBNAME' WHERE settinggroup='customer' AND varname='mysqlprefix'\"); "
                "cur.close(); conn.close()"
            ),
        ],
        check=True,
    )

    password = _pw(20)
    try:
        api.call(
            "Mysqls.add",
            {
                "customerid": customer_id,
                "mysql_password": password,
                "description": description,
                "custom_suffix": custom_suffix,
                "mysql_server": mysql_server,
                "sendinfomail": False,
            },
        )
    except ApiError:
        fallback_name = f"{customer_login}_{custom_suffix}".replace("-", "_")
        sql_lines = [
            f"CREATE DATABASE IF NOT EXISTS `{fallback_name}`;",
            f"CREATE USER IF NOT EXISTS '{fallback_name}'@'%' IDENTIFIED BY '{password}';",
            f"GRANT ALL PRIVILEGES ON `{fallback_name}`.* TO '{fallback_name}'@'%';",
            "FLUSH PRIVILEGES;",
            (
                "INSERT INTO panel_databases (customerid, databasename, description, dbserver) "
                f"SELECT {customer_id}, '{fallback_name}', '{description}', {mysql_server} "
                f"WHERE NOT EXISTS (SELECT 1 FROM panel_databases WHERE databasename = '{fallback_name}');"
            ),
        ]
        subprocess.run(
            [
                "python3",
                "-c",
                (
                    "import pymysql,sys; "
                    f"conn=pymysql.connect(host={db_host!r}, port={int(db_port)!r}, user={db_root_user!r}, password={db_root_pass!r}, database={panel_db_name!r}, autocommit=True); "
                    "cur=conn.cursor(); "
                    f"[cur.execute(q) for q in {sql_lines!r}]; "
                    "cur.close(); conn.close()"
                ),
            ],
            check=True,
        )
    db_rows = api.listing("Mysqls.listing", {"customerid": customer_id})
    for row in db_rows:
        dbname = str(_pick(row, "databasename", "dbname", default=""))
        if dbname == expected_name or dbname.endswith(custom_suffix):
            return dbname, password
    raise ApiError(f"Database was not created with suffix: {custom_suffix}")


def ensure_mysql_server(
    api: FroxlorApi,
    host: str,
    port: str,
    privileged_user: str,
    privileged_password: str,
) -> int:
    servers = api.listing("MysqlServer.listing")
    payload = {
        "mysql_host": host,
        "mysql_port": port,
        "privileged_user": privileged_user,
        "privileged_password": privileged_password,
        "description": "seeded-mysql-server",
        "allow_all_customers": True,
        "test_connection": False,
    }

    if servers:
        first = servers[0]
        server_id = _to_int(_pick(first, "id", default=0))
        dbserver_id = _to_int(_pick(first, "dbserver", default=0))
        if server_id > 0:
            api.call("MysqlServer.update", {"id": server_id, **payload})
        else:
            api.call("MysqlServer.update", {"dbserver": dbserver_id, **payload})
        refreshed = api.listing("MysqlServer.listing")
        if refreshed:
            return _to_int(_pick(refreshed[0], "id", "dbserver", default=0))

    api.call(
        "MysqlServer.add",
        payload,
    )
    servers = api.listing("MysqlServer.listing")
    if servers:
        return _to_int(_pick(servers[0], "id", "dbserver", default=0))
    for row in servers:
        if str(_pick(row, "mysql_host", default="")) == host:
            return _to_int(_pick(row, "id", "dbserver", default=0))
    raise ApiError("MySQL server config was not created")


def ensure_mailbox(
    api: FroxlorApi,
    customer_id: int,
    mailbox: str,
    catchall: bool = False,
    spam_tag_level: int = 7,
    rewrite_subject: bool = True,
    spam_kill_level: int = 14,
    bypass_spam: bool = False,
    policy_greylist: bool = True,
) -> None:
    local, domain = mailbox.split("@", 1)
    existing = api.listing("Emails.listing", {"customerid": customer_id})
    existing_set = {str(_pick(x, "email_full", "email", "emailaddr", default="")).lower() for x in existing}
    if mailbox.lower() not in existing_set:
        api.call(
            "Emails.add",
            {
                "email_part": local,
                "domain": domain,
                "customerid": customer_id,
                "iscatchall": catchall,
                "spam_tag_level": spam_tag_level,
                "rewrite_subject": rewrite_subject,
                "spam_kill_level": spam_kill_level,
                "bypass_spam": bypass_spam,
                "policy_greylist": policy_greylist,
            },
        )
        api.call(
            "EmailAccounts.add",
            {
                "emailaddr": mailbox,
                "customerid": customer_id,
                "email_password": _pw(20),
                "sendinfomail": False,
            },
        )

    api.call(
        "Emails.update",
        {
            "emailaddr": mailbox,
            "customerid": customer_id,
            "iscatchall": catchall,
            "spam_tag_level": spam_tag_level,
            "rewrite_subject": rewrite_subject,
            "spam_kill_level": spam_kill_level,
            "bypass_spam": bypass_spam,
            "policy_greylist": policy_greylist,
        },
    )


def ensure_subdomain(api: FroxlorApi, customer_id: int, domain: str, subdomain: str, path: str, phpsettingid: int) -> str:
    fqdn = f"{subdomain}.{domain}".lower()
    rows = api.listing("SubDomains.listing", {"customerid": customer_id})
    existing = None
    for row in rows:
        if str(_pick(row, "domain", "domainname", default="")).lower() == fqdn:
            existing = row
            break
    payload = {
        "domainname": fqdn,
        "path": path,
        "phpsettingid": phpsettingid,
        "sslenabled": True,
        "ssl_redirect": False,
        "letsencrypt": False,
        "http2": True,
        "hsts_maxage": 3600,
        "customerid": customer_id,
    }
    if existing:
        api.call("SubDomains.update", {"id": _to_int(_pick(existing, "id", default=0)), **payload})
    else:
        api.call(
            "SubDomains.add",
            {
                "subdomain": subdomain,
                "domain": domain,
                "path": path,
                "phpsettingid": phpsettingid,
                "sslenabled": True,
                "ssl_redirect": False,
                "letsencrypt": False,
                "http2": True,
                "hsts_maxage": 3600,
                "customerid": customer_id,
            },
        )
    return fqdn


def ensure_email_forwarder(api: FroxlorApi, customer_id: int, mailbox: str, destination: str) -> None:
    rows = api.call("EmailForwarders.listing", {"emailaddr": mailbox})
    entries = rows.get("list", []) if isinstance(rows, dict) else (rows or [])
    existing = {str(_pick(item, "destination", default="")).strip().lower() for item in entries}
    if destination.lower() in existing:
        return
    try:
        api.call(
            "EmailForwarders.add",
            {
                "emailaddr": mailbox,
                "destination": destination,
                "customerid": customer_id,
            },
        )
    except ApiError as exc:
        if "already defined" not in str(exc).lower():
            raise


def ensure_email_sender_alias(api: FroxlorApi, customer_id: int, mailbox: str, allowed_sender: str) -> None:
    try:
        rows = api.call("EmailSender.listing", {"emailaddr": mailbox})
    except ApiError:
        return
    entries = rows.get("list", []) if isinstance(rows, dict) else (rows or [])
    existing = {str(_pick(item, "allowed_sender", default="")).strip().lower() for item in entries}
    if allowed_sender.lower() in existing:
        return
    try:
        api.call(
            "EmailSender.add",
            {
                "emailaddr": mailbox,
                "allowed_sender": allowed_sender,
                "customerid": customer_id,
            },
        )
    except ApiError:
        return


def ensure_ftp_account(api: FroxlorApi, customer_id: int, username: str, path: str, login_enabled: bool) -> None:
    rows = api.listing("Ftps.listing", {"customerid": customer_id})
    existing = None
    for row in rows:
        if str(_pick(row, "username", "ftpuser", default="")).strip().lower() == username.lower():
            existing = row
            break

    payload = {
        "path": path,
        "ftp_description": "seeded ftp account",
        "shell": "/bin/false",
        "login_enabled": login_enabled,
        "customerid": customer_id,
    }
    if existing:
        api.call(
            "Ftps.update",
            {
                "id": _to_int(_pick(existing, "id", default=0)),
                "username": username,
                **payload,
            },
        )
        return
    api.call(
        "Ftps.add",
        {
            **payload,
            "ftp_username": username,
            "ftp_password": _pw(20),
            "sendinfomail": False,
        },
    )


def ensure_dir_protection(api: FroxlorApi, customer_id: int, path: str, username: str, authname: str) -> None:
    rows = api.listing("DirProtections.listing", {"customerid": customer_id})
    existing = None
    for row in rows:
        row_path = str(_pick(row, "path", default="")).strip().lower()
        row_username = str(_pick(row, "username", default="")).strip().lower()
        if row_path == path.strip().lower() and row_username == username.strip().lower():
            existing = row
            break

    if existing:
        api.call(
            "DirProtections.update",
            {
                "id": _to_int(_pick(existing, "id", default=0)),
                "customerid": customer_id,
                "username": username,
                "directory_authname": authname,
                "directory_password": _pw(20),
            },
        )
        return

    try:
        api.call(
            "DirProtections.add",
            {
                "customerid": customer_id,
                "path": path,
                "username": username,
                "directory_authname": authname,
                "directory_password": _pw(20),
            },
        )
    except ApiError as exc:
        if "already exists" not in str(exc).lower():
            raise


def ensure_dir_option(api: FroxlorApi, customer_id: int, path: str) -> None:
    rows = api.listing("DirOptions.listing", {"customerid": customer_id})
    existing = None
    for row in rows:
        if str(_pick(row, "path", default="")).strip().lower() == path.strip().lower():
            existing = row
            break

    payload = {
        "customerid": customer_id,
        "path": path,
        "options_indexes": False,
        "options_cgi": False,
        "error404path": "/404-custom.html",
        "error403path": "/403-custom.html",
        "error500path": "/500-custom.html",
    }
    if existing:
        api.call("DirOptions.update", {"id": _to_int(_pick(existing, "id", default=0)), **payload})
    else:
        try:
            api.call("DirOptions.add", payload)
        except ApiError as exc:
            if "already exists" not in str(exc).lower():
                raise


def ensure_ssh_key(api: FroxlorApi, customer_id: int, ftp_username: str, ssh_pubkey: str, description: str) -> None:
    rows = api.listing("SshKeys.listing", {"customerid": customer_id})
    existing = None
    for row in rows:
        row_username = str(_pick(row, "username", "ftpuser", default="")).strip().lower()
        row_key = str(_pick(row, "ssh_pubkey", default="")).strip()
        if row_username == ftp_username.strip().lower() and row_key == ssh_pubkey.strip():
            existing = row
            break
    if existing:
        api.call(
            "SshKeys.update",
            {
                "id": _to_int(_pick(existing, "id", default=0)),
                "customerid": customer_id,
                "description": description,
            },
        )
        return
    api.call(
        "SshKeys.add",
        {
            "ftpuser": ftp_username,
            "customerid": customer_id,
            "ssh_pubkey": ssh_pubkey,
            "description": description,
        },
    )


def ensure_data_dump(
    api: FroxlorApi,
    customer_id: int,
    path: str,
    dump_dbs: bool,
    dump_mail: bool,
    dump_web: bool,
    pgp_public_key: str = "",
) -> dict[str, Any] | None:
    try:
        rows = api.listing("DataDump.listing", {"customerid": customer_id})
    except ApiError as exc:
        if "HTTP 405" in str(exc) or "cannot access this resource" in str(exc).lower():
            return None
        raise
    for row in rows:
        if (
            str(_pick(row, "path", default="")).strip() == path
            and _to_int(_pick(row, "dump_dbs", default=0)) == int(dump_dbs)
            and _to_int(_pick(row, "dump_mail", default=0)) == int(dump_mail)
            and _to_int(_pick(row, "dump_web", default=0)) == int(dump_web)
            and str(_pick(row, "pgp_public_key", default="")).strip() == pgp_public_key.strip()
        ):
            return row
    try:
        api.call(
            "DataDump.add",
            {
                "customerid": customer_id,
                "path": path,
                "dump_dbs": dump_dbs,
                "dump_mail": dump_mail,
                "dump_web": dump_web,
                "pgp_public_key": pgp_public_key,
            },
        )
    except ApiError as exc:
        if "HTTP 405" in str(exc) or "cannot access this resource" in str(exc).lower():
            return None
        raise
    try:
        rows = api.listing("DataDump.listing", {"customerid": customer_id})
    except ApiError:
        return None
    for row in rows:
        if str(_pick(row, "path", default="")).strip() == path:
            return row
    return None


def set_customer_2fa(
    loginname: str,
    type_2fa: int,
    data_2fa: str,
    db_host: str,
    db_port: str,
    db_root_user: str,
    db_root_pass: str,
    panel_db_name: str,
) -> None:
    subprocess.run(
        [
            "python3",
            "-c",
            (
                "import pymysql; "
                f"conn=pymysql.connect(host={db_host!r}, port={int(db_port)!r}, user={db_root_user!r}, password={db_root_pass!r}, database={panel_db_name!r}, autocommit=True); "
                "cur=conn.cursor(); "
                f'cur.execute("UPDATE panel_customers SET type_2fa=%s, data_2fa=%s WHERE loginname=%s", ({int(type_2fa)!r}, {data_2fa!r}, {loginname!r})); '
                "cur.close(); conn.close()"
            ),
        ],
        check=True,
    )


def ensure_wordpress_files(target_dir: Path, db_name: str, db_password: str) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    if (target_dir / "wp-includes").exists():
        return

    with tempfile.TemporaryDirectory() as tmp:
        archive_path = Path(tmp) / "wordpress.tar.gz"
        urlretrieve("https://wordpress.org/latest.tar.gz", archive_path)
        with tarfile.open(archive_path, "r:gz") as tar:
            safe_extract_tar(tar, Path(tmp))
        source_root = Path(tmp) / "wordpress"
        for child in source_root.iterdir():
            destination = target_dir / child.name
            if child.is_dir():
                shutil.copytree(child, destination, dirs_exist_ok=True)
            else:
                shutil.copy2(child, destination)

    config = f"""<?php
define('DB_NAME', '{db_name}');
define('DB_USER', '{db_name}');
define('DB_PASSWORD', '{db_password}');
define('DB_HOST', 'source-db');
define('DB_CHARSET', 'utf8');
define('DB_COLLATE', '');
$table_prefix = 'wp_';
define('WP_DEBUG', false);
if (!defined('ABSPATH')) {{
    define('ABSPATH', __DIR__ . '/');
}}
require_once ABSPATH . 'wp-settings.php';
"""
    (target_dir / "wp-config.php").write_text(config, encoding="utf-8")


def ensure_static_site(target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    html = """<!DOCTYPE html>
<html lang=\"en\">
<head><meta charset=\"utf-8\"><title>Static Demo</title></head>
<body>
  <h1>Static demo domain</h1>
  <p>This domain is used for migration tests.</p>
</body>
</html>
"""
    (target_dir / "index.html").write_text(html, encoding="utf-8")


def generate_self_signed_cert(common_name: str) -> tuple[str, str]:
    with tempfile.TemporaryDirectory() as tmp:
        key_path = Path(tmp) / "key.pem"
        cert_path = Path(tmp) / "cert.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key_path),
                "-out",
                str(cert_path),
                "-days",
                "365",
                "-subj",
                f"/CN={common_name}",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return cert_path.read_text(encoding="utf-8"), key_path.read_text(encoding="utf-8")


def ensure_certificate(api: FroxlorApi, domain_name: str, cert_pem: str, key_pem: str) -> None:
    payload = {
        "domainname": domain_name,
        "ssl_cert_file": cert_pem,
        "ssl_key_file": key_pem,
        "ssl_ca_file": "",
        "ssl_cert_chainfile": "",
    }
    try:
        api.call("Certificates.update", payload)
        return
    except ApiError as exc:
        message = str(exc).lower()
        if "does not have a certificate" not in message:
            if "already has a certificate" in message:
                return
            raise
    try:
        api.call("Certificates.add", payload)
    except ApiError as exc:
        if "already has a certificate" not in str(exc).lower():
            raise


def set_domain_dkim_keys(
    domain_name: str,
    public_key: str,
    private_key: str,
    db_host: str,
    db_port: str,
    db_root_user: str,
    db_root_pass: str,
    panel_db_name: str,
) -> None:
    subprocess.run(
        [
            "python3",
            "-c",
            (
                "import pymysql; "
                f"conn=pymysql.connect(host={db_host!r}, port={int(db_port)!r}, user={db_root_user!r}, password={db_root_pass!r}, database={panel_db_name!r}, autocommit=True); "
                "cur=conn.cursor(); "
                f'cur.execute("UPDATE panel_domains SET dkim=1, dkim_pubkey=%s, dkim_privkey=%s WHERE domain=%s", ({public_key!r}, {private_key!r}, {domain_name!r})); '
                "cur.close(); conn.close()"
            ),
        ],
        check=True,
    )


def set_domain_redirect(
    source_domain: str,
    destination_domain: str,
    redirect_code_id: int,
    db_host: str,
    db_port: str,
    db_root_user: str,
    db_root_pass: str,
    panel_db_name: str,
) -> None:
    subprocess.run(
        [
            "python3",
            "-c",
            (
                "import pymysql; "
                f"conn=pymysql.connect(host={db_host!r}, port={int(db_port)!r}, user={db_root_user!r}, password={db_root_pass!r}, database={panel_db_name!r}, autocommit=True); "
                "cur=conn.cursor(); "
                f'cur.execute("UPDATE panel_domains src JOIN panel_domains dst ON dst.domain=%s SET src.aliasdomain=dst.id WHERE src.domain=%s", ({destination_domain!r}, {source_domain!r})); '
                f'cur.execute("INSERT INTO domain_redirect_codes (did, rid) SELECT id, %s FROM panel_domains WHERE domain=%s ON DUPLICATE KEY UPDATE rid=VALUES(rid)", ({int(redirect_code_id)!r}, {source_domain!r})); '
                "cur.close(); conn.close()"
            ),
        ],
        check=True,
    )


def main() -> None:
    api_url = os.environ.get("SOURCE_API_URL", "http://127.0.0.1:8081/api.php")
    api_key = os.environ.get("SOURCE_API_KEY", "")
    api_secret = os.environ.get("SOURCE_API_SECRET", "")
    content_root = Path(os.environ.get("SOURCE_CONTENT_ROOT", "./data/source/customers")).resolve()
    db_host = os.environ.get("SOURCE_MYSQL_HOST", "source-db")
    db_port = os.environ.get("SOURCE_MYSQL_PORT", "3306")
    mysql_server_host = os.environ.get("SOURCE_API_MYSQL_HOST", "source-db")
    mysql_server_port = os.environ.get("SOURCE_API_MYSQL_PORT", "3306")
    db_root_user = os.environ.get("SOURCE_DB_ROOT_USER", "root")
    db_root_pass = os.environ.get("SOURCE_DB_ROOT_PASSWORD", "source-root")
    panel_db_name = os.environ.get("SOURCE_DB_NAME", "froxlor")

    if not api_key or not api_secret:
        raise SystemExit("Set SOURCE_API_KEY and SOURCE_API_SECRET before running seed script")

    api = FroxlorApi(api_url=api_url, api_key=api_key, api_secret=api_secret)
    api.call("Froxlor.listFunctions")
    mysql_server_id = ensure_mysql_server(api, mysql_server_host, mysql_server_port, db_root_user, db_root_pass)

    php_a, php_b = ensure_php_settings(api)

    customer_a = ensure_customer(
        api,
        login="custalpha",
        email="custalpha@example.test",
        firstname="Alpha",
        lastname="Customer",
        default_php_setting_id=php_a,
        mysql_server_id=mysql_server_id,
    )
    customer_b = ensure_customer(
        api,
        login="custbeta",
        email="custbeta@example.test",
        firstname="Beta",
        lastname="Customer",
        default_php_setting_id=php_b,
        mysql_server_id=mysql_server_id,
    )
    customer_c = ensure_customer(
        api,
        login="custgamma",
        email="custgamma@example.test",
        firstname="Gamma",
        lastname="Customer",
        default_php_setting_id=php_a,
        mysql_server_id=mysql_server_id,
    )

    cust_a_id = _to_int(_pick(customer_a, "customerid", "id", default=0))
    cust_b_id = _to_int(_pick(customer_b, "customerid", "id", default=0))
    cust_c_id = _to_int(_pick(customer_c, "customerid", "id", default=0))

    ensure_domain(
        api,
        customer_id=cust_a_id,
        domain="wp-demo.test",
        documentroot="/data/customers/custalpha/wp-demo.test",
        phpsettingid=php_a,
    )
    ensure_domain(
        api,
        customer_id=cust_a_id,
        domain="static-demo.test",
        documentroot="/data/customers/custalpha/static-demo.test",
        phpsettingid=php_b,
    )
    ensure_domain(
        api,
        customer_id=cust_b_id,
        domain="mail-demo.test",
        documentroot="/data/customers/custbeta/mail-demo.test",
        phpsettingid=php_a,
        is_email_domain=True,
    )
    ensure_domain(
        api,
        customer_id=cust_b_id,
        domain="empty-demo.test",
        documentroot="/data/customers/custbeta/empty-demo.test",
        phpsettingid=php_b,
        is_email_domain=False,
    )
    ensure_domain(
        api,
        customer_id=cust_c_id,
        domain="secure-demo.test",
        documentroot="/data/customers/custgamma/secure-demo.test",
        phpsettingid=php_a,
        is_email_domain=True,
        extra_settings={
            "sslenabled": True,
            "letsencrypt": False,
            "ssl_redirect": True,
            "specialsettings": "RewriteEngine On\nRewriteRule ^legacy/?$ /new-target [R=302,L]",
            "ssl_specialsettings": "Header always set X-Migrator-Test on",
            "include_specialsettings": True,
            "openbasedir": False,
            "openbasedir_path": "none",
            "writeaccesslog": False,
            "writeerrorlog": True,
            "http2": True,
            "hsts_maxage": 31536000,
            "hsts_sub": True,
            "hsts_preload": False,
            "ocsp_stapling": True,
            "override_tls": True,
            "ssl_protocols": "TLSv1.2 TLSv1.3",
            "ssl_cipher_list": "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384",
            "tlsv13_cipher_list": "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256",
            "honorcipherorder": True,
            "sessiontickets": False,
            "dkim": True,
            "description": "secure domain for migration settings test",
        },
    )
    secure_subdomain = ensure_subdomain(
        api,
        customer_id=cust_c_id,
        domain="secure-demo.test",
        subdomain="app",
        path="/data/customers/custgamma/secure-demo.test/app",
        phpsettingid=php_a,
    )
    ensure_domain(
        api,
        customer_id=cust_c_id,
        domain="redirect-demo.test",
        documentroot="/data/customers/custgamma/redirect-demo.test",
        phpsettingid=php_b,
        is_email_domain=False,
        extra_settings={
            "sslenabled": True,
            "letsencrypt": False,
            "ssl_redirect": True,
            "specialsettings": "RewriteEngine On\nRewriteRule ^(.*)$ https://secure-demo.test/$1 [R=301,L]",
            "ssl_specialsettings": "Header set X-Redirect-Demo yes",
            "include_specialsettings": True,
            "openbasedir": True,
            "openbasedir_path": "/data/customers/custgamma/redirect-demo.test:/tmp",
            "http2": True,
            "override_tls": True,
            "ssl_protocols": "TLSv1.2 TLSv1.3",
            "description": "redirect domain for migration settings test",
        },
    )
    ensure_domain(
        api,
        customer_id=cust_c_id,
        domain="forward-demo.test",
        documentroot="/data/customers/custgamma/forward-demo.test",
        phpsettingid=php_b,
        is_email_domain=False,
        extra_settings={
            "sslenabled": True,
            "letsencrypt": False,
            "ssl_redirect": False,
            "description": "domain forwarding fixture (alias redirect code)",
        },
    )

    wp_db, wp_pw = ensure_database(
        api,
        customer_id=cust_a_id,
        customer_login="custalpha",
        custom_suffix="wpdemo",
        description="WordPress test",
        mysql_server=mysql_server_id,
        db_host=db_host,
        db_port=db_port,
        db_root_user=db_root_user,
        db_root_pass=db_root_pass,
        panel_db_name=panel_db_name,
    )
    ensure_database(
        api,
        customer_id=cust_a_id,
        customer_login="custalpha",
        custom_suffix="analytics",
        description="Aux analytics test",
        mysql_server=mysql_server_id,
        db_host=db_host,
        db_port=db_port,
        db_root_user=db_root_user,
        db_root_pass=db_root_pass,
        panel_db_name=panel_db_name,
    )

    ensure_mailbox(api, customer_id=cust_b_id, mailbox="info@mail-demo.test", catchall=True)
    ensure_mailbox(api, customer_id=cust_b_id, mailbox="sales@mail-demo.test", catchall=False)
    ensure_mailbox(
        api,
        customer_id=cust_c_id,
        mailbox="alerts@secure-demo.test",
        catchall=False,
        spam_tag_level=4,
        rewrite_subject=False,
        spam_kill_level=9,
        bypass_spam=False,
        policy_greylist=False,
    )
    ensure_mailbox(
        api,
        customer_id=cust_c_id,
        mailbox="ops@secure-demo.test",
        catchall=False,
        spam_tag_level=10,
        rewrite_subject=True,
        spam_kill_level=20,
        bypass_spam=True,
        policy_greylist=True,
    )
    ensure_email_forwarder(
        api,
        customer_id=cust_c_id,
        mailbox="alerts@secure-demo.test",
        destination="ops@secure-demo.test",
    )
    ensure_email_sender_alias(
        api,
        customer_id=cust_c_id,
        mailbox="ops@secure-demo.test",
        allowed_sender="alerts@secure-demo.test",
    )
    ensure_ftp_account(
        api,
        customer_id=cust_c_id,
        username="custgammaftp1",
        path="secure-demo.test",
        login_enabled=True,
    )
    ensure_dir_protection(
        api,
        customer_id=cust_c_id,
        path="secure-demo.test/protected",
        username="secureops",
        authname="Secure Demo Protected Area",
    )
    ensure_dir_option(
        api,
        customer_id=cust_c_id,
        path="secure-demo.test/protected",
    )
    ssh_pubkey_fixture = os.environ.get("SEED_SSH_PUBKEY", "").strip()
    if not ssh_pubkey_fixture:
        local_pubkey = Path(__file__).resolve().parents[1] / "ssh" / "id_ed25519.pub"
        if local_pubkey.exists():
            ssh_pubkey_fixture = local_pubkey.read_text(encoding="utf-8").strip()
    if not ssh_pubkey_fixture:
        ssh_pubkey_fixture = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKlVpyX2dT+cmwoEiUs8SBTNfyvr3IiJYhSLt2FEm0US seed@migrator"
    ensure_ssh_key(
        api,
        customer_id=cust_c_id,
        ftp_username="custgammaftp1",
        ssh_pubkey=ssh_pubkey_fixture,
        description="seeded ssh key for migration",
    )
    data_dump_row = ensure_data_dump(
        api,
        customer_id=cust_c_id,
        path="/var/customers/backups/custgamma",
        dump_dbs=True,
        dump_mail=True,
        dump_web=True,
    )

    cert_pem, key_pem = generate_self_signed_cert("secure-demo.test")
    ensure_certificate(api, "secure-demo.test", cert_pem, key_pem)
    ensure_certificate(api, "redirect-demo.test", cert_pem, key_pem)

    set_domain_dkim_keys(
        "secure-demo.test",
        public_key="v=DKIM1; k=rsa; p=MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtestSeededDkimPublicKey0001",
        private_key="-----BEGIN PRIVATE KEY-----\\nseeded-dkim-private-key-for-bootstrap-tests\\n-----END PRIVATE KEY-----",
        db_host=db_host,
        db_port=db_port,
        db_root_user=db_root_user,
        db_root_pass=db_root_pass,
        panel_db_name=panel_db_name,
    )
    set_domain_redirect(
        source_domain="forward-demo.test",
        destination_domain="secure-demo.test",
        redirect_code_id=2,
        db_host=db_host,
        db_port=db_port,
        db_root_user=db_root_user,
        db_root_pass=db_root_pass,
        panel_db_name=panel_db_name,
    )
    set_customer_2fa(
        loginname="custgamma",
        type_2fa=1,
        data_2fa="MIGRATOR2FASEED-CUSTGAMMA",
        db_host=db_host,
        db_port=db_port,
        db_root_user=db_root_user,
        db_root_pass=db_root_pass,
        panel_db_name=panel_db_name,
    )

    wp_dir = content_root / "custalpha" / "wp-demo.test"
    static_dir = content_root / "custalpha" / "static-demo.test"
    mail_domain_dir = content_root / "custbeta" / "mail-demo.test"
    empty_dir = content_root / "custbeta" / "empty-demo.test"
    secure_dir = content_root / "custgamma" / "secure-demo.test"
    redirect_dir = content_root / "custgamma" / "redirect-demo.test"
    forward_dir = content_root / "custgamma" / "forward-demo.test"
    secure_sub_dir = content_root / "custgamma" / "secure-demo.test" / "app"
    secure_protected_dir = content_root / "custgamma" / "secure-demo.test" / "protected"

    ensure_wordpress_files(wp_dir, wp_db, wp_pw)
    ensure_static_site(static_dir)
    mail_domain_dir.mkdir(parents=True, exist_ok=True)
    empty_dir.mkdir(parents=True, exist_ok=True)
    ensure_static_site(secure_dir)
    ensure_static_site(redirect_dir)
    ensure_static_site(forward_dir)
    ensure_static_site(secure_sub_dir)
    ensure_static_site(secure_protected_dir)

    refreshed_secure_domain = None
    for row in api.listing("Domains.listing"):
        if str(_pick(row, "domain", "domainname", default="")) == "secure-demo.test":
            refreshed_secure_domain = row
            break
    if not refreshed_secure_domain:
        raise ApiError("Could not reload secure-demo.test after DKIM update")

    cert_rows = api.listing("Certificates.listing")
    cert_domains = sorted([
        str(_pick(cert, "domainname", "domain", default="")).lower() for cert in cert_rows if _pick(cert, "domainname", "domain", default="")
    ])

    summary = {
        "customers": ["custalpha", "custbeta", "custgamma"],
        "domains": [
            "wp-demo.test",
            "static-demo.test",
            "mail-demo.test",
            "empty-demo.test",
            "secure-demo.test",
            "redirect-demo.test",
            "forward-demo.test",
        ],
        "subdomains": [secure_subdomain],
        "mailboxes": [
            "info@mail-demo.test",
            "sales@mail-demo.test",
            "alerts@secure-demo.test",
            "ops@secure-demo.test",
        ],
        "wordpress_db": wp_db,
        "php_settings_used": [php_a, php_b],
        "php_settings_profiles": ["php8.3", "php8.4"],
        "domain_settings": {
            "secure-demo.test": {
                "ssl_enabled": 1,
                "letsencrypt": 0,
                "specialsettings_contains": [
                    "RewriteRule ^legacy/?$ /new-target",
                    "RewriteEngine On",
                ],
                "ssl_cipher_list_contains": ["ECDHE-ECDSA-AES256-GCM-SHA384"],
                "dkim": 1,
                "dkim_pubkey_nonempty": 1,
                "openbasedir": 0,
                "writeaccesslog": 0,
            },
            "redirect-demo.test": {
                "ssl_enabled": 1,
                "letsencrypt": 0,
                "specialsettings_contains": ["https://secure-demo.test/$1", "RewriteEngine On"],
                "ssl_cipher_list_contains": [],
                "dkim": 0,
                "openbasedir": 1,
                "writeaccesslog": 1,
            },
        },
        "mailbox_settings": {
            "alerts@secure-demo.test": {
                "spam_tag_level": 4,
                "rewrite_subject": 0,
                "spam_kill_level": 9,
                "bypass_spam": 0,
                "policy_greylist": 0,
            },
            "ops@secure-demo.test": {
                "spam_tag_level": 10,
                "rewrite_subject": 1,
                "spam_kill_level": 20,
                "bypass_spam": 1,
                "policy_greylist": 1,
            },
        },
        "email_forwarders": [
            {
                "email": "alerts@secure-demo.test",
                "destination": "ops@secure-demo.test",
            }
        ],
        "email_sender_aliases": [
            {
                "email": "ops@secure-demo.test",
                "allowed_sender": "alerts@secure-demo.test",
            }
        ],
        "ftp_accounts": [
            {
                "username": "custgammaftp1",
                "path": "secure-demo.test",
                "login_enabled": 1,
            }
        ],
        "dir_protections": [
            {
                "path": "secure-demo.test/protected",
                "username": "secureops",
                "authname": "Secure Demo Protected Area",
            }
        ],
        "dir_options": [
            {
                "path": "secure-demo.test/protected",
                "options_indexes": 0,
                "options_cgi": 0,
                "error404path": "/404-custom.html",
                "error403path": "/403-custom.html",
                "error500path": "/500-custom.html",
            }
        ],
        "ssh_keys": [
            {
                "ftp_username": "custgammaftp1",
                "description": "seeded ssh key for migration",
                "ssh_pubkey": ssh_pubkey_fixture,
            }
        ],
        "customer_security": {
            "custgamma": {
                "type_2fa": 1,
                "data_2fa": "MIGRATOR2FASEED-CUSTGAMMA",
            }
        },
        "data_dumps": [
            {
                "path": str(_pick(data_dump_row or {}, "path", default="")),
                "dump_dbs": _to_int(_pick(data_dump_row or {}, "dump_dbs", default=0)),
                "dump_mail": _to_int(_pick(data_dump_row or {}, "dump_mail", default=0)),
                "dump_web": _to_int(_pick(data_dump_row or {}, "dump_web", default=0)),
                "pgp_public_key": str(_pick(data_dump_row or {}, "pgp_public_key", default="")),
            }
        ]
        if data_dump_row
        else [],
        "certificate_domains": cert_domains,
        "domain_redirects": [
            {
                "domain": "forward-demo.test",
                "destination": "secure-demo.test",
                "redirect_code_id": 2,
            }
        ],
    }
    out = content_root.parent / "seed-summary.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Seed complete. Summary: {out}")


if __name__ == "__main__":
    random.seed()
    main()
