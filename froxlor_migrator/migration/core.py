from __future__ import annotations

import json
import re
import shlex
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..api import FroxlorApiError, FroxlorClient
from ..config import AppConfig
from ..froxlor_mysql import (
    _credential_score,
    connect_kwargs_from_credentials,
    extract_sql_root_credentials,
    froxlor_userdata_paths,
    load_local_sql_credentials,
    load_local_sql_root_credentials,
    mysql_defaults_content,
)
from ..mysql_driver import execute as mysql_execute
from ..mysql_driver import query as mysql_query
from ..mysql_tunnel import open_ssh_tunnel
from ..transfer import TransferRunner
from ..util import as_int, pick
from .types import MigrationError, ResourceRow, Selection


class MigratorCore:
    def _relative_customer_path(self, path: str, customer_login: str) -> str:
        cleaned = path.strip().strip("/")
        if not cleaned:
            return ""
        marker = f"/{customer_login.strip('/')}/"
        lowered = cleaned.lower()
        if marker.lower() in f"/{lowered}/":
            original = cleaned
            while marker in f"/{original}/":
                if marker in original:
                    original = original.split(marker, 1)[1].strip("/")
                else:
                    break
            cleaned = original
        if cleaned.startswith(customer_login.strip("/") + "/"):
            cleaned = cleaned[len(customer_login.strip("/")) + 1 :]
        return cleaned

    def __init__(
        self,
        config: AppConfig,
        source: FroxlorClient,
        target: FroxlorClient,
        runner: TransferRunner,
    ) -> None:
        self.config = config
        self.source = source
        self.target = target
        self.runner = runner
        self._source_sql_credentials: dict[str, str] | None = None
        self._source_sql_root_credentials: dict[str, str] | None = None
        self._target_sql_root_credentials: dict[str, str] | None = None
        self.progress_callback = None

    def set_progress_callback(self, callback: Any) -> None:
        self.progress_callback = callback

    def _emit_progress(self, step: int, total: int, status: str) -> None:
        callback = getattr(self, "progress_callback", None)
        if callback is None:
            return
        callback(step, total, status)

    def _customer_login(self, customer: ResourceRow) -> str:
        return str(pick(customer, "loginname", "login", default="")).strip()

    def _customer_email(self, customer: ResourceRow) -> str:
        return str(pick(customer, "email", default="")).strip().lower()

    def _domain_name(self, domain: ResourceRow) -> str:
        return str(pick(domain, "domain", "domainname", default="")).strip().lower()

    def _mailbox_address(self, mailbox: ResourceRow) -> str:
        return str(pick(mailbox, "email_full", "email", "emailaddr", default="")).strip().lower()

    def _coerce_id_list(self, value: Any, fallback: list[int]) -> list[int]:
        if isinstance(value, list):
            result = [as_int(item) for item in value if as_int(item) > 0]
            return result or fallback
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return fallback
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                result = [as_int(item) for item in parsed if as_int(item) > 0]
                return result or fallback
            if text.isdigit() and as_int(text) > 0:
                return [as_int(text)]
        numeric = as_int(value)
        if numeric > 0:
            return [numeric]
        return fallback

    def preflight(self, selection: Selection) -> None:
        self.source.test_connection()
        self.target.test_connection()
        needs_ssh = (
            selection.include_files
            or selection.include_databases
            or selection.include_mail
            or any(as_int(pick(domain, "dkim", default=0)) == 1 for domain in selection.domains)
        )
        for command in self.runner.preflight_commands(
            include_ssh=needs_ssh,
            include_database_tools=selection.include_databases or needs_ssh,
            include_mail_tools=selection.include_mail,
        ):
            self.runner.run(command)

    def _find_target_customer(self, source_customer: ResourceRow) -> ResourceRow | None:
        source_login = self._customer_login(source_customer)
        source_email = self._customer_email(source_customer)
        for customer in self.target.list_customers():
            customer_login = self._customer_login(customer)
            customer_email = self._customer_email(customer)
            if source_login and customer_login == source_login:
                return customer
            if source_email and customer_email and source_email == customer_email:
                return customer
        return None

    def _customer_payload(self, source_customer: ResourceRow) -> dict[str, Any]:
        return {
            "email": str(pick(source_customer, "email", default="migration@example.invalid")),
            "name": str(pick(source_customer, "name", "lastname", default="Migrated")),
            "firstname": str(pick(source_customer, "firstname", default="Customer")),
            "company": str(pick(source_customer, "company", default="")),
            "street": str(pick(source_customer, "street", default="")),
            "zipcode": str(pick(source_customer, "zipcode", default="")),
            "city": str(pick(source_customer, "city", default="")),
            "phone": str(pick(source_customer, "phone", default="")),
            "fax": str(pick(source_customer, "fax", default="")),
            "customernumber": str(pick(source_customer, "customernumber", default="")),
            "def_language": str(pick(source_customer, "def_language", default="en")),
            "gui_access": bool(as_int(pick(source_customer, "gui_access", default=1))),
            "api_allowed": bool(as_int(pick(source_customer, "api_allowed", default=1))),
            "shell_allowed": bool(as_int(pick(source_customer, "shell_allowed", default=0))),
            "gender": as_int(pick(source_customer, "gender", default=0)),
            "custom_notes": str(pick(source_customer, "custom_notes", default="")),
            "custom_notes_show": bool(as_int(pick(source_customer, "custom_notes_show", default=0))),
            "sendpassword": False,
            "deactivated": bool(as_int(pick(source_customer, "deactivated", default=0))),
            "diskspace": as_int(pick(source_customer, "diskspace", default=-1024)),
            "diskspace_ul": bool(as_int(pick(source_customer, "diskspace_ul", default=1))),
            "traffic": as_int(pick(source_customer, "traffic", default=-1048576)),
            "traffic_ul": bool(as_int(pick(source_customer, "traffic_ul", default=1))),
            "subdomains": as_int(pick(source_customer, "subdomains", default=-1)),
            "subdomains_ul": bool(as_int(pick(source_customer, "subdomains_ul", default=1))),
            "emails": as_int(pick(source_customer, "emails", default=-1)),
            "emails_ul": bool(as_int(pick(source_customer, "emails_ul", default=1))),
            "email_accounts": as_int(pick(source_customer, "email_accounts", default=-1)),
            "email_accounts_ul": bool(as_int(pick(source_customer, "email_accounts_ul", default=1))),
            "email_forwarders": as_int(pick(source_customer, "email_forwarders", default=-1)),
            "email_forwarders_ul": bool(as_int(pick(source_customer, "email_forwarders_ul", default=1))),
            "email_quota": as_int(pick(source_customer, "email_quota", default=-1)),
            "email_quota_ul": bool(as_int(pick(source_customer, "email_quota_ul", default=1))),
            "email_imap": bool(as_int(pick(source_customer, "imap", "email_imap", default=0))),
            "email_pop3": bool(as_int(pick(source_customer, "pop3", "email_pop3", default=0))),
            "ftps": as_int(pick(source_customer, "ftps", default=-1)),
            "ftps_ul": bool(as_int(pick(source_customer, "ftps_ul", default=1))),
            "mysqls": as_int(pick(source_customer, "mysqls", default=-1)),
            "mysqls_ul": bool(as_int(pick(source_customer, "mysqls_ul", default=1))),
            "createstdsubdomain": bool(as_int(pick(source_customer, "createstdsubdomain", default=1))),
            "phpenabled": bool(as_int(pick(source_customer, "phpenabled", default=1))),
            "allowed_phpconfigs": self._coerce_id_list(pick(source_customer, "allowed_phpconfigs", default=[]), [1]),
            "perlenabled": bool(as_int(pick(source_customer, "perlenabled", default=0))),
            "dnsenabled": bool(as_int(pick(source_customer, "dnsenabled", default=0))),
            "logviewenabled": bool(as_int(pick(source_customer, "logviewenabled", default=0))),
            "store_defaultindex": bool(as_int(pick(source_customer, "store_defaultindex", default=0))),
            "theme": str(pick(source_customer, "theme", default="")),
            "allowed_mysqlserver": self._coerce_id_list(pick(source_customer, "allowed_mysqlserver", default=[]), [0]),
            "type_2fa": as_int(pick(source_customer, "type_2fa", default=0)),
            "data_2fa": str(pick(source_customer, "data_2fa", default="")),
        }

    def _ensure_target_customer(self, source_customer: ResourceRow, target_customer: ResourceRow | None = None) -> int:
        if target_customer:
            customer_id = as_int(pick(target_customer, "customerid", "id", default=0))
            if not customer_id:
                raise MigrationError("Could not resolve pre-selected target customer id")
            return customer_id

        existing = self._find_target_customer(source_customer)
        payload = self._customer_payload(source_customer)
        if existing:
            customer_id = as_int(pick(existing, "customerid", "id", default=0))
            if not customer_id:
                raise MigrationError("Could not resolve existing target customer id")
            self.target.call(
                "Customers.update",
                {
                    "id": customer_id,
                    "loginname": str(pick(existing, "loginname", "login", default="")),
                    **payload,
                },
            )
            return customer_id

        add_payload = {
            **{key: value for key, value in payload.items() if key not in {"deactivated", "theme"}},
            "new_loginname": str(pick(source_customer, "loginname", "login", default="")),
        }
        try:
            data = self.target.call("Customers.add", add_payload)
        except FroxlorApiError as exc:
            existing = self._find_target_customer(source_customer)
            if existing:
                resolved_id = as_int(pick(existing, "customerid", "id", default=0))
                if resolved_id:
                    return resolved_id
            raise MigrationError(f"Failed to create target customer via API: {exc}") from exc
        customer_id = as_int(pick(data or {}, "customerid", "id", default=0))
        if customer_id:
            return customer_id
        existing = self._find_target_customer(source_customer)
        if existing:
            return as_int(pick(existing, "customerid", "id", default=0))
        raise MigrationError("Failed to create target customer")

    def _get_target_domain(self, domain_name: str) -> ResourceRow | None:
        for domain in self.target.list_domains():
            if self._domain_name(domain) == domain_name.lower():
                return domain
        return None

    def _source_sql_root(self) -> dict[str, str]:
        if self._source_sql_root_credentials is not None:
            return self._source_sql_root_credentials
        self._source_sql_root_credentials = load_local_sql_root_credentials(froxlor_userdata_paths())
        return self._source_sql_root_credentials

    def _source_sql(self) -> dict[str, str]:
        if self._source_sql_credentials is not None:
            return self._source_sql_credentials
        self._source_sql_credentials = load_local_sql_credentials(froxlor_userdata_paths())
        return self._source_sql_credentials

    def _target_sql_root(self) -> dict[str, str]:
        if self._target_sql_root_credentials is not None:
            return self._target_sql_root_credentials
        if self.runner.dry_run:
            raise MigrationError("Cannot resolve target sql_root credentials in dry-run mode")
        found: list[dict[str, str]] = []
        for path in froxlor_userdata_paths():
            try:
                content = self.runner.read_remote_file(path)
            except Exception:
                continue
            creds = extract_sql_root_credentials(content)
            if creds:
                found.append(creds)
        if found:
            self._target_sql_root_credentials = max(found, key=_credential_score)
            return self._target_sql_root_credentials
        raise MigrationError("Could not parse target sql_root credentials from froxlor userdata files")

    @contextmanager
    def _target_mysql_connect_kwargs(self) -> Iterator[dict[str, Any]]:
        creds = self._target_sql_root()
        kwargs = connect_kwargs_from_credentials(creds)
        remote_host = str(kwargs.get("host", "localhost"))
        remote_port = int(kwargs.get("port", 3306))
        transport = self.runner.ssh_transport()
        with open_ssh_tunnel(transport, remote_host, remote_port) as (_, local_port):
            tunneled = dict(kwargs)
            tunneled.pop("unix_socket", None)
            tunneled["host"] = "127.0.0.1"
            tunneled["port"] = local_port
            yield tunneled

    def _sql_utf8_literal(self, value: str) -> str:
        if value == "":
            return "''"
        return f"CONVERT(0x{value.encode('utf-8').hex()} USING utf8mb4)"

    def _sql_string_literal(self, value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace("\x00", "\\0").replace("\n", "\\n").replace("\r", "\\r").replace("\x1a", "\\Z").replace("'", "\\'")
        return f"'{escaped}'"

    def _run_source_mysql_query(self, sql: str, database: str) -> list[list[str]]:
        if self.runner.dry_run:
            return []
        try:
            return mysql_query(connect_kwargs_from_credentials(self._source_sql_root()), database, sql)
        except Exception as exc:
            raise MigrationError(f"Source SQL query failed: {str(exc)[:400]}") from exc

    def _run_source_panel_query(self, sql: str) -> list[list[str]]:
        if self.runner.dry_run:
            return []
        try:
            return mysql_query(connect_kwargs_from_credentials(self._source_sql()), self.config.mysql.source_panel_database, sql)
        except Exception as exc:
            raise MigrationError(f"Source panel SQL query failed: {str(exc)[:400]}") from exc

    def _run_target_mysql_query(self, sql: str, database: str) -> list[list[str]]:
        if self.runner.dry_run:
            return []
        try:
            with self._target_mysql_connect_kwargs() as connect_kwargs:
                return mysql_query(connect_kwargs, database, sql)
        except Exception as exc:
            raise MigrationError(f"Target SQL query failed: {str(exc)[:400]}") from exc

    def _run_target_panel_query(self, sql: str) -> list[list[str]]:
        return self._run_target_mysql_query(sql, self.config.mysql.target_panel_database)

    def _exec_target_mysql_sql(self, sql: str, database: str) -> None:
        try:
            with self._target_mysql_connect_kwargs() as connect_kwargs:
                mysql_execute(connect_kwargs, database, sql)
        except Exception as exc:
            raise MigrationError(f"Target SQL execution failed: {str(exc)[:400]}") from exc

    def _exec_target_panel_sql(self, sql: str) -> None:
        self._exec_target_mysql_sql(sql, self.config.mysql.target_panel_database)

    def _transfer_database_with_defaults(self, source_db: str, target_db: str) -> None:
        if self.runner.dry_run:
            return
        source_defaults_content = mysql_defaults_content(self._source_sql_root())
        target_defaults_content = mysql_defaults_content(self._target_sql_root())

        with (
            tempfile.NamedTemporaryFile(prefix="froxlor-src-", suffix=".cnf", delete=False) as source_defaults,
            tempfile.NamedTemporaryFile(prefix="froxlor-dump-", suffix=".sql", delete=False) as dump_file,
        ):
            source_defaults_path = Path(source_defaults.name)
            dump_path = Path(dump_file.name)
            source_defaults.write(source_defaults_content.encode("utf-8"))
            source_defaults.flush()

        remote_defaults = f"/tmp/froxlor-target-{target_db}.cnf"
        remote_dump = f"/tmp/froxlor-dump-{target_db}.sql"

        try:
            dump_cmd = (
                f"{shlex.quote(self.config.commands.mysqldump)} "
                f"--defaults-extra-file={shlex.quote(str(source_defaults_path))} "
                "--single-transaction --quick --skip-lock-tables "
                f"{shlex.quote(source_db)} > {shlex.quote(str(dump_path))}"
            )
            self.runner.run(dump_cmd)
            self.runner.write_remote_file(remote_defaults, target_defaults_content, mode=0o600)
            self.runner.upload_file(str(dump_path), remote_dump, mode=0o600)
            restore_cmd = (
                f"{shlex.quote(self.config.commands.mysql)} "
                f"--defaults-extra-file={shlex.quote(remote_defaults)} "
                f"{shlex.quote(target_db)} < {shlex.quote(remote_dump)}"
            )
            self.runner.run_remote(restore_cmd)
        finally:
            try:
                source_defaults_path.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                dump_path.unlink(missing_ok=True)
            except Exception:
                pass
            self.runner.run_remote(f"rm -f {shlex.quote(remote_defaults)} {shlex.quote(remote_dump)}", check=False)

    def _sync_dkim_keys_db(self, domain_name: str, dkim_pubkey: str, dkim_privkey: str) -> None:
        update_sql = (
            "UPDATE panel_domains "
            f"SET dkim=1, dkim_pubkey={self._sql_utf8_literal(dkim_pubkey)}, "
            f"dkim_privkey={self._sql_utf8_literal(dkim_privkey)} "
            f"WHERE domain={self._sql_utf8_literal(domain_name)};"
        )
        self._exec_target_panel_sql(update_sql)

    def _source_mysql_prefix_setting(self) -> str:
        rows = self._run_source_panel_query("SELECT value FROM panel_settings WHERE settinggroup='customer' AND varname='mysqlprefix' LIMIT 1;")
        if not rows or not rows[0]:
            return ""
        return str(rows[0][0]).strip()

    def _sync_target_mysql_prefix_setting(self) -> None:
        value = self._source_mysql_prefix_setting()
        if not value:
            return
        sql = f"UPDATE panel_settings SET value={self._sql_utf8_literal(value)} WHERE settinggroup='customer' AND varname='mysqlprefix';"
        self._exec_target_panel_sql(sql)

    def _load_source_mail_password_hashes(self, mailboxes: list[dict[str, Any]]) -> dict[str, tuple[str, str]]:
        emails = {self._mailbox_address(item) for item in mailboxes if self._mailbox_address(item)}
        if not emails:
            return {}
        email_list_sql = ", ".join(self._sql_utf8_literal(email) for email in sorted(emails))
        rows = self._run_source_panel_query(f"SELECT email, password, password_enc FROM mail_users WHERE email IN ({email_list_sql});")
        out: dict[str, tuple[str, str]] = {}
        for row in rows:
            if len(row) < 3:
                continue
            out[row[0].strip().lower()] = (row[1], row[2])
        return out

    def _load_source_database_user_hashes(self, source_db_names: list[str]) -> dict[str, tuple[str, str]]:
        db_users = [name.strip() for name in source_db_names if name.strip()]
        if not db_users:
            return {}
        user_literals = ", ".join(self._sql_utf8_literal(name) for name in sorted(set(db_users)))
        rows = self._run_source_mysql_query(
            f"SELECT User, plugin, authentication_string FROM mysql.user WHERE User IN ({user_literals});",
            "mysql",
        )
        out: dict[str, tuple[str, str]] = {}
        for row in rows:
            if len(row) < 3:
                continue
            out[row[0].strip()] = (row[1], row[2])
        return out

    def _sync_customer_password_hash(self, source_customer: dict[str, Any], target_customer_id: int) -> None:
        password_hash = str(pick(source_customer, "password", default="")).strip()
        if not password_hash:
            return
        sql = f"UPDATE panel_customers SET password={self._sql_utf8_literal(password_hash)} WHERE customerid={target_customer_id};"
        self._exec_target_panel_sql(sql)

    def _sync_customer_2fa_settings(self, source_customer: dict[str, Any], target_customer_id: int) -> None:
        type_2fa = as_int(pick(source_customer, "type_2fa", default=0))
        data_2fa = str(pick(source_customer, "data_2fa", default="")).strip()
        sql = f"UPDATE panel_customers SET type_2fa={type_2fa}, data_2fa={self._sql_utf8_literal(data_2fa)} WHERE customerid={target_customer_id};"
        self._exec_target_panel_sql(sql)

    def _sync_ftp_password_hashes(self, target_customer_id: int, ftp_accounts: list[dict[str, Any]]) -> None:
        statements: list[str] = []
        for row in ftp_accounts:
            username = str(pick(row, "username", "ftpuser", default="")).strip().lower()
            password_hash = str(pick(row, "password", default="")).strip()
            if not username:
                continue
            if not password_hash:
                raise MigrationError(f"Source FTP account has empty password hash: {username}")
            statements.append(
                "UPDATE ftp_users "
                f"SET password={self._sql_utf8_literal(password_hash)} "
                f"WHERE customerid={target_customer_id} AND username={self._sql_utf8_literal(username)};"
            )
        if statements:
            self._exec_target_panel_sql(" ".join(statements))

    def _sync_mail_password_hashes(self, target_customer_id: int, mailboxes: list[dict[str, Any]]) -> None:
        source_hashes = self._load_source_mail_password_hashes(mailboxes)
        statements: list[str] = []
        for mailbox in mailboxes:
            emailaddr = self._mailbox_address(mailbox)
            if not emailaddr:
                continue
            if emailaddr not in source_hashes:
                raise MigrationError(f"Source mailbox login hash missing in mail_users table: {emailaddr}")
            password_hash, password_enc = source_hashes[emailaddr]
            if not password_hash and not password_enc:
                raise MigrationError(f"Source mailbox login hash empty for: {emailaddr}")
            statements.append(
                "UPDATE mail_users "
                f"SET password={self._sql_utf8_literal(password_hash)}, "
                f"password_enc={self._sql_utf8_literal(password_enc)} "
                f"WHERE customerid={target_customer_id} AND email={self._sql_utf8_literal(emailaddr)};"
            )
        if statements:
            self._exec_target_panel_sql(" ".join(statements))

    def _sync_dir_protection_password_hashes(
        self,
        target_customer_id: int,
        dir_protections: list[dict[str, Any]],
        customer_login: str,
    ) -> None:
        target_rows = self.target.list_dir_protections(customerid=target_customer_id)
        target_by_key = {
            (
                self._relative_customer_path(str(pick(row, "path", default="")), customer_login).lower(),
                str(pick(row, "username", default="")).strip().lower(),
            ): str(pick(row, "path", default="")).strip()
            for row in target_rows
        }
        statements: list[str] = []
        for row in dir_protections:
            path = self._relative_customer_path(str(pick(row, "path", default="")), customer_login)
            username = str(pick(row, "username", default="")).strip().lower()
            password_hash = str(pick(row, "password", default="")).strip()
            if not path or not username or not password_hash:
                continue
            target_path = target_by_key.get((path.lower(), username), "")
            if not target_path:
                continue
            statements.append(
                "UPDATE panel_htpasswds "
                f"SET password={self._sql_utf8_literal(password_hash)} "
                f"WHERE customerid={target_customer_id} "
                f"AND path={self._sql_utf8_literal(target_path)} "
                f"AND username={self._sql_utf8_literal(username)};"
            )
        if statements:
            self._exec_target_panel_sql(" ".join(statements))

    def _sync_database_login_hashes(self, source_to_target_db: dict[str, str]) -> None:
        if not source_to_target_db:
            return
        source_hashes = self._load_source_database_user_hashes(list(source_to_target_db.keys()))
        statements: list[str] = []
        for source_db, target_db in source_to_target_db.items():
            auth = source_hashes.get(source_db)
            if not auth:
                raise MigrationError(f"Source DB login hash missing in mysql.user for database user: {source_db}")
            plugin, auth_hash = auth
            if not auth_hash:
                raise MigrationError(f"Source DB login hash empty for database user: {source_db}")
            if not re.fullmatch(r"[A-Za-z0-9_]+", plugin):
                raise MigrationError(f"Unsupported SQL auth plugin name for database user {source_db}: {plugin!r}")
            for host in ["%", "localhost", "target-db", "127.0.0.1", self.config.ssh.host]:
                if plugin == "mysql_native_password":
                    statements.append(
                        "ALTER USER IF EXISTS "
                        f"{self._sql_string_literal(target_db)}@{self._sql_string_literal(host)} "
                        f"IDENTIFIED BY PASSWORD {self._sql_string_literal(auth_hash)};"
                    )
                else:
                    statements.append(
                        "ALTER USER IF EXISTS "
                        f"{self._sql_string_literal(target_db)}@{self._sql_string_literal(host)} "
                        f"IDENTIFIED VIA {plugin} USING {self._sql_string_literal(auth_hash)};"
                    )
        if not statements:
            return
        self._exec_target_mysql_sql(" ".join(statements), "mysql")

    def _sync_password_hashes(
        self,
        target_customer_id: int,
        source_customer: dict[str, Any],
        ftp_accounts: list[dict[str, Any]],
        mailboxes: list[dict[str, Any]],
        dir_protections: list[dict[str, Any]],
        customer_login: str,
    ) -> None:
        self._sync_customer_password_hash(source_customer, target_customer_id)
        self._sync_customer_2fa_settings(source_customer, target_customer_id)
        self._sync_ftp_password_hashes(target_customer_id, ftp_accounts)
        self._sync_mail_password_hashes(target_customer_id, mailboxes)
        self._sync_dir_protection_password_hashes(target_customer_id, dir_protections, customer_login)
