from __future__ import annotations

import json
import re
import shlex
import tempfile
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

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
from ..mysql_tunnel import open_ssh_tunnel, open_ssh_unix_socket_tunnel
from ..transfer import TransferRunner, remote_sudo_prefix
from ..util import as_int, domain_name, mailbox_address, pick, relative_customer_path
from .types import MigrationError, ResourceRow, Selection

T = TypeVar("T")


class MigratorCore:
    def _debug(self, message: str, **payload: Any) -> None:
        # Some unit tests create Migrator/MigratorCore objects without a runner.
        # In that case, skip debug calls rather than raising AttributeError.
        runner = getattr(self, "runner", None)
        if runner is None:
            return
        runner.debug_event(message, **payload)

    @staticmethod
    def _redact_connect_kwargs(connect_kwargs: dict[str, Any]) -> dict[str, Any]:
        redacted = dict(connect_kwargs)
        if "password" in redacted:
            redacted["password"] = "***"
        return redacted

    def _allow_remote_mysql_fallback(self, database: str) -> bool:
        panel_db = self.config.mysql.target_panel_database.strip().lower()
        return database.strip().lower() != panel_db

    @staticmethod
    def _mysql_socket_candidates() -> list[str]:
        return [
            "/run/mysqld/mysqld.sock",
            "/var/run/mysqld/mysqld.sock",
            "/tmp/mysql.sock",
            "/run/mysql/mysql.sock",
            "/var/lib/mysql/mysql.sock",
        ]

    def _discover_remote_mysql_socket(self) -> str:
        for candidate in self._mysql_socket_candidates():
            result = self.runner.run_remote(f"test -S {shlex.quote(candidate)}", check=False)
            if result.returncode == 0:
                return candidate
        return ""


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

        # Keep a single SSH tunnel open for all remote MySQL operations during a migration run
        self._target_mysql_tunnel_stack: ExitStack | None = None
        self._target_mysql_tunnel_connect_kwargs: dict[str, Any] | None = None
        self._target_mysql_tunnel_refcount: int = 0

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

    def _domain_name(self, domain: ResourceRow) -> str:
        return domain_name(domain)

    def _mailbox_address(self, mailbox: ResourceRow) -> str:
        return mailbox_address(mailbox)

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
        # Match on login only: shared contact email is not a safe identity key
        # and could bind the migration to an unrelated customer.
        source_login = self._customer_login(source_customer)
        if not source_login:
            return None
        for customer in self.target.list_customers():
            if self._customer_login(customer) == source_login:
                return customer
        return None

    def _customer_payload(self, source_customer: ResourceRow, php_setting_map: dict[int, int] | None = None) -> dict[str, Any]:
        source_php_configs = self._coerce_id_list(pick(source_customer, "allowed_phpconfigs", default=[]), [])
        mapped_php_configs = sorted({php_setting_map[config_id] for config_id in source_php_configs if php_setting_map and config_id in php_setting_map})
        payload = {
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
            "perlenabled": bool(as_int(pick(source_customer, "perlenabled", default=0))),
            "dnsenabled": bool(as_int(pick(source_customer, "dnsenabled", default=0))),
            "logviewenabled": bool(as_int(pick(source_customer, "logviewenabled", default=0))),
            "store_defaultindex": bool(as_int(pick(source_customer, "store_defaultindex", default=0))),
            "theme": str(pick(source_customer, "theme", default="")),
            "type_2fa": as_int(pick(source_customer, "type_2fa", default=0)),
            "data_2fa": str(pick(source_customer, "data_2fa", default="")),
        }
        if mapped_php_configs:
            payload["allowed_phpconfigs"] = mapped_php_configs
        return payload

    def _ensure_target_customer(
        self,
        source_customer: ResourceRow,
        target_customer: ResourceRow | None = None,
        php_setting_map: dict[int, int] | None = None,
    ) -> int:
        if target_customer:
            customer_id = as_int(pick(target_customer, "customerid", "id", default=0))
            if not customer_id:
                raise MigrationError("Could not resolve pre-selected target customer id")
            return customer_id

        existing = self._find_target_customer(source_customer)
        payload = self._customer_payload(source_customer, php_setting_map)
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
            "new_customer_password": str(pick(source_customer, "new_customer_password", default="")),
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
        try:
            self._source_sql_root_credentials = load_local_sql_root_credentials(froxlor_userdata_paths())
        except RuntimeError as exc:
            raise MigrationError(str(exc)) from exc
        return self._source_sql_root_credentials

    def _source_sql(self) -> dict[str, str]:
        if self._source_sql_credentials is not None:
            return self._source_sql_credentials
        try:
            self._source_sql_credentials = load_local_sql_credentials(froxlor_userdata_paths())
        except RuntimeError as exc:
            raise MigrationError(str(exc)) from exc
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
        if not found:
            run_remote = getattr(self.runner, "run_remote", None)
            if run_remote is not None:
                sudo = remote_sudo_prefix(self.config)
                for path in froxlor_userdata_paths():
                    try:
                        result = run_remote(f"{sudo}cat {shlex.quote(path)}", check=False, sensitive=True)
                    except TypeError:
                        try:
                            result = run_remote(f"{sudo}cat {shlex.quote(path)}", check=False)
                        except Exception:
                            continue
                    except Exception:
                        continue
                    if result.returncode != 0:
                        continue
                    creds = extract_sql_root_credentials(result.stdout or "")
                    if creds:
                        found.append(creds)
        if found:
            self._target_sql_root_credentials = max(found, key=_credential_score)
            self._debug(
                "resolved_target_sql_root_credentials",
                host=self._target_sql_root_credentials.get("host", ""),
                port=self._target_sql_root_credentials.get("port", ""),
                socket=self._target_sql_root_credentials.get("socket", ""),
                user=self._target_sql_root_credentials.get("user", ""),
            )
            return self._target_sql_root_credentials
        raise MigrationError("Could not parse target sql_root credentials from froxlor userdata files")

    @contextmanager
    def _target_mysql_connect_kwargs(self) -> Iterator[dict[str, Any]]:
        # Re-use a single SSH tunnel across multiple MySQL operations when possible.
        # When running unit tests, Migrator may be constructed without __init__;
        # guard against missing tunnel-cache attributes.
        if not hasattr(self, "_target_mysql_tunnel_connect_kwargs"):
            self._target_mysql_tunnel_connect_kwargs = None
            self._target_mysql_tunnel_stack = None
            self._target_mysql_tunnel_refcount = 0

        if self._target_mysql_tunnel_connect_kwargs is not None:
            self._target_mysql_tunnel_refcount += 1
            try:
                yield self._target_mysql_tunnel_connect_kwargs
            finally:
                self._target_mysql_tunnel_refcount -= 1
                if self._target_mysql_tunnel_refcount <= 0:
                    self._close_target_mysql_tunnel()
            return

        # Surface credential failures as a real error instead of silently
        # connecting with defaults (which would produce a misleading pymysql
        # error, or no fallback at all for the panel DB).
        try:
            creds = self._target_sql_root()
            kwargs = connect_kwargs_from_credentials(creds)
        except Exception as exc:
            raise MigrationError(f"Could not resolve target MySQL credentials: {exc}") from exc

        socket_path = str(kwargs.get("unix_socket", "")).strip()
        if not socket_path:
            # Only probe for a local socket when the credentials point at the
            # SSH host itself — an explicit remote DB host must go through the
            # TCP tunnel, otherwise we'd silently connect to the wrong server.
            cred_host = str(kwargs.get("host", "")).strip().lower()
            if cred_host in {"", "localhost", "127.0.0.1", "::1"}:
                discovered = self._discover_remote_mysql_socket()
                if discovered:
                    socket_path = discovered
                    kwargs["unix_socket"] = discovered
                    kwargs.pop("host", None)
                    kwargs.pop("port", None)
                    self._debug(
                        "discovered_target_mysql_socket",
                        remote_socket=discovered,
                        user=str(kwargs.get("user", "")),
                    )

        stack = ExitStack()
        self._target_mysql_tunnel_stack = stack

        try:
            if socket_path:
                self._debug(
                    "opening_target_mysql_socket_tunnel",
                    remote_socket=socket_path,
                    user=str(kwargs.get("user", "")),
                )
                local_socket = stack.enter_context(self._open_ssh_unix_socket_tunnel(socket_path))
                tunneled = dict(kwargs)
                tunneled.pop("host", None)
                tunneled.pop("port", None)
                tunneled["unix_socket"] = local_socket
                self._debug(
                    "target_mysql_socket_tunnel_ready",
                    local_socket=local_socket,
                    connect_kwargs=self._redact_connect_kwargs(tunneled),
                )
            else:
                remote_host = str(kwargs.get("host", "localhost"))
                remote_port = int(kwargs.get("port", 3306))
                self._debug(
                    "opening_target_mysql_tunnel",
                    remote_host=remote_host,
                    remote_port=remote_port,
                    has_unix_socket=bool(kwargs.get("unix_socket")),
                    user=str(kwargs.get("user", "")),
                )
                transport = self.runner.ssh_transport()
                _, local_port = stack.enter_context(open_ssh_tunnel(transport, remote_host, remote_port))
                tunneled = dict(kwargs)
                tunneled.pop("unix_socket", None)
                tunneled["host"] = "127.0.0.1"
                tunneled["port"] = local_port
                self._debug(
                    "target_mysql_tunnel_ready",
                    local_host="127.0.0.1",
                    local_port=local_port,
                    connect_kwargs=self._redact_connect_kwargs(tunneled),
                )

            self._target_mysql_tunnel_connect_kwargs = tunneled
            self._target_mysql_tunnel_refcount = 1
            yield tunneled
        finally:
            self._target_mysql_tunnel_refcount -= 1
            if self._target_mysql_tunnel_refcount <= 0:
                self._close_target_mysql_tunnel()

    def _close_target_mysql_tunnel(self) -> None:
        stack = getattr(self, "_target_mysql_tunnel_stack", None)
        if stack is not None:
            try:
                stack.close()
            finally:
                self._target_mysql_tunnel_stack = None
                self._target_mysql_tunnel_connect_kwargs = None
                self._target_mysql_tunnel_refcount = 0

    @contextmanager
    def _open_ssh_unix_socket_tunnel(self, remote_socket: str) -> Iterator[str]:
        try:
            with open_ssh_unix_socket_tunnel(self.config, remote_socket) as local_socket:
                yield local_socket
        except MigrationError:
            raise
        except Exception as exc:
            raise MigrationError(str(exc)) from exc

    def _run_target_mysql_via_remote_cli(self, sql: str, database: str) -> str:
        suffix = uuid4().hex[:8]
        remote_defaults = f"/tmp/froxlor-target-sql-{suffix}.cnf"
        remote_script = f"/tmp/froxlor-target-sql-{suffix}.sql"
        defaults_content = mysql_defaults_content(self._target_sql_root())
        try:
            self.runner.write_remote_file(remote_defaults, defaults_content, mode=0o600)
            self.runner.write_remote_file(remote_script, sql, mode=0o600)
            sudo = remote_sudo_prefix(self.config)
            cmd = (
                f"{sudo}{shlex.quote(self.config.commands.mysql)} "
                f"--defaults-extra-file={shlex.quote(remote_defaults)} "
                "--batch --raw --skip-column-names "
                f"{shlex.quote(database)} < {shlex.quote(remote_script)}"
            )
            self._debug("target_mysql_remote_cli_execute", database=database, command=cmd)
            result = self.runner.run_remote(cmd, sensitive=True)
            return result.stdout or ""
        finally:
            self.runner.run_remote(f"rm -f {shlex.quote(remote_defaults)} {shlex.quote(remote_script)}", check=False)

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

    def _with_target_mysql(
        self,
        action: str,
        database: str,
        tunnel_fn: Callable[[dict[str, Any]], T],
        cli_fallback_fn: Callable[[], T],
    ) -> T:
        """Run ``tunnel_fn`` over the SSH MySQL tunnel; on failure, fall back to
        the remote mysql CLI via ``cli_fallback_fn`` when the database allows it.
        """
        connect_summary: dict[str, Any] | None = None
        try:
            with self._target_mysql_connect_kwargs() as connect_kwargs:
                connect_summary = self._redact_connect_kwargs(connect_kwargs)
                return tunnel_fn(connect_kwargs)
        except Exception as exc:
            self._debug(
                f"target_sql_{action}_failed_over_tunnel",
                database=database,
                error=str(exc)[:400],
                connect_kwargs=connect_summary,
            )
            if not self._allow_remote_mysql_fallback(database):
                raise MigrationError(f"Target SQL {action} failed: {str(exc)[:300]} (remote mysql fallback disabled for panel DB {database!r})") from exc
            try:
                result = cli_fallback_fn()
                self._debug(f"target_sql_{action}_fallback_remote_cli_success", database=database)
                return result
            except Exception as fallback_exc:
                raise MigrationError(
                    f"Target SQL {action} failed: {str(exc)[:250]} | fallback via remote mysql failed: {str(fallback_exc)[:250]}"
                ) from fallback_exc

    def _run_target_mysql_query(self, sql: str, database: str) -> list[list[str]]:
        if self.runner.dry_run:
            return []

        def parse_cli_output() -> list[list[str]]:
            output = self._run_target_mysql_via_remote_cli(sql, database)
            return [["" if cell == "NULL" else cell for cell in line.split("\t")] for line in output.splitlines()]

        return self._with_target_mysql(
            "query",
            database,
            lambda connect_kwargs: mysql_query(connect_kwargs, database, sql),
            parse_cli_output,
        )

    def _run_target_panel_query(self, sql: str) -> list[list[str]]:
        return self._run_target_mysql_query(sql, self.config.mysql.target_panel_database)

    def _exec_target_mysql_sql(self, sql: str, database: str) -> None:
        def cli_fallback() -> None:
            self._run_target_mysql_via_remote_cli(sql, database)

        self._with_target_mysql(
            "execution",
            database,
            lambda connect_kwargs: mysql_execute(connect_kwargs, database, sql),
            cli_fallback,
        )

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
                "--single-transaction --quick --skip-lock-tables --routines --events "
                f"{shlex.quote(source_db)} > {shlex.quote(str(dump_path))}"
            )
            self.runner.run(dump_cmd)
            self.runner.write_remote_file(remote_defaults, target_defaults_content, mode=0o600)
            self.runner.upload_file(str(dump_path), remote_dump, mode=0o600)
            restore_cmd = (
                f"{remote_sudo_prefix(self.config)}{shlex.quote(self.config.commands.mysql)} "
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

    def _load_source_dkim_private_key(self, domain_name: str) -> str:
        # Domains.listing/get strip dkim_privkey — it only exists in the panel DB.
        rows = self._run_source_panel_query(
            f"SELECT dkim_privkey FROM panel_domains WHERE domain={self._sql_utf8_literal(domain_name)} LIMIT 1;"
        )
        if not rows or not rows[0]:
            return ""
        return str(rows[0][0]).strip()

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

    def _load_source_database_user_hashes(self, source_db_names: list[str]) -> dict[str, dict[str, tuple[str, str]]]:
        db_users = [name.strip() for name in source_db_names if name.strip()]
        if not db_users:
            return {}
        user_literals = ", ".join(self._sql_utf8_literal(name) for name in sorted(set(db_users)))
        rows = self._run_source_mysql_query(
            f"SELECT User, Host, plugin, authentication_string FROM mysql.user WHERE User IN ({user_literals});",
            "mysql",
        )
        # mysql.user is keyed on (Host, User) — keep auth per host so a user
        # with different credentials per host does not get flattened.
        out: dict[str, dict[str, tuple[str, str]]] = {}
        for row in rows:
            if len(row) < 4:
                continue
            out.setdefault(row[0].strip(), {})[row[1].strip()] = (row[2], row[3])
        return out

    def _load_source_customer_secrets(self, source_customer: dict[str, Any]) -> tuple[str, int, str] | None:
        """(password, type_2fa, data_2fa) from source panel_customers — the API
        strips password and data_2fa from customer rows."""
        login = self._customer_login(source_customer)
        customer_id = as_int(pick(source_customer, "customerid", "id", default=0))
        clauses = []
        if login:
            clauses.append(f"loginname={self._sql_utf8_literal(login)}")
        if customer_id > 0:
            clauses.append(f"customerid={customer_id}")
        if not clauses:
            return None
        rows = self._run_source_panel_query(
            f"SELECT password, type_2fa, data_2fa FROM panel_customers WHERE {' OR '.join(clauses)} LIMIT 1;"
        )
        if not rows or not rows[0]:
            return None
        row = rows[0]
        password = str(row[0]).strip() if len(row) > 0 and row[0] is not None else ""
        type_2fa = as_int(row[1], default=0) if len(row) > 1 else 0
        data_2fa = str(row[2]).strip() if len(row) > 2 and row[2] is not None else ""
        return (password, type_2fa, data_2fa)

    def _sync_customer_password_hash(self, source_customer: dict[str, Any], target_customer_id: int) -> None:
        secrets = self._load_source_customer_secrets(source_customer)
        if secrets is None:
            self._debug("no panel_customers row found for source customer; skipping password hash sync")
            return
        password_hash = secrets[0]
        if not password_hash:
            return
        sql = f"UPDATE panel_customers SET password={self._sql_utf8_literal(password_hash)} WHERE customerid={target_customer_id};"
        self._exec_target_panel_sql(sql)

    def _sync_customer_2fa_settings(self, source_customer: dict[str, Any], target_customer_id: int) -> None:
        # type_2fa survives listing, but data_2fa is stripped — load both from
        # the source panel DB so the secret is never silently zeroed.
        secrets = self._load_source_customer_secrets(source_customer)
        if secrets is not None:
            type_2fa = secrets[1]
            data_2fa = secrets[2]
            if type_2fa > 0 and not data_2fa:
                raise MigrationError("Source customer has 2FA enabled but its panel DB secret is empty; refusing to sync a secretless 2FA flag")
        else:
            # type_2fa>0 without its secret yields a TOTP-enabled account that
            # can never authenticate — refuse instead of corrupting it.
            if as_int(pick(source_customer, "type_2fa", default=0)) > 0:
                raise MigrationError(
                    "Source customer has 2FA enabled but the panel DB row could not be read; refusing to sync a secretless 2FA flag"
                )
            self._debug("no panel_customers row found for source customer; falling back to API fields for 2FA")
            type_2fa = as_int(pick(source_customer, "type_2fa", default=0))
            data_2fa = str(pick(source_customer, "data_2fa", default="")).strip()
        sql = f"UPDATE panel_customers SET type_2fa={type_2fa}, data_2fa={self._sql_utf8_literal(data_2fa)} WHERE customerid={target_customer_id};"
        self._exec_target_panel_sql(sql)

    def _load_source_ftp_password_hashes(self, ftp_accounts: list[dict[str, Any]]) -> dict[str, str]:
        usernames = {str(pick(row, "username", "ftpuser", default="")).strip().lower() for row in ftp_accounts}
        usernames.discard("")
        if not usernames:
            return {}
        user_list_sql = ", ".join(self._sql_utf8_literal(name) for name in sorted(usernames))
        rows = self._run_source_panel_query(f"SELECT username, password FROM ftp_users WHERE username IN ({user_list_sql});")
        out: dict[str, str] = {}
        for row in rows:
            if len(row) < 2:
                continue
            out[row[0].strip().lower()] = row[1]
        return out

    def _sync_ftp_password_hashes(self, target_customer_id: int, ftp_accounts: list[dict[str, Any]]) -> None:
        # Ftps.listing strips `password` — hashes only exist in the panel DB.
        source_hashes = self._load_source_ftp_password_hashes(ftp_accounts)
        statements: list[str] = []
        for row in ftp_accounts:
            username = str(pick(row, "username", "ftpuser", default="")).strip().lower()
            if not username:
                continue
            if username not in source_hashes:
                self._debug("skip_ftp_without_source_hash", username=username)
                continue
            password_hash = source_hashes[username].strip()
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
            emailaddr = self._mailbox_address(mailbox).lower()
            if not emailaddr:
                continue
            if emailaddr not in source_hashes:
                # This can happen for forward-only addresses that have no mail_users entry.
                self._debug("skip_mailbox_without_source_hash", email=emailaddr)
                continue
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
        target_login: str | None = None,
    ) -> None:
        target_login = target_login or customer_login
        target_rows = self.target.list_dir_protections(customerid=target_customer_id)
        target_by_key = {
            (
                relative_customer_path(str(pick(row, "path", default="")), target_login).lower(),
                str(pick(row, "username", default="")).strip().lower(),
            ): str(pick(row, "path", default="")).strip()
            for row in target_rows
        }
        statements: list[str] = []
        for row in dir_protections:
            path = relative_customer_path(str(pick(row, "path", default="")), customer_login)
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

    def _target_mysql_access_hosts(self) -> list[str]:
        rows = self._run_target_panel_query("SELECT value FROM panel_settings WHERE settinggroup='system' AND varname='mysql_access_host' LIMIT 1;")
        raw = str(rows[0][0] if rows and rows[0] else "").strip()
        hosts = [item.strip() for item in raw.split(",") if item.strip()]
        if not hosts:
            return ["localhost"]
        return hosts

    def _sync_database_login_hashes(self, source_to_target_db: dict[str, str]) -> None:
        if not source_to_target_db:
            return
        source_hashes = self._load_source_database_user_hashes(list(source_to_target_db.keys()))
        statements: list[str] = []
        for source_db, target_db in source_to_target_db.items():
            per_host = source_hashes.get(source_db)
            if not per_host:
                raise MigrationError(f"Source DB login hash missing in mysql.user for database user: {source_db}")
            hosts = list(dict.fromkeys([*self._target_mysql_access_hosts(), "%", "localhost"]))
            for host in hosts:
                # Prefer the same host's auth entry; fall back to localhost or
                # any recorded host since per-host auth is usually identical.
                plugin, auth_hash = per_host.get(host) or per_host.get("localhost") or next(iter(per_host.values()))
                if not auth_hash:
                    raise MigrationError(f"Source DB login hash empty for database user: {source_db}")
                if not re.fullmatch(r"[A-Za-z0-9_]+", plugin):
                    raise MigrationError(f"Unsupported SQL auth plugin name for database user {source_db}: {plugin!r}")
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
        target_login: str | None = None,
    ) -> None:
        self._sync_customer_password_hash(source_customer, target_customer_id)
        self._sync_customer_2fa_settings(source_customer, target_customer_id)
        self._sync_ftp_password_hashes(target_customer_id, ftp_accounts)
        self._sync_mail_password_hashes(target_customer_id, mailboxes)
        self._sync_dir_protection_password_hashes(target_customer_id, dir_protections, customer_login, target_login)
