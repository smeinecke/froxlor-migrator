from __future__ import annotations

import contextlib
import json
import os
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from froxlor_migrator.api import FroxlorApiError
from froxlor_migrator.migration.core import MigratorCore
from froxlor_migrator.migration.core import MigrationError


class DummyRunner:
    def __init__(self):
        self.debug_events: list[tuple[str, dict[str, object]]] = []
        self._run_remote_calls: list[str] = []
        self.dry_run = False

    def debug_event(self, message: str, **payload: object) -> None:
        self.debug_events.append((message, payload))

    def run_remote(self, command: str, check: bool = True):
        self._run_remote_calls.append(command)
        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        return Result()

    def ssh_transport(self):
        return object()

    def run(self, command: str):
        return None

    def read_remote_file(self, path: str) -> str:
        return ""

    def write_remote_file(self, path: str, content: str, mode: int = 0o600) -> None:
        pass

    def upload_file(self, source: str, dest: str, mode: int = 0o600) -> None:
        pass


class MigratorCoreMoreTests(unittest.TestCase):
    def setUp(self) -> None:
        # Minimal config necessary for tested functionality
        self.config = SimpleNamespace(
            mysql=SimpleNamespace(target_panel_database="froxlor"),
            commands=SimpleNamespace(ssh="ssh", mysql="mysql", mysqldump="mysqldump"),
            ssh=SimpleNamespace(strict_host_key_checking=False, port=22, user="root", host="localhost"),
        )
        self.source = MagicMock()
        self.target = MagicMock()
        self.runner = DummyRunner()
        self.core = MigratorCore(self.config, self.source, self.target, self.runner)

    def test_redact_connect_kwargs_does_not_modify_original(self) -> None:
        original = {"host": "x", "password": "secret"}
        redacted = self.core._redact_connect_kwargs(original)
        self.assertEqual({"host": "x", "password": "***"}, redacted)
        # Original not mutated
        self.assertEqual("secret", original["password"])

    def test_sql_literal_helpers_escape_values(self) -> None:
        self.assertEqual("''", self.core._sql_utf8_literal(""))
        self.assertIn("CONVERT(0x", self.core._sql_utf8_literal("ä"))
        self.assertEqual("'foo\\'bar'", self.core._sql_string_literal("foo'bar"))

    def test_allow_remote_mysql_fallback_respects_panel_db(self) -> None:
        self.assertFalse(self.core._allow_remote_mysql_fallback("froxlor"))
        self.assertTrue(self.core._allow_remote_mysql_fallback("other"))

    def test_coerce_id_list_with_invalid_values_uses_fallback(self) -> None:
        self.assertEqual([9], self.core._coerce_id_list("invalid", [9]))

    def test_target_mysql_connect_kwargs_uses_tunnel_when_no_socket(self) -> None:
        # Setup _target_sql_root to return credentials without socket
        self.core._target_sql_root_credentials = {"host": "remote", "port": "3306", "user": "u"}

        # Ensure no remote socket is discovered to force tunnel branch
        self.runner.run_remote = lambda command, check=True: type("R", (), {"returncode": 1})()

        with patch("froxlor_migrator.migration.core.open_ssh_tunnel", return_value=contextlib.nullcontext((None, 1234))):
            with self.core._target_mysql_connect_kwargs() as kwargs:
                self.assertEqual("127.0.0.1", kwargs["host"])
                self.assertEqual(1234, kwargs["port"])

    def test_target_mysql_connect_kwargs_uses_unix_socket_when_discovered(self) -> None:
        self.core._target_sql_root_credentials = {"unix_socket": "/remote.sock", "user": "u"}
        # Patch discovery and tunnel helper to avoid subprocess
        self.core._discover_remote_mysql_socket = lambda: "/remote.sock"

        with patch.object(self.core, "_open_ssh_unix_socket_tunnel", return_value=contextlib.nullcontext("/tmp/local.sock")):
            with self.core._target_mysql_connect_kwargs() as kwargs:
                self.assertEqual("/tmp/local.sock", kwargs["unix_socket"])

    def test_open_ssh_unix_socket_tunnel_fails_when_ssh_cmd_empty(self) -> None:
        self.config.commands.ssh = ""
        with self.assertRaises(MigrationError):
            with self.core._open_ssh_unix_socket_tunnel("/remote.sock"):
                pass

    def test_run_source_panel_query_raises_on_error(self) -> None:
        self.runner.dry_run = False
        self.core._source_sql_root_credentials = {"host": "x", "user": "u", "password": "p"}
        with patch("froxlor_migrator.migration.core.mysql_query", side_effect=Exception("boom")):
            with self.assertRaises(MigrationError):
                self.core._run_source_panel_query("SELECT 1")

    def test_run_target_mysql_query_fallbacks_on_error(self) -> None:
        self.runner.dry_run = False
        self.core._target_sql_root_credentials = {"host": "x", "user": "u"}

        def raise_exc(*args, **kwargs):
            raise Exception("fail")

        with patch.object(self.core, "_target_mysql_connect_kwargs", side_effect=Exception("fail")):
            with patch.object(self.core, "_run_target_mysql_via_remote_cli", return_value="a\tb\n"):
                rows = self.core._run_target_mysql_query("SELECT 1", "other")
                self.assertEqual([["a", "b"]], rows)

    def test_exec_target_mysql_sql_fallbacks_on_error(self) -> None:
        self.runner.dry_run = False
        self.core._target_sql_root_credentials = {"host": "x", "user": "u"}

        with patch.object(self.core, "_target_mysql_connect_kwargs", side_effect=Exception("fail")):
            with patch.object(self.core, "_run_target_mysql_via_remote_cli", return_value=""):
                # Should not raise
                self.core._exec_target_mysql_sql("SELECT 1", "other")

    def test_run_target_mysql_via_remote_cli_writes_and_cleans_up(self) -> None:
        self.runner.dry_run = False
        self.core._target_sql_root_credentials = {"host": "x", "user": "u"}

        self.runner.write_remote_file = lambda path, content, mode=0o600: setattr(self, "wrote", path)
        self.runner.upload_file = lambda src, dest, mode=0o600: setattr(self, "uploaded", (src, dest))
        calls: list[str] = []

        def run_remote(cmd: str, check: bool = True):
            calls.append(cmd)
            return type("R", (), {"stdout": "a\tb\n"})()

        self.runner.run_remote = run_remote

        with patch("froxlor_migrator.migration.core.uuid4", return_value=type("X", (), {"hex": "deadbeef"})):
            rows = self.core._run_target_mysql_via_remote_cli("SELECT 1", "db")

        self.assertEqual("a\tb\n", rows)
        self.assertTrue(any("rm -f" in c for c in calls))

    def test_transfer_database_with_defaults_executes_commands_and_cleans_up(self) -> None:
        calls: list[str] = []
        self.runner.run = lambda cmd: calls.append(cmd)
        self.runner.write_remote_file = lambda path, content, mode=0o600: calls.append(f"write:{path}")
        self.runner.upload_file = lambda src, dest, mode=0o600: calls.append(f"upload:{src}->{dest}")
        self.runner.run_remote = lambda cmd, check=True: calls.append(cmd)

        self.core._source_sql_root_credentials = {"host": "x", "user": "u"}
        self.core._target_sql_root_credentials = {"host": "x", "user": "u"}

        self.core._transfer_database_with_defaults("src", "dst")

        self.assertTrue(any("mysqldump" in c for c in calls))
        self.assertTrue(any("mysql" in c for c in calls))
        self.assertTrue(any(c.startswith("write:") for c in calls))

    def test_sync_ftp_password_hashes_errors_and_builds_sql(self) -> None:
        self.core._exec_target_panel_sql = lambda sql: setattr(self, "executed_sql", sql)
        with self.assertRaises(Exception):
            self.core._sync_ftp_password_hashes(1, [{"username": "u", "password": ""}])

        self.core._sync_ftp_password_hashes(2, [{"username": "u", "password": "hash"}])
        self.assertIn("UPDATE ftp_users", getattr(self, "executed_sql", ""))

    def test_sync_mail_password_hashes_fails_on_missing_and_empty(self) -> None:
        self.core._run_source_panel_query = lambda sql: [["u@example.com", "", ""]]
        self.core._exec_target_panel_sql = lambda sql: setattr(self, "executed_mail_sql", sql)

        with self.assertRaises(Exception):
            self.core._sync_mail_password_hashes(1, [{"email": "u2@example.com"}])

        with self.assertRaises(Exception):
            self.core._sync_mail_password_hashes(1, [{"email": "u@example.com"}])

        self.core._run_source_panel_query = lambda sql: [["u@example.com", "p", "e"]]
        self.core._sync_mail_password_hashes(1, [{"email": "u@example.com"}])
        self.assertIn("UPDATE mail_users", getattr(self, "executed_mail_sql", ""))

    def test_sync_database_login_hashes_validates_plugins_and_hosts(self) -> None:
        self.core._run_source_mysql_query = lambda sql, db: [["user", "mysql_native_password", "hash"]]
        self.core._exec_target_mysql_sql = lambda sql, db: setattr(self, "executed_mysql_sql", sql)
        self.core._target_mysql_user_exists = lambda username, host: True

        self.core._sync_database_login_hashes({"user": "user"})
        self.assertIn("ALTER USER", getattr(self, "executed_mysql_sql", ""))

        # Support alternate auth plugin syntax
        self.core._run_source_mysql_query = lambda sql, db: [["user", "caching_sha2_password", "hash"]]
        self.core._sync_database_login_hashes({"user": "user"})
        self.assertIn("IDENTIFIED VIA caching_sha2_password", getattr(self, "executed_mysql_sql", ""))

        # Unsupported plugin should raise
        self.core._run_source_mysql_query = lambda sql, db: [["user", "bad-plugin", "hash"]]
        with self.assertRaises(Exception):
            self.core._sync_database_login_hashes({"user": "user"})

    def test_sync_dir_protection_password_hashes_generates_update_sql(self) -> None:
        # Setup existing target entry so it updates rather than inserting
        self.core.target = MagicMock()
        self.core.target.list_dir_protections.return_value = [{"id": 1, "path": "/foo", "username": "u"}]
        self.core._exec_target_panel_sql = lambda sql: setattr(self, "executed_dir_sql", sql)

        self.core._sync_dir_protection_password_hashes(1, [{"path": "/foo", "username": "u", "password": "hash"}], "customer")
        self.assertIn("UPDATE panel_htpasswds", getattr(self, "executed_dir_sql", ""))

    def test_discover_remote_mysql_socket_returns_first_candidate(self) -> None:
        calls: list[str] = []

        def run_remote(command: str, check: bool = True):
            calls.append(command)
            return type("R", (), {"returncode": 0})()

        self.runner.run_remote = run_remote
        socket = self.core._discover_remote_mysql_socket()
        self.assertEqual(self.core._mysql_socket_candidates()[0], socket)
        self.assertTrue(calls)

    def test_discover_remote_mysql_socket_returns_empty_when_none_found(self) -> None:
        def run_remote(command: str, check: bool = True):
            return type("R", (), {"returncode": 1})()

        self.runner.run_remote = run_remote
        self.assertEqual("", self.core._discover_remote_mysql_socket())

    def test_find_target_customer_matches_by_login_and_email(self) -> None:
        self.core.target.list_customers.return_value = [
            {"loginname": "bob", "email": "bob@example.com", "customerid": 10},
            {"loginname": "alice", "email": "alice@example.com", "customerid": 11},
        ]
        source = {"login": "alice", "email": "alice@example.com"}
        found = self.core._find_target_customer(source)
        self.assertIsNotNone(found)
        self.assertEqual(11, found["customerid"])

    def test_ensure_target_customer_raises_when_preselected_has_no_id(self) -> None:
        with self.assertRaises(Exception):
            self.core._ensure_target_customer({"login": "x"}, {"id": 0})

    def test_ensure_target_customer_updates_existing_customer(self) -> None:
        existing = {"loginname": "bob", "customerid": 42}
        self.core.target.list_customers.return_value = [existing]
        self.core._customer_payload = lambda src: {"email": "x"}

        called: dict[str, object] = {}

        def fake_call(method: str, payload: dict[str, object]):
            called["method"] = method
            called["payload"] = payload
            return {"customerid": 42}

        self.core.target.call = fake_call
        cid = self.core._ensure_target_customer({"login": "bob", "email": "x"})
        self.assertEqual(42, cid)
        self.assertEqual("Customers.update", called["method"])

    def test_ensure_target_customer_creates_when_missing_and_handles_api_error(self) -> None:
        self.core._customer_payload = lambda src: {"email": "x"}

        def failing_call(method: str, payload: dict[str, object]):
            raise FroxlorApiError("boom")

        self.core.target.call = failing_call
        # First no customer exists, but after API error it appears
        self.core.target.list_customers.side_effect = [[], [{"loginname": "bob", "customerid": 99}]]
        cid = self.core._ensure_target_customer({"login": "bob", "email": "x"})
        self.assertEqual(99, cid)

    def test_get_target_domain_returns_matching_domain(self) -> None:
        self.core.target.list_domains.return_value = [{"domain": "Example.com"}, {"domainname": "foo"}]
        self.assertEqual("Example.com", self.core._get_target_domain("example.com")["domain"])

    def test_source_and_root_sql_credentials_are_cached(self) -> None:
        with patch("froxlor_migrator.migration.core.load_local_sql_root_credentials", return_value={"user": "root"}) as root_loader:
            creds1 = self.core._source_sql_root()
            creds2 = self.core._source_sql_root()
            self.assertIs(creds1, creds2)
            self.assertEqual(1, root_loader.call_count)

        with patch("froxlor_migrator.migration.core.load_local_sql_credentials", return_value={"user": "normal"}) as loader:
            creds1 = self.core._source_sql()
            creds2 = self.core._source_sql()
            self.assertIs(creds1, creds2)
            self.assertEqual(1, loader.call_count)

    def test_target_sql_root_raises_if_no_credentials_found(self) -> None:
        self.runner.dry_run = False
        self.runner.read_remote_file = lambda path: "invalid"
        with patch("froxlor_migrator.migration.core.froxlor_userdata_paths", return_value=["/a"]):
            with self.assertRaises(Exception):
                self.core._target_sql_root()

    def test_run_source_mysql_query_returns_rows_and_wraps_errors(self) -> None:
        self.runner.dry_run = False
        self.core._source_sql_root_credentials = {"host": "x", "user": "u"}
        with patch("froxlor_migrator.migration.core.mysql_query", return_value=[["a"]]):
            rows = self.core._run_source_mysql_query("SELECT 1", "db")
            self.assertEqual([["a"]], rows)

        with patch("froxlor_migrator.migration.core.mysql_query", side_effect=Exception("boom")):
            with self.assertRaises(MigrationError):
                self.core._run_source_mysql_query("SELECT 1", "db")

    def test_run_target_mysql_query_direct_connect(self) -> None:
        self.runner.dry_run = False
        self.core._target_sql_root_credentials = {"host": "x", "user": "u"}
        with patch.object(self.core, "_target_mysql_connect_kwargs", return_value=contextlib.nullcontext({"host": "127.0.0.1", "port": 3306})):
            with patch("froxlor_migrator.migration.core.mysql_query", return_value=[["x"]]):
                rows = self.core._run_target_mysql_query("SELECT 1", "other")
                self.assertEqual([["x"]], rows)

        # Panel database should not fallback
        with patch.object(self.core, "_target_mysql_connect_kwargs", side_effect=Exception("fail")):
            with self.assertRaises(MigrationError):
                self.core._run_target_mysql_query("SELECT 1", self.config.mysql.target_panel_database)

    def test_exec_target_mysql_sql_direct_connect(self) -> None:
        self.runner.dry_run = False
        self.core._target_sql_root_credentials = {"host": "x", "user": "u"}
        executed: list[str] = []
        with patch.object(self.core, "_target_mysql_connect_kwargs", return_value=contextlib.nullcontext({"host": "127.0.0.1", "port": 3306})):
            with patch("froxlor_migrator.migration.core.mysql_execute", lambda connect_kwargs, db, sql: executed.append(sql)):
                self.core._exec_target_mysql_sql("SELECT 1", "other")
        self.assertEqual(["SELECT 1"], executed)

    def test_sync_dkim_keys_db_builds_update_statement(self) -> None:
        executed: list[str] = []
        self.core._exec_target_panel_sql = lambda sql: executed.append(sql)
        self.core._sync_dkim_keys_db("example.com", "pub", "priv")
        self.assertIn("dkim_pubkey", executed[0])

    def test_source_mysql_prefix_setting_returns_empty_and_value(self) -> None:
        self.core._run_source_panel_query = lambda sql: []
        self.assertEqual("", self.core._source_mysql_prefix_setting())
        self.core._run_source_panel_query = lambda sql: [["  pfx  "]]
        self.assertEqual("pfx", self.core._source_mysql_prefix_setting())

    def test_sync_target_mysql_prefix_setting_noop_when_empty(self) -> None:
        called: list[str] = []
        self.core._source_mysql_prefix_setting = lambda: ""
        self.core._exec_target_panel_sql = lambda sql: called.append(sql)
        self.core._sync_target_mysql_prefix_setting()
        self.assertEqual([], called)

    def test_sync_target_mysql_prefix_setting_executes_update_when_present(self) -> None:
        called: list[str] = []
        self.core._source_mysql_prefix_setting = lambda: "pfx"
        self.core._exec_target_panel_sql = lambda sql: called.append(sql)
        self.core._sync_target_mysql_prefix_setting()
        self.assertTrue(called and "UPDATE panel_settings" in called[0])

    def test_load_source_mail_password_hashes_parses_rows(self) -> None:
        self.core._run_source_panel_query = lambda sql: [["a@example.com", "h1", "e1"], ["b@example.com", "h2", "e2"]]
        out = self.core._load_source_mail_password_hashes([{"email": "a@example.com"}, {"email":"b@example.com"}])
        self.assertEqual({"a@example.com": ("h1", "e1"), "b@example.com": ("h2", "e2")}, out)

    def test_load_source_database_user_hashes_parses_rows(self) -> None:
        self.core._run_source_mysql_query = lambda sql, db: [["u", "mysql_native_password", "h"], ["v", "", ""]]
        out = self.core._load_source_database_user_hashes(["u", "v"])
        self.assertEqual({"u": ("mysql_native_password", "h"), "v": ("", "")}, out)

    def test_sync_customer_password_hash_updates_panel_customers(self) -> None:
        executed: list[str] = []
        self.core._exec_target_panel_sql = lambda sql: executed.append(sql)
        self.core._sync_customer_password_hash({"password": "hash"}, 5)
        self.assertTrue(executed[0].startswith("UPDATE panel_customers"))

    def test_preflight_runs_commands_based_on_selection(self) -> None:
        called: list[str] = []
        self.source.test_connection = lambda: called.append("src")
        self.target.test_connection = lambda: called.append("tgt")
        self.runner.preflight_commands = lambda **kwargs: ["cmd1", "cmd2"]
        self.runner.run = lambda cmd: called.append(cmd)

        from froxlor_migrator.migration.types import Selection

        selection = Selection(
            customer={},
            target_customer=None,
            domains=[],
            subdomains=[],
            databases=[],
            mailboxes=[],
            email_forwarders=[],
            email_senders=[],
            ftp_accounts=[],
            ssh_keys=[],
            data_dumps=[],
            dir_protections=[],
            dir_options=[],
            domain_zones=[],
            include_files=True,
            include_databases=True,
            include_mail=True,
            include_subdomains=False,
            validate_database_names=False,
            php_setting_map={},
            ip_mapping={},
        )

        self.core.preflight(selection)
        self.assertIn("src", called)
        self.assertIn("tgt", called)
        self.assertIn("cmd1", called)

    def test_preflight_does_not_require_ssh_when_features_disabled(self) -> None:
        called: list[str] = []
        self.source.test_connection = lambda: called.append("src")
        self.target.test_connection = lambda: called.append("tgt")
        self.runner.preflight_commands = lambda **kwargs: [f"cmd_{kwargs}" ]
        self.runner.run = lambda cmd: called.append(cmd)

        from froxlor_migrator.migration.types import Selection

        selection = Selection(
            customer={},
            target_customer=None,
            domains=[],
            subdomains=[],
            databases=[],
            mailboxes=[],
            email_forwarders=[],
            email_senders=[],
            ftp_accounts=[],
            ssh_keys=[],
            data_dumps=[],
            dir_protections=[],
            dir_options=[],
            domain_zones=[],
            include_files=False,
            include_databases=False,
            include_mail=False,
            include_subdomains=False,
            validate_database_names=False,
            php_setting_map={},
            ip_mapping={},
        )

        self.core.preflight(selection)
        self.assertTrue(any("include_ssh" in c for c in called))

    def test_coerce_id_list_parses_json_and_numeric_strings(self) -> None:
        self.assertEqual([1, 2], self.core._coerce_id_list("[1, 2]", [9]))
        self.assertEqual([5], self.core._coerce_id_list("5", [9]))

    def test_open_ssh_unix_socket_tunnel_errors_when_socket_never_ready(self) -> None:
        self.config.commands.ssh = "ssh"

        class DummyProcess:
            def __init__(self):
                self._polled = False

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                pass

            @property
            def stderr(self):
                return open(os.devnull, "r")

        def fake_popen(cmd, stdout, stderr, text):
            return DummyProcess()

        with patch("subprocess.Popen", fake_popen):
            with patch("os.path.exists", return_value=False):
                with self.assertRaises(MigrationError):
                    with self.core._open_ssh_unix_socket_tunnel("/remote.sock"):
                        pass

    def test_ensure_target_customer_creates_new_customer_successfully(self) -> None:
        self.core.target.list_customers.return_value = []
        self.core._customer_payload = lambda src: {"email": "x"}

        def add_call(method: str, payload: dict[str, object]):
            return {"customerid": 123}

        self.core.target.call = add_call
        cid = self.core._ensure_target_customer({"login": "bob", "email": "x"})
        self.assertEqual(123, cid)

    def test_ensure_target_customer_creates_new_customer_sets_createstdsubdomain(self) -> None:
        self.core.target.list_customers.return_value = []

        def add_call(method: str, payload: dict[str, object]):
            # createstdsubdomain must be passed when creating a new customer
            self.assertIn("createstdsubdomain", payload)
            self.assertTrue(payload["createstdsubdomain"])
            return {"customerid": 123}

        self.core.target.call = add_call
        cid = self.core._ensure_target_customer({"login": "bob", "email": "x"})
        self.assertEqual(123, cid)

    def test_ensure_target_customer_creates_new_customer_migrates_all_known_fields(self) -> None:
        self.core.target.list_customers.return_value = []
        source = {
            "login": "bob",
            "email": "bob@example.test",
            "name": "Bob",
            "firstname": "B",
            "company": "Acme",
            "street": "123 Main St",
            "zipcode": "12345",
            "city": "Testville",
            "phone": "0123456789",
            "fax": "9876543210",
            "customernumber": "CUST-123",
            "def_language": "de",
            "gui_access": "1",
            "api_allowed": "0",
            "shell_allowed": "1",
            "gender": "1",
            "custom_notes": "note",
            "custom_notes_show": "1",
            "sendpassword": "0",
            "diskspace": "1000",
            "diskspace_ul": "1",
            "traffic": "2000",
            "traffic_ul": "0",
            "subdomains": "5",
            "subdomains_ul": "1",
            "emails": "10",
            "emails_ul": "0",
            "email_accounts": "2",
            "email_accounts_ul": "1",
            "email_forwarders": "3",
            "email_forwarders_ul": "0",
            "email_quota": "400",
            "email_quota_ul": "1",
            "imap": "1",
            "pop3": "0",
            "ftps": "2",
            "ftps_ul": "1",
            "mysqls": "3",
            "mysqls_ul": "0",
            "createstdsubdomain": "1",
            "phpenabled": "1",
            "allowed_phpconfigs": "[2, 3]",
            "perlenabled": "1",
            "dnsenabled": "0",
            "logviewenabled": "1",
            "store_defaultindex": "0",
            "hosting_plan_id": "7",
            "new_customer_password": "pw123",
            "allowed_mysqlserver": "9",
        }

        def add_call(method: str, payload: dict[str, object]):
            # All expected keys must be forwarded to the API on creation
            expected = {
                "email": "bob@example.test",
                "name": "Bob",
                "firstname": "B",
                "company": "Acme",
                "street": "123 Main St",
                "zipcode": "12345",
                "city": "Testville",
                "phone": "0123456789",
                "fax": "9876543210",
                "customernumber": "CUST-123",
                "def_language": "de",
                "gui_access": True,
                "api_allowed": False,
                "shell_allowed": True,
                "gender": 1,
                "custom_notes": "note",
                "custom_notes_show": True,
                "sendpassword": False,
                "diskspace": 1000,
                "diskspace_ul": True,
                "traffic": 2000,
                "traffic_ul": False,
                "subdomains": 5,
                "subdomains_ul": True,
                "emails": 10,
                "emails_ul": False,
                "email_accounts": 2,
                "email_accounts_ul": True,
                "email_forwarders": 3,
                "email_forwarders_ul": False,
                "email_quota": 400,
                "email_quota_ul": True,
                "email_imap": True,
                "email_pop3": False,
                "ftps": 2,
                "ftps_ul": True,
                "mysqls": 3,
                "mysqls_ul": False,
                "createstdsubdomain": True,
                "phpenabled": True,
                "allowed_phpconfigs": [2, 3],
                "perlenabled": True,
                "dnsenabled": False,
                "logviewenabled": True,
                "store_defaultindex": False,
                "hosting_plan_id": 7,
                "new_customer_password": "pw123",
                "allowed_mysqlserver": [9],
                "new_loginname": "bob",
            }
            for k, v in expected.items():
                self.assertIn(k, payload)
                self.assertEqual(v, payload[k])

            return {"customerid": 123}

        self.core.target.call = add_call
        cid = self.core._ensure_target_customer(source)
        self.assertEqual(123, cid)


if __name__ == "__main__":
    unittest.main()
