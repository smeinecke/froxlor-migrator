from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from froxlor_migrator.config import load_config
from froxlor_migrator.migration.core import MigratorCore


class StubRunner:
    def __init__(self) -> None:
        self.remote_calls: list[str] = []

    def run_remote(self, command: str, check: bool = True):
        self.remote_calls.append(command)
        # mimic a successful remote check for known socket
        class Result:
            returncode = 0

        return Result()

    def run(self, command: str):
        return None

    def debug_event(self, message: str, **payload: object) -> None:
        pass


class StubClient:
    def __init__(self, customers: list[dict[str, object]]) -> None:
        self._customers = customers

    def list_customers(self) -> list[dict[str, object]]:
        return self._customers


class MigratorCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        content = """
        [source]
        api_url = "https://s"
        api_key = "k"
        api_secret = "s"

        [target]
        api_url = "https://t"
        api_key = "k"
        api_secret = "s"

        [ssh]
        host = "localhost"
        user = "root"

        [paths]
        source_web_root = "/var/www"
        target_web_root = "/var/www"
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.toml"
            path.write_text(content, encoding="utf-8")
            self.config = load_config(path)

        # Minimal stubs used for core unit tests
        self.runner = StubRunner()
        self.source = StubClient([])
        self.target = StubClient([])
        self.core = MigratorCore(self.config, self.source, self.target, self.runner)

    def test_redact_connect_kwargs_hides_password(self) -> None:
        self.assertEqual({"host": "h", "password": "***"}, self.core._redact_connect_kwargs({"host": "h", "password": "p"}))

    def test_allow_remote_mysql_fallback(self) -> None:
        self.assertFalse(self.core._allow_remote_mysql_fallback("froxlor"))
        self.assertTrue(self.core._allow_remote_mysql_fallback("other"))

    def test_mysql_socket_candidates_contains_expected_entries(self) -> None:
        sockets = self.core._mysql_socket_candidates()
        self.assertIn("/run/mysqld/mysqld.sock", sockets)
        self.assertIn("/tmp/mysql.sock", sockets)

    def test_relative_customer_path_strips_login_prefixes(self) -> None:
        self.assertEqual("sub/path", self.core._relative_customer_path("/user/sub/path", "user"))
        self.assertEqual("sub/path", self.core._relative_customer_path("user/sub/path", "user"))
        self.assertEqual("", self.core._relative_customer_path("/", "user"))

    def test_coerce_id_list_handles_various_inputs(self) -> None:
        self.assertEqual([1, 2], self.core._coerce_id_list(["1", "2"], [99]))
        self.assertEqual([3], self.core._coerce_id_list("[3]", [99]))
        self.assertEqual([4], self.core._coerce_id_list("4", [99]))
        self.assertEqual([99], self.core._coerce_id_list("", [99]))
        self.assertEqual([99], self.core._coerce_id_list(None, [99]))

    def test_find_target_customer_uses_login_and_email(self) -> None:
        source = {"loginname": "alice", "email": "alice@example.com"}
        self.target = StubClient([
            {"loginname": "bob", "email": "bob@example.com"},
            {"loginname": "alice", "email": "alice@example.com"},
        ])
        self.core.target = self.target
        found = self.core._find_target_customer(source)
        self.assertIsNotNone(found)
        self.assertEqual("alice", found["loginname"])


if __name__ == "__main__":
    unittest.main()
