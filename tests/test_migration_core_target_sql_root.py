from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from froxlor_migrator.migration.core import MigratorCore, MigrationError


class DummyRunner:
    def __init__(self):
        self.dry_run = False
        self.debug_events: list[tuple[str, dict[str, object]]] = []

    def debug_event(self, message: str, **payload: object) -> None:
        self.debug_events.append((message, payload))

    def read_remote_file(self, path: str) -> str:
        raise FileNotFoundError(path)


class TargetSqlRootTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SimpleNamespace()
        self.source = SimpleNamespace()
        self.target = SimpleNamespace()
        self.runner = DummyRunner()
        self.core = MigratorCore(self.config, self.source, self.target, self.runner)

    def test_target_sql_root_raises_in_dry_run(self) -> None:
        self.runner.dry_run = True
        with self.assertRaises(MigrationError):
            self.core._target_sql_root()

    def test_target_sql_root_picks_best_credentials_from_multiple_files(self) -> None:
        self.runner.dry_run = False

        # Provide two candidate files where the second has a higher score (password + socket)
        def read_remote_file(path: str) -> str:
            if path.endswith("1"):
                return """
$sql_root[0]['user'] = 'root';
$sql_root[0]['password'] = 'x';
$sql_root[0]['host'] = 'h1';
"""
            return """
$sql_root[0]['user'] = 'root';
$sql_root[0]['password'] = 'x';
$sql_root[0]['host'] = 'h2';
$sql_root[0]['socket'] = '/tmp/sock';
"""

        self.runner.read_remote_file = read_remote_file

        with patch("froxlor_migrator.migration.core.froxlor_userdata_paths", return_value=["/a/1", "/b/2"]):
            creds = self.core._target_sql_root()

        self.assertEqual(creds["host"], "h2")
        self.assertEqual(creds["socket"], "/tmp/sock")
        self.assertEqual(creds["user"], "root")
        self.assertEqual(self.runner.debug_events[0][0], "resolved_target_sql_root_credentials")


if __name__ == "__main__":
    unittest.main()
