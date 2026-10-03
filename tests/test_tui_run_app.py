from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from froxlor_migrator import tui as tui_module


class DummyRunner:
    def __init__(self, *args, **kwargs):
        self.manifest_path = Path(tempfile.gettempdir()) / "manifest.json"

    def progress_event(self, step: int, total: int, status: str) -> None:
        pass


class DummyMigrator:
    def __init__(self, *args, **kwargs):
        self._callback = None

    def set_progress_callback(self, callback):
        self._callback = callback

    def execute(self, selection):
        # Simulate progress updates
        if self._callback:
            self._callback(1, 1, "done")
        return SimpleNamespace(target_customer_id=1, source_to_target_db={})


class DummyClient:
    def __init__(self, *args, **kwargs):
        pass

    def list_customers(self):
        return [
            {"customerid": 1, "loginname": "alice", "email": "alice@example.com", "name": "Alice"},
            {"customerid": 2, "loginname": "bob", "email": "bob@example.com", "name": "Bob"},
        ]

    def list_domains(self, **kwargs):
        return [{"domain": "example.com", "documentroot": "/var/www/example.com", "phpsettingid": 1}]

    def list_subdomains(self, **kwargs):
        return []

    def list_mysqls(self, **kwargs):
        return []

    def list_emails(self, **kwargs):
        return []

    def list_ftps(self, **kwargs):
        return []

    def list_email_forwarders(self, **kwargs):
        return []

    def list_email_senders(self, **kwargs):
        return []

    def list_dir_protections(self, **kwargs):
        return []

    def list_dir_options(self, **kwargs):
        return []

    def list_ssh_keys(self, **kwargs):
        return []

    def list_data_dumps(self, **kwargs):
        return []

    def list_php_settings(self):
        return [{"id": 1, "description": "php", "binary": "php"}]

    def list_domain_zones(self, **kwargs):
        return []


def make_config() -> SimpleNamespace:
    return SimpleNamespace(
        source=SimpleNamespace(api_url="", api_key="", api_secret="", timeout_seconds=30),
        target=SimpleNamespace(api_url="", api_key="", api_secret="", timeout_seconds=30),
        paths=SimpleNamespace(source_web_root="/var/www", source_transfer_root="/var/www", target_web_root="/var/www"),
        behavior=SimpleNamespace(dry_run_default=True),
        commands=SimpleNamespace(ssh="ssh"),
    )


class RunAppTests(unittest.TestCase):
    def test_run_app_non_interactive_completes(self) -> None:
        with (
            patch.object(tui_module, "load_config", return_value=make_config()),
            patch("froxlor_migrator.plan.FroxlorClient", DummyClient),
            patch.object(tui_module, "TransferRunner", DummyRunner),
            patch.object(tui_module, "Migrator", DummyMigrator),
            patch("froxlor_migrator.plan.Selection", lambda **kwargs: SimpleNamespace(**kwargs)),
        ):
            sys_argv = sys.argv
            try:
                sys.argv = [
                    "run",
                    "--config",
                    "config.toml",
                    "--non-interactive",
                    "--yes",
                    "--source-customer",
                    "alice",
                    "--domain-only",
                ]
                # Should not raise
                tui_module.run_app()
            finally:
                sys.argv = sys_argv

    def test_run_app_migration_failure_exits_nonzero(self) -> None:
        class FailingMigrator(DummyMigrator):
            def execute(self, selection):
                raise tui_module.MigrationError("boom")

        with (
            patch.object(tui_module, "load_config", return_value=make_config()),
            patch("froxlor_migrator.plan.FroxlorClient", DummyClient),
            patch.object(tui_module, "TransferRunner", DummyRunner),
            patch.object(tui_module, "Migrator", FailingMigrator),
            patch("froxlor_migrator.plan.Selection", lambda **kwargs: SimpleNamespace(**kwargs)),
        ):
            sys_argv = sys.argv
            try:
                sys.argv = [
                    "run",
                    "--config",
                    "config.toml",
                    "--non-interactive",
                    "--yes",
                    "--source-customer",
                    "alice",
                    "--domain-only",
                ]
                with self.assertRaises(SystemExit) as ctx:
                    tui_module.run_app()
                self.assertEqual(1, ctx.exception.code)
            finally:
                sys.argv = sys_argv

    def test_run_app_batch_migrates_each_customer(self) -> None:
        executed: list[str] = []

        class RecordingMigrator(DummyMigrator):
            def execute(self, selection):
                executed.append(selection.customer["loginname"])
                return SimpleNamespace(target_customer_id=100 + len(executed), source_to_target_db={})

        with (
            patch.object(tui_module, "load_config", return_value=make_config()),
            patch("froxlor_migrator.plan.FroxlorClient", DummyClient),
            patch.object(tui_module, "TransferRunner", DummyRunner),
            patch.object(tui_module, "Migrator", RecordingMigrator),
            patch("froxlor_migrator.plan.Selection", lambda **kwargs: SimpleNamespace(**kwargs)),
        ):
            sys_argv = sys.argv
            try:
                sys.argv = ["run", "--config", "config.toml", "--non-interactive", "--yes", "--all-customers"]
                tui_module.run_app()
            finally:
                sys.argv = sys_argv

        self.assertEqual(executed, ["alice", "bob"])

    def test_run_app_batch_continues_after_failure(self) -> None:
        executed: list[str] = []

        class FlakyMigrator(DummyMigrator):
            def execute(self, selection):
                executed.append(selection.customer["loginname"])
                if selection.customer["loginname"] == "alice":
                    raise tui_module.MigrationError("boom")
                return SimpleNamespace(target_customer_id=42, source_to_target_db={})

        with (
            patch.object(tui_module, "load_config", return_value=make_config()),
            patch("froxlor_migrator.plan.FroxlorClient", DummyClient),
            patch.object(tui_module, "TransferRunner", DummyRunner),
            patch.object(tui_module, "Migrator", FlakyMigrator),
            patch("froxlor_migrator.plan.Selection", lambda **kwargs: SimpleNamespace(**kwargs)),
        ):
            sys_argv = sys.argv
            try:
                sys.argv = [
                    "run",
                    "--config",
                    "config.toml",
                    "--non-interactive",
                    "--yes",
                    "--source-customer",
                    "alice,bob",
                ]
                with self.assertRaises(SystemExit) as ctx:
                    tui_module.run_app()
                self.assertEqual(1, ctx.exception.code)
            finally:
                sys.argv = sys_argv

        # alice failed but bob still migrated; the batch exits non-zero.
        self.assertEqual(executed, ["alice", "bob"])

    def test_run_app_batch_rejects_token_matching_no_customer(self) -> None:
        executed: list[str] = []

        class RecordingMigrator(DummyMigrator):
            def execute(self, selection):
                executed.append(selection.customer["loginname"])
                return SimpleNamespace(target_customer_id=1, source_to_target_db={})

        with (
            patch.object(tui_module, "load_config", return_value=make_config()),
            patch("froxlor_migrator.plan.FroxlorClient", DummyClient),
            patch.object(tui_module, "TransferRunner", DummyRunner),
            patch.object(tui_module, "Migrator", RecordingMigrator),
            patch("froxlor_migrator.plan.Selection", lambda **kwargs: SimpleNamespace(**kwargs)),
        ):
            sys_argv = sys.argv
            try:
                sys.argv = [
                    "run",
                    "--config",
                    "config.toml",
                    "--non-interactive",
                    "--yes",
                    "--all-customers",
                    "--domains",
                    "no-such-domain.invalid",
                ]
                with self.assertRaises(SystemExit) as ctx:
                    tui_module.run_app()
                self.assertEqual(1, ctx.exception.code)
            finally:
                sys.argv = sys_argv

        # The typo'd --domains token matched no customer — nothing executed.
        self.assertEqual(executed, [])

    def test_run_app_batch_rejects_ambiguous_customer_token(self) -> None:
        executed: list[str] = []

        class CollisionClient(DummyClient):
            def list_customers(self):
                return [
                    {"customerid": 1, "loginname": "alice", "email": "alice@example.com", "name": "Alice"},
                    {"customerid": 2, "loginname": "bob", "email": "bob@example.com", "name": "alice"},
                ]

        class RecordingMigrator(DummyMigrator):
            def execute(self, selection):
                executed.append(selection.customer["loginname"])
                return SimpleNamespace(target_customer_id=1, source_to_target_db={})

        with (
            patch.object(tui_module, "load_config", return_value=make_config()),
            patch("froxlor_migrator.plan.FroxlorClient", CollisionClient),
            patch.object(tui_module, "TransferRunner", DummyRunner),
            patch.object(tui_module, "Migrator", RecordingMigrator),
            patch("froxlor_migrator.plan.Selection", lambda **kwargs: SimpleNamespace(**kwargs)),
        ):
            sys_argv = sys.argv
            try:
                # "alice" matches alice's login *and* bob's name — ambiguous,
                # must error instead of silently batching both.
                sys.argv = ["run", "--config", "config.toml", "--non-interactive", "--yes", "--source-customer", "alice"]
                with self.assertRaises(SystemExit) as ctx:
                    tui_module.run_app()
                self.assertEqual(1, ctx.exception.code)
            finally:
                sys.argv = sys_argv

        self.assertEqual(executed, [])

    def test_run_app_batch_domains_none_exits(self) -> None:
        executed: list[str] = []

        class RecordingMigrator(DummyMigrator):
            def execute(self, selection):
                executed.append(selection.customer["loginname"])
                return SimpleNamespace(target_customer_id=1, source_to_target_db={})

        with (
            patch.object(tui_module, "load_config", return_value=make_config()),
            patch("froxlor_migrator.plan.FroxlorClient", DummyClient),
            patch.object(tui_module, "TransferRunner", DummyRunner),
            patch.object(tui_module, "Migrator", RecordingMigrator),
            patch("froxlor_migrator.plan.Selection", lambda **kwargs: SimpleNamespace(**kwargs)),
        ):
            sys_argv = sys.argv
            try:
                sys.argv = ["run", "--config", "config.toml", "--non-interactive", "--yes", "--all-customers", "--domains", "none"]
                with self.assertRaises(SystemExit) as ctx:
                    tui_module.run_app()
                self.assertEqual(1, ctx.exception.code)
            finally:
                sys.argv = sys_argv

        # Batch has no resources-only mode — --domains none leaves nothing to do.
        self.assertEqual(executed, [])

    def test_build_ip_map_records_matched_before_empty_target(self) -> None:
        # --ip-map tokens that match source rows count as "matched" even when
        # the target lists no IPs — otherwise the batch reports a false
        # "matched no customer" error.
        class NoIpTarget:
            def listing(self, _method):
                return []

        domains = [{"domain": "example.com", "ipsandports": [{"id": 5, "ip": "1.2.3.4", "port": 80, "ssl": 0}]}]
        matched: dict = {}
        mapping, _source_rows, target_rows = tui_module._build_ip_map(
            domains,
            NoIpTarget(),
            preset_mapping={"1.2.3.4:80": "9.9.9.9:80"},
            matched=matched,
        )
        self.assertEqual(mapping, {})
        self.assertEqual(target_rows, [])
        self.assertEqual(matched["IP mapping"], {"1.2.3.4:80"})

    def test_run_app_interactive_requires_tty(self) -> None:
        with (
            patch.object(tui_module, "load_config", return_value=make_config()),
            patch.object(sys.stdin, "isatty", return_value=False),
            patch.object(sys.stdout, "isatty", return_value=False),
        ):
            sys_argv = sys.argv
            try:
                sys.argv = ["run", "--config", "config.toml"]
                with self.assertRaises(SystemExit) as ctx:
                    tui_module.run_app()
                self.assertEqual(2, ctx.exception.code)
            finally:
                sys.argv = sys_argv

    def test_run_app_unresolvable_customer_exits_nonzero(self) -> None:
        with patch.object(tui_module, "load_config", return_value=make_config()), patch("froxlor_migrator.plan.FroxlorClient", DummyClient):
            sys_argv = sys.argv
            try:
                sys.argv = [
                    "run",
                    "--config",
                    "config.toml",
                    "--non-interactive",
                    "--yes",
                    "--source-customer",
                    "no-such-login",
                ]
                with self.assertRaises(SystemExit) as ctx:
                    tui_module.run_app()
                self.assertEqual(1, ctx.exception.code)
            finally:
                sys.argv = sys_argv


if __name__ == "__main__":
    unittest.main()
