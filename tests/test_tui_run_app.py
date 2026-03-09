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
        return [{"customerid": 1, "loginname": "alice", "email": "alice@example.com", "name": "Alice"}]

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


class RunAppTests(unittest.TestCase):
    def test_run_app_non_interactive_completes(self) -> None:
        class DummyConfig:
            class Api:
                api_url = ""
                api_key = ""
                api_secret = ""
                timeout_seconds = 30

            source = Api()
            target = Api()

            class Paths:
                source_web_root = "/var/www"
                source_transfer_root = "/var/www"
                target_web_root = "/var/www"

            paths = Paths()

            class Behavior:
                dry_run_default = True

            behavior = Behavior()

            class Commands:
                ssh = "ssh"

            commands = Commands()

        with patch.object(tui_module, "load_config", return_value=DummyConfig()), patch.object(
            tui_module, "FroxlorClient", DummyClient
        ), patch.object(tui_module, "TransferRunner", DummyRunner), patch.object(
            tui_module, "Migrator", DummyMigrator
        ), patch.object(tui_module, "Selection", lambda **kwargs: SimpleNamespace(**kwargs)):
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


if __name__ == "__main__":
    unittest.main()
