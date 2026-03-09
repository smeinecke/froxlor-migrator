from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from froxlor_migrator.config import load_config


def _base_config_text(*, behavior_block: str = "") -> str:
    return f"""
[source]
api_url = "https://source.invalid/api.php"
api_key = "source-key"
api_secret = "source-secret"

[target]
api_url = "https://target.invalid/api.php"
api_key = "target-key"
api_secret = "target-secret"

[ssh]
host = "target.invalid"
user = "root"

[paths]
source_web_root = "/var/customers/webs"
target_web_root = "/var/customers/webs"

{behavior_block}
""".strip()


class ConfigTests(unittest.TestCase):
    def test_behavior_exists_values_are_normalized_to_lowercase(self) -> None:
        content = _base_config_text(
            behavior_block="""
[behavior]
domain_exists = "SKIP"
database_exists = "Update"
mailbox_exists = "FAIL"
""",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.toml"
            path.write_text(content, encoding="utf-8")
            cfg = load_config(path)

        self.assertEqual("skip", cfg.behavior.domain_exists)
        self.assertEqual("update", cfg.behavior.database_exists)
        self.assertEqual("fail", cfg.behavior.mailbox_exists)

    def test_invalid_behavior_exists_value_raises(self) -> None:
        content = _base_config_text(
            behavior_block="""
[behavior]
mailbox_exists = "maybe"
""",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.toml"
            path.write_text(content, encoding="utf-8")
            with self.assertRaises(ValueError):
                load_config(path)

    def test_load_config_expands_env_and_applies_defaults(self) -> None:
        content = """
[source]
api_url = "${SRC_API_URL}"
api_key = "source-key"
api_secret = "source-secret"
timeout_seconds = 45

[target]
api_url = "https://target.invalid/api.php"
api_key = "target-key"
api_secret = "target-secret"

[ssh]
host = "target.invalid"
user = "${SSH_USER}"
port = 2222
strict_host_key_checking = false

[paths]
source_web_root = "/var/source"
source_transfer_root = "/var/tmp"
target_web_root = "/var/target"

[mysql]
source_panel_database = "source_db"
target_panel_database = "target_db"

[commands]
ssh = "/usr/local/bin/ssh"
sudo = "/usr/local/bin/sudo"
tar = "/usr/local/bin/tar"
mysqldump = "/usr/local/bin/mysqldump"
mysql = "/usr/local/bin/mysql"
doveadm = "/usr/local/bin/doveadm"
pzstd = "/usr/local/bin/pzstd"
pigz = "/usr/local/bin/pigz"

[behavior]
dry_run_default = false
domain_exists = "UPDATE"
database_exists = "skip"
mailbox_exists = "FAIL"
parallel = 0

[output]
manifest_dir = "/tmp/manifests"
""".strip()

        env_overrides = {"SRC_API_URL": "https://source-env.invalid/api.php", "SSH_USER": "deploy"}
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(os.environ, env_overrides, clear=False):
            path = Path(tmpdir) / "config.toml"
            path.write_text(content, encoding="utf-8")
            cfg = load_config(path)

        self.assertEqual("https://source-env.invalid/api.php", cfg.source.api_url)
        self.assertEqual("deploy", cfg.ssh.user)
        self.assertFalse(cfg.ssh.strict_host_key_checking)
        self.assertEqual("/var/tmp", cfg.paths.source_transfer_root)
        self.assertEqual("/usr/local/bin/ssh", cfg.commands.ssh)
        self.assertEqual("update", cfg.behavior.domain_exists)
        self.assertEqual("skip", cfg.behavior.database_exists)
        self.assertEqual("fail", cfg.behavior.mailbox_exists)
        self.assertFalse(cfg.behavior.dry_run_default)
        self.assertEqual(1, cfg.behavior.parallel)
        self.assertEqual("/tmp/manifests", cfg.output.manifest_dir)

    def test_source_transfer_root_defaults_to_source_web_root(self) -> None:
        content = _base_config_text()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.toml"
            path.write_text(content, encoding="utf-8")
            cfg = load_config(path)
        self.assertEqual(cfg.paths.source_web_root, cfg.paths.source_transfer_root)

    def test_empty_required_key_raises_value_error(self) -> None:
        content = """
[source]
api_url = "https://source.invalid/api.php"
api_key = "source-key"
api_secret = "source-secret"

[target]
api_url = "https://target.invalid/api.php"
api_key = "target-key"
api_secret = "target-secret"

[ssh]
host = "target.invalid"
user = ""

[paths]
source_web_root = "/var/source"
target_web_root = "/var/target"
""".strip()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.toml"
            path.write_text(content, encoding="utf-8")
            with self.assertRaises(ValueError):
                load_config(path)


if __name__ == "__main__":
    unittest.main()
