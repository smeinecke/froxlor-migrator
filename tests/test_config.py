from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
