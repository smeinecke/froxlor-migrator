from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from froxlor_migrator.config import (
    ApiConfig,
    AppConfig,
    BehaviorConfig,
    CommandsConfig,
    MysqlConfig,
    OutputConfig,
    PathsConfig,
    SshConfig,
)
from froxlor_migrator.transfer import TransferError, TransferRunner


def _config(manifest_dir: str) -> AppConfig:
    return AppConfig(
        source=ApiConfig(api_url="https://source.invalid/api.php", api_key="k", api_secret="s"),
        target=ApiConfig(api_url="https://target.invalid/api.php", api_key="k", api_secret="s"),
        ssh=SshConfig(host="example.invalid", user="root", port=22, strict_host_key_checking=True),
        paths=PathsConfig(source_web_root="/src", source_transfer_root="/src", target_web_root="/dst"),
        mysql=MysqlConfig(source_panel_database="froxlor", target_panel_database="froxlor"),
        commands=CommandsConfig(),
        behavior=BehaviorConfig(),
        output=OutputConfig(manifest_dir=manifest_dir),
    )


class TransferRunnerTests(unittest.TestCase):
    def test_failed_command_logs_stderr_to_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = TransferRunner(config=_config(tmpdir), dry_run=False, manifest_name="test")
            with self.assertRaises(TransferError):
                runner.run("echo boom >&2; exit 64")

            manifest = Path(tmpdir) / "test.json"
            events = json.loads(manifest.read_text(encoding="utf-8"))
            error_events = [event for event in events if event.get("kind") == "error"]
            self.assertEqual(1, len(error_events))
            self.assertIn("boom", error_events[0].get("stderr", ""))
            self.assertEqual(64, int(error_events[0].get("returncode", 0)))

    def test_transfer_files_uses_tar_over_ssh(self) -> None:
        class CaptureRunner(TransferRunner):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.commands: list[str] = []

            def run(self, command: str, check: bool = True):  # noqa: ARG002
                self.commands.append(command)
                return None

            def _command_available(self, command: str) -> bool:  # noqa: ARG002
                return False

            def _remote_command_available(self, command: str) -> bool:  # noqa: ARG002
                return False

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = CaptureRunner(config=_config(tmpdir), dry_run=False, manifest_name="test")
            source_dir = Path(tmpdir) / "src"
            source_dir.mkdir()
            (source_dir / "index.txt").write_text("hello", encoding="utf-8")
            runner.transfer_files(str(source_dir), "/dst/site")
            self.assertEqual(1, len(runner.commands))
            self.assertIn("tar -C", runner.commands[0])
            self.assertIn("| ssh ", runner.commands[0])
            self.assertIn("mkdir -p /dst/site", runner.commands[0])

    def test_debug_event_is_only_written_in_debug_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            disabled = TransferRunner(config=_config(tmpdir), dry_run=True, manifest_name="disabled", debug=False)
            disabled.debug_event("hidden", foo="bar")
            self.assertFalse((Path(tmpdir) / "disabled.json").exists())

            enabled = TransferRunner(config=_config(tmpdir), dry_run=True, manifest_name="enabled", debug=True)
            enabled.debug_event("visible", foo="bar")
            events = json.loads((Path(tmpdir) / "enabled.json").read_text(encoding="utf-8"))
            self.assertEqual("debug", events[-1]["kind"])
            self.assertEqual("visible", events[-1]["message"])

    def test_progress_event_is_logged_and_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = TransferRunner(config=_config(tmpdir), dry_run=True, manifest_name="progress", debug=False)
            runner.progress_event(1, 10, "Step one")
            runner.progress_event(1, 10, "Step one")
            runner.progress_event(2, 10, "Step two")
            events = json.loads((Path(tmpdir) / "progress.json").read_text(encoding="utf-8"))
            progress_events = [event for event in events if event.get("kind") == "progress"]
            self.assertEqual(2, len(progress_events))
            self.assertEqual("Step one", progress_events[0]["status"])
            self.assertEqual("Step two", progress_events[1]["status"])

    def test_transfer_mailbox_fails_when_ssh_target_is_local(self) -> None:
        class GuardedRunner(TransferRunner):
            def _ssh_target_is_local(self) -> bool:
                return True

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = GuardedRunner(config=_config(tmpdir), dry_run=False, manifest_name="mailbox")
            with self.assertRaises(TransferError):
                runner.transfer_mailbox("info@example.test")

    def test_transfer_mailbox_uses_doveadm_backup_command(self) -> None:
        class CaptureRunner(TransferRunner):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.commands: list[str] = []

            def _ssh_target_is_local(self) -> bool:
                return False

            def run(self, command: str, check: bool = True):  # noqa: ARG002
                self.commands.append(command)
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = CaptureRunner(config=_config(tmpdir), dry_run=False, manifest_name="mailbox")
            runner.transfer_mailbox("info@example.test")
            self.assertEqual(1, len(runner.commands))
            self.assertIn("doveadm backup -u info@example.test", runner.commands[0])
            self.assertIn("dsync-server -u info@example.test", runner.commands[0])


if __name__ == "__main__":
    unittest.main()
