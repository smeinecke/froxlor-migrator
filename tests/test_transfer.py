from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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


def _config(
    manifest_dir: str,
    *,
    ssh_user: str = "root",
    ssh_host: str = "example.invalid",
    ssh_port: int = 22,
    strict_host_key_checking: bool = True,
    commands: CommandsConfig | None = None,
    paths: PathsConfig | None = None,
    behavior: BehaviorConfig | None = None,
    output: OutputConfig | None = None,
) -> AppConfig:
    return AppConfig(
        source=ApiConfig(api_url="https://source.invalid/api.php", api_key="k", api_secret="s"),
        target=ApiConfig(api_url="https://target.invalid/api.php", api_key="k", api_secret="s"),
        ssh=SshConfig(host=ssh_host, user=ssh_user, port=ssh_port, strict_host_key_checking=strict_host_key_checking),
        paths=paths or PathsConfig(source_web_root="/src", source_transfer_root="/src", target_web_root="/dst"),
        mysql=MysqlConfig(source_panel_database="froxlor", target_panel_database="froxlor"),
        commands=commands or CommandsConfig(),
        behavior=behavior or BehaviorConfig(),
        output=output or OutputConfig(manifest_dir=manifest_dir),
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

    def test_preflight_mail_tools_do_not_use_local_sudo(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = TransferRunner(config=_config(tmpdir), dry_run=True, manifest_name="preflight")
            commands = runner.preflight_commands(include_ssh=False, include_database_tools=False, include_mail_tools=True)
            self.assertIn("doveadm process status >/dev/null 2>&1", commands)
            self.assertTrue(all(not cmd.startswith("sudo ") for cmd in commands))

    def test_preflight_mail_tools_skip_remote_sudo_for_root_user(self) -> None:
        class SshStub:
            def __init__(self) -> None:
                self.commands: list[str] = []

            def run(self, command: str):
                self.commands.append(command)
                return type("Result", (), {"returncode": 0})()

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = TransferRunner(config=_config(tmpdir, ssh_user="root"), dry_run=False, manifest_name="preflight")
            ssh = SshStub()
            runner._ssh = ssh  # type: ignore[assignment]
            runner.preflight_commands(include_ssh=True, include_database_tools=False, include_mail_tools=True)
            self.assertIn("doveadm process status >/dev/null 2>&1", ssh.commands)
            self.assertTrue(all(not cmd.startswith("sudo ") for cmd in ssh.commands))

    def test_preflight_mail_tools_use_remote_sudo_for_non_root_user(self) -> None:
        class SshStub:
            def __init__(self) -> None:
                self.commands: list[str] = []

            def run(self, command: str):
                self.commands.append(command)
                return type("Result", (), {"returncode": 0})()

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = TransferRunner(config=_config(tmpdir, ssh_user="deploy"), dry_run=False, manifest_name="preflight")
            ssh = SshStub()
            runner._ssh = ssh  # type: ignore[assignment]
            runner.preflight_commands(include_ssh=True, include_database_tools=False, include_mail_tools=True)
            self.assertIn("sudo doveadm process status >/dev/null 2>&1", ssh.commands)

    def test_select_file_transfer_codec_prefers_available_tools_and_caches_result(self) -> None:
        class CodecRunner(TransferRunner):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.local: dict[str, bool] = {}
                self.remote: dict[str, bool] = {}

            def _command_available(self, command: str) -> bool:  # noqa: ARG002
                return self.local.get(command, False)

            def _remote_command_available(self, command: str) -> bool:  # noqa: ARG002
                return self.remote.get(command, False)

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = CodecRunner(config=_config(tmpdir), dry_run=True, manifest_name="codec")
            runner.local[runner.config.commands.pzstd] = True
            runner.remote[runner.config.commands.pzstd] = True
            codec = runner._select_file_transfer_codec()
            self.assertIn("pzstd", codec[0])
            self.assertIn("pzstd", codec[1])

            runner.local[runner.config.commands.pzstd] = False
            runner.remote[runner.config.commands.pzstd] = False
            self.assertEqual(codec, runner._select_file_transfer_codec())

            runner = CodecRunner(config=_config(tmpdir), dry_run=True, manifest_name="codec2")
            runner.local[runner.config.commands.pigz] = True
            runner.remote[runner.config.commands.pigz] = True
            codec = runner._select_file_transfer_codec()
            self.assertIn("pigz", codec[0])
            self.assertIn("pigz", codec[1])

            runner = CodecRunner(config=_config(tmpdir), dry_run=True, manifest_name="codec3")
            codec = runner._select_file_transfer_codec()
            self.assertEqual(("", ""), codec)

    def test_ssh_prefix_includes_expected_options(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = _config(
                tmpdir,
                ssh_user="deploy",
                ssh_host="remote.example",
                ssh_port=2222,
                strict_host_key_checking=False,
            )
            runner = TransferRunner(config=cfg, dry_run=True, manifest_name="prefix")
            prefix = runner._ssh_prefix()
            self.assertIn("StrictHostKeyChecking=no", prefix)
            self.assertIn("UserKnownHostsFile=/dev/null", prefix)
            self.assertIn("-p 2222", prefix)
            self.assertIn("-l deploy", prefix)
            self.assertIn("remote.example", prefix)

    def test_ssh_target_is_local_matches_hostname(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = TransferRunner(config=_config(tmpdir), dry_run=True, manifest_name="local")
            with (
                mock.patch("froxlor_migrator.transfer.socket.gethostname", return_value="example.invalid"),
                mock.patch("froxlor_migrator.transfer.socket.getfqdn", return_value="example.invalid"),
                mock.patch("froxlor_migrator.transfer.socket.gethostbyname", return_value="203.0.113.1"),
                mock.patch(
                    "froxlor_migrator.transfer.socket.gethostbyname_ex",
                    return_value=("example.invalid", [], ["203.0.113.1"]),
                ),
            ):
                self.assertTrue(runner._ssh_target_is_local())

    def test_ssh_target_is_local_returns_false_for_remote_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = _config(tmpdir, ssh_host="remote.example")
            runner = TransferRunner(config=cfg, dry_run=True, manifest_name="remote")
            with (
                mock.patch("froxlor_migrator.transfer.socket.gethostname", return_value="example.invalid"),
                mock.patch("froxlor_migrator.transfer.socket.getfqdn", return_value="example.invalid"),
                mock.patch("froxlor_migrator.transfer.socket.gethostbyname", return_value="198.51.100.10"),
                mock.patch(
                    "froxlor_migrator.transfer.socket.gethostbyname_ex",
                    return_value=("example.invalid", [], ["203.0.113.1"]),
                ),
            ):
                self.assertFalse(runner._ssh_target_is_local())

    def test_needs_remote_sudo_depends_on_user(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = TransferRunner(config=_config(tmpdir, ssh_user="deploy"), dry_run=True, manifest_name="sudo")
            self.assertTrue(runner._needs_remote_sudo())

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = TransferRunner(config=_config(tmpdir, ssh_user="root"), dry_run=True, manifest_name="sudo")
            self.assertFalse(runner._needs_remote_sudo())

    def test_truncate_output_appends_suffix_when_limit_exceeded(self) -> None:
        value = "x" * 10
        truncated = TransferRunner._truncate_output(value, limit=5)
        self.assertTrue(truncated.endswith("...[truncated]..."))


if __name__ == "__main__":
    unittest.main()
