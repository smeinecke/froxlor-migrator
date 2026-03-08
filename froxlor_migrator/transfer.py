from __future__ import annotations

import json
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import AppConfig
from .util import ensure_dir


class TransferError(RuntimeError):
    pass


@dataclass
class CommandResult:
    command: str
    returncode: int
    started_at: str
    finished_at: str


class TransferRunner:
    def __init__(self, config: AppConfig, dry_run: bool, manifest_name: str) -> None:
        self.config = config
        self.dry_run = dry_run
        manifest_dir = ensure_dir(config.output.manifest_dir)
        self.manifest_path = manifest_dir / f"{manifest_name}.json"
        self.events: list[dict[str, Any]] = []

    def _log_event(self, kind: str, payload: dict[str, Any]) -> None:
        self.events.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "kind": kind,
            **payload,
        })
        self.manifest_path.write_text(json.dumps(self.events, indent=2), encoding="utf-8")

    @staticmethod
    def _truncate_output(value: str, limit: int = 16000) -> str:
        if len(value) <= limit:
            return value
        return value[:limit] + "\n...[truncated]..."

    def run(self, command: str, check: bool = True) -> CommandResult:
        started = datetime.now(timezone.utc).isoformat()
        self._log_event("command", {"command": command, "dry_run": self.dry_run})
        if self.dry_run:
            finished = datetime.now(timezone.utc).isoformat()
            return CommandResult(command=command, returncode=0, started_at=started, finished_at=finished)

        completed = subprocess.run(
            ["bash", "-o", "pipefail", "-c", command],
            capture_output=True,
            text=True,
        )
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
        finished = datetime.now(timezone.utc).isoformat()
        result = CommandResult(
            command=command,
            returncode=completed.returncode,
            started_at=started,
            finished_at=finished,
        )
        self._log_event(
            "result",
            {
                "command": command,
                "returncode": completed.returncode,
                "stdout": self._truncate_output(completed.stdout or ""),
                "stderr": self._truncate_output(completed.stderr or ""),
            },
        )
        if check and completed.returncode != 0:
            self._log_event(
                "error",
                {
                    "command": command,
                    "returncode": completed.returncode,
                    "stdout": self._truncate_output(completed.stdout or ""),
                    "stderr": self._truncate_output(completed.stderr or ""),
                },
            )
            raise TransferError(f"Command failed ({completed.returncode}): {command}")
        return result

    def _ssh_prefix(self) -> str:
        ssh = self.config.commands.ssh
        options = []
        if not self.config.ssh.strict_host_key_checking:
            options.append("-o StrictHostKeyChecking=no")
            options.append("-o UserKnownHostsFile=/dev/null")
        options.extend([f"-p {self.config.ssh.port}"])
        return f"{ssh} {' '.join(options)} -l {shlex.quote(self.config.ssh.user)} {shlex.quote(self.config.ssh.host)}"

    @staticmethod
    def _command_available(command: str) -> bool:
        try:
            result = subprocess.run(["bash", "-c", f"command -v {shlex.quote(command)}"], capture_output=True, text=True)
            return result.returncode == 0
        except Exception:
            return False

    def _remote_command_available(self, command: str) -> bool:
        try:
            result = subprocess.run(
                ["bash", "-c", f"{self._ssh_prefix()} command -v {shlex.quote(command)}"],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception:
            return False

    def preflight_commands(
        self,
        *,
        include_ssh: bool,
        include_database_tools: bool,
        include_mail_tools: bool,
    ) -> list[str]:
        sudo = shlex.quote(self.config.commands.sudo)
        doveadm = shlex.quote(self.config.commands.doveadm)
        ssh_binary = shlex.quote(shlex.split(self.config.commands.ssh)[0])
        commands = [f"command -v {shlex.quote(self.config.commands.tar)}"]

        # Compression tools are optional and auto-fallback at transfer time.
        if self._command_available(self.config.commands.pzstd):
            commands.append(f"command -v {shlex.quote(self.config.commands.pzstd)}")
        if self._command_available(self.config.commands.pigz):
            commands.append(f"command -v {shlex.quote(self.config.commands.pigz)}")

        if include_database_tools:
            commands.append(f"command -v {shlex.quote(self.config.commands.mysqldump)}")
            commands.append(f"command -v {shlex.quote(self.config.commands.mysql)}")
        if include_ssh:
            commands.append(f"command -v {ssh_binary}")
            ssh_prefix = self._ssh_prefix()
            commands.append(f"{ssh_prefix} command -v {shlex.quote(self.config.commands.tar)}")
            if include_database_tools:
                commands.append(f"{ssh_prefix} command -v {shlex.quote(self.config.commands.mysql)}")
        if include_mail_tools:
            commands.append(f"{sudo} {doveadm} process status >/dev/null 2>&1")
            if include_ssh:
                ssh_prefix = self._ssh_prefix()
                commands.append(f"{ssh_prefix} {sudo} {doveadm} process status >/dev/null 2>&1")
        return commands

    def _get_compression_command(self) -> tuple[str, str]:
        """Get compression command and decompression command.
        Returns (compress_cmd, decompress_cmd) tuple.
        Prefers pzstd with -3 level, falls back to pigz, then no compression.
        """
        pzstd = self.config.commands.pzstd
        pigz = self.config.commands.pigz

        # Prefer zstd stream when both ends can decode it.
        if self._command_available(pzstd):
            if self._remote_command_available(pzstd):
                return f"{shlex.quote(pzstd)} -3", f"{shlex.quote(pzstd)} -d"
            if self._remote_command_available("zstd"):
                return f"{shlex.quote(pzstd)} -3", "zstd -d"

        # gzip stream fallback.
        if self._command_available(pigz):
            if self._remote_command_available(pigz):
                return shlex.quote(pigz), f"{shlex.quote(pigz)} -d"
            if self._remote_command_available("gzip"):
                return shlex.quote(pigz), "gzip -d"

        # No compression available - use cat as no-op
        compress_cmd = "cat"
        decompress_cmd = "cat"
        return compress_cmd, decompress_cmd

    def transfer_files(self, source_dir: str, target_dir: str) -> None:
        tar = shlex.quote(self.config.commands.tar)
        ssh_prefix = self._ssh_prefix()

        # Get compression commands
        compress_cmd, decompress_cmd = self._get_compression_command()

        remote_cmd = (
            f"mkdir -p {shlex.quote(target_dir)}"
            f" && {shlex.quote(self.config.commands.tar)} -xf - -C {shlex.quote(target_dir)}"
        )
        if decompress_cmd == "cat":
            remote_pipeline = remote_cmd
        else:
            remote_pipeline = f"{decompress_cmd} | {remote_cmd}"

        command = (
            f"{tar} -cf - -C {shlex.quote(source_dir)} ."
            f" | {compress_cmd} | {ssh_prefix} {shlex.quote(remote_pipeline)}"
        )
        self.run(command)

    def transfer_database(self, source_db: str, target_db: str) -> None:
        dump_args = " ".join(shlex.quote(x) for x in self.config.mysql.source_dump_args)
        import_args = " ".join(shlex.quote(x) for x in self.config.mysql.target_import_args)
        mysqldump = shlex.quote(self.config.commands.mysqldump)
        mysql = shlex.quote(self.config.commands.mysql)
        ssh_prefix = self._ssh_prefix()
        remote_cmd = f"{mysql} {import_args} {shlex.quote(target_db)}"
        command = f"{mysqldump} {dump_args} {shlex.quote(source_db)} | {ssh_prefix} {shlex.quote(remote_cmd)}"
        self.run(command)

    def transfer_mailbox(self, mailbox: str) -> None:
        sudo = shlex.quote(self.config.commands.sudo)
        doveadm = shlex.quote(self.config.commands.doveadm)
        ssh_prefix = self._ssh_prefix()
        remote = f"{ssh_prefix} {shlex.quote(self.config.commands.sudo + ' ' + self.config.commands.doveadm + ' dsync-server -u ' + mailbox)}"
        command = f"{sudo} {doveadm} backup -u {shlex.quote(mailbox)} {remote}"
        self.run(command)
