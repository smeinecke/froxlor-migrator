from __future__ import annotations

import json
import shlex
import subprocess
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

    def run(self, command: str, check: bool = True) -> CommandResult:
        started = datetime.now(timezone.utc).isoformat()
        self._log_event("command", {"command": command, "dry_run": self.dry_run})
        if self.dry_run:
            finished = datetime.now(timezone.utc).isoformat()
            return CommandResult(command=command, returncode=0, started_at=started, finished_at=finished)

        completed = subprocess.run(["bash", "-o", "pipefail", "-c", command])
        finished = datetime.now(timezone.utc).isoformat()
        result = CommandResult(
            command=command,
            returncode=completed.returncode,
            started_at=started,
            finished_at=finished,
        )
        if check and completed.returncode != 0:
            self._log_event("error", {"command": command, "returncode": completed.returncode})
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

        # Check for available compression tools (optional)
        compression_commands = []
        try:
            pzstd = shlex.quote(self.config.commands.pzstd)
            result = subprocess.run(["bash", "-c", f"command -v {pzstd}"], capture_output=True, text=True)
            if result.returncode == 0:
                compression_commands.append(f"command -v {pzstd}")
        except Exception:
            pass

        try:
            pigz = shlex.quote(self.config.commands.pigz)
            result = subprocess.run(["bash", "-c", f"command -v {pigz}"], capture_output=True, text=True)
            if result.returncode == 0:
                compression_commands.append(f"command -v {pigz}")
        except Exception:
            pass

        # Only add compression commands if they're available
        commands.extend(compression_commands)

        if include_database_tools:
            commands.append(f"command -v {shlex.quote(self.config.commands.mysqldump)}")
            commands.append(f"command -v {shlex.quote(self.config.commands.mysql)}")
        if include_ssh:
            commands.append(f"command -v {ssh_binary}")
            ssh_prefix = self._ssh_prefix()
            commands.append(f"{ssh_prefix} command -v {shlex.quote(self.config.commands.tar)}")
            # Only add compression commands for SSH if they're available locally
            for cmd in compression_commands:
                commands.append(f"{ssh_prefix} {cmd}")
            if include_database_tools:
                commands.append(f"{ssh_prefix} command -v {shlex.quote(self.config.commands.mysql)}")
        if include_mail_tools:
            commands.append(f"{sudo} {doveadm} --version >/dev/null 2>&1")
            if include_ssh:
                ssh_prefix = self._ssh_prefix()
                commands.append(f"{ssh_prefix} {sudo} {doveadm} --version >/dev/null 2>&1")
        return commands

    def _get_compression_command(self) -> tuple[str, str]:
        """Get compression command and decompression command.
        Returns (compress_cmd, decompress_cmd) tuple.
        Prefers pzstd with -3 level, falls back to pigz, then no compression.
        """
        pzstd = shlex.quote(self.config.commands.pzstd)
        pigz = shlex.quote(self.config.commands.pigz)

        # Try pzstd first
        try:
            result = subprocess.run(["bash", "-c", f"command -v {pzstd}"], capture_output=True, text=True)
            if result.returncode == 0:
                # Use pzstd with compression level 3
                compress_cmd = f"{pzstd} -3"
                decompress_cmd = f"{pzstd} -d"
                return compress_cmd, decompress_cmd
        except Exception:
            pass

        # Fall back to pigz
        try:
            result = subprocess.run(["bash", "-c", f"command -v {pigz}"], capture_output=True, text=True)
            if result.returncode == 0:
                compress_cmd = f"{pigz}"
                decompress_cmd = f"{pigz} -d"
                return compress_cmd, decompress_cmd
        except Exception:
            pass

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
            f" && {shlex.quote(self.config.commands.tar)} -xf - -C {shlex.quote(target_dir)} --preserve-permissions --preserve-owner"
        )

        command = (
            f"{tar} -cf - -C {shlex.quote(source_dir)} . --preserve-permissions --preserve-owner"
            f" | {compress_cmd} | {ssh_prefix} '{decompress_cmd} | {shlex.quote(remote_cmd)}'"
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
