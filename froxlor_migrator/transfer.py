from __future__ import annotations

import json
import logging
import shlex
import socket
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import AppConfig
from .ssh_driver import SshDriver
from .util import ensure_dir


class TransferError(RuntimeError):
    pass


logger = logging.getLogger(__name__)


@dataclass
class CommandResult:
    command: str
    returncode: int
    started_at: str
    finished_at: str
    stdout: str = ""
    stderr: str = ""


class TransferRunner:
    def __init__(self, config: AppConfig, dry_run: bool, manifest_name: str, debug: bool = False) -> None:
        self.config = config
        self.dry_run = dry_run
        self.debug = debug
        self._ssh = SshDriver(config)
        self._file_transfer_codec: tuple[str, str] | None = None
        self._last_progress: tuple[int, int, str] | None = None
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

    def debug_event(self, message: str, **payload: Any) -> None:
        if not self.debug:
            return
        self._log_event("debug", {"message": message, **payload})

    def progress_event(self, step: int, total: int, status: str) -> None:
        key = (step, total, status)
        if key == self._last_progress:
            return
        self._last_progress = key
        self._log_event(
            "progress",
            {
                "step": step,
                "total": total,
                "status": status,
            },
        )

    @staticmethod
    def _truncate_output(value: str, limit: int = 16000) -> str:
        if len(value) <= limit:
            return value
        return value[:limit] + "\n...[truncated]..."

    def run(self, command: str, check: bool = True) -> CommandResult:
        started = datetime.now(timezone.utc).isoformat()
        self._log_event("command", {"command": command, "dry_run": self.dry_run})
        logger.debug("Local command start: check=%s dry_run=%s command=%s", check, self.dry_run, command)
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
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
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
        logger.debug(
            "Local command result: returncode=%s command=%s",
            completed.returncode,
            command,
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

    def _ssh_target_is_local(self) -> bool:
        host = self.config.ssh.host.strip().lower()
        if host in {"localhost", "127.0.0.1", "::1"}:
            return True

        local_names = {
            socket.gethostname().lower(),
            socket.getfqdn().lower(),
        }
        if host in local_names:
            return True

        try:
            target_ip = socket.gethostbyname(host)
        except Exception:
            return False

        local_ips: set[str] = set()
        for name in local_names:
            try:
                local_ips.update(socket.gethostbyname_ex(name)[2])
            except Exception:
                continue
        return target_ip in local_ips

    def ssh_transport(self):
        transport = self._ssh.transport()
        if transport is None:
            raise TransferError("SSH transport is not available")
        return transport

    @staticmethod
    def _command_available(command: str) -> bool:
        try:
            result = subprocess.run(["bash", "-c", f"command -v {shlex.quote(command)}"], capture_output=True, text=True)
            return result.returncode == 0
        except Exception:
            return False

    def _remote_command_available(self, command: str) -> bool:
        try:
            logger.debug("Checking remote command availability: command=%s", command)
            result = self._ssh.run(f"command -v {shlex.quote(command)}")
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
        commands: list[str] = []

        # Compression tools are optional and auto-fallback at transfer time.
        if self._command_available(self.config.commands.pzstd):
            commands.append(f"command -v {shlex.quote(self.config.commands.pzstd)}")
        if self._command_available(self.config.commands.pigz):
            commands.append(f"command -v {shlex.quote(self.config.commands.pigz)}")

        if include_database_tools:
            commands.append(f"command -v {shlex.quote(self.config.commands.mysqldump)}")
        if include_ssh and not self.dry_run:
            logger.debug("Running remote command preflight checks")
            if not self._remote_command_available(self.config.commands.sudo):
                raise TransferError(f"Remote command not found: {self.config.commands.sudo}")
            if include_database_tools and not self._remote_command_available(self.config.commands.mysql):
                raise TransferError(f"Remote command not found: {self.config.commands.mysql}")
        if include_mail_tools:
            commands.append(f"{sudo} {doveadm} process status >/dev/null 2>&1")
            if include_ssh and not self.dry_run:
                remote_status = self._ssh.run(f"{sudo} {doveadm} process status >/dev/null 2>&1")
                if remote_status.returncode != 0:
                    raise TransferError("Remote doveadm preflight failed")
        return commands

    def transfer_files(self, source_dir: str, target_dir: str) -> None:
        local_codec, remote_codec = self._select_file_transfer_codec()
        ssh_prefix = self._ssh_prefix()
        tar = shlex.quote(self.config.commands.tar)
        src = shlex.quote(source_dir)
        remote_tar = shlex.quote(self.config.commands.tar)
        remote_cmd = f"mkdir -p {shlex.quote(target_dir)} && {remote_codec} {remote_tar} -C {shlex.quote(target_dir)} -xpf -"
        command = f"{tar} -C {src} -cvf - . {local_codec}| {ssh_prefix} {shlex.quote(remote_cmd)}"
        self.run(command)

    def _select_file_transfer_codec(self) -> tuple[str, str]:
        if self._file_transfer_codec is not None:
            return self._file_transfer_codec

        pzstd = shlex.quote(self.config.commands.pzstd)
        pigz = shlex.quote(self.config.commands.pigz)

        if self._command_available(self.config.commands.pzstd) and self._remote_command_available(self.config.commands.pzstd):
            self._file_transfer_codec = (f"| {pzstd} -3 ", f"{pzstd} -d | ")
            return self._file_transfer_codec
        if self._command_available(self.config.commands.pigz) and self._remote_command_available(self.config.commands.pigz):
            self._file_transfer_codec = (f"| {pigz} -3 ", f"{pigz} -d | ")
            return self._file_transfer_codec

        self._file_transfer_codec = ("", "")
        return self._file_transfer_codec

    def upload_file(self, local_path: str, remote_path: str, mode: int = 0o600) -> None:
        if self.dry_run:
            return
        sftp = self._ssh.open_sftp()
        try:
            sftp.put(local_path, remote_path)
            sftp.chmod(remote_path, mode)
        finally:
            sftp.close()

    def write_remote_file(self, remote_path: str, content: str, mode: int = 0o600) -> None:
        if self.dry_run:
            return
        sftp = self._ssh.open_sftp()
        try:
            with sftp.file(remote_path, "w") as handle:
                handle.write(content)
            sftp.chmod(remote_path, mode)
        finally:
            sftp.close()

    def transfer_mailbox(self, mailbox: str) -> None:
        if self._ssh_target_is_local():
            raise TransferError(
                "Mailbox transfer requires running on the source mail host; "
                "configured SSH target resolves to this local machine."
            )
        sudo = shlex.quote(self.config.commands.sudo)
        doveadm = shlex.quote(self.config.commands.doveadm)
        ssh_prefix = self._ssh_prefix()
        remote = f"{ssh_prefix} {shlex.quote(self.config.commands.sudo + ' ' + self.config.commands.doveadm + ' dsync-server -u ' + mailbox)}"
        command = f"{sudo} {doveadm} backup -u {shlex.quote(mailbox)} {remote}"
        logger.debug("Mailbox transfer command prepared: mailbox=%s command=%s", mailbox, command)
        self.run(command)

    def read_remote_file(self, path: str) -> str:
        if self.dry_run:
            return ""
        return self._ssh.read_file(path)

    def run_remote(self, command: str, check: bool = True) -> CommandResult:
        started = datetime.now(timezone.utc).isoformat()
        self._log_event("command", {"command": command, "dry_run": self.dry_run, "remote": True})
        logger.debug("Remote command start: check=%s dry_run=%s command=%s", check, self.dry_run, command)
        if self.dry_run:
            finished = datetime.now(timezone.utc).isoformat()
            return CommandResult(command=command, returncode=0, started_at=started, finished_at=finished)
        completed = self._ssh.run(command)
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
        finished = datetime.now(timezone.utc).isoformat()
        self._log_event(
            "result",
            {
                "command": command,
                "returncode": completed.returncode,
                "stdout": self._truncate_output(completed.stdout or ""),
                "stderr": self._truncate_output(completed.stderr or ""),
                "remote": True,
            },
        )
        logger.debug(
            "Remote command result: returncode=%s command=%s",
            completed.returncode,
            command,
        )
        if check and completed.returncode != 0:
            raise TransferError(f"Remote command failed ({completed.returncode}): {command}")
        return CommandResult(
            command=command,
            returncode=completed.returncode,
            started_at=started,
            finished_at=finished,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )
