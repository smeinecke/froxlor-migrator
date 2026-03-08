from __future__ import annotations

import shlex
from dataclasses import dataclass
from pathlib import Path

import paramiko

from .config import AppConfig


@dataclass(frozen=True)
class SshCommandResult:
    returncode: int
    stdout: str
    stderr: str


def _identity_file_from_ssh_command(ssh_command: str) -> str | None:
    tokens = shlex.split(ssh_command)
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "-i" and i + 1 < len(tokens):
            return tokens[i + 1]
        if token.startswith("-i") and len(token) > 2:
            return token[2:]
        i += 1
    return None


class SshDriver:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._client: paramiko.SSHClient | None = None

    def _connect(self) -> paramiko.SSHClient:
        if self._client is not None:
            return self._client

        client = paramiko.SSHClient()
        if self.config.ssh.strict_host_key_checking:
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        identity_file = _identity_file_from_ssh_command(self.config.commands.ssh)
        key_filename = str(Path(identity_file).expanduser()) if identity_file else None

        # Prefer ssh-agent keys, then discovered keys, then explicit identity file if provided.
        client.connect(
            hostname=self.config.ssh.host,
            port=self.config.ssh.port,
            username=self.config.ssh.user,
            allow_agent=True,
            look_for_keys=True,
            key_filename=key_filename,
            timeout=20,
            banner_timeout=20,
            auth_timeout=20,
        )
        self._client = client
        return client

    def run(self, command: str) -> SshCommandResult:
        client = self._connect()
        stdin, stdout, stderr = client.exec_command(command)
        stdin.close()
        out = stdout.read().decode("utf-8", errors="ignore")
        err = stderr.read().decode("utf-8", errors="ignore")
        code = stdout.channel.recv_exit_status()
        return SshCommandResult(returncode=code, stdout=out, stderr=err)

    def read_file(self, path: str) -> str:
        client = self._connect()
        sftp = client.open_sftp()
        try:
            with sftp.file(path, "r") as handle:
                return handle.read().decode("utf-8", errors="ignore")
        finally:
            sftp.close()

    def open_sftp(self) -> paramiko.SFTPClient:
        return self._connect().open_sftp()

    def transport(self) -> paramiko.Transport:
        transport = self._connect().get_transport()
        if transport is None:
            raise RuntimeError("SSH transport is not available")
        return transport

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
