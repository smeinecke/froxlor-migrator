from __future__ import annotations

import logging
import shlex
import time
from dataclasses import dataclass
from pathlib import Path

import paramiko

from .config import AppConfig

logger = logging.getLogger(__name__)


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
            # Deliberate opt-out when strict_host_key_checking=false.
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # nosec B507

        identity_file = _identity_file_from_ssh_command(self.config.commands.ssh)
        key_filename = str(Path(identity_file).expanduser()) if identity_file else None

        # Prefer ssh-agent keys, then discovered keys, then explicit identity file if provided.
        logger.debug(
            "Opening SSH connection: host=%s port=%s user=%s strict_host_key_checking=%s key_filename=%s",
            self.config.ssh.host,
            self.config.ssh.port,
            self.config.ssh.user,
            self.config.ssh.strict_host_key_checking,
            key_filename or "",
        )
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
        logger.debug("SSH connection established: host=%s port=%s", self.config.ssh.host, self.config.ssh.port)
        self._client = client
        return client

    def run(self, command: str, timeout: float | None = None, sensitive: bool = False) -> SshCommandResult:
        if timeout is None:
            timeout = float(getattr(self.config.ssh, "command_timeout_seconds", 3600))
        if timeout <= 0:
            timeout = None
        client = self._connect()
        logger.debug("SSH command start: %s", "[redacted]" if sensitive else command)
        stdin, stdout, _stderr = client.exec_command(command)
        stdin.close()
        channel = stdout.channel

        # Drain stdout and stderr concurrently: they share a single channel
        # window, so a remote process filling the stderr buffer while we block
        # on stdout.read() deadlocks the channel.
        out_parts: list[bytes] = []
        err_parts: list[bytes] = []
        deadline = time.monotonic() + timeout if timeout else None
        while True:
            while channel.recv_ready():
                out_parts.append(channel.recv(65536))
            while channel.recv_stderr_ready():
                err_parts.append(channel.recv_stderr(65536))
            # The exit status can arrive while data packets are still in
            # flight — only stop once the remote signaled EOF (all stream
            # data received) or the channel was closed.
            if channel.exit_status_ready() and (channel.eof_received or channel.closed):
                while channel.recv_ready():
                    out_parts.append(channel.recv(65536))
                while channel.recv_stderr_ready():
                    err_parts.append(channel.recv_stderr(65536))
                break
            if deadline is not None and time.monotonic() > deadline:
                channel.close()
                raise TimeoutError(f"SSH command timed out after {timeout}s: {'[redacted]' if sensitive else command[:200]}")
            time.sleep(0.01)
        code = channel.recv_exit_status()
        channel.close()
        out = b"".join(out_parts).decode("utf-8", errors="ignore")
        err = b"".join(err_parts).decode("utf-8", errors="ignore")
        logger.debug("SSH command result: returncode=%s command=%s", code, "[redacted]" if sensitive else command)
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
