from __future__ import annotations

import os
import select
import shlex
import socketserver
import subprocess
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import paramiko

if TYPE_CHECKING:
    from .config import AppConfig


class _ForwardHandler(socketserver.BaseRequestHandler):
    transport: paramiko.Transport
    remote_host: str
    remote_port: int

    def handle(self) -> None:
        channel = self.transport.open_channel(
            "direct-tcpip",
            (self.remote_host, self.remote_port),
            self.request.getpeername(),
        )
        if channel is None:
            return
        try:
            while True:
                readable, _, _ = select.select([self.request, channel], [], [])
                if self.request in readable:
                    data = self.request.recv(1024)
                    if not data:
                        break
                    channel.sendall(data)
                if channel in readable:
                    data = channel.recv(1024)
                    if not data:
                        break
                    self.request.sendall(data)
        finally:
            channel.close()
            self.request.close()


class _ForwardServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


@contextmanager
def open_ssh_tunnel(transport: paramiko.Transport, remote_host: str, remote_port: int) -> Iterator[tuple[str, int]]:
    handler = type(
        "ForwardHandler",
        (_ForwardHandler,),
        {
            "transport": transport,
            "remote_host": remote_host,
            "remote_port": remote_port,
        },
    )
    server = _ForwardServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = int(server.server_address[1])
        yield "127.0.0.1", port
    finally:
        server.shutdown()
        server.server_close()


@contextmanager
def open_ssh_unix_socket_tunnel(config: AppConfig, remote_socket: str) -> Iterator[str]:
    """Forward a remote unix socket to a local unix socket via ``ssh -L``.

    Paramiko cannot open ``direct-streamlocal@openssh.com`` channels, so this
    shells out to the OpenSSH client instead. Yields the local socket path,
    which can be passed to PyMySQL as ``unix_socket``.
    """
    with tempfile.TemporaryDirectory(prefix="froxlor-mysql-sock-") as tmpdir:
        local_socket = os.path.join(tmpdir, "mysql.sock")
        cmd = shlex.split(config.commands.ssh)
        if not cmd:
            raise RuntimeError("SSH command is empty; cannot open unix socket tunnel")
        if not config.ssh.strict_host_key_checking:
            cmd.extend(["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"])
        cmd.extend(
            [
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=15",
                "-N",
                "-L",
                f"{local_socket}:{remote_socket}",
                "-p",
                str(config.ssh.port),
                "-l",
                config.ssh.user,
                config.ssh.host,
            ]
        )

        process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            ready = False
            for _ in range(50):
                if process.poll() is not None:
                    break
                if os.path.exists(local_socket):
                    ready = True
                    break
                time.sleep(0.1)
            if not ready:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except Exception:
                        process.kill()
                        process.wait()
                stderr_text = ""
                if process.stderr is not None:
                    stderr_text = process.stderr.read().strip()
                raise RuntimeError(f"Could not establish SSH unix socket tunnel for MySQL: {stderr_text[:300]}")
            yield local_socket
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except Exception:
                    process.kill()
