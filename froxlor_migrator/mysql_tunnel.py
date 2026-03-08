from __future__ import annotations

import select
import socketserver
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import paramiko


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
