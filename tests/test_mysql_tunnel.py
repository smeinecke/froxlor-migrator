from __future__ import annotations

import socket
import threading
import time
import unittest
from unittest.mock import patch

from froxlor_migrator.mysql_tunnel import _ForwardHandler, open_ssh_tunnel


class DummyTransport:
    def __init__(self):
        # We create a socketpair that will be used as the "remote" end of the tunnel.
        self.local_socket, self.remote_socket = socket.socketpair()
        self.local_socket.settimeout(2)
        self.remote_socket.settimeout(2)

    def open_channel(self, kind: str, dest: tuple[str, int], src: tuple[str, int]):
        # Return the "remote" end that the handler will write to and read from.
        return self.remote_socket


class MysqlTunnelTests(unittest.TestCase):
    def test_forward_handler_exits_cleanly_when_channel_none(self) -> None:
        # If the transport cannot open a channel, the handler should just return.
        class BrokenTransport:
            def open_channel(self, *_: object, **__: object) -> None:
                return None

        req, _ = socket.socketpair()
        handler = _ForwardHandler.__new__(_ForwardHandler)
        handler.request = req
        handler.transport = BrokenTransport()
        handler.remote_host = "127.0.0.1"
        handler.remote_port = 3306
        handler.client_address = ("127.0.0.1", 0)
        handler.server = None

        # Should not raise or block for long.
        handler.handle()

    def test_forward_handler_forwards_data_between_request_and_channel(self) -> None:
        # Use fake socket-like objects and patch select.select to exercise both data paths.
        class FakeSocket:
            def __init__(self, responses: list[bytes]):
                self._responses = responses
                self.sent: list[bytes] = []

            def recv(self, _size: int) -> bytes:
                return self._responses.pop(0) if self._responses else b""

            def sendall(self, data: bytes) -> None:
                self.sent.append(data)

            def close(self) -> None:
                pass

            def getpeername(self) -> tuple[str, int]:
                return ("127.0.0.1", 1234)

            def fileno(self) -> int:
                # select requires a fileno; return a stable integer.
                return 1

        request = FakeSocket([b"from_request", b""])
        channel = FakeSocket([b"from_channel", b""])

        select_sequence = iter([
            ([request], [], []),
            ([channel], [], []),
            ([request], [], []),
        ])

        def fake_select(r, w, x, *args, **kwargs):
            return next(select_sequence)

        class DummyTransport:
            def open_channel(self, *_: object, **__: object) -> FakeSocket:
                return channel

        handler = _ForwardHandler.__new__(_ForwardHandler)
        handler.request = request
        handler.transport = DummyTransport()
        handler.remote_host = "127.0.0.1"
        handler.remote_port = 3306
        handler.client_address = ("127.0.0.1", 0)
        handler.server = None

        with patch("froxlor_migrator.mysql_tunnel.select.select", fake_select):
            handler.handle()

        # Ensure data was forwarded in both directions.
        self.assertEqual([b"from_request"], channel.sent)
        self.assertEqual([b"from_channel"], request.sent)

    def test_open_ssh_tunnel_context_manager_closes_server(self) -> None:
        transport = DummyTransport()
        with open_ssh_tunnel(transport, "127.0.0.1", 3306) as (host, port):
            conn = socket.create_connection((host, port), timeout=2)
            conn.close()

        # Server should be gone after exiting the context manager
        with self.assertRaises(ConnectionRefusedError):
            socket.create_connection((host, port), timeout=0.5)
