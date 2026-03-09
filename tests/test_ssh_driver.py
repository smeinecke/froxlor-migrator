from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from froxlor_migrator.config import load_config
from froxlor_migrator.ssh_driver import SshDriver, _identity_file_from_ssh_command


class SshClientStub:
    def __init__(self):
        self.connected = False
        self.connect_kwargs = {}
        self._transport = object()

    def load_system_host_keys(self):
        pass

    def set_missing_host_key_policy(self, policy):
        self._policy = policy

    def connect(self, **kwargs):
        self.connected = True
        self.connect_kwargs = kwargs

    def exec_command(self, command: str):
        class Channel:
            def recv_exit_status(self_inner):
                return 0

        class File:
            def __init__(self):
                self.channel = Channel()

            def read(self):
                return b"out"

            def decode(self, *_):
                return "out"

            def close(self):
                pass

        return (File(), File(), File())

    def open_sftp(self):
        class FileHandle:
            def __init__(self):
                self._content = b"hello"

            def read(self):
                return self._content

            def decode(self, *_):
                return "hello"

            def close(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class SFTP:
            def file(self, path, mode):
                return FileHandle()

            def close(self):
                pass

        return SFTP()

    def get_transport(self):
        return self._transport

    def close(self):
        self.connected = False


class SshDriverTests(unittest.TestCase):
    def setUp(self) -> None:
        content = """
        [source]
        api_url = "https://s"
        api_key = "k"
        api_secret = "s"

        [target]
        api_url = "https://t"
        api_key = "k"
        api_secret = "s"

        [ssh]
        host = "localhost"
        user = "root"
        strict_host_key_checking = true

        [paths]
        source_web_root = "/var/www"
        target_web_root = "/var/www"

        [commands]
        ssh = "ssh -i /tmp/id"
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.toml"
            path.write_text(content, encoding="utf-8")
            self.config = load_config(path)

    def test_identity_file_parsing(self) -> None:
        self.assertEqual("/tmp/id", _identity_file_from_ssh_command("ssh -i /tmp/id"))
        self.assertEqual("/tmp/id", _identity_file_from_ssh_command("ssh -i/tmp/id"))
        self.assertIsNone(_identity_file_from_ssh_command("ssh"))

    @patch("froxlor_migrator.ssh_driver.paramiko.SSHClient", autospec=True)
    def test_connect_uses_strict_host_key_checking(self, ssh_client_cls):
        stub = SshClientStub()
        ssh_client_cls.return_value = stub

        driver = SshDriver(self.config)
        client = driver._connect()
        self.assertTrue(stub.connected)
        self.assertEqual("localhost", client.connect_kwargs["hostname"])

    @patch("froxlor_migrator.ssh_driver.paramiko.SSHClient", autospec=True)
    def test_run_and_read_file_work(self, ssh_client_cls):
        stub = SshClientStub()
        ssh_client_cls.return_value = stub
        driver = SshDriver(self.config)

        result = driver.run("echo hi")
        self.assertEqual(0, result.returncode)
        self.assertEqual("out", result.stdout)

        self.assertEqual("hello", driver.read_file("/tmp/dummy"))

    @patch("froxlor_migrator.ssh_driver.paramiko.SSHClient", autospec=True)
    def test_transport_raises_if_none(self, ssh_client_cls):
        stub = SshClientStub()
        stub._transport = None
        ssh_client_cls.return_value = stub

        driver = SshDriver(self.config)
        with self.assertRaises(RuntimeError):
            driver.transport()


if __name__ == "__main__":
    unittest.main()
