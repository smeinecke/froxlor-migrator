from __future__ import annotations

import unittest
from contextlib import contextmanager

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
from froxlor_migrator.migrate import MigrationError, Migrator


def _config() -> AppConfig:
    return AppConfig(
        source=ApiConfig(api_url="https://source.invalid/api.php", api_key="k", api_secret="s"),
        target=ApiConfig(api_url="https://target.invalid/api.php", api_key="k", api_secret="s"),
        ssh=SshConfig(host="example.invalid", user="root", port=22, strict_host_key_checking=True),
        paths=PathsConfig(source_web_root="/src", source_transfer_root="/src", target_web_root="/dst"),
        mysql=MysqlConfig(source_panel_database="froxlor", target_panel_database="froxlor"),
        commands=CommandsConfig(),
        behavior=BehaviorConfig(),
        output=OutputConfig(manifest_dir="./manifests"),
    )


class _RunnerStub:
    dry_run = False

    def debug_event(self, message: str, **payload) -> None:  # noqa: ANN003
        return


class PanelDbFallbackTests(unittest.TestCase):
    def test_exec_target_panel_sql_does_not_use_remote_cli_fallback(self) -> None:
        migrator = object.__new__(Migrator)
        migrator.config = _config()  # type: ignore[assignment]
        migrator.runner = _RunnerStub()  # type: ignore[assignment]

        @contextmanager
        def _failing_tunnel():
            raise RuntimeError("tunnel connect failed")
            yield {}  # pragma: no cover

        migrator._target_mysql_connect_kwargs = _failing_tunnel  # type: ignore[method-assign]
        migrator._run_target_mysql_via_remote_cli = lambda sql, db: (_ for _ in ()).throw(AssertionError("remote CLI fallback must not be used"))  # type: ignore[method-assign]

        with self.assertRaises(MigrationError) as exc:
            migrator._exec_target_mysql_sql("SELECT 1;", "froxlor")

        self.assertIn("fallback disabled", str(exc.exception))

    def test_query_target_panel_sql_does_not_use_remote_cli_fallback(self) -> None:
        migrator = object.__new__(Migrator)
        migrator.config = _config()  # type: ignore[assignment]
        migrator.runner = _RunnerStub()  # type: ignore[assignment]

        @contextmanager
        def _failing_tunnel():
            raise RuntimeError("tunnel connect failed")
            yield {}  # pragma: no cover

        migrator._target_mysql_connect_kwargs = _failing_tunnel  # type: ignore[method-assign]
        migrator._run_target_mysql_via_remote_cli = lambda sql, db: (_ for _ in ()).throw(AssertionError("remote CLI fallback must not be used"))  # type: ignore[method-assign]

        with self.assertRaises(MigrationError) as exc:
            migrator._run_target_mysql_query("SELECT 1;", "froxlor")

        self.assertIn("fallback disabled", str(exc.exception))


if __name__ == "__main__":
    unittest.main()
