from __future__ import annotations

import unittest
from types import SimpleNamespace

from froxlor_migrator.migrate import Migrator
from froxlor_migrator.migration.types import MigrationError


class DatabaseFallbackTests(unittest.TestCase):
    def test_default_mysql_server_from_allowed(self) -> None:
        migrator = object.__new__(Migrator)

        self.assertEqual(0, migrator._default_mysql_server_from_allowed(""))
        self.assertEqual(0, migrator._default_mysql_server_from_allowed("[0,2]"))
        self.assertEqual(2, migrator._default_mysql_server_from_allowed("[2]"))
        self.assertEqual(1, migrator._default_mysql_server_from_allowed("[3,1,2]"))

    def test_fallback_last_account_number(self) -> None:
        migrator = object.__new__(Migrator)

        self.assertEqual(7, migrator._fallback_last_account_number("cust_7", "cust", "_"))
        self.assertEqual(0, migrator._fallback_last_account_number("cust_blog", "cust", "DBNAME"))
        self.assertEqual(0, migrator._fallback_last_account_number("cust-r4x", "cust", "RANDOM"))
        self.assertEqual(0, migrator._fallback_last_account_number("other_7", "cust", "_"))

    def test_create_database_on_target_uses_manual_creation_without_api_call(self) -> None:
        class TargetStub:
            def call(self, command: str, params=None):
                raise AssertionError(f"unexpected call: {command}")

        migrator = object.__new__(Migrator)
        migrator.target = TargetStub()
        migrator.config = SimpleNamespace(behavior=SimpleNamespace(database_exists="fail"))

        recreate_calls: list[tuple[int, str, str]] = []
        migrator._recreate_database_like_froxlor = lambda cid, name, desc: recreate_calls.append((cid, name, desc))  # type: ignore[method-assign]

        created = migrator._create_database_on_target(
            10,
            {"databasename": "cust_5", "description": "test db"},
            {},
        )

        self.assertEqual("cust_5", created)
        self.assertEqual([(10, "cust_5", "test db")], recreate_calls)

    def test_create_database_on_target_skip_policy_returns_none_for_own_db(self) -> None:
        migrator = object.__new__(Migrator)
        migrator.config = SimpleNamespace(behavior=SimpleNamespace(database_exists="skip"))

        created = migrator._create_database_on_target(
            10,
            {"databasename": "cust_5"},
            {"cust_5": 10},
        )
        self.assertIsNone(created)

    def test_create_database_on_target_update_policy_restores_own_db(self) -> None:
        migrator = object.__new__(Migrator)
        migrator.config = SimpleNamespace(behavior=SimpleNamespace(database_exists="update"))

        created = migrator._create_database_on_target(
            10,
            {"databasename": "cust_5"},
            {"cust_5": 10},
        )
        self.assertEqual("cust_5", created)

    def test_create_database_on_target_refuses_foreign_owned_db(self) -> None:
        migrator = object.__new__(Migrator)
        migrator.config = SimpleNamespace(behavior=SimpleNamespace(database_exists="update"))
        migrator._recreate_database_like_froxlor = lambda *a, **kw: self.fail("must not recreate foreign db")

        with self.assertRaises(MigrationError):
            migrator._create_database_on_target(
                10,
                {"databasename": "cust_5"},
                {"cust_5": 99},
            )

    def test_create_database_on_target_fail_policy_raises_for_own_db(self) -> None:
        migrator = object.__new__(Migrator)
        migrator.config = SimpleNamespace(behavior=SimpleNamespace(database_exists="fail"))

        with self.assertRaises(MigrationError):
            migrator._create_database_on_target(
                10,
                {"databasename": "cust_5"},
                {"cust_5": 10},
            )

    def test_recreate_database_uses_if_not_exists_for_idempotent_retries(self) -> None:
        migrator = object.__new__(Migrator)
        migrator._run_target_panel_query = lambda sql: [["custalpha", "[0]", "4"]] if "panel_customers" in sql else [["0"]]  # type: ignore[method-assign]
        sql_calls: list[str] = []
        migrator._exec_target_mysql_sql = lambda sql, db: sql_calls.append(sql)  # type: ignore[method-assign]
        migrator._exec_target_panel_sql = lambda sql: None  # type: ignore[method-assign]
        migrator._target_mysql_access_hosts = lambda: ["localhost"]  # type: ignore[method-assign]
        migrator._default_mysql_server_from_allowed = lambda _: 0  # type: ignore[method-assign]
        migrator._target_mysql_user_exists = lambda user, host: False  # type: ignore[method-assign]
        migrator._target_mysql_prefix_setting = lambda: "_"  # type: ignore[method-assign]

        migrator._recreate_database_like_froxlor(10, "custalpha_wpdemo", "test")

        self.assertTrue(any(sql.startswith("CREATE DATABASE IF NOT EXISTS `custalpha_wpdemo`") for sql in sql_calls))


if __name__ == "__main__":
    unittest.main()
