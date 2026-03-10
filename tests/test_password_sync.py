from __future__ import annotations

import unittest

from froxlor_migrator.migrate import MigrationError, Migrator


class PasswordSyncTests(unittest.TestCase):
    def test_sql_utf8_literal_handles_empty_string(self) -> None:
        migrator = object.__new__(Migrator)
        self.assertEqual("''", migrator._sql_utf8_literal(""))

    def test_sql_string_literal_escapes_mysql_special_chars(self) -> None:
        migrator = object.__new__(Migrator)
        literal = migrator._sql_string_literal("a'\\b\n\r\x00\x1a")
        self.assertEqual("'a\\'\\\\b\\n\\r\\0\\Z'", literal)

    def test_sync_mail_password_hashes_updates_target_table(self) -> None:
        migrator = object.__new__(Migrator)
        migrator._load_source_mail_password_hashes = lambda mailboxes: {"alerts@example.test": ("hash1", "enc1")}
        executed: list[str] = []
        migrator._exec_target_panel_sql = lambda sql: executed.append(sql)

        migrator._sync_mail_password_hashes(42, [{"email_full": "alerts@example.test"}])

        self.assertEqual(1, len(executed))
        self.assertIn("UPDATE mail_users", executed[0])
        self.assertIn("customerid=42", executed[0])

    def test_sync_mail_password_hashes_skips_missing_source_hash(self) -> None:
        migrator = object.__new__(Migrator)
        migrator._load_source_mail_password_hashes = lambda mailboxes: {}
        executed: list[str] = []
        migrator._exec_target_panel_sql = lambda sql: executed.append(sql)

        migrator._sync_mail_password_hashes(7, [{"email_full": "ops@example.test"}])
        self.assertEqual([], executed)

    def test_sync_database_login_hashes_fails_if_source_user_missing(self) -> None:
        migrator = object.__new__(Migrator)
        migrator._load_source_database_user_hashes = lambda source_db_names: {}
        migrator._exec_target_mysql_sql = lambda sql, database: None

        with self.assertRaises(MigrationError):
            migrator._sync_database_login_hashes({"srcdb": "srcdb"})

    def test_sync_customer_2fa_settings_updates_customer_table(self) -> None:
        migrator = object.__new__(Migrator)
        executed: list[str] = []
        migrator._exec_target_panel_sql = lambda sql: executed.append(sql)

        migrator._sync_customer_2fa_settings({"type_2fa": 1, "data_2fa": "seed2fa"}, 11)

        self.assertEqual(1, len(executed))
        self.assertIn("UPDATE panel_customers", executed[0])
        self.assertIn("type_2fa=1", executed[0])


if __name__ == "__main__":
    unittest.main()
