from __future__ import annotations

import unittest
from types import SimpleNamespace

from froxlor_migrator.froxlor_mysql import extract_sql_credentials, extract_sql_root_credentials, mysql_defaults_content
from froxlor_migrator.migrate import Migrator


class CustomerPayloadTests(unittest.TestCase):
    def test_allowed_phpconfigs_remapped_via_php_setting_map(self) -> None:
        migrator = object.__new__(Migrator)
        payload = migrator._customer_payload(
            {
                "email": "user@example.test",
                "allowed_phpconfigs": "[2, 5]",
                "allowed_mysqlserver": "3",
                "hosting_plan_id": 7,
            },
            {2: 20, 5: 50},
        )

        self.assertEqual([20, 50], payload["allowed_phpconfigs"])
        # Source-only identifiers must not be sent to the target API.
        self.assertNotIn("allowed_mysqlserver", payload)
        self.assertNotIn("hosting_plan_id", payload)
        self.assertNotIn("adminid", payload)

    def test_allowed_phpconfigs_omitted_when_unmapped_or_empty(self) -> None:
        migrator = object.__new__(Migrator)
        payload = migrator._customer_payload({"email": "user@example.test", "allowed_phpconfigs": ""}, {1: 10})

        self.assertNotIn("allowed_phpconfigs", payload)

        payload = migrator._customer_payload({"email": "user@example.test", "allowed_phpconfigs": "[9]"}, {1: 10})
        self.assertNotIn("allowed_phpconfigs", payload)

    def test_php_map_fallback_keeps_existing_target_id(self) -> None:
        migrator = object.__new__(Migrator)
        migrator.target = SimpleNamespace(list_php_settings=lambda: [{"id": 3}, {"id": 5}])

        resolved = migrator._php_map_with_customer_fallback({"phpenabled": 1, "allowed_phpconfigs": "[2, 5]"}, {2: 20})

        self.assertEqual({2: 20, 5: 5}, resolved)

    def test_php_map_fallback_uses_first_target_when_source_id_absent(self) -> None:
        migrator = object.__new__(Migrator)
        migrator.target = SimpleNamespace(list_php_settings=lambda: [{"id": 4}, {"id": 7}])

        resolved = migrator._php_map_with_customer_fallback({"phpenabled": 1, "allowed_phpconfigs": "[9]"}, {})

        self.assertEqual({9: 4}, resolved)

    def test_php_map_fallback_skipped_without_php(self) -> None:
        migrator = object.__new__(Migrator)

        def _boom() -> list[dict]:
            raise AssertionError("list_php_settings must not be called")

        migrator.target = SimpleNamespace(list_php_settings=_boom)

        resolved = migrator._php_map_with_customer_fallback({"phpenabled": 0, "allowed_phpconfigs": "[9]"}, {})

        self.assertEqual({}, resolved)

    def test_extract_sql_root_credentials_from_userdata(self) -> None:
        content = """
<?php
// Managed by Ansible - froxlor role
$sql['host']     = 'localhost';
$sql['user']     = 'froxlor';
$sql['password'] = '11111111';
$sql['db']       = 'froxlor';
$sql_root[0]['caption']  = 'localhost';
$sql_root[0]['host']     = 'localhost';
$sql_root[0]['user']     = 'root';
$sql_root[0]['password'] = '222222222';
// enable debugging to browser in case of SQL errors
$sql['debug'] = false;
"""
        creds = extract_sql_root_credentials(content)
        self.assertEqual(
            {
                "host": "localhost",
                "user": "root",
                "password": "222222222",
            },
            creds,
        )

    def test_extract_credentials_value_may_contain_other_quote(self) -> None:
        content = """
<?php
$sql['host'] = 'localhost';
$sql['user'] = 'froxlor';
$sql['password'] = 'pa"ss;with;junk';
$sql_root[0]['host'] = 'db.internal';
$sql_root[0]['user'] = "ro'ot";
$sql_root[0]['password'] = "double'quoted;";
"""
        sql_creds = extract_sql_credentials(content)
        root_creds = extract_sql_root_credentials(content)

        self.assertEqual('pa"ss;with;junk', sql_creds["password"])
        self.assertEqual("ro'ot", root_creds["user"])
        self.assertEqual("double'quoted;", root_creds["password"])

    def test_extract_sql_root_credentials_keeps_single_index_consistent(self) -> None:
        content = """
<?php
$sql_root[0]['host'] = '127.0.0.1';
$sql_root[0]['user'] = 'root';
$sql_root[0]['password'] = '';
$sql_root[1]['host'] = 'localhost';
$sql_root[1]['user'] = 'froxlor_root';
$sql_root[1]['password'] = 'secret';
"""
        creds = extract_sql_root_credentials(content)
        self.assertEqual(
            {
                "host": "localhost",
                "user": "froxlor_root",
                "password": "secret",
            },
            creds,
        )

    def test_build_mysql_defaults_content(self) -> None:
        content = mysql_defaults_content({
            "user": "root",
            "password": "pw",
            "host": "localhost",
            "socket": "/run/mysqld/mysqld.sock",
        })
        self.assertIn("[client]\n", content)
        self.assertIn("user=root\n", content)
        self.assertIn("password=pw\n", content)
        self.assertIn("socket=/run/mysqld/mysqld.sock\n", content)


if __name__ == "__main__":
    unittest.main()
