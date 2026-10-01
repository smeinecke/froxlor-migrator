from __future__ import annotations

import unittest

from froxlor_migrator.froxlor_mysql import extract_sql_root_credentials, mysql_defaults_content
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
