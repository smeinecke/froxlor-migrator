from __future__ import annotations

import unittest

from froxlor_migrator.migrate import Migrator
from froxlor_migrator.verify_migration import (
    _compare_dir_option,
    _compare_dir_protection,
    _is_custom_zone_record,
)


class HelperParityTests(unittest.TestCase):
    def test_custom_zone_record_classification_parity(self) -> None:
        migrator = object.__new__(Migrator)
        rows = [
            ({"type": "A", "record": "www", "content": "1.2.3.4"}, True),
            ({"type": "NS", "record": "@", "content": "ns1.example.tld"}, False),
            ({"type": "SOA", "record": "@", "content": "ns1.example.tld"}, False),
            ({"type": "TXT", "is_default": 1, "content": "default"}, False),
            ({"type": "MX", "default_entry": "1", "content": "mail.example.tld"}, False),
        ]

        for row, expected in rows:
            with self.subTest(row=row):
                self.assertEqual(expected, migrator._is_custom_zone_record(row))
                self.assertEqual(expected, _is_custom_zone_record(row))

    def test_compare_dir_protection_detects_password_mismatch(self) -> None:
        errs = _compare_dir_protection(
            {"path": "secure", "username": "alice", "authname": "Restricted", "password": "hash-a"},
            {"path": "secure", "username": "alice", "authname": "Restricted", "password": "hash-b"},
        )
        self.assertEqual(["password source='hash-a' target='hash-b'"], errs)

    def test_compare_dir_option_detects_flag_mismatch(self) -> None:
        errs = _compare_dir_option(
            {
                "options_indexes": 1,
                "options_cgi": 0,
                "error404path": "",
                "error403path": "",
                "error500path": "",
                "error401path": "",
            },
            {
                "options_indexes": 0,
                "options_cgi": 0,
                "error404path": "",
                "error403path": "",
                "error500path": "",
                "error401path": "",
            },
        )
        self.assertEqual(["options_indexes source=True target=False"], errs)


if __name__ == "__main__":
    unittest.main()
