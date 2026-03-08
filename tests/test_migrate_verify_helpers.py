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


class DnsIpReplacementTests(unittest.TestCase):
    def test_build_ip_value_mapping_uses_source_and_target_ip_ids(self) -> None:
        class TargetStub:
            def listing(self, command: str) -> list[dict]:
                if command == "IpsAndPorts.listing":
                    return [
                        {"id": 101, "ip": "203.0.113.10"},
                        {"id": 102, "ip": "2001:db8::10"},
                    ]
                return []

        migrator = object.__new__(Migrator)
        migrator.target = TargetStub()
        domains = [
            {
                "ipsandports": [
                    {"id": 11, "ip": "198.51.100.10"},
                    {"id": 12, "ip": "2001:db8::1"},
                ]
            }
        ]

        mapping = migrator._build_ip_value_mapping(domains, {11: 101, 12: 102})

        self.assertEqual(
            {
                "198.51.100.10": "203.0.113.10",
                "2001:db8::1": "2001:db8::10",
            },
            mapping,
        )

    def test_ensure_domain_zones_rewrites_custom_a_record_content(self) -> None:
        class TargetStub:
            def __init__(self) -> None:
                self.calls: list[tuple[str, dict]] = []

            def list_domain_zones(self, domainname: str | None = None) -> list[dict]:
                return []

            def call(self, command: str, params: dict) -> None:
                self.calls.append((command, params))

        migrator = object.__new__(Migrator)
        migrator.target = TargetStub()

        migrator._ensure_domain_zones(
            [
                {
                    "domainname": "example.test",
                    "record": "@",
                    "type": "A",
                    "prio": 0,
                    "content": "198.51.100.10",
                    "ttl": 3600,
                }
            ],
            {"198.51.100.10": "203.0.113.10"},
        )

        self.assertEqual(1, len(migrator.target.calls))
        self.assertEqual("DomainZones.add", migrator.target.calls[0][0])
        self.assertEqual("203.0.113.10", migrator.target.calls[0][1]["content"])

    def test_replace_ip_tokens_updates_zonefile_ip_values(self) -> None:
        migrator = object.__new__(Migrator)
        zonefile = "@ 3600 IN A 198.51.100.10\n@ 3600 IN AAAA 2001:db8::1"

        replaced = migrator._replace_ip_tokens(
            zonefile,
            {
                "198.51.100.10": "203.0.113.10",
                "2001:db8::1": "2001:db8::10",
            },
        )

        self.assertIn("203.0.113.10", replaced)
        self.assertIn("2001:db8::10", replaced)
        self.assertNotIn("198.51.100.10", replaced.split())
        self.assertNotIn("2001:db8::1", replaced.split())


class DryRunAndNormalizeTests(unittest.TestCase):
    def test_normalize_domain_setting_for_compare_handles_regex_backslashes(self) -> None:
        migrator = object.__new__(Migrator)
        expected = r"location ~ ^/\d\.\d\.\d/ { try_files $uri \.php; }"
        actual = "location ~ ^/d.d.d/ { try_files $uri .php; }"

        self.assertEqual(
            migrator._normalize_domain_setting_for_compare(expected),
            migrator._normalize_domain_setting_for_compare(actual),
        )

    def test_execute_dry_run_returns_without_mutations(self) -> None:
        class ClientStub:
            def __init__(self) -> None:
                self.test_calls = 0

            def test_connection(self) -> None:
                self.test_calls += 1

            def call(self, command: str, params=None):
                raise AssertionError(f"Unexpected mutating API call in dry-run: {command}")

        class RunnerStub:
            dry_run = True

            def preflight_commands(self, **kwargs):
                return []

            def run(self, command: str, check: bool = True):
                return None

        migrator = object.__new__(Migrator)
        migrator.source = ClientStub()
        migrator.target = ClientStub()
        migrator.runner = RunnerStub()

        selection = type(
            "SelectionStub",
            (),
            {
                "target_customer": {"customerid": 42},
                "include_files": False,
                "include_databases": False,
                "include_mail": False,
                "domains": [],
            },
        )()

        ctx = migrator.execute(selection)  # type: ignore[arg-type]

        self.assertEqual(42, ctx.target_customer_id)
        self.assertEqual({}, ctx.source_to_target_db)
        self.assertEqual(1, migrator.source.test_calls)
        self.assertEqual(1, migrator.target.test_calls)


if __name__ == "__main__":
    unittest.main()
