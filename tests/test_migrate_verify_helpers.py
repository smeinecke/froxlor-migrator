from __future__ import annotations

import unittest

from froxlor_migrator.migrate import Migrator
from froxlor_migrator.migration.types import Selection
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

        target = TargetStub()
        migrator = object.__new__(Migrator)
        migrator.target = target  # type: ignore[assignment]
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

        target = TargetStub()
        migrator = object.__new__(Migrator)
        migrator.target = target  # type: ignore[assignment]

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

        self.assertEqual(1, len(target.calls))
        self.assertEqual("DomainZones.add", target.calls[0][0])
        self.assertEqual("203.0.113.10", target.calls[0][1]["content"])

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

        source = ClientStub()
        target = ClientStub()
        runner = RunnerStub()
        migrator = object.__new__(Migrator)
        migrator.source = source  # type: ignore[assignment]
        migrator.target = target  # type: ignore[assignment]
        migrator.runner = runner  # type: ignore[assignment]

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
        self.assertEqual(1, source.test_calls)
        self.assertEqual(1, target.test_calls)


class MysqlPrefixSyncTests(unittest.TestCase):
    def test_source_mysql_prefix_setting_reads_panel_setting(self) -> None:
        migrator = object.__new__(Migrator)
        migrator._run_source_panel_query = lambda sql: [["NONE"]]  # type: ignore[method-assign]
        self.assertEqual("NONE", migrator._source_mysql_prefix_setting())

    def test_sync_target_mysql_prefix_setting_updates_target(self) -> None:
        migrator = object.__new__(Migrator)
        migrator._run_source_panel_query = lambda sql: [["DBNAME"]]  # type: ignore[method-assign]
        calls: list[str] = []
        migrator._exec_target_panel_sql = lambda sql: calls.append(sql)  # type: ignore[method-assign]
        migrator._sync_target_mysql_prefix_setting()
        self.assertEqual(1, len(calls))
        self.assertIn("varname='mysqlprefix'", calls[0])
        self.assertIn("0x44424e414d45", calls[0])


class ExecuteToggleTests(unittest.TestCase):
    def test_execute_skips_certificates_dns_and_password_sync_when_disabled(self) -> None:
        class ClientStub:
            def test_connection(self) -> None:
                return

        class RunnerStub:
            dry_run = False

            def preflight_commands(self, **kwargs):
                return []

            def run(self, command: str, check: bool = True):
                return None

            def transfer_files(self, source_dir: str, target_dir: str) -> None:  # noqa: ARG002
                return

            def transfer_mailbox(self, mailbox: str) -> None:  # noqa: ARG002
                return

        source = ClientStub()
        target = ClientStub()
        runner = RunnerStub()
        migrator = object.__new__(Migrator)
        migrator.source = source  # type: ignore[assignment]
        migrator.target = target  # type: ignore[assignment]
        migrator.runner = runner  # type: ignore[assignment]

        calls: list[str] = []
        migrator._emit_progress = lambda step, total, status: None  # type: ignore[method-assign]
        migrator._ensure_target_customer = lambda customer, target_customer: 23  # type: ignore[method-assign]
        migrator._build_ip_value_mapping = lambda domains, ip_mapping: {}  # type: ignore[method-assign]
        migrator._ensure_domains = lambda *args, **kwargs: calls.append("domains")  # type: ignore[method-assign]
        migrator._sync_domain_redirects = lambda domains: calls.append("redirects")  # type: ignore[method-assign]
        migrator._ensure_subdomains = lambda *args, **kwargs: calls.append("subdomains")  # type: ignore[method-assign]
        migrator._migrate_domain_certificates = lambda domains: calls.append("certs")  # type: ignore[method-assign]
        migrator._ensure_ftp_accounts = lambda *args, **kwargs: calls.append("ftps")  # type: ignore[method-assign]
        migrator._ensure_ssh_keys = lambda *args, **kwargs: calls.append("ssh")  # type: ignore[method-assign]
        migrator._ensure_data_dumps = lambda *args, **kwargs: calls.append("dumps")  # type: ignore[method-assign]
        migrator._ensure_dir_options = lambda *args, **kwargs: calls.append("dir_options")  # type: ignore[method-assign]
        migrator._ensure_dir_protections = lambda *args, **kwargs: calls.append("dir_protections")  # type: ignore[method-assign]
        migrator._ensure_domain_zones = lambda *args, **kwargs: calls.append("zones")  # type: ignore[method-assign]
        migrator._enable_letsencrypt_after_dns = lambda domains: calls.append("letsencrypt")  # type: ignore[method-assign]
        migrator._ensure_mailboxes = lambda target_customer_id, mailboxes: []  # type: ignore[method-assign]
        migrator._ensure_email_forwarders = lambda *args, **kwargs: calls.append("forwarders")  # type: ignore[method-assign]
        migrator._ensure_email_sender_aliases = lambda *args, **kwargs: calls.append("senders")  # type: ignore[method-assign]
        migrator._sync_password_hashes = lambda *args, **kwargs: calls.append("passwords")  # type: ignore[method-assign]

        selection = Selection(
            customer={"loginname": "custalpha", "customerid": 1},
            target_customer=None,
            domains=[],
            subdomains=[],
            databases=[],
            mailboxes=[],
            email_forwarders=[],
            email_senders=[],
            ftp_accounts=[],
            ssh_keys=[],
            data_dumps=[],
            dir_protections=[],
            dir_options=[],
            domain_zones=[],
            include_files=False,
            include_databases=False,
            include_mail=False,
            include_subdomains=False,
            validate_database_names=True,
            php_setting_map={},
            ip_mapping={},
            include_certificates=False,
            include_domain_zones=False,
            include_password_sync=False,
            include_forwarders=False,
            include_sender_aliases=False,
        )

        context = migrator.execute(selection)

        self.assertEqual(23, context.target_customer_id)
        self.assertNotIn("certs", calls)
        self.assertNotIn("zones", calls)
        self.assertNotIn("passwords", calls)
        self.assertNotIn("forwarders", calls)
        self.assertNotIn("senders", calls)


if __name__ == "__main__":
    unittest.main()
