from __future__ import annotations

import unittest
from types import SimpleNamespace

from froxlor_migrator.migration.domains import MigratorDomainOps
from froxlor_migrator.migration.types import MigrationError


class DummyDomainOps(MigratorDomainOps):
    def __init__(self):
        self.source = SimpleNamespace(listing=lambda command: [])
        self.target = SimpleNamespace(
            listing=lambda command: [],
            call=lambda *args, **kwargs: None,
            list_domain_zones=lambda domainname=None: [],
        )
        self.runner = SimpleNamespace(dry_run=True)
        # minimal config attributes used by some helpers
        self.config = SimpleNamespace(
            paths=SimpleNamespace(source_web_root="/var/www", source_transfer_root="/var/www/transfer", target_web_root="/var/www"),
            behavior=SimpleNamespace(domain_exists="skip"),
        )

    def _domain_name(self, domain):
        return str(domain.get("domain") or domain.get("domainname") or "").strip().lower()

    def _exec_target_panel_sql(self, sql: str) -> None:
        self._last_sql = sql

    def _get_target_domain(self, domain_name: str):
        return None

    def _run_source_panel_query(self, sql: str):
        return []

    def _run_target_panel_query(self, sql: str):
        return []

    # Required by the base class logic
    def _sql_utf8_literal(self, value: str) -> str:
        return f"'{value}'"

    def _sql_string_literal(self, value: str) -> str:
        return f"'{value}'"


class MigratorDomainOpsTests(unittest.TestCase):
    def test_replace_ip_tokens_replaces_only_tokens(self) -> None:
        ops = DummyDomainOps()
        self.assertEqual("9.9.9.9 foo", ops._replace_ip_tokens("1.2.3.4 foo", {"1.2.3.4": "9.9.9.9"}))

    def test_normalize_domain_setting_for_compare_removes_escaped_backslashes(self) -> None:
        ops = DummyDomainOps()
        self.assertEqual("abc", ops._normalize_domain_setting_for_compare("a\\bc"))

    def test_mapped_domain_ip_ids_returns_ssl_ips(self) -> None:
        ops = DummyDomainOps()
        domain = {"ipsandports": [{"id": 1, "ssl": 1}, {"id": 2, "ssl": 0}]}
        ip_mapping = {1: 10, 2: 20}
        mapped, ssl_mapped = ops._mapped_domain_ip_ids(domain, ip_mapping)
        self.assertEqual([10, 20], mapped)
        self.assertEqual([10], ssl_mapped)

    def test_is_custom_zone_record_detects_default_records(self) -> None:
        ops = DummyDomainOps()
        self.assertFalse(ops._is_custom_zone_record({"is_default": 1}))
        self.assertFalse(ops._is_custom_zone_record({"type": "NS"}))
        self.assertTrue(ops._is_custom_zone_record({"type": "A"}))

    def test_build_ip_value_mapping_uses_source_and_target_listings(self) -> None:
        ops = DummyDomainOps()
        # Setup source domain without ips so it triggers listing lookup
        ops.source = SimpleNamespace(listing=lambda command: [{"id": 2, "ip": "1.1.1.1"}])
        ops.target = SimpleNamespace(listing=lambda command: [{"id": 100, "ip": "2.2.2.2"}])
        domains = [{"ipsandports": []}]
        mapping = ops._build_ip_value_mapping(domains, {2: 100})
        self.assertEqual({"1.1.1.1": "2.2.2.2"}, mapping)

    def test_load_source_domain_redirects_filters_invalid_rows(self) -> None:
        ops = DummyDomainOps()
        ops._run_source_panel_query = lambda sql: [["src", "dest", 301], ["", "x", 301]]
        redirects = ops._load_source_domain_redirects([{"domain": "src"}])
        self.assertEqual([("src", "dest", 301)], redirects)

    def test_sync_domain_redirects_executes_sql(self) -> None:
        ops = DummyDomainOps()
        ops._load_source_domain_redirects = lambda domains: [("a", "b", 301)]
        ops._exec_target_panel_sql = lambda sql: setattr(ops, "executed", sql)
        ops._sync_domain_redirects([{}])
        self.assertIn("UPDATE panel_domains", ops.executed)

    def test_resolve_source_and_target_docroot_paths(self) -> None:
        ops = DummyDomainOps()
        domain = {"documentroot": "/var/www/customer/site"}
        self.assertEqual("/var/www/transfer/customer/site", ops._resolve_source_docroot(domain, "customer"))
        self.assertEqual("/var/www/customer/site", ops._resolve_target_docroot(domain, "customer", "/var/www/transfer/customer/site"))

    def test_fix_transferred_docroot_ownership_skips_in_dry_run(self) -> None:
        ops = DummyDomainOps()
        ops.runner.dry_run = True
        ops.runner.run_remote = lambda cmd: setattr(ops, "ran", cmd)
        ops._fix_transferred_docroot_ownership("/tmp/foo", "src", "tgt")
        self.assertFalse(hasattr(ops, "ran"))

    def test_ensure_domains_updates_existing_domain(self) -> None:
        calls: list[tuple[str, dict[str, object]]] = []

        class Target:
            def list_domains(self, **kwargs):
                return [{"domain": "example.com", "documentroot": "/var/www", "ssl_redirect": 0}]

            def call(self, method: str, payload: dict[str, object]) -> None:
                calls.append((method, payload))

        ops = DummyDomainOps()
        ops.target = Target()
        ops.config.behavior.domain_exists = "update"
        ops._get_target_domain = lambda name: {"domain": name, "documentroot": "/var/www", "ssl_redirect": 0}
        ops._verify_domain_settings = lambda domain_name, target_docroot, payload, target_domain: None

        ops._ensure_domains(1, [{"domain": "example.com", "documentroot": "/var/www"}], {}, {}, {}, "user")
        self.assertTrue(any(m == "Domains.update" for m, _ in calls))

    def test_ensure_domains_handles_letsencrypt_fallback(self) -> None:
        # _domain_payload currently sets letsencrypt False, so this is mostly a
        # regression guard: even if FroxlorApiError is thrown, it should attempt
        # to retry with letsencrypt disabled.
        from froxlor_migrator.api import FroxlorApiError

        class Target:
            def __init__(self):
                self._calls: list[tuple[str, dict[str, object]]] = []
                self._first = True

            def list_domains(self, **kwargs):
                return []

            def call(self, method: str, payload: dict[str, object]) -> None:
                self._calls.append((method, payload))
                if method == "Domains.add" and self._first:
                    self._first = False
                    raise FroxlorApiError("Let's Encrypt error")

        ops = DummyDomainOps()
        ops.target = Target()
        ops.config.behavior.domain_exists = "skip"
        ops._get_target_domain = lambda name: {"domain": name, "documentroot": "/var/www", "ssl_redirect": 0}
        ops._verify_domain_settings = lambda domain_name, target_docroot, payload, target_domain: None

        ops._ensure_domains(1, [{"domain": "example.com", "documentroot": "/var/www"}], {}, {}, {}, "user")
        self.assertEqual(2, len([c for c in ops.target._calls if c[0] == "Domains.add"]))


if __name__ == "__main__":
    unittest.main()

    def test_default_mysql_server_from_allowed_parses_various_formats(self) -> None:
        ops = DummyDomainOps()
        self.assertEqual(0, ops._default_mysql_server_from_allowed(""))
        self.assertEqual(0, ops._default_mysql_server_from_allowed("[0]"))
        self.assertEqual(1, ops._default_mysql_server_from_allowed("[1,2]"))
        self.assertEqual(3, ops._default_mysql_server_from_allowed("3"))

    def test_fallback_last_account_number_parses_prefix_patterns(self) -> None:
        ops = DummyDomainOps()
        self.assertEqual(0, ops._fallback_last_account_number("foo", "user", ""))
        self.assertEqual(0, ops._fallback_last_account_number("userDBNAME123", "user", "DBNAME"))
        self.assertEqual(0, ops._fallback_last_account_number("userRANDOM123", "user", "RANDOM"))
        self.assertEqual(123, ops._fallback_last_account_number("userXX123", "user", "XX"))

    def test_target_mysql_access_hosts_and_prefix_setting(self) -> None:
        ops = DummyDomainOps()
        ops._run_target_panel_query = lambda sql: [["host1,host2"]]
        self.assertEqual(["host1", "host2"], ops._target_mysql_access_hosts())
        ops._run_target_panel_query = lambda sql: [[]]
        self.assertEqual(["localhost"], ops._target_mysql_access_hosts())

        ops._run_target_panel_query = lambda sql: [["prefix"]]
        self.assertEqual("prefix", ops._target_mysql_prefix_setting())
        ops._run_target_panel_query = lambda sql: [[]]
        self.assertEqual("", ops._target_mysql_prefix_setting())

    def test_ensure_domain_zones_adds_missing_records(self) -> None:
        op = DummyDomainOps()
        calls: list[tuple[str, dict[str, object]]] = []
        op.target = SimpleNamespace(
            list_domain_zones=lambda domainname=None: [],
            call=lambda method, payload: calls.append((method, payload)),
        )

        op._ensure_domain_zones([
            {
                "domainname": "example.com",
                "record": "www",
                "type": "A",
                "prio": 0,
                "content": "1.1.1.1",
                "ttl": 300,
                "is_default": 0,
            }
        ], {"1.1.1.1": "2.2.2.2"})

        self.assertTrue(any(m == "DomainZones.add" for m, _ in calls))


if __name__ == "__main__":
    unittest.main()
