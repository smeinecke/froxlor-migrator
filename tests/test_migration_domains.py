from __future__ import annotations

import unittest

from froxlor_migrator.migration.domains import MigratorDomainOps


class StubClient:
    def __init__(self, listings: dict[str, list[dict[str, object]]]):
        self._listings = listings

    def listing(self, command: str) -> list[dict[str, object]]:
        return self._listings.get(command, [])

    def call(self, command: str, payload: dict[str, object]) -> None:
        # For tests we only assert that this method is called without failing
        self.last_call = (command, payload)


class StubDomainOps(MigratorDomainOps):
    def __init__(self, source: StubClient, target: StubClient) -> None:
        self.source = source
        self.target = target
        self._executed_sql = ""

    def _sql_utf8_literal(self, value: str) -> str:
        return f"'{value}'"

    def _run_source_panel_query(self, sql: str) -> list[list[object]]:
        self._last_query = sql
        return self._source_query_result

    def _exec_target_panel_sql(self, sql: str) -> None:
        self._executed_sql = sql


class MigratorDomainOpsTests(unittest.TestCase):
    def test_load_source_domain_redirects_filters_invalid_rows(self) -> None:
        source = StubClient({})
        target = StubClient({})
        ops = StubDomainOps(source, target)
        ops._source_query_result = [["example.com", "alias.com", 301], ["", "", 0], ["a", "b"]]

        redirects = ops._load_source_domain_redirects([{"domain": "example.com"}])
        self.assertEqual([("example.com", "alias.com", 301)], redirects)

    def test_sync_domain_redirects_builds_sql_statements(self) -> None:
        source = StubClient({})
        target = StubClient({})
        ops = StubDomainOps(source, target)
        ops._source_query_result = [["example.com", "alias.com", 301]]

        ops._sync_domain_redirects([{"domain": "example.com"}])
        self.assertIn("UPDATE panel_domains d", ops._executed_sql)
        self.assertIn("INSERT INTO domain_redirect_codes", ops._executed_sql)

    def test_build_ip_value_mapping_skips_missing_and_identical(self) -> None:
        source = StubClient({"IpsAndPorts.listing": [{"id": 2, "ip": "10.0.0.2"}]})
        target = StubClient({"IpsAndPorts.listing": [{"id": 3, "ip": "10.0.0.3"}]})
        ops = StubDomainOps(source, target)

        mapping = ops._build_ip_value_mapping([{"ipsandports": [{"id": 1, "ip": "10.0.0.1"}]}], {1: 3})
        self.assertEqual({"10.0.0.1": "10.0.0.3"}, mapping)

    def test_replace_ip_tokens_replaces_and_preserves_spacing(self) -> None:
        ops = StubDomainOps(StubClient({}), StubClient({}))
        self.assertEqual("10.0.0.3 foo", ops._replace_ip_tokens("10.0.0.1 foo", {"10.0.0.1": "10.0.0.3"}))

    def test_normalize_domain_setting_for_compare_escapes_backslashes(self) -> None:
        ops = StubDomainOps(StubClient({}), StubClient({}))
        self.assertEqual("abc", ops._normalize_domain_setting_for_compare("\\abc"))

    def test_mapped_domain_ip_ids_filters_non_matching(self) -> None:
        ops = StubDomainOps(StubClient({}), StubClient({}))
        domain = {"ipsandports": [{"id": 1, "ssl": 1}, {"id": 2, "ssl": 0}]}
        mapped, ssl_mapped = ops._mapped_domain_ip_ids(domain, {1: 10, 2: 20})
        self.assertEqual([10, 20], mapped)
        self.assertEqual([10], ssl_mapped)


if __name__ == "__main__":
    unittest.main()
