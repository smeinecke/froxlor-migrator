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
            ssh=SimpleNamespace(user="deploy"),
            commands=SimpleNamespace(sudo="sudo"),
        )

    def _domain_name(self, domain):
        return str(domain.get("domain") or domain.get("domainname") or "").strip().lower()

    def _exec_target_panel_sql(self, sql: str) -> None:
        self._last_sql = sql

    def _get_target_domain(self, domain_name: str):
        return None

    def _target_domains(self):
        list_domains = getattr(self.target, "list_domains", None)
        rows = list_domains() if callable(list_domains) else []
        return {name: row for row in rows if (name := self._domain_name(row))}

    def _remember_target_domain(self, row):
        return row if isinstance(row, dict) else None

    def _refresh_target_domain(self, domain_name: str):
        return self._get_target_domain(domain_name)

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
        self.assertEqual("/var/www/customer/site", ops._resolve_target_docroot(domain, "customer", "customer", "/var/www/transfer/customer/site"))
        # Renamed target customer: the customer's directory component is remapped
        self.assertEqual("/var/www/newlogin/site", ops._resolve_target_docroot(domain, "customer", "newlogin", "/var/www/transfer/customer/site"))
        # Docroots outside the customer's own directory are preserved as-is
        self.assertEqual("/var/www/shared/site", ops._resolve_target_docroot(domain, "customer", "newlogin", "/var/www/transfer/shared/site"))

    def test_fix_transferred_docroot_ownership_skips_in_dry_run(self) -> None:
        ops = DummyDomainOps()
        ops.runner.dry_run = True
        ops.runner.run_remote = lambda cmd: setattr(ops, "ran", cmd)
        ops._fix_transferred_docroot_ownership("/tmp/foo", "tgt")
        self.assertFalse(hasattr(ops, "ran"))

    def test_fix_transferred_docroot_ownership_chowns_to_target_login(self) -> None:
        import types as _t

        ops = DummyDomainOps()
        ops.runner.dry_run = False
        ops.runner.run_remote = lambda cmd, check=True, sensitive=False: setattr(ops, "ran", cmd) or _t.SimpleNamespace(returncode=0)
        ops._fix_transferred_docroot_ownership("/tmp/foo", "tgt")
        self.assertIn("sudo chown -R tgt:tgt /tmp/foo", ops.ran)

    def test_fix_transferred_docroot_ownership_skips_when_system_user_missing(self) -> None:
        import types as _t

        ops = DummyDomainOps()
        ops.runner.dry_run = False
        calls: list[str] = []
        warnings: list[str] = []

        def run_remote(cmd, check=True, sensitive=False):
            calls.append(cmd)
            return _t.SimpleNamespace(returncode=1 if cmd.startswith("id -u") else 0)

        ops.runner.run_remote = run_remote
        ops._debug = lambda msg, **kw: warnings.append(msg)
        ops._fix_transferred_docroot_ownership("/tmp/foo", "tgt")
        self.assertFalse(any("chown" in c for c in calls))
        self.assertTrue(any("not found" in w for w in warnings))

    def test_fix_transferred_docroot_ownership_uses_guid_when_user_missing(self) -> None:
        import types as _t

        ops = DummyDomainOps()
        ops.runner.dry_run = False
        calls: list[str] = []

        def run_remote(cmd, check=True, sensitive=False):
            calls.append(cmd)
            return _t.SimpleNamespace(returncode=1 if cmd.startswith("id -u") else 0)

        ops.runner.run_remote = run_remote
        ops._run_target_panel_query = lambda sql: [["10042"]]
        ops._fix_transferred_docroot_ownership("/tmp/foo", "tgt")
        self.assertIn("sudo chown -R 10042:10042 /tmp/foo", calls)

    def test_fix_transferred_docroot_ownership_ignores_nonnumeric_guid(self) -> None:
        import types as _t

        ops = DummyDomainOps()
        ops.runner.dry_run = False
        calls: list[str] = []

        def run_remote(cmd, check=True, sensitive=False):
            calls.append(cmd)
            return _t.SimpleNamespace(returncode=1 if cmd.startswith("id -u") else 0)

        ops.runner.run_remote = run_remote
        ops._run_target_panel_query = lambda sql: [["abc"]]
        ops._debug = lambda msg, **kw: None
        ops._fix_transferred_docroot_ownership("/tmp/foo", "tgt")
        self.assertFalse(any("chown" in c for c in calls))

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

    def test_domain_payload_includes_all_expected_fields(self) -> None:
        ops = DummyDomainOps()
        # Set up mapping so ipandport and ssl_ipandport are included
        php_setting_map = {5: 10}
        ip_mapping = {1: 100}
        ip_value_mapping = {"1.1.1.1": "2.2.2.2"}

        domain = {
            "domain": "example.com",
            "customerid": 42,
            "adminid": 77,
            "is_stdsubdomain": "1",
            "documentroot": "/var/www/customer/site",
            "isemaildomain": "1",
            "email_only": "1",
            "phpenabled": "1",
            "sslenabled": "0",
            "specialsettings": "foo",
            "ssl_specialsettings": "bar",
            "include_specialsettings": "1",
            "ssl_redirect": "1",
            "openbasedir": "0",
            "openbasedir_path": "/tmp",
            "notryfiles": "1",
            "writeaccesslog": "0",
            "writeerrorlog": "0",
            "http2": "1",
            "http3": "0",
            "hsts": "123",
            "hsts_sub": "1",
            "hsts_preload": "1",
            "ocsp_stapling": "1",
            "override_tls": "1",
            "ssl_protocols": "TLSv1.3",
            "ssl_cipher_list": "cipher",
            "tlsv13_cipher_list": "cipher13",
            "ssl_honorcipherorder": "1",
            "ssl_sessiontickets": "0",
            "description": "desc",
            "wwwserveralias": "1",
            "subcanemaildomain": "1",
            "speciallogfile": "1",
            "alias": "1",
            "registration_date": "2021-01-01",
            "termination_date": "2022-01-01",
            "caneditdomain": "1",
            "isbinddomain": "1",
            "zonefile": "A 1.1.1.1",
            "dkim": "1",
            "specialsettingsforsubdomains": "1",
            "phpsettingsforsubdomains": "1",
            "phpsettingid": "5",
            "mod_fcgid_starter": "10",
            "mod_fcgid_maxrequests": "20",
            "dont_use_default_ssl_ipandport_if_empty": "1",
            "deactivated": "1",
            "ipsandports": [{"id": 1, "ssl": 1}],
        }

        name, docroot, payload, mapped_ip_ids = ops._domain_payload(
            target_customer_id=42,
            domain=domain,
            customer_login="bob",
            target_login="bob",
            php_setting_map=php_setting_map,
            ip_mapping=ip_mapping,
            ip_value_mapping=ip_value_mapping,
        )

        self.assertEqual("example.com", name)
        self.assertEqual("/var/www/customer/site", docroot)
        self.assertEqual([100], mapped_ip_ids)

        expected = {
            "customerid": 42,
            "loginname": "bob",
            "is_stdsubdomain": True,
            "documentroot": "/var/www/customer/site",
            "isemaildomain": True,
            "email_only": True,
            "phpenabled": True,
            "sslenabled": False,
            "letsencrypt": False,
            "specialsettings": "foo",
            "ssl_specialsettings": "bar",
            "include_specialsettings": True,
            "ssl_redirect": True,
            "openbasedir": False,
            "openbasedir_path": "/tmp",
            "notryfiles": True,
            "writeaccesslog": False,
            "writeerrorlog": False,
            "http2": True,
            "http3": False,
            "hsts_maxage": 123,
            "hsts_sub": True,
            "hsts_preload": True,
            "ocsp_stapling": True,
            "override_tls": True,
            "ssl_protocols": "TLSv1.3",
            "ssl_cipher_list": "cipher",
            "tlsv13_cipher_list": "cipher13",
            "honorcipherorder": True,
            "sessiontickets": False,
            "description": "desc",
            "selectserveralias": 1,
            "subcanemaildomain": 1,
            "speciallogfile": True,
            "registration_date": "2021-01-01",
            "termination_date": "2022-01-01",
            "caneditdomain": True,
            "isbinddomain": True,
            "zonefile": "A 2.2.2.2",
            "dkim": True,
            "specialsettingsforsubdomains": True,
            "phpsettingsforsubdomains": True,
            "phpsettingid": 10,
            "mod_fcgid_starter": 10,
            "mod_fcgid_maxrequests": 20,
            "dont_use_default_ssl_ipandport_if_empty": True,
            "deactivated": True,
            "ipandport": [{"id": 100}],
            "ssl_ipandport": [{"id": 100}],
        }

        for key, expected_value in expected.items():
            self.assertIn(key, payload)
            self.assertEqual(expected_value, payload[key])
        for key in ("adminid", "alias"):
            self.assertNotIn(key, payload)

    def test_ensure_domains_surfaces_letsencrypt_error(self) -> None:
        # Regression guard: a failed Domains.add must propagate as a
        # MigrationError instead of retrying with an identical payload.
        from froxlor_migrator.api import FroxlorApiError

        class Target:
            def __init__(self):
                self._calls: list[tuple[str, dict[str, object]]] = []

            def list_domains(self, **kwargs):
                return []

            def call(self, method: str, payload: dict[str, object]) -> None:
                self._calls.append((method, payload))
                if method == "Domains.add":
                    raise FroxlorApiError("Let's Encrypt error")

        ops = DummyDomainOps()
        ops.target = Target()
        ops.config.behavior.domain_exists = "skip"

        with self.assertRaises(MigrationError):
            ops._ensure_domains(1, [{"domain": "example.com", "documentroot": "/var/www"}], {}, {}, {}, "user")
        self.assertEqual(1, len([c for c in ops.target._calls if c[0] == "Domains.add"]))


class DomainZoneAndSubdomainTests(unittest.TestCase):
    def _ops_with_target(self, target) -> MigratorDomainOps:
        ops = DummyDomainOps()
        ops.target = target
        return ops

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

    def test_ensure_domain_zones_adds_missing_records(self) -> None:
        op = DummyDomainOps()
        calls: list[tuple[str, dict[str, object]]] = []
        op.target = SimpleNamespace(
            list_domain_zones=lambda domainname=None: [],
            call=lambda method, payload: calls.append((method, payload)),
        )

        op._ensure_domain_zones(
            [
                {
                    "domainname": "example.com",
                    "record": "www",
                    "type": "A",
                    "prio": 0,
                    "content": "1.1.1.1",
                    "ttl": 300,
                    "is_default": 0,
                }
            ],
            {"1.1.1.1": "2.2.2.2"},
        )

        self.assertTrue(any(m == "DomainZones.add" for m, _ in calls))

    def test_ensure_domain_zones_updates_drifted_content(self) -> None:
        # A target record identical in (record, type, prio, ttl) but with
        # different content (e.g. case drift in a TXT record) must be
        # replaced, not silently skipped by the case-insensitive dedup.
        # Froxlor has no DomainZones.update — it throws 303 — so the
        # migrator deletes and re-adds the entry.
        calls: list[tuple[str, dict[str, object]]] = []
        target = SimpleNamespace(
            list_domain_zones=lambda domainname=None: [{"record": "www", "type": "TXT", "prio": 0, "content": "ABCdef", "ttl": 300, "id": 7}],
            call=lambda method, payload: calls.append((method, payload)),
        )
        ops = self._ops_with_target(target)

        ops._ensure_domain_zones(
            [{"domainname": "example.com", "record": "www", "type": "TXT", "prio": 0, "content": "abcdef", "ttl": 300}],
            {},
        )

        deletes = [c for c in calls if c[0] == "DomainZones.delete"]
        adds = [c for c in calls if c[0] == "DomainZones.add"]
        self.assertEqual(1, len(deletes))
        self.assertEqual(7, deletes[0][1]["entry_id"])
        self.assertEqual(1, len(adds))
        self.assertEqual("abcdef", adds[0][1]["content"])

    def test_ensure_domain_zones_adds_when_content_differs_ambiguously(self) -> None:
        # Multiple records share (record, type, prio, ttl): cannot pick one
        # to update safely, so the source record must be added.
        calls: list[tuple[str, dict[str, object]]] = []
        target = SimpleNamespace(
            list_domain_zones=lambda domainname=None: [
                {"record": "@", "type": "TXT", "prio": 0, "content": "one", "ttl": 300, "id": 1},
                {"record": "@", "type": "TXT", "prio": 0, "content": "two", "ttl": 300, "id": 2},
            ],
            call=lambda method, payload: calls.append((method, payload)),
        )
        ops = self._ops_with_target(target)

        ops._ensure_domain_zones(
            [{"domainname": "example.com", "record": "@", "type": "TXT", "prio": 0, "content": "three", "ttl": 300}],
            {},
        )

        commands = [c for c, _ in calls]
        self.assertIn("DomainZones.add", commands)
        self.assertNotIn("DomainZones.update", commands)

    def test_ensure_subdomains_resolves_multi_level_parent(self) -> None:
        calls: list[tuple[str, dict[str, object]]] = []
        target = SimpleNamespace(
            list_subdomains=lambda customerid=None: [],
            list_domains=lambda customerid=None: [{"domain": "example.com"}],
            call=lambda method, payload: calls.append((method, payload)),
        )
        ops = self._ops_with_target(target)

        ops._ensure_subdomains(1, [{"domain": "a.b.example.com", "path": "/a/b"}], {})

        adds = [c for c in calls if c[0] == "SubDomains.add"]
        self.assertEqual(1, len(adds))
        self.assertEqual("a.b", adds[0][1]["subdomain"])
        self.assertEqual("example.com", adds[0][1]["domain"])

    def test_ensure_subdomains_skips_without_parent_and_logs(self) -> None:
        events: list[tuple[str, dict[str, object]]] = []
        target = SimpleNamespace(
            list_subdomains=lambda customerid=None: [],
            list_domains=lambda customerid=None: [{"domain": "example.com"}],
            call=lambda method, payload: None,
        )
        ops = self._ops_with_target(target)
        ops.runner = SimpleNamespace(dry_run=True, debug_event=lambda msg, **kw: events.append((msg, kw)))

        ops._ensure_subdomains(1, [{"domain": "a.b.other.com"}], {})

        self.assertEqual(
            [("subdomain_skipped", {"subdomain": "a.b.other.com", "reason": "no matching target parent domain"})],
            events,
        )


if __name__ == "__main__":
    unittest.main()


class CertificateMigrationTests(unittest.TestCase):
    def test_existing_certificate_update_passes_domain_id(self) -> None:
        ops = DummyDomainOps()
        calls: list[tuple[str, dict]] = []
        # `id` is the ssl-settings row id; Certificates.update expects the
        # *domain* id, which the listing exposes as `domainid`.
        certs = [{"domainname": "ex.com", "id": 55, "domainid": 77, "ssl_cert_file": "CERT", "ssl_key_file": "KEY"}]

        def listing(command: str):
            if command == "Certificates.listing":
                return certs
            return []

        ops.source = SimpleNamespace(listing=listing)
        ops.target = SimpleNamespace(
            listing=listing,
            call=lambda method, payload=None: calls.append((method, payload or {})),
        )
        ops._migrate_domain_certificates([{"domain": "ex.com", "letsencrypt": 0}])
        self.assertIn(
            (
                "Certificates.update",
                {"domainname": "ex.com", "ssl_cert_file": "CERT", "ssl_key_file": "KEY", "ssl_ca_file": "", "ssl_cert_chainfile": "", "id": 77},
            ),
            calls,
        )

    def test_existing_certificate_update_without_domain_id_omits_id(self) -> None:
        ops = DummyDomainOps()
        calls: list[tuple[str, dict]] = []
        certs = [{"domainname": "ex.com", "id": 55, "ssl_cert_file": "CERT", "ssl_key_file": "KEY"}]

        def listing(command: str):
            if command == "Certificates.listing":
                return certs
            return []

        ops.source = SimpleNamespace(listing=listing)
        ops.target = SimpleNamespace(
            listing=listing,
            call=lambda method, payload=None: calls.append((method, payload or {})),
        )
        ops._migrate_domain_certificates([{"domain": "ex.com", "letsencrypt": 0}])
        self.assertIn(
            (
                "Certificates.update",
                {"domainname": "ex.com", "ssl_cert_file": "CERT", "ssl_key_file": "KEY", "ssl_ca_file": "", "ssl_cert_chainfile": ""},
            ),
            calls,
        )
