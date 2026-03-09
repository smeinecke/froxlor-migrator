from __future__ import annotations

import unittest

from froxlor_migrator.verify_migration import (
    _compare_domain,
    _data_dump_key,
    _dir_option_name,
    _dir_protection_name,
    _docroot_in_any_root,
    _domain_name,
    _expected_target_docroot,
    _ftp_name,
    _mail_name,
    _normalize_customer_map,
    _normalize_php_setting_map,
    _ssh_key_name,
    _subdomain_name,
)


class VerifyMigrationHelpersTests(unittest.TestCase):
    def test_simple_name_helpers_lowercase(self) -> None:
        self.assertEqual("example.com", _domain_name({"domain": "Example.com"}))
        self.assertEqual("a@b", _mail_name({"email": "A@B"}))
        self.assertEqual("a@b", _subdomain_name({"domain": "A@B"}))
        self.assertEqual(("/path", "user"), _dir_protection_name({"path": "/Path", "username": "User"}))
        self.assertEqual("/path", _dir_option_name({"path": "/Path"}))
        self.assertEqual(("user", "key"), _ssh_key_name({"username": "User", "ssh_pubkey": "key"}))
        self.assertEqual(("/tmp", 1, 2, 3, "k"), _data_dump_key({"path": "/tmp", "dump_dbs": 1, "dump_mail": 2, "dump_web": 3, "pgp_public_key": "k"}))
        self.assertEqual("ftpuser", _ftp_name({"username": "FTPUser"}))

    def test_docroot_in_any_root(self) -> None:
        self.assertTrue(_docroot_in_any_root("/var/www/site", ["/var/www"]))
        self.assertFalse(_docroot_in_any_root("/other", ["/var/www"]))
        self.assertTrue(_docroot_in_any_root("/var/www", ["/var/www/"]))
        self.assertFalse(_docroot_in_any_root("/var/www/other", [""]))

    def test_expected_target_docroot(self) -> None:
        self.assertEqual("/target/site", _expected_target_docroot("/src/site", ["/src"], "/target"))
        self.assertEqual("/target", _expected_target_docroot("/src", ["/src"], "/target"))
        self.assertEqual("/target/other", _expected_target_docroot("/src/other", ["/src"], "/target"))

    def test_normalize_customer_map_ignores_empty_login(self) -> None:
        rows = [{"login": "Alice"}, {"loginname": ""}, {"loginname": "Bob"}]
        normalized = _normalize_customer_map(rows)
        self.assertIn("alice", normalized)
        self.assertIn("bob", normalized)

    def test_normalize_php_setting_map_filters_nonpositive_id(self) -> None:
        rows = [{"id": 0, "description": "x"}, {"id": 10, "description": "Y"}]
        result = _normalize_php_setting_map(rows)
        self.assertEqual({10: "y"}, result)

    def test_compare_domain_returns_errors_for_mismatches(self) -> None:
        source = {"documentroot": "/src", "phpenabled": 1, "ssl_enabled": 0, "letsencrypt": 0, "isemaildomain": 0, "email_only": 0, "specialsettings": "A", "ssl_specialsettings": "B", "openbasedir": 1, "openbasedir_path": "/", "writeaccesslog": 1, "writeerrorlog": 1, "dkim": 0, "alias": 0, "specialsettingsforsubdomains": 0, "phpsettingsforsubdomains": 0, "mod_fcgid_starter": -1, "mod_fcgid_maxrequests": -1, "deactivated": 0}
        target = {"documentroot": "/different", "phpenabled": 0, "ssl_enabled": 1, "letsencrypt": 1, "isemaildomain": 1, "email_only": 1, "specialsettings": "C", "ssl_specialsettings": "D", "openbasedir": 0, "openbasedir_path": "/x", "writeaccesslog": 0, "writeerrorlog": 0, "dkim": 1, "alias": 1, "specialsettingsforsubdomains": 1, "phpsettingsforsubdomains": 1, "mod_fcgid_starter": 0, "mod_fcgid_maxrequests": 0, "deactivated": 1}
        errors = _compare_domain(source, target, {}, {}, ["/src"], "/tgt")
        self.assertTrue(any("documentroot" in e for e in errors))
        self.assertTrue(any("phpenabled" in e for e in errors))


if __name__ == "__main__":
    unittest.main()
