from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from froxlor_migrator.verify_migration import (
    _compare_customer,
    _compare_domain,
    _compare_ftp,
    _customer_warnings,
    _data_dump_key,
    _dir_option_name,
    _dir_protection_name,
    _docroot_in_any_root,
    _domain_name,
    _expected_ftp_path,
    _expected_target_docroot,
    _ftp_name,
    _mail_name,
    _normalize_customer_map,
    _normalize_php_setting_map,
    _ssh_key_name,
    _subdomain_name,
    _target_connect_kwargs_via_ssh,
    is_custom_zone_record,
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

    def test_docroot_in_any_root_accepts_relative_and_empty(self) -> None:
        # Relative/empty docroots resolve inside the customer homedir, so they
        # are always in scope (mirrors the TUI/migrator behaviour).
        roots = ["/var/customers/webs", "/var/customers/tmp"]
        self.assertTrue(_docroot_in_any_root("site/public", roots))
        self.assertTrue(_docroot_in_any_root("", roots))
        self.assertTrue(_docroot_in_any_root("  ", roots))

    def test_expected_target_docroot(self) -> None:
        roots = ["/src", "/transfer"]
        # Absolute docroot under the source web root: first path component is
        # the customer login and is remapped onto the target web root.
        self.assertEqual("/target/alice/site", _expected_target_docroot("/src/alice/site", roots, "/target", "alice"))
        # Shared directory (first component != login) is preserved.
        self.assertEqual("/target/shared/site", _expected_target_docroot("/src/shared/site", roots, "/target", "alice"))
        # Docroot already under the transfer root is handled the same way.
        self.assertEqual("/target/alice/site", _expected_target_docroot("/transfer/alice/site", roots, "/target", "alice"))
        # Relative and empty docroots land in the target customer's homedir.
        self.assertEqual("/target/alice/site/public", _expected_target_docroot("site/public", roots, "/target", "alice"))
        self.assertEqual("/target/alice/", _expected_target_docroot("", roots, "/target", "alice"))
        # Absolute docroot outside all known roots is relocated under the
        # target login (mirrors _resolve_target_docroot's fallback).
        self.assertEqual("/target/alice/srv/www/site", _expected_target_docroot("/srv/www/site", roots, "/target", "alice"))
        # Without a login the helper still produces a sane path.
        self.assertEqual("/target/srv/www/site", _expected_target_docroot("/srv/www/site", roots, "/target"))

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
        source = {
            "documentroot": "/src",
            "phpenabled": 1,
            "ssl_enabled": 0,
            "letsencrypt": 0,
            "isemaildomain": 0,
            "email_only": 0,
            "specialsettings": "A",
            "ssl_specialsettings": "B",
            "openbasedir": 1,
            "openbasedir_path": "/",
            "writeaccesslog": 1,
            "writeerrorlog": 1,
            "dkim": 0,
            "alias": 0,
            "specialsettingsforsubdomains": 0,
            "phpsettingsforsubdomains": 0,
            "mod_fcgid_starter": -1,
            "mod_fcgid_maxrequests": -1,
            "deactivated": 0,
        }
        target = {
            "documentroot": "/different",
            "phpenabled": 0,
            "ssl_enabled": 1,
            "letsencrypt": 1,
            "isemaildomain": 1,
            "email_only": 1,
            "specialsettings": "C",
            "ssl_specialsettings": "D",
            "openbasedir": 0,
            "openbasedir_path": "/x",
            "writeaccesslog": 0,
            "writeerrorlog": 0,
            "dkim": 1,
            "alias": 1,
            "specialsettingsforsubdomains": 1,
            "phpsettingsforsubdomains": 1,
            "mod_fcgid_starter": 0,
            "mod_fcgid_maxrequests": 0,
            "deactivated": 1,
        }
        errors = _compare_domain(source, target, {}, {}, ["/src"], "/tgt")
        self.assertTrue(any("documentroot" in e for e in errors))
        self.assertTrue(any("phpenabled" in e for e in errors))

    def test_compare_domain_uses_customer_login_for_docroot(self) -> None:
        row = {
            "documentroot": "site",
            "phpenabled": 0,
            "ssl_enabled": 0,
            "letsencrypt": 0,
            "isemaildomain": 0,
            "email_only": 0,
            "specialsettings": "",
            "ssl_specialsettings": "",
            "openbasedir": 0,
            "openbasedir_path": "",
            "writeaccesslog": 0,
            "writeerrorlog": 0,
            "dkim": 0,
            "alias": 0,
            "specialsettingsforsubdomains": 0,
            "phpsettingsforsubdomains": 0,
            "mod_fcgid_starter": -1,
            "mod_fcgid_maxrequests": -1,
            "deactivated": 0,
        }
        target = dict(row)
        target["documentroot"] = "/tgt/alice/site"
        roots = ["/var/customers/webs", "/var/customers/tmp"]
        errors = _compare_domain(row, target, {}, {}, roots, "/tgt", "alice")
        self.assertFalse(any("documentroot" in e for e in errors), errors)

    def test_is_custom_zone_record_keeps_delegated_ns(self) -> None:
        # Apex NS and SOA are Froxlor-managed; delegated sub-zone NS records
        # are custom and must be verified.
        self.assertFalse(is_custom_zone_record({"record": "example.com", "type": "NS"}, "example.com"))
        self.assertFalse(is_custom_zone_record({"record": "@", "type": "NS"}, "example.com"))
        self.assertFalse(is_custom_zone_record({"record": "", "type": "NS"}, "example.com"))
        self.assertFalse(is_custom_zone_record({"record": "sub", "type": "SOA"}, "example.com"))
        self.assertTrue(is_custom_zone_record({"record": "sub", "type": "NS"}, "example.com"))
        self.assertTrue(is_custom_zone_record({"record": "sub.example.com", "type": "NS"}, "example.com"))
        self.assertTrue(is_custom_zone_record({"record": "www", "type": "A"}, "example.com"))
        # Records flagged as defaults are excluded regardless of type.
        self.assertFalse(is_custom_zone_record({"record": "sub", "type": "NS", "is_default": 1}, "example.com"))

    def test_customer_warnings_report_not_fail_for_add_only_fields(self) -> None:
        # deactivated/theme are stripped from Customers.add, so mismatches are
        # warnings, not verification failures.
        warnings = _customer_warnings({"deactivated": 1, "theme": "Dark"}, {"deactivated": 0, "theme": "default"})
        self.assertTrue(any("deactivated" in w for w in warnings))
        self.assertTrue(any("theme" in w for w in warnings))
        self.assertEqual([], _customer_warnings({"deactivated": 0, "theme": "Sparkle"}, {"deactivated": 0, "theme": "sparkle"}))

    def test_target_connect_kwargs_via_ssh_unix_socket(self) -> None:
        userdata = "\n".join([
            "<?php",
            "$sql_root[0]['socket'] = '/run/mysqld/mysqld.sock';",
            "$sql_root[0]['user'] = 'root';",
            "$sql_root[0]['password'] = 'secret';",
        ])
        config = SimpleNamespace(ssh=SimpleNamespace(user="deploy"), commands=SimpleNamespace(sudo="sudo"))
        with (
            patch("froxlor_migrator.verify_migration.SshDriver") as ssh_cls,
            patch("froxlor_migrator.verify_migration.open_ssh_unix_socket_tunnel") as sock_tunnel,
            patch("froxlor_migrator.verify_migration.open_ssh_tunnel") as tcp_tunnel,
        ):
            ssh = ssh_cls.return_value
            ssh.read_file.return_value = userdata
            sock_tunnel.return_value.__enter__.return_value = "/tmp/local-mysql.sock"

            with _target_connect_kwargs_via_ssh(config) as kwargs:
                pass

        sock_tunnel.assert_called_once_with(config, "/run/mysqld/mysqld.sock")
        tcp_tunnel.assert_not_called()
        self.assertEqual("/tmp/local-mysql.sock", kwargs["unix_socket"])
        self.assertEqual("root", kwargs["user"])
        self.assertEqual("secret", kwargs["password"])
        ssh.close.assert_called_once()

    def test_target_connect_kwargs_via_ssh_tcp_and_sudo_fallback(self) -> None:
        userdata = "\n".join([
            "<?php",
            "$sql_root[0]['host'] = '10.0.0.5';",
            "$sql_root[0]['port'] = '3307';",
            "$sql_root[0]['user'] = 'root';",
            "$sql_root[0]['password'] = 'secret';",
        ])
        config = SimpleNamespace(ssh=SimpleNamespace(user="deploy"), commands=SimpleNamespace(sudo="sudo"))
        with (
            patch("froxlor_migrator.verify_migration.SshDriver") as ssh_cls,
            patch("froxlor_migrator.verify_migration.open_ssh_unix_socket_tunnel") as sock_tunnel,
            patch("froxlor_migrator.verify_migration.open_ssh_tunnel") as tcp_tunnel,
        ):
            ssh = ssh_cls.return_value
            # SFTP read is denied for the non-root user; sudo cat works.
            ssh.read_file.side_effect = PermissionError("denied")
            ssh.run.return_value = SimpleNamespace(returncode=0, stdout=userdata, stderr="")
            tcp_tunnel.return_value.__enter__.return_value = ("127.0.0.1", 4407)

            with _target_connect_kwargs_via_ssh(config) as kwargs:
                pass

        self.assertTrue(any("sudo cat" in str(call.args[0]) for call in ssh.run.call_args_list))
        sock_tunnel.assert_not_called()
        tcp_tunnel.assert_called_once_with(ssh.transport.return_value, "10.0.0.5", 3307)
        self.assertEqual({"host": "127.0.0.1", "port": 4407, "user": "root", "password": "secret"}, kwargs)
        ssh.close.assert_called_once()

    def test_expected_ftp_path_mirrors_migrator_fallback(self) -> None:
        # Empty source path + homedir under the customer dir → homedir suffix.
        source = {"path": "", "homedir": "/var/www/srcuser/web/site"}
        self.assertEqual("web/site", _expected_ftp_path(source, "srcuser", "dstuser"))
        # Empty path + homedir outside customer dir → target login fallback.
        source = {"path": "", "homedir": "/home/other"}
        self.assertEqual("dstuser", _expected_ftp_path(source, "srcuser", "dstuser"))
        # Explicit path is kept (stripped).
        source = {"path": "/web/custom/", "homedir": "/var/www/srcuser/"}
        self.assertEqual("web/custom", _expected_ftp_path(source, "srcuser", "dstuser"))

    def test_compare_ftp_accepts_derived_target_path(self) -> None:
        source = {"path": "", "homedir": "/var/www/user/web", "password": "h", "description": "", "shell": "/bin/false"}
        target = {"path": "web", "password": "h", "description": "", "shell": "/bin/false"}
        self.assertEqual([], _compare_ftp(source, target, source_login="user", target_login="user"))

    def test_compare_ftp_password_check_can_be_skipped(self) -> None:
        source = {"path": "web", "password": "src-hash"}
        target = {"path": "web", "password": "different"}
        self.assertTrue(_compare_ftp(source, target, check_password=True))
        self.assertEqual([], _compare_ftp(source, target, check_password=False))

    def test_compare_customer_password_check_can_be_skipped(self) -> None:
        source = {"password": "src-hash", "type_2fa": 1, "data_2fa": "secret"}
        target = {"password": "other", "type_2fa": 0, "data_2fa": ""}
        self.assertTrue(_compare_customer(source, target, check_password=True))
        self.assertEqual([], _compare_customer(source, target, check_password=False))


if __name__ == "__main__":
    unittest.main()
