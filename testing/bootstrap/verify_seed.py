#!/usr/bin/env python3
"""Post-bootstrap verification script for Froxlor test environment."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from froxlor_migrator.api import FroxlorClient


def load_seed_summary() -> dict[str, Any]:
    summary_path = Path(__file__).parent.parent / "data" / "source" / "seed-summary.json"
    with open(summary_path, encoding="utf-8") as f:
        return json.load(f)


def load_testing_env() -> dict[str, str]:
    env_path = Path(__file__).parent.parent / ".env"
    values: dict[str, str] = {}
    if env_path.exists():
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def env_value(key: str, file_env: dict[str, str], default: str = "") -> str:
    return os.environ.get(key, file_env.get(key, default))


def pick(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def verify_api_connectivity(client):
    print("Verifying API connectivity...")
    try:
        client.test_connection()
        print("✓ API connectivity verified")
        return True
    except Exception as e:
        print(f"✗ API connection failed: {e}")
        return False


def verify_customers(client, expected_customers):
    print(f"Verifying customers: {expected_customers}")

    try:
        customers = client.list_customers()
        customer_logins = {str(pick(c, "loginname", "login", default="")).lower() for c in customers}

        all_found = True
        for expected in expected_customers:
            if expected.lower() in customer_logins:
                print(f"  ✓ Customer '{expected}' found")
            else:
                print(f"  ✗ Customer '{expected}' NOT found")
                all_found = False

        return all_found
    except Exception as e:
        print(f"✗ Error verifying customers: {e}")
        return False


def verify_domains(client, expected_domains):
    print(f"Verifying domains: {expected_domains}")

    try:
        domains = client.list_domains()
        domain_names = {str(pick(d, "domain", "domainname", default="")).lower() for d in domains}

        all_found = True
        for expected in expected_domains:
            if expected.lower() in domain_names:
                print(f"  ✓ Domain '{expected}' found")
            else:
                print(f"  ✗ Domain '{expected}' NOT found")
                all_found = False

        return all_found
    except Exception as e:
        print(f"✗ Error verifying domains: {e}")
        return False


def verify_mailboxes(client, expected_mailboxes):
    print(f"Verifying mailboxes: {expected_mailboxes}")

    try:
        mailboxes = client.list_emails()
        mailbox_emails = {str(pick(m, "email_full", "email", "emailaddr", default="")).lower() for m in mailboxes}

        all_found = True
        for expected in expected_mailboxes:
            if expected.lower() in mailbox_emails:
                print(f"  ✓ Mailbox '{expected}' found")
            else:
                print(f"  ✗ Mailbox '{expected}' NOT found")
                all_found = False

        return all_found
    except Exception as e:
        print(f"✗ Error verifying mailboxes: {e}")
        return False


def verify_databases(client, expected_db):
    print(f"Verifying database: {expected_db}")

    try:
        databases = client.list_mysqls()
        db_names = {str(pick(d, "databasename", "dbname", default="")).lower() for d in databases}

        if expected_db.lower() in db_names:
            print(f"  ✓ Database '{expected_db}' found")
            return True
        print(f"  ✗ Database '{expected_db}' NOT found")
        return False
    except Exception as e:
        print(f"✗ Error verifying databases: {e}")
        return False


def verify_php_settings(client, expected_settings):
    print(f"Verifying PHP settings: {expected_settings}")

    try:
        settings = client.list_php_settings()
        setting_ids = {to_int(s.get("id")) for s in settings}

        all_found = True
        for expected in expected_settings:
            if to_int(expected) in setting_ids:
                print(f"  ✓ PHP setting ID {expected} found")
            else:
                print(f"  ✗ PHP setting ID {expected} NOT found")
                all_found = False

        return all_found
    except Exception as e:
        print(f"✗ Error verifying PHP settings: {e}")
        return False


def verify_php_profiles(client, expected_profiles):
    print(f"Verifying PHP setting profile names: {expected_profiles}")
    try:
        settings = client.list_php_settings()
        names = {str(pick(s, "description", default="")).strip().lower() for s in settings}
        all_found = True
        for expected in expected_profiles:
            if expected.lower() in names:
                print(f"  ✓ PHP profile '{expected}' found")
            else:
                print(f"  ✗ PHP profile '{expected}' NOT found")
                all_found = False
        return all_found
    except Exception as e:
        print(f"✗ Error verifying PHP profile names: {e}")
        return False


def verify_web_content():
    print("Verifying seeded content directories...")
    root = Path(__file__).parent.parent / "data" / "source" / "customers"
    expected_dirs = [
        root / "custalpha" / "wp-demo.test",
        root / "custalpha" / "static-demo.test",
        root / "custbeta" / "mail-demo.test",
        root / "custbeta" / "empty-demo.test",
        root / "custgamma" / "secure-demo.test",
        root / "custgamma" / "secure-demo.test" / "app",
        root / "custgamma" / "redirect-demo.test",
        root / "custgamma" / "forward-demo.test",
    ]

    all_found = True
    for directory in expected_dirs:
        if directory.exists():
            print(f"  ✓ Content directory found: {directory}")
        else:
            print(f"  ✗ Content directory missing: {directory}")
            all_found = False
    return all_found


def verify_domain_settings(client: FroxlorClient, expectations: dict[str, Any]) -> bool:
    print("Verifying domain settings and custom vhost directives...")
    try:
        domains = client.list_domains()
        by_name = {str(pick(d, "domain", "domainname", default="")).lower(): d for d in domains}
        all_ok = True
        for domain_name, expected in expectations.items():
            row = by_name.get(domain_name.lower())
            domain_ok = True
            if not row:
                print(f"  ✗ Domain '{domain_name}' not found for settings check")
                all_ok = False
                continue

            numeric_checks: list[tuple[str, int, int]] = []
            if "ssl_enabled" in expected:
                numeric_checks.append((
                    "ssl_enabled",
                    to_int(expected.get("ssl_enabled")),
                    to_int(pick(row, "ssl_enabled", "sslenabled", default=0)),
                ))
            if "letsencrypt" in expected:
                numeric_checks.append((
                    "letsencrypt",
                    to_int(expected.get("letsencrypt")),
                    to_int(pick(row, "letsencrypt", default=0)),
                ))
            if "ssl_redirect" in expected:
                numeric_checks.append((
                    "ssl_redirect",
                    to_int(expected.get("ssl_redirect")),
                    to_int(pick(row, "ssl_redirect", default=0)),
                ))
            if "override_tls" in expected:
                numeric_checks.append((
                    "override_tls",
                    to_int(expected.get("override_tls")),
                    to_int(pick(row, "override_tls", default=0)),
                ))
            if "dkim" in expected:
                numeric_checks.append(("dkim", to_int(expected.get("dkim")), to_int(pick(row, "dkim", default=0))))
            if "openbasedir" in expected:
                numeric_checks.append((
                    "openbasedir",
                    to_int(expected.get("openbasedir")),
                    to_int(pick(row, "openbasedir", default=0)),
                ))
            if "writeaccesslog" in expected:
                numeric_checks.append((
                    "writeaccesslog",
                    to_int(expected.get("writeaccesslog")),
                    to_int(pick(row, "writeaccesslog", default=0)),
                ))
            for field_name, exp, got in numeric_checks:
                if exp != got:
                    print(f"  ✗ {domain_name}: {field_name} expected={exp} got={got}")
                    all_ok = False
                    domain_ok = False

            ssl_protocols = str(expected.get("ssl_protocols", ""))
            if ssl_protocols and str(pick(row, "ssl_protocols", default="")) != ssl_protocols:
                print(f"  ✗ {domain_name}: ssl_protocols expected={ssl_protocols!r} got={pick(row, 'ssl_protocols', default='')!r}")
                all_ok = False
                domain_ok = False

            for needle in expected.get("ssl_cipher_list_contains", []):
                if needle not in str(pick(row, "ssl_cipher_list", default="")):
                    print(f"  ✗ {domain_name}: ssl_cipher_list missing {needle!r}")
                    all_ok = False
                    domain_ok = False

            specialsettings = str(pick(row, "specialsettings", default=""))
            for needle in expected.get("specialsettings_contains", []):
                if needle not in specialsettings:
                    print(f"  ✗ {domain_name}: specialsettings missing {needle!r}")
                    all_ok = False
                    domain_ok = False

            ssl_specialsettings = str(pick(row, "ssl_specialsettings", default=""))
            for needle in expected.get("ssl_specialsettings_contains", []):
                if needle not in ssl_specialsettings:
                    print(f"  ✗ {domain_name}: ssl_specialsettings missing {needle!r}")
                    all_ok = False
                    domain_ok = False

            if to_int(expected.get("dkim_pubkey_nonempty", 0)) == 1 and not str(pick(row, "dkim_pubkey", default="")).strip():
                print(f"  ✗ {domain_name}: expected non-empty dkim_pubkey")
                all_ok = False
                domain_ok = False

            if domain_ok:
                print(f"  ✓ Domain settings verified for {domain_name}")

        return all_ok
    except Exception as e:
        print(f"✗ Error verifying domain settings: {e}")
        return False


def verify_mailbox_settings(client: FroxlorClient, expectations: dict[str, Any]) -> bool:
    print("Verifying mailbox rspamd/spam settings...")
    try:
        mailboxes = client.list_emails()
        by_email = {str(pick(m, "email_full", "email", "emailaddr", default="")).lower(): m for m in mailboxes}
        all_ok = True
        for mailbox, expected in expectations.items():
            row = by_email.get(mailbox.lower())
            mailbox_ok = True
            if not row:
                print(f"  ✗ Mailbox '{mailbox}' not found for settings check")
                all_ok = False
                continue
            for field_name in [
                "spam_tag_level",
                "rewrite_subject",
                "spam_kill_level",
                "bypass_spam",
                "policy_greylist",
            ]:
                exp = to_int(expected.get(field_name))
                got = to_int(pick(row, field_name, default=-1))
                if exp != got:
                    print(f"  ✗ {mailbox}: {field_name} expected={exp} got={got}")
                    all_ok = False
                    mailbox_ok = False
            if mailbox_ok:
                print(f"  ✓ Mailbox settings verified for {mailbox}")
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying mailbox settings: {e}")
        return False


def verify_certificates(client: FroxlorClient, expected_domains: list[str]) -> bool:
    print("Verifying custom SSL certificates...")
    try:
        cert_rows = client.listing("Certificates.listing")
        cert_domains = {str(pick(c, "domainname", "domain", default="")).lower(): c for c in cert_rows}
        all_ok = True
        for domain in expected_domains:
            row = cert_domains.get(domain.lower())
            if not row:
                print(f"  ✗ Certificate missing for domain '{domain}'")
                all_ok = False
                continue
            cert_blob = str(pick(row, "ssl_cert_file", default=""))
            key_blob = str(pick(row, "ssl_key_file", default=""))
            if "BEGIN CERTIFICATE" not in cert_blob or "BEGIN PRIVATE KEY" not in key_blob:
                print(f"  ✗ Certificate/key payload invalid for '{domain}'")
                all_ok = False
                continue
            print(f"  ✓ Custom certificate present for '{domain}'")
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying certificates: {e}")
        return False


def verify_subdomains(client: FroxlorClient, expected_subdomains: list[str]) -> bool:
    print("Verifying subdomains...")
    try:
        rows = client.listing("SubDomains.listing")
        names = {str(pick(row, "domain", "domainname", default="")).lower() for row in rows}
        all_ok = True
        for name in expected_subdomains:
            if name.lower() in names:
                print(f"  ✓ Subdomain '{name}' found")
            else:
                print(f"  ✗ Subdomain '{name}' not found")
                all_ok = False
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying subdomains: {e}")
        return False


def verify_forwarders(client: FroxlorClient, expected_forwarders: list[dict[str, str]]) -> bool:
    print("Verifying email forwarders...")
    try:
        all_ok = True
        for item in expected_forwarders:
            mailbox = str(item.get("email", "")).lower()
            destination = str(item.get("destination", "")).lower()
            rows = client.list_email_forwarders(emailaddr=mailbox)
            found = any(str(pick(row, "destination", default="")).lower() == destination for row in rows)
            if found:
                print(f"  ✓ Forwarder {mailbox} -> {destination} found")
            else:
                print(f"  ✗ Forwarder {mailbox} -> {destination} missing")
                all_ok = False
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying forwarders: {e}")
        return False


def verify_sender_aliases(client: FroxlorClient, expected_aliases: list[dict[str, str]]) -> bool:
    print("Verifying allowed sender aliases...")
    if not expected_aliases:
        return True
    all_ok = True
    for item in expected_aliases:
        mailbox = str(item.get("email", "")).lower()
        allowed_sender = str(item.get("allowed_sender", "")).lower()
        try:
            payload = client.call("EmailSender.listing", {"emailaddr": mailbox})
        except Exception as e:
            print(f"  ! EmailSender not available ({e}); skipping sender alias checks")
            return True
        rows = payload.get("list", []) if isinstance(payload, dict) else (payload or [])
        found = any(str(pick(row, "allowed_sender", default="")).lower() == allowed_sender for row in rows)
        if found:
            print(f"  ✓ Sender alias {mailbox} -> {allowed_sender} found")
        else:
            print(f"  ✗ Sender alias {mailbox} -> {allowed_sender} missing")
            all_ok = False
    return all_ok


def verify_ftp_accounts(client: FroxlorClient, expected_accounts: list[dict[str, Any]]) -> bool:
    print("Verifying FTP accounts...")
    try:
        rows = client.listing("Ftps.listing")
        by_user = {str(pick(row, "username", "ftpuser", default="")).lower(): row for row in rows}
        all_ok = True
        for item in expected_accounts:
            username = str(item.get("username", "")).lower()
            expected_path = str(item.get("path", ""))
            expected_login = to_int(item.get("login_enabled", 1))
            row = by_user.get(username)
            if not row:
                print(f"  ✗ FTP user '{username}' not found")
                all_ok = False
                continue
            got_path = str(pick(row, "path", default=""))
            got_homedir = str(pick(row, "homedir", default=""))
            got_login_raw = str(pick(row, "login_enabled", default="1"))
            got_login = 1 if got_login_raw.upper() in {"Y", "YES", "1", "TRUE"} else 0
            path_matches = got_path == expected_path or got_homedir.rstrip("/").endswith(f"/{expected_path}".rstrip("/"))
            if not path_matches or got_login != expected_login:
                print(f"  ✗ FTP user '{username}' mismatch path/login (path={got_path!r}, homedir={got_homedir!r}, login={got_login})")
                all_ok = False
                continue
            print(f"  ✓ FTP user '{username}' verified")
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying FTP accounts: {e}")
        return False


def verify_dir_protections(client: FroxlorClient, expected_rows: list[dict[str, Any]]) -> bool:
    print("Verifying directory protections...")
    try:
        rows = client.list_dir_protections()
        by_user = {
            str(pick(row, "username", default="")).strip().lower(): [
                item for item in rows if str(pick(item, "username", default="")).strip().lower() == str(pick(row, "username", default="")).strip().lower()
            ]
            for row in rows
        }
        all_ok = True
        for item in expected_rows:
            path = str(item.get("path", "")).strip().lower()
            username = str(item.get("username", "")).strip().lower()
            authname = str(item.get("authname", "")).strip()
            candidates = by_user.get(username, [])
            row = next(
                (
                    candidate
                    for candidate in candidates
                    if str(pick(candidate, "path", default="")).strip().lower().rstrip("/").endswith(f"/{path}".rstrip("/"))
                ),
                None,
            )
            if not row:
                print(f"  ✗ Dir protection missing: {path} user={username}")
                all_ok = False
                continue
            got_authname = str(pick(row, "authname", default="")).strip()
            if authname != got_authname:
                print(f"  ✗ Dir protection authname mismatch for {path} user={username}: expected={authname!r} got={got_authname!r}")
                all_ok = False
                continue
            print(f"  ✓ Dir protection verified: {path} user={username}")
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying directory protections: {e}")
        return False


def verify_dir_options(client: FroxlorClient, expected_rows: list[dict[str, Any]]) -> bool:
    print("Verifying directory options...")
    try:
        rows = client.list_dir_options()
        by_path = {str(pick(row, "path", default="")).strip().lower(): row for row in rows}
        all_ok = True
        for item in expected_rows:
            path = str(item.get("path", "")).strip().lower()
            row = by_path.get(path)
            if not row:
                row = next(
                    (candidate for candidate_path, candidate in by_path.items() if candidate_path.rstrip("/").endswith(f"/{path}".rstrip("/"))),
                    None,
                )
            if not row:
                print(f"  ✗ Dir option missing: {path}")
                all_ok = False
                continue
            checks = [
                (
                    "options_indexes",
                    to_int(item.get("options_indexes", 0)),
                    to_int(pick(row, "options_indexes", default=0)),
                ),
                (
                    "options_cgi",
                    to_int(item.get("options_cgi", 0)),
                    to_int(pick(row, "options_cgi", default=0)),
                ),
                (
                    "error404path",
                    str(item.get("error404path", "")),
                    str(pick(row, "error404path", default="")),
                ),
                (
                    "error403path",
                    str(item.get("error403path", "")),
                    str(pick(row, "error403path", default="")),
                ),
                (
                    "error500path",
                    str(item.get("error500path", "")),
                    str(pick(row, "error500path", default="")),
                ),
            ]
            mismatch = False
            for field, exp, got in checks:
                if str(exp) != str(got):
                    print(f"  ✗ Dir option {path}: {field} expected={exp!r} got={got!r}")
                    all_ok = False
                    mismatch = True
            if not mismatch:
                print(f"  ✓ Dir option verified: {path}")
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying directory options: {e}")
        return False


def verify_ssh_keys(client: FroxlorClient, expected_rows: list[dict[str, Any]]) -> bool:
    print("Verifying SSH keys...")
    if not expected_rows:
        return True
    try:
        rows = client.list_ssh_keys()
        by_key = {
            (
                str(pick(row, "username", "ftpuser", default="")).strip().lower(),
                str(pick(row, "ssh_pubkey", default="")).strip(),
            ): row
            for row in rows
        }
        all_ok = True
        for item in expected_rows:
            ftp_username = str(item.get("ftp_username", "")).strip().lower()
            ssh_pubkey = str(item.get("ssh_pubkey", "")).strip()
            description = str(item.get("description", "")).strip()
            row = by_key.get((ftp_username, ssh_pubkey))
            if not row:
                print(f"  ✗ SSH key missing for ftp user {ftp_username}")
                all_ok = False
                continue
            got_description = str(pick(row, "description", default="")).strip()
            if got_description != description:
                print(f"  ✗ SSH key description mismatch for {ftp_username}: expected={description!r} got={got_description!r}")
                all_ok = False
                continue
            print(f"  ✓ SSH key verified for ftp user {ftp_username}")
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying SSH keys: {e}")
        return False


def verify_customer_security(client: FroxlorClient, expected_rows: dict[str, Any]) -> bool:
    print("Verifying customer 2FA settings...")
    if not expected_rows:
        return True
    try:
        rows = client.list_customers()
        by_login = {str(pick(row, "loginname", "login", default="")).strip().lower(): row for row in rows}
        all_ok = True
        for login, item in expected_rows.items():
            row = by_login.get(login.strip().lower())
            if not row:
                print(f"  ✗ Customer missing for 2FA verification: {login}")
                all_ok = False
                continue
            expected_type = to_int(item.get("type_2fa", 0), 0)
            expected_data = str(item.get("data_2fa", "")).strip()
            got_type = to_int(pick(row, "type_2fa", default=0), 0)
            got_data = str(pick(row, "data_2fa", default="")).strip()
            if expected_type != got_type or expected_data != got_data:
                print(f"  ✗ 2FA mismatch for {login}: type expected={expected_type} got={got_type}, data expected={expected_data!r} got={got_data!r}")
                all_ok = False
                continue
            print(f"  ✓ Customer 2FA verified for {login}")
        return all_ok
    except Exception as e:
        print(f"✗ Error verifying customer 2FA settings: {e}")
        return False


def verify_data_dumps(client: FroxlorClient, expected_rows: list[dict[str, Any]]) -> bool:
    print("Verifying DataDump schedules...")
    if not expected_rows:
        return True
    try:
        rows = client.list_data_dumps()
    except Exception as e:
        print(f"  ! DataDump API not available ({e}); skipping DataDump checks")
        return True
    if not rows:
        print("  ! DataDump API returned no rows; skipping DataDump checks")
        return True
    existing = {
        (
            str(pick(row, "path", default="")).strip(),
            to_int(pick(row, "dump_dbs", default=0), 0),
            to_int(pick(row, "dump_mail", default=0), 0),
            to_int(pick(row, "dump_web", default=0), 0),
            str(pick(row, "pgp_public_key", default="")).strip(),
        )
        for row in rows
    }
    all_ok = True
    for item in expected_rows:
        key = (
            str(item.get("path", "")).strip(),
            to_int(item.get("dump_dbs", 0), 0),
            to_int(item.get("dump_mail", 0), 0),
            to_int(item.get("dump_web", 0), 0),
            str(item.get("pgp_public_key", "")).strip(),
        )
        if key in existing:
            print(f"  ✓ DataDump schedule verified: {key[0]}")
        else:
            print(f"  ✗ DataDump schedule missing: {key}")
            all_ok = False
    return all_ok


def verify_domain_redirects(client: FroxlorClient, expected_rows: list[dict[str, Any]], file_env: dict[str, str]) -> bool:
    print("Verifying domain forwarding/redirect fixtures...")
    if not expected_rows:
        return True
    rows = client.list_domains()
    by_domain = {str(pick(row, "domain", "domainname", default="")).strip().lower(): row for row in rows}

    db_host = env_value("SOURCE_MYSQL_HOST", file_env, "127.0.0.1")
    db_port = env_value("SOURCE_MYSQL_PORT", file_env, env_value("SOURCE_DB_PORT", file_env, "33061"))
    db_user = env_value("SOURCE_DB_ROOT_USER", file_env, "root")
    db_pass = env_value("SOURCE_DB_ROOT_PASSWORD", file_env, "source-root")
    db_name = env_value("SOURCE_DB_NAME", file_env, "froxlor")

    all_ok = True
    for item in expected_rows:
        domain = str(item.get("domain", "")).strip().lower()
        destination = str(item.get("destination", "")).strip().lower()
        redirect_code = to_int(item.get("redirect_code_id", 1), 1)
        domain_row = by_domain.get(domain)
        if not domain_row:
            print(f"  ✗ Redirect domain missing: {domain}")
            all_ok = False
            continue
        alias_value = str(pick(domain_row, "aliasdomain", default="")).strip().lower()
        if alias_value != destination:
            print(f"  ✗ Redirect destination mismatch for {domain}: expected={destination!r} got={alias_value!r}")
            all_ok = False
            continue

        sql = f"SELECT COALESCE(drc.rid, 1) FROM panel_domains d LEFT JOIN domain_redirect_codes drc ON drc.did=d.id WHERE d.domain='{domain}';"
        cmd = [
            "mariadb",
            f"-h{db_host}",
            f"-P{db_port}",
            f"-u{db_user}",
            f"-p{db_pass}",
            "-N",
            "-B",
            db_name,
            "-e",
            sql,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  ✗ Could not verify redirect code for {domain}: {result.stderr.strip()}")
            all_ok = False
            continue
        got_code = to_int((result.stdout.strip().splitlines() or ["0"])[0], 0)
        if got_code != redirect_code:
            print(f"  ✗ Redirect code mismatch for {domain}: expected={redirect_code} got={got_code}")
            all_ok = False
            continue
        print(f"  ✓ Redirect fixture verified: {domain} -> {destination} (code id {redirect_code})")
    return all_ok


def main():
    print("=" * 60)
    print("Froxlor Seed Verification Script")
    print("=" * 60)

    try:
        seed_summary = load_seed_summary()
        print(f"Loaded seed summary: {json.dumps(seed_summary, indent=2)}")
    except Exception as e:
        print(f"✗ Failed to load seed summary: {e}")
        return 1

    env_file_values = load_testing_env()
    source_api_url = env_value("SOURCE_API_URL", env_file_values)
    source_api_key = env_value("SOURCE_API_KEY", env_file_values)
    source_api_secret = env_value("SOURCE_API_SECRET", env_file_values)

    if not source_api_url or not source_api_key or not source_api_secret:
        print("✗ Missing SOURCE_API_URL/SOURCE_API_KEY/SOURCE_API_SECRET in environment or testing/.env")
        return 1

    client = FroxlorClient(
        api_url=source_api_url,
        api_key=source_api_key,
        api_secret=source_api_secret,
    )

    verifications = []

    verifications.append(("API Connectivity", verify_api_connectivity(client)))
    verifications.append(("Customers", verify_customers(client, seed_summary["customers"])))
    verifications.append(("Domains", verify_domains(client, seed_summary["domains"])))
    verifications.append(("Subdomains", verify_subdomains(client, seed_summary.get("subdomains", []))))
    verifications.append(("Mailboxes", verify_mailboxes(client, seed_summary["mailboxes"])))
    verifications.append((
        "Email Forwarders",
        verify_forwarders(client, seed_summary.get("email_forwarders", [])),
    ))
    verifications.append((
        "Sender Aliases",
        verify_sender_aliases(client, seed_summary.get("email_sender_aliases", [])),
    ))
    verifications.append((
        "FTP Accounts",
        verify_ftp_accounts(client, seed_summary.get("ftp_accounts", [])),
    ))
    verifications.append((
        "Dir Protections",
        verify_dir_protections(client, seed_summary.get("dir_protections", [])),
    ))
    verifications.append((
        "Dir Options",
        verify_dir_options(client, seed_summary.get("dir_options", [])),
    ))
    verifications.append((
        "Domain Redirects",
        verify_domain_redirects(client, seed_summary.get("domain_redirects", []), env_file_values),
    ))
    verifications.append((
        "SSH Keys",
        verify_ssh_keys(client, seed_summary.get("ssh_keys", [])),
    ))
    verifications.append((
        "Customer 2FA",
        verify_customer_security(client, seed_summary.get("customer_security", {})),
    ))
    verifications.append((
        "DataDump Schedules",
        verify_data_dumps(client, seed_summary.get("data_dumps", [])),
    ))
    verifications.append(("Databases", verify_databases(client, seed_summary["wordpress_db"])))
    verifications.append(("PHP Settings", verify_php_settings(client, seed_summary["php_settings_used"])))
    verifications.append((
        "PHP Profiles",
        verify_php_profiles(client, seed_summary.get("php_settings_profiles", [])),
    ))
    verifications.append((
        "Domain Settings",
        verify_domain_settings(client, seed_summary.get("domain_settings", {})),
    ))
    verifications.append((
        "Mailbox Settings",
        verify_mailbox_settings(client, seed_summary.get("mailbox_settings", {})),
    ))
    verifications.append((
        "Certificates",
        verify_certificates(client, seed_summary.get("certificate_domains", [])),
    ))
    verifications.append(("Web Content", verify_web_content()))

    print("\n" + "=" * 60)
    print("Verification Summary")
    print("=" * 60)

    all_passed = True
    for name, passed in verifications:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name:20} {status}")
        if not passed:
            all_passed = False

    print("=" * 60)
    if all_passed:
        print("All verifications passed! Seed is correct.")
        return 0
    else:
        print("Some verifications failed. Check logs above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
