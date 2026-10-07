#!/usr/bin/env python3
"""Presence/absence assertions for migrated resources on the target panel.

Used by migrate_and_verify.sh to prove that selector tokens and --skip-* /
--include-* no flags produced exactly the expected target state: flagged
resources absent, everything else present. Distinct from
assert_target_clean.py, which only checks whole customers.

Packed multi-value flags use colons: --zone-absent domain:record:type,
--alias-absent mailbox:allowed-sender, --php-map-expect domain:expected-desc,
--ssl-ip-expect domain:expected-ip.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from froxlor_migrator.api import FroxlorClient
from froxlor_migrator.util import as_int, pick


class Checks:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.checks = 0

    def absent(self, label: str, found: bool) -> None:
        self.checks += 1
        if found:
            self.failures.append(f"expected ABSENT but found: {label}")

    def present(self, label: str, found: bool) -> None:
        self.checks += 1
        if not found:
            self.failures.append(f"expected PRESENT but missing: {label}")


def _domain_names(rows: list[dict[str, Any]]) -> set[str]:
    return {str(pick(row, "domain", "domainname", default="")).strip().lower() for row in rows}


def _mailbox_names(rows: list[dict[str, Any]]) -> set[str]:
    return {
        str(pick(row, "email_full", "email", "emailaddr", default="")).strip().lower() for row in rows
    }


def _db_names(rows: list[dict[str, Any]]) -> set[str]:
    return {str(pick(row, "databasename", "dbname", default="")).strip().lower() for row in rows}


def _ftp_names(rows: list[dict[str, Any]]) -> set[str]:
    return {str(pick(row, "username", "loginname", default="")).strip().lower() for row in rows}


def _split_packed(value: str, parts: int, flag: str) -> list[str]:
    tokens = [token.strip() for token in value.split(":")]
    if len(tokens) != parts or any(not token for token in tokens):
        raise SystemExit(f"{flag} expects {parts} colon-separated values, got: {value!r}")
    return tokens


def main() -> int:
    parser = argparse.ArgumentParser(description="Assert expected resource state on the target panel")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--api-secret", required=True)
    parser.add_argument("--customer", required=True, help="Customer login scoping customer-level resources")
    parser.add_argument("--domain-absent", action="append", default=[])
    parser.add_argument("--domain-present", action="append", default=[])
    parser.add_argument("--subdomain-absent", action="append", default=[])
    parser.add_argument("--subdomain-present", action="append", default=[])
    parser.add_argument("--mailbox-absent", action="append", default=[])
    parser.add_argument("--mailbox-present", action="append", default=[])
    parser.add_argument("--db-absent", action="append", default=[])
    parser.add_argument("--db-present", action="append", default=[])
    parser.add_argument("--ftp-absent", action="append", default=[])
    parser.add_argument("--ftp-present", action="append", default=[])
    parser.add_argument("--zone-absent", action="append", default=[], help="domain:record:type")
    parser.add_argument("--zone-present", action="append", default=[], help="domain:record:type")
    parser.add_argument("--cert-absent", action="append", default=[], help="domain name")
    parser.add_argument("--cert-present", action="append", default=[], help="domain name")
    parser.add_argument("--forwarder-absent", action="append", default=[], help="mailbox address")
    parser.add_argument("--forwarder-present", action="append", default=[], help="mailbox:destination")
    parser.add_argument("--alias-absent", action="append", default=[], help="mailbox:allowed-sender")
    parser.add_argument("--alias-present", action="append", default=[], help="mailbox:allowed-sender")
    parser.add_argument("--php-map-expect", action="append", default=[], help="domain:expected-php-description")
    parser.add_argument("--ssl-ip-expect", action="append", default=[], help="domain:expected-ssl-bound-ip")
    args = parser.parse_args()

    client = FroxlorClient(api_url=args.api_url, api_key=args.api_key, api_secret=args.api_secret)
    checks = Checks()

    login = args.customer.strip().lower()
    customer_id = 0
    for row in client.list_customers():
        if str(pick(row, "loginname", "login", default="")).strip().lower() == login:
            customer_id = as_int(pick(row, "customerid", "id", default=0))
            break
    if customer_id <= 0:
        print(f"FAIL: target customer '{login}' not found")
        return 1

    domains = client.list_domains(customerid=customer_id)
    domain_rows = {str(pick(row, "domain", "domainname", default="")).strip().lower(): row for row in domains}
    domain_names = set(domain_rows)
    for name in args.domain_absent:
        checks.absent(f"domain {name}", name.lower() in domain_names)
    for name in args.domain_present:
        checks.present(f"domain {name}", name.lower() in domain_names)

    subdomains = _domain_names(client.list_subdomains(customerid=customer_id))
    for name in args.subdomain_absent:
        checks.absent(f"subdomain {name}", name.lower() in subdomains)
    for name in args.subdomain_present:
        checks.present(f"subdomain {name}", name.lower() in subdomains)

    mailboxes = _mailbox_names(client.list_emails(customerid=customer_id))
    for name in args.mailbox_absent:
        checks.absent(f"mailbox {name}", name.lower() in mailboxes)
    for name in args.mailbox_present:
        checks.present(f"mailbox {name}", name.lower() in mailboxes)

    databases = _db_names(client.list_mysqls(customerid=customer_id))
    for name in args.db_absent:
        checks.absent(f"database {name}", name.lower() in databases)
    for name in args.db_present:
        checks.present(f"database {name}", name.lower() in databases)

    ftps = _ftp_names(client.list_ftps(customerid=customer_id))
    for name in args.ftp_absent:
        checks.absent(f"ftp account {name}", name.lower() in ftps)
    for name in args.ftp_present:
        checks.present(f"ftp account {name}", name.lower() in ftps)

    zone_cache: dict[str, set[tuple[str, str]]] = {}

    def zone_records(domain: str) -> set[tuple[str, str]]:
        key = domain.lower()
        if key not in zone_cache:
            zone_cache[key] = {
                (
                    str(pick(row, "record", default="")).strip().lower(),
                    str(pick(row, "type", default="")).strip().upper(),
                )
                for row in client.list_domain_zones(domainname=key)
            }
        return zone_cache[key]

    for packed in args.zone_absent:
        domain, record, rtype = _split_packed(packed, 3, "--zone-absent")
        checks.absent(f"zone {domain} {record} {rtype}", (record.lower(), rtype.upper()) in zone_records(domain))
    for packed in args.zone_present:
        domain, record, rtype = _split_packed(packed, 3, "--zone-present")
        checks.present(f"zone {domain} {record} {rtype}", (record.lower(), rtype.upper()) in zone_records(domain))

    cert_domains: set[str] | None = None
    if args.cert_absent or args.cert_present:
        cert_domains = {
            str(pick(row, "domainname", "domain", default="")).strip().lower()
            for row in client.listing("Certificates.listing")
        }
    for name in args.cert_absent:
        checks.absent(f"certificate {name}", name.lower() in (cert_domains or set()))
    for name in args.cert_present:
        checks.present(f"certificate {name}", name.lower() in (cert_domains or set()))

    for mailbox in args.forwarder_absent:
        rows = client.list_email_forwarders(emailaddr=mailbox)
        checks.absent(f"forwarders on {mailbox}", bool(rows))
    for packed in args.forwarder_present:
        mailbox, destination = _split_packed(packed, 2, "--forwarder-present")
        rows = client.list_email_forwarders(emailaddr=mailbox)
        destinations = {str(pick(row, "destination", default="")).strip().lower() for row in rows}
        checks.present(f"forwarder {mailbox}->{destination}", destination.lower() in destinations)

    for packed in args.alias_absent:
        mailbox, allowed = _split_packed(packed, 2, "--alias-absent")
        rows = client.list_email_senders(emailaddr=mailbox)
        allowed_senders = {str(pick(row, "allowed_sender", "sender", default="")).strip().lower() for row in rows}
        checks.absent(f"sender alias {mailbox}:{allowed}", allowed.lower() in allowed_senders)
    for packed in args.alias_present:
        mailbox, allowed = _split_packed(packed, 2, "--alias-present")
        rows = client.list_email_senders(emailaddr=mailbox)
        allowed_senders = {str(pick(row, "allowed_sender", "sender", default="")).strip().lower() for row in rows}
        checks.present(f"sender alias {mailbox}:{allowed}", allowed.lower() in allowed_senders)

    if args.php_map_expect:
        php_by_id = {
            as_int(pick(row, "id", default=0)): str(pick(row, "description", default="")).strip().lower()
            for row in client.list_php_settings()
        }
        for packed in args.php_map_expect:
            domain, expected = _split_packed(packed, 2, "--php-map-expect")
            row = domain_rows.get(domain.lower())
            actual = php_by_id.get(as_int(pick(row or {}, "phpsettingid", default=0)), "")
            checks.present(f"{domain} phpsetting='{expected}'", actual == expected.lower())

    for packed in args.ssl_ip_expect:
        domain, expected_ip = _split_packed(packed, 2, "--ssl-ip-expect")
        row = domain_rows.get(domain.lower())
        ssl_ips = {
            str(pick(ip_row, "ip", default="")).strip().lower()
            for ip_row in pick(row or {}, "ipsandports", default=[]) or []
            if as_int(pick(ip_row, "ssl", default=0)) == 1
        }
        checks.present(f"{domain} ssl ip {expected_ip}", expected_ip.lower() in ssl_ips)

    if checks.failures:
        for failure in checks.failures:
            print(f"FAIL: {failure}")
        return 1
    print(f"OK: {checks.checks} presence/absence checks passed for {login}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
