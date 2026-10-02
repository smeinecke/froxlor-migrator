#!/usr/bin/env python3
"""Assert the rename/domain-only migration path worked.

After `--domain-only --target-customer <login>` the source customer's domains
must be owned by the *target* login and their docroots must have been
transferred under that login's directory.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from froxlor_migrator.api import FroxlorClient
from froxlor_migrator.util import as_int, pick


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--api-secret", required=True)
    parser.add_argument("--login", required=True, help="target customer login that received the domains")
    parser.add_argument("--domain", action="append", required=True, help="expected domain (repeatable)")
    parser.add_argument("--data-root", required=True, help="path to the target customer data root")
    args = parser.parse_args()

    client = FroxlorClient(api_url=args.api_url, api_key=args.api_key, api_secret=args.api_secret)
    login = args.login.strip().lower()

    customers = {
        str(pick(row, "loginname", "login", default="")).strip().lower(): row for row in client.list_customers()
    }
    target = customers.get(login)
    if not target:
        print(f"FAIL: target customer '{login}' not found")
        return 1
    target_id = as_int(pick(target, "customerid", "id", default=0))

    failures: list[str] = []
    domains = {str(pick(row, "domain", "domainname", default="")).strip().lower(): row for row in client.list_domains()}
    for name in args.domain:
        name = name.strip().lower()
        row = domains.get(name)
        if not row:
            failures.append(f"domain '{name}' missing on target")
            continue
        owner_id = as_int(pick(row, "customerid", "cid", default=0))
        if owner_id != target_id:
            owner_login = str(pick(row, "loginname", "login", default="?")).strip()
            failures.append(f"domain '{name}' owned by {owner_login or owner_id}, expected {login} ({target_id})")
            continue
        docroot = Path(args.data_root) / login / name
        if not docroot.is_dir():
            failures.append(f"docroot missing on target fs: {docroot}")
        elif not (docroot / "index.html").exists() and not (docroot / "index.php").exists():
            failures.append(f"docroot {docroot} has no index file")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print(f"OK: rename path verified — {len(args.domain)} domain(s) under {login}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
