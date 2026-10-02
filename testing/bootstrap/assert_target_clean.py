#!/usr/bin/env python3
"""Assert that no customer with the given logins exists on the target panel.

Used by migrate_and_verify.sh after a --dry-run apply to prove the dry-run
path performs no writes.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from froxlor_migrator.api import FroxlorClient
from froxlor_migrator.util import pick


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--api-secret", required=True)
    parser.add_argument("--absent", action="append", required=True)
    args = parser.parse_args()

    client = FroxlorClient(api_url=args.api_url, api_key=args.api_key, api_secret=args.api_secret)
    existing = {
        str(pick(row, "loginname", "login", default="")).strip().lower() for row in client.list_customers()
    }
    leaked = existing & {name.strip().lower() for name in args.absent}
    if leaked:
        print(f"FAIL: customers unexpectedly present on target: {sorted(leaked)}")
        return 1
    print(f"OK: none of {args.absent} exist on target")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
