#!/usr/bin/env python3
"""Pre-create a same-login customer on the TARGET panel.

migrate_and_verify.sh runs this before the real apply so the migrator
exercises the existing-customer path (Customers.add fails -> lookup ->
Customers.update) instead of the create path.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from seed_source import FroxlorApi, _pick, _to_int, ensure_customer, ensure_mysql_server


def main() -> int:
    login = sys.argv[1] if len(sys.argv) > 1 else "custbeta"
    api = FroxlorApi(
        api_url=os.environ["TARGET_API_URL"],
        api_key=os.environ["TARGET_API_KEY"],
        api_secret=os.environ["TARGET_API_SECRET"],
    )
    mysql_server_id = ensure_mysql_server(
        api,
        os.environ.get("TARGET_API_MYSQL_HOST", "target-db"),
        os.environ.get("TARGET_API_MYSQL_PORT", "3306"),
        os.environ.get("TARGET_DB_ROOT_USER", "root"),
        os.environ.get("TARGET_DB_ROOT_PASSWORD", "target-root"),
    )
    php_rows = api.listing("PhpSettings.listing")
    php_setting_id = 0
    for row in php_rows:
        php_setting_id = _to_int(_pick(row, "id", default=0))
        if php_setting_id > 0:
            break
    customer = ensure_customer(
        api,
        login=login,
        email=f"{login}@target-precreated.test",
        firstname="PreCreated",
        lastname="TargetSide",
        default_php_setting_id=php_setting_id,
        mysql_server_id=mysql_server_id,
    )
    print(f"Pre-created target customer: {login} (id={_pick(customer, 'customerid', 'id', default='?')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
