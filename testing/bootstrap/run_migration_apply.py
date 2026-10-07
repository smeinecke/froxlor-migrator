#!/usr/bin/env python3
"""Drive the real migrator CLI (main.py / froxlor_migrator.tui.run_app)
non-interactively for the seeded test customers.

Runs the actual argparse -> selection -> mapping -> confirmation ->
Migrator.execute path instead of constructing Selection objects directly, so
the whole CLI front-end is covered end to end.

Two modes:
  * default: one main.py invocation per --customer (legacy per-customer runs)
  * --batch / --all-customers: a single main.py invocation carrying all
    customer tokens, exercising the real multi-customer batch path (shared
    plan table, per-customer manifests, continue-on-failure, exit status)
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
MAIN_PY = REPO_DIR / "main.py"


def _base_cmd(
    config_path: str,
    *,
    apply_changes: bool,
    include_mail: bool,
    domain_only: bool,
    target_customer: str | None,
    ip_map: str | None,
    php_map: str | None,
    extra: list[str],
) -> list[str]:
    cmd = [
        sys.executable,
        str(MAIN_PY),
        "--config",
        config_path,
        "--non-interactive",
        "--yes",
        "--domain-only" if domain_only else "--whole-customer",
        "--include-mail",
        "yes" if include_mail else "no",
        *extra,
    ]
    if apply_changes:
        cmd.append("--apply")
    if target_customer:
        cmd += ["--target-customer", target_customer]
    if ip_map:
        cmd += ["--ip-map", ip_map]
    if php_map:
        cmd += ["--php-map", php_map]
    return cmd


def _run_customer(
    config_path: str,
    login: str,
    *,
    apply_changes: bool,
    include_mail: bool,
    domain_only: bool,
    target_customer: str | None,
    ip_map: str | None,
    php_map: str | None,
    extra: list[str],
    check: bool,
) -> int:
    cmd = _base_cmd(
        config_path,
        apply_changes=apply_changes,
        include_mail=include_mail,
        domain_only=domain_only,
        target_customer=target_customer,
        ip_map=ip_map,
        php_map=php_map,
        extra=extra,
    )
    cmd += ["--source-customer", login]
    proc = subprocess.run(cmd, check=check, cwd=REPO_DIR, env=os.environ.copy())
    return proc.returncode


def _run_batch(
    config_path: str,
    logins: list[str],
    *,
    all_customers: bool,
    apply_changes: bool,
    include_mail: bool,
    domain_only: bool,
    target_customer: str | None,
    ip_map: str | None,
    php_map: str | None,
    extra: list[str],
) -> int:
    """One main.py invocation for the whole customer set - the real batch path."""
    cmd = _base_cmd(
        config_path,
        apply_changes=apply_changes,
        include_mail=include_mail,
        domain_only=domain_only,
        target_customer=target_customer,
        ip_map=ip_map,
        php_map=php_map,
        extra=extra,
    )
    if all_customers:
        cmd.append("--all-customers")
    else:
        # Comma-separated tokens in a single --source-customer value: that is
        # the documented batch selector form.
        cmd += ["--source-customer", ",".join(logins)]
    proc = subprocess.run(cmd, check=False, cwd=REPO_DIR, env=os.environ.copy())
    return proc.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run non-interactive apply migration for selected customers")
    parser.add_argument("--config", required=True, help="Path to config TOML")
    parser.add_argument("--customer", action="append", default=[], help="Customer login (repeatable)")
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run all --customer values in ONE main.py invocation (real batch mode)",
    )
    parser.add_argument(
        "--all-customers",
        action="store_true",
        help="Pass --all-customers to the CLI (implies --batch; ignores --customer)",
    )
    parser.add_argument("--include-mail", action="store_true", help="Also migrate mailbox content via doveadm")
    parser.add_argument("--dry-run", action="store_true", help="Plan only - omit --apply (config dry_run_default applies)")
    parser.add_argument("--domain-only", action="store_true", help="Domain-only mode (attach to existing/new target customer)")
    parser.add_argument("--target-customer", help="Target customer selector for --domain-only (login or 'new')")
    parser.add_argument("--ip-map", help="IP map argument passed through to the CLI, e.g. 'srcid=dstid'")
    parser.add_argument("--php-map", help="PHP map argument passed through to the CLI, e.g. 'src=>dst'")
    parser.add_argument(
        "--expect-fail",
        action="store_true",
        help="Invert result: exit 0 when the migration exits non-zero, fail when it succeeds",
    )
    parser.add_argument(
        "--extra-arg",
        action="append",
        default=[],
        help="Extra raw CLI flag passed to every run (repeatable)",
    )
    args = parser.parse_args()

    batch = args.batch or args.all_customers
    if not args.customer and not args.all_customers:
        parser.error("--customer is required unless --all-customers is used")

    if batch:
        rc = _run_batch(
            args.config,
            list(args.customer),
            all_customers=args.all_customers,
            apply_changes=not args.dry_run,
            include_mail=args.include_mail,
            domain_only=args.domain_only,
            target_customer=args.target_customer,
            ip_map=args.ip_map,
            php_map=args.php_map,
            extra=list(args.extra_arg),
        )
    else:
        rc = 0
        for login in args.customer:
            run_rc = _run_customer(
                args.config,
                login,
                apply_changes=not args.dry_run,
                include_mail=args.include_mail,
                domain_only=args.domain_only,
                target_customer=args.target_customer,
                ip_map=args.ip_map,
                php_map=args.php_map,
                extra=list(args.extra_arg),
                check=not args.expect_fail,
            )
            if run_rc != 0 and rc == 0:
                rc = run_rc

    if args.expect_fail:
        if rc == 0:
            print("FAIL: migration run unexpectedly succeeded", file=sys.stderr)
            return 1
        print(f"Expected failure observed (exit {rc})")
        return 0
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
