#!/usr/bin/env python3
"""Drive the real migrator CLI (main.py / froxlor_migrator.tui.run_app)
non-interactively for the seeded test customers.

Runs the actual argparse -> selection -> mapping -> confirmation ->
Migrator.execute path instead of constructing Selection objects directly, so
the whole CLI front-end is covered end to end.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
MAIN_PY = REPO_DIR / "main.py"


def _run_customer(
    config_path: str,
    login: str,
    *,
    apply_changes: bool,
    include_mail: bool,
    domain_only: bool,
    target_customer: str | None,
    ip_map: str | None,
    extra: list[str],
) -> None:
    cmd = [
        sys.executable,
        str(MAIN_PY),
        "--config",
        config_path,
        "--non-interactive",
        "--yes",
        "--source-customer",
        login,
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
    subprocess.run(cmd, check=True, cwd=REPO_DIR, env=os.environ.copy())


def main() -> int:
    parser = argparse.ArgumentParser(description="Run non-interactive apply migration for selected customers")
    parser.add_argument("--config", required=True, help="Path to config TOML")
    parser.add_argument("--customer", action="append", required=True, help="Customer login (repeatable)")
    parser.add_argument("--include-mail", action="store_true", help="Also migrate mailbox content via doveadm")
    parser.add_argument("--dry-run", action="store_true", help="Plan only — omit --apply (config dry_run_default applies)")
    parser.add_argument("--domain-only", action="store_true", help="Domain-only mode (attach to existing/new target customer)")
    parser.add_argument("--target-customer", help="Target customer selector for --domain-only (login or 'new')")
    parser.add_argument("--ip-map", help="IP map argument passed through to the CLI, e.g. 'srcid=dstid'")
    parser.add_argument(
        "--extra-arg",
        action="append",
        default=[],
        help="Extra raw CLI flag passed to every run (repeatable)",
    )
    args = parser.parse_args()

    for login in args.customer:
        _run_customer(
            args.config,
            login,
            apply_changes=not args.dry_run,
            include_mail=args.include_mail,
            domain_only=args.domain_only,
            target_customer=args.target_customer,
            ip_map=args.ip_map,
            extra=list(args.extra_arg),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
