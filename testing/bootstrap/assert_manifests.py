#!/usr/bin/env python3
"""Assert per-customer manifest files were written and are valid.

The migrator writes one JSON event log per migrated customer into
config.output.manifest_dir (<login>-<timestamp>-<index>.json). For each given
login the newest matching file must exist, parse as a JSON list, and contain
at least one event with timestamp+kind fields.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Assert migration manifests exist and are valid JSON")
    parser.add_argument("--dir", required=True, help="manifest_dir used by the migration config")
    parser.add_argument("--customer", action="append", required=True, help="customer login (repeatable)")
    args = parser.parse_args()

    manifest_dir = Path(args.dir)
    failures: list[str] = []
    for login in args.customer:
        candidates = sorted(manifest_dir.glob(f"{login}-*.json"), key=lambda p: p.stat().st_mtime)
        if not candidates:
            failures.append(f"no manifest file matching {login}-*.json in {manifest_dir}")
            continue
        newest = candidates[-1]
        try:
            events = json.loads(newest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            failures.append(f"manifest {newest.name} is not valid JSON: {exc}")
            continue
        if not isinstance(events, list) or not events:
            failures.append(f"manifest {newest.name} is empty or not a list")
            continue
        bad = [ev for ev in events if not isinstance(ev, dict) or "timestamp" not in ev or "kind" not in ev]
        if bad:
            failures.append(f"manifest {newest.name} has {len(bad)} malformed event(s)")
            continue
        print(f"  manifest {newest.name}: {len(events)} events")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print(f"OK: manifests for {', '.join(args.customer)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
