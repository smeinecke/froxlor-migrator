from __future__ import annotations

import random
import re
import secrets
import string
from pathlib import Path
from typing import Any


def pick(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", ""}:
            return False
    return default


def random_password(length: int = 24) -> str:
    alphabet = string.ascii_letters + string.digits + "-_"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def slugify(value: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    if clean:
        return clean
    return f"migration-{random.randint(1000, 9999)}"


def parse_multi_select(raw: str, max_index: int) -> list[int]:
    raw = raw.strip().lower()
    if raw in {"none", "empty", ""}:
        return []
    if raw in {"all", "*"}:
        return list(range(max_index))
    chosen: set[int] = set()
    for part in [x.strip() for x in raw.split(",") if x.strip()]:
        if "-" in part:
            left, right = part.split("-", 1)
            a = int(left)
            b = int(right)
            for idx in range(min(a, b), max(a, b) + 1):
                if 1 <= idx <= max_index:
                    chosen.add(idx - 1)
        else:
            idx = int(part)
            if 1 <= idx <= max_index:
                chosen.add(idx - 1)
    return sorted(chosen)


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def resolve_subdomain_parts(
    full_name: str,
    parent_hint: str,
    known_domains: set[str],
) -> tuple[str, str] | None:
    """Resolve a possibly multi-level subdomain into (label, parent domain).

    Returns None when no suffix of ``full_name`` is a known domain.
    """
    name = full_name.strip().lower()
    hint = parent_hint.strip().lower()
    if hint and hint in known_domains and name.endswith(f".{hint}"):
        remainder = name[: -len(hint)].rstrip(".")
        if remainder:
            return remainder, hint
    labels = name.split(".")
    for i in range(1, len(labels) - 1):
        candidate = ".".join(labels[i:])
        if candidate in known_domains:
            return ".".join(labels[:i]), candidate
    return None


def is_custom_zone_record(row: dict[str, Any], domainname: str = "") -> bool:
    """True when a zone row is a user-managed record (not a Froxlor default).

    Apex SOA and apex NS records are auto-managed by the panel; everything
    else (including delegated sub-zone NS records) is custom.
    """
    for flag in (
        "is_default",
        "isdefault",
        "is_default_record",
        "isfroxlordefault",
        "default_entry",
    ):
        if as_int(pick(row, flag, default=0)) == 1:
            return False
    record_type = str(pick(row, "type", default="")).upper()
    if record_type == "SOA":
        return False
    if record_type == "NS":
        record_name = str(pick(row, "record", default="")).strip().lower().rstrip(".")
        apex = domainname.strip().lower().rstrip(".")
        if record_name in {"", "@"} or (apex and record_name == apex):
            return False
    return True
