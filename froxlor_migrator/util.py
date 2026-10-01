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
    return f"migration-{random.randint(1000, 9999)}"  # nosec B311


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


def domain_name(row: dict[str, Any]) -> str:
    return str(pick(row, "domain", "domainname", default="")).strip().lower()


def mailbox_address(row: dict[str, Any]) -> str:
    return str(pick(row, "email_full", "email", "emailaddr", default="")).strip().lower()


def ftp_username(row: dict[str, Any]) -> str:
    return str(pick(row, "username", "ftpuser", default="")).strip().lower()


def ssh_key_identity(row: dict[str, Any]) -> tuple[str, str]:
    return (ftp_username(row), str(pick(row, "ssh_pubkey", default="")).strip())


def data_dump_key(row: dict[str, Any]) -> tuple[str, int, int, int, str]:
    # DataDump.listing returns panel_tasks rows; the dump configuration is the
    # decoded JSON in `data` (destdir, dump_*, pgp_public_key, loginname).
    data = row.get("data")
    if not isinstance(data, dict):
        data = {}
    destdir = str(data.get("destdir") or pick(row, "path", default="")).strip()
    loginname = str(data.get("loginname") or pick(row, "loginname", default="")).strip()
    marker = f"/{loginname.strip('/')}/"
    if loginname and marker in destdir:
        destdir = destdir.split(marker, 1)[1]
    return (
        destdir.strip("/"),
        as_int(data.get("dump_dbs") if "dump_dbs" in data else pick(row, "dump_dbs", default=0)),
        as_int(data.get("dump_mail") if "dump_mail" in data else pick(row, "dump_mail", default=0)),
        as_int(data.get("dump_web") if "dump_web" in data else pick(row, "dump_web", default=0)),
        str(data.get("pgp_public_key") if data.get("pgp_public_key") is not None else pick(row, "pgp_public_key", default="")).strip(),
    )
