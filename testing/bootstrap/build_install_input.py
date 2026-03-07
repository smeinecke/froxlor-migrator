from __future__ import annotations

import argparse
import json
from pathlib import Path


def _normalize_example_value(value: object) -> object:
    if not isinstance(value, str):
        return value
    if value == "******":
        return ""
    if "|" in value:
        return value.split("|", 1)[0]
    return value


def build_payload(
    example: dict[str, object],
    *,
    db_host: str,
    db_root_user: str,
    db_root_pass: str,
    db_user: str,
    db_pass: str,
    db_name: str,
    admin_name: str,
    admin_user: str,
    admin_pass: str,
    admin_email: str,
    servername: str,
    distribution: str | None,
    webserver: str,
    webserver_backend: str,
    use_ssl: bool,
    manual_config: bool,
) -> dict[str, object]:
    payload = {key: _normalize_example_value(value) for key, value in example.items()}

    payload.update({
        "mysql_host": db_host,
        "mysql_root_user": db_root_user,
        "mysql_root_pass": db_root_pass,
        "mysql_unprivileged_user": db_user,
        "mysql_unprivileged_pass": db_pass,
        "mysql_database": db_name,
        "mysql_force_create": 1,
        "mysql_ssl_verify_server_certificate": 0,
        "admin_name": admin_name,
        "admin_user": admin_user,
        "admin_pass": admin_pass,
        "admin_pass_confirm": admin_pass,
        "admin_email": admin_email,
        "use_admin_email_as_sender": 1,
        "sender_email": admin_email,
        "servername": servername,
        "webserver": webserver,
        "webserver_backend": webserver_backend,
        "use_ssl": 1 if use_ssl else 0,
        "activate_newsfeed": 0,
        "manual_config": 1 if manual_config else 0,
    })

    if distribution:
        payload["distribution"] = distribution

    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build unattended froxlor install input JSON")
    parser.add_argument("--example", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-root-user", required=True)
    parser.add_argument("--db-root-pass", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--admin-name", required=True)
    parser.add_argument("--admin-user", required=True)
    parser.add_argument("--admin-pass", required=True)
    parser.add_argument("--admin-email", required=True)
    parser.add_argument("--servername", required=True)
    parser.add_argument("--distribution")
    parser.add_argument("--webserver", default="apache24")
    parser.add_argument("--webserver-backend", default="php-fpm")
    parser.add_argument("--use-ssl", action="store_true")
    parser.add_argument("--manual-config", action="store_true")
    args = parser.parse_args()

    example = json.loads(Path(args.example).read_text(encoding="utf-8"))
    payload = build_payload(
        example,
        db_host=args.db_host,
        db_root_user=args.db_root_user,
        db_root_pass=args.db_root_pass,
        db_user=args.db_user,
        db_pass=args.db_pass,
        db_name=args.db_name,
        admin_name=args.admin_name,
        admin_user=args.admin_user,
        admin_pass=args.admin_pass,
        admin_email=args.admin_email,
        servername=args.servername,
        distribution=args.distribution,
        webserver=args.webserver,
        webserver_backend=args.webserver_backend,
        use_ssl=args.use_ssl,
        manual_config=args.manual_config,
    )

    Path(args.output).write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
