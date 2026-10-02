from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from .api import FroxlorApiError, FroxlorClient
from .config import load_config
from .migrate import MigrationError, Migrator, Selection
from .plan import (
    build_clients,
    build_ip_mapping_tokens,
    build_php_mapping_tokens,
    build_replay_command,
    build_selection,
    collect_ip_mapping_candidates,
    collect_php_mapping_candidates,
    customer_selector_values,
    customer_view,
    derive_scoped_resources,
    discover_customer_resources,
    domain_in_source_root,
    fetch_domain_zones,
    ip_aliases,
    ip_view,
    mail_view,
    narrow_subdomains,
    parse_mapping_arg,
    php_setting_aliases,
    plan_rows,
    resolve_named_mapping,
    select_rows_by_tokens,
)
from .transfer import TransferError, TransferRunner
from .util import as_int, domain_name, ftp_username, mailbox_address, pick, slugify

console = Console()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Froxlor full migration helper")
    parser.add_argument("--config", default="config.toml", help="Path to config TOML")
    parser.add_argument("--apply", action="store_true", help="Execute changes (default is dry-run)")
    parser.add_argument("--debug", action="store_true", help="Enable verbose manifest debug tracing")
    parser.add_argument("--non-interactive", action="store_true", help="Run without prompts; use defaults and CLI selections")
    parser.add_argument("--yes", action="store_true", help="Skip final confirmation prompt and start migration")
    parser.add_argument("--source-customer", help="Source customer selector (id, login, name, or email)")
    parser.add_argument("--target-customer", help="Target customer selector for domain-only mode (id, login, name, email, or 'new')")
    parser.add_argument("--domain-only", action="store_true", help="Disable whole-customer mode")
    parser.add_argument("--whole-customer", action="store_true", help="Enable whole-customer mode")
    parser.add_argument("--domains", help="Selected domains (comma-separated names, 'all', or 'none')")
    parser.add_argument("--subdomains", help="Selected subdomains (comma-separated names, 'all', or 'none')")
    parser.add_argument("--databases", help="Selected databases (comma-separated names, 'all', or 'none')")
    parser.add_argument("--mailboxes", help="Selected mailboxes (comma-separated addresses, 'all', or 'none')")
    parser.add_argument("--ftp-accounts", help="Selected FTP accounts (comma-separated usernames, 'all', or 'none')")
    parser.add_argument(
        "--php-map",
        help="Source to target PHP mapping (source=>target, comma-separated; use description|binary names)",
    )
    parser.add_argument(
        "--ip-map",
        help="Source to target IP mapping (source=>target, comma-separated; use ip:port:ssl names)",
    )
    parser.add_argument("--include-files", choices=["yes", "no"], help="Transfer website files")
    parser.add_argument("--include-databases", choices=["yes", "no"], help="Transfer database schema+data")
    parser.add_argument("--include-mail", choices=["yes", "no"], help="Transfer mailbox content via doveadm backup")
    parser.add_argument("--skip-subdomains", action="store_true", help="Skip subdomain creation/update on target")
    parser.add_argument("--skip-certificates", action="store_true", help="Skip certificate migration")
    parser.add_argument("--skip-dns-zones", action="store_true", help="Skip custom DNS zone migration")
    parser.add_argument("--skip-password-sync", action="store_true", help="Skip password hash synchronization")
    parser.add_argument("--skip-forwarders", action="store_true", help="Skip email forwarder migration")
    parser.add_argument("--skip-sender-aliases", action="store_true", help="Skip sender alias migration")
    parser.add_argument(
        "--skip-database-name-validation",
        action="store_true",
        help="Allow source/target database names to differ after Mysqls.add",
    )
    return parser.parse_args()


def _load_config_or_exit(path: str):
    try:
        return load_config(path)
    except Exception as exc:
        console.print(f"[red]Config error:[/red] {exc}")
        raise SystemExit(1) from exc


def _resolve_dry_run(args: argparse.Namespace, config) -> bool:
    if args.apply:
        return False
    return config.behavior.dry_run_default


def _parse_mapping_args(args: argparse.Namespace) -> tuple[dict[str, str], dict[str, str]]:
    try:
        return parse_mapping_arg(args.php_map, "--php-map"), parse_mapping_arg(args.ip_map, "--ip-map")
    except ValueError as exc:
        console.print(f"[red]Argument error:[/red] {exc}")
        raise SystemExit(1) from exc


def _resolve_source_customer(args: argparse.Namespace, customer_rows: list[dict]) -> dict[str, Any]:
    """Headless source-customer resolution: --source-customer, else the single
    available customer, else an error."""
    if args.source_customer:
        try:
            selected_rows = select_rows_by_tokens(customer_rows, args.source_customer, customer_selector_values, "source customer")
        except ValueError as exc:
            console.print(f"[red]Source customer selection error:[/red] {exc}")
            raise SystemExit(1) from exc
        if len(selected_rows) != 1:
            console.print("[red]--source-customer must resolve to exactly one customer.[/red]")
            raise SystemExit(1)
        return selected_rows[0]["_raw"]

    if len(customer_rows) != 1:
        console.print("[red]Non-interactive mode requires --source-customer when multiple source customers exist.[/red]")
        raise SystemExit(1)
    return customer_rows[0]["_raw"]


def _resolve_mode(args: argparse.Namespace) -> bool:
    if args.whole_customer:
        return True
    if args.domain_only:
        return False
    return True


def _resolve_target_customer(args: argparse.Namespace, target: FroxlorClient) -> dict[str, Any] | None:
    if not args.target_customer:
        console.print("[yellow]New customer will be created from source customer data.[/yellow]")
        return None
    try:
        target_customer_rows = customer_view(target.list_customers())
    except FroxlorApiError as exc:
        console.print(f"[red]API error while listing target customers:[/red] {exc}")
        raise SystemExit(1) from exc

    if args.target_customer.strip().lower() == "new":
        console.print("[yellow]New customer will be created from source customer data.[/yellow]")
        return None
    try:
        selected_target_rows = select_rows_by_tokens(
            target_customer_rows,
            args.target_customer,
            customer_selector_values,
            "target customer",
        )
    except ValueError as exc:
        console.print(f"[red]Target customer selection error:[/red] {exc}")
        raise SystemExit(1) from exc
    if len(selected_target_rows) != 1:
        console.print("[red]--target-customer must resolve to exactly one customer or 'new'.[/red]")
        raise SystemExit(1)
    target_customer = selected_target_rows[0]["_raw"]
    console.print(f"[green]Using existing target customer: {pick(target_customer, 'loginname', 'login', default='unknown')}[/green]")
    return target_customer


def _domain_selector(row: dict) -> list[str]:
    return [domain_name(row)]


def _select_domains(args: argparse.Namespace, domains: list[dict], migrate_whole_customer: bool, source_web_root: str) -> list[dict]:
    if migrate_whole_customer:
        selected_domains = [d for d in domains if domain_in_source_root(d, source_web_root)]
        skipped = len(domains) - len(selected_domains)
        if skipped > 0:
            console.print(f"[yellow]Skipped {skipped} domain(s) outside source_web_root {source_web_root}[/yellow]")
        candidates = selected_domains
    else:
        candidates = domains

    if args.domains is not None:
        try:
            return select_rows_by_tokens(candidates, args.domains, _domain_selector, "domain")
        except ValueError as exc:
            console.print(f"[red]Domain selection error:[/red] {exc}")
            raise SystemExit(1) from exc
    return list(candidates)


def _select_whole_customer_resources(
    args: argparse.Namespace,
    dbs: list[dict],
    emails: list[dict],
    ftps: list[dict],
    subdomains: list[dict],
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    try:
        selected_databases = select_rows_by_tokens(
            dbs,
            args.databases,
            lambda row: [str(pick(row, "databasename", "dbname", default=""))],
            "database",
        )
        selected_mailboxes = select_rows_by_tokens(
            emails,
            args.mailboxes,
            lambda row: [mailbox_address(row)],
            "mailbox",
        )
        selected_ftps = select_rows_by_tokens(
            ftps,
            args.ftp_accounts,
            lambda row: [ftp_username(row)],
            "FTP account",
        )
        selected_subdomains = select_rows_by_tokens(
            subdomains,
            args.subdomains,
            _domain_selector,
            "subdomain",
        )
    except ValueError as exc:
        console.print(f"[red]Selection error:[/red] {exc}")
        raise SystemExit(1) from exc
    return selected_databases, selected_mailboxes, selected_ftps, selected_subdomains


def _select_domain_resources(
    args: argparse.Namespace,
    resources: dict[str, list[dict]],
    selected_subdomains: list[dict],
    mailbox_domain_names: set[str],
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Domain-only mode headless selection: explicit --* filters or all
    candidates."""
    try:
        selected_databases = select_rows_by_tokens(
            resources["dbs"],
            args.databases,
            lambda row: [str(pick(row, "databasename", "dbname", default=""))],
            "database",
        )
        mailbox_candidates = mail_view(resources["emails"], mailbox_domain_names)
        selected_mailboxes = [
            x.get("_raw", x) for x in select_rows_by_tokens(mailbox_candidates, args.mailboxes, lambda row: [str(row.get("email", ""))], "mailbox")
        ]
        selected_subdomains = select_rows_by_tokens(selected_subdomains, args.subdomains, _domain_selector, "subdomain")
        selected_ftps = select_rows_by_tokens(
            resources["ftps"],
            args.ftp_accounts,
            lambda row: [ftp_username(row)],
            "FTP account",
        )
    except ValueError as exc:
        console.print(f"[red]Selection error:[/red] {exc}")
        raise SystemExit(1) from exc
    return selected_databases, selected_mailboxes, selected_ftps, selected_subdomains


def _select_resources(
    args: argparse.Namespace,
    resources: dict[str, list[dict]],
    migrate_whole_customer: bool,
    selected_domains: list[dict],
    subdomains: list[dict],
) -> dict[str, Any]:
    """Choose every per-resource collection after domains are known.

    Subdomains are first narrowed to children of the selected domains."""
    selected_domain_names = {domain_name(domain) for domain in selected_domains}
    narrowed_subdomains = narrow_subdomains(subdomains, selected_domain_names)

    if migrate_whole_customer:
        selected_databases, selected_mailboxes, selected_ftps, selected_subdomains = _select_whole_customer_resources(
            args, resources["dbs"], resources["emails"], resources["ftps"], narrowed_subdomains
        )
    else:
        mailbox_domain_names = selected_domain_names | {domain_name(row) for row in narrowed_subdomains if domain_name(row)}
        selected_databases, selected_mailboxes, selected_ftps, selected_subdomains = _select_domain_resources(
            args, resources, narrowed_subdomains, mailbox_domain_names
        )

    scoped = derive_scoped_resources(resources, selected_mailboxes, selected_ftps)
    return {
        "domains": selected_domains,
        "subdomains": selected_subdomains,
        "databases": selected_databases,
        "mailboxes": selected_mailboxes,
        "ftps": selected_ftps,
        "domain_names": selected_domain_names,
        **scoped,
    }


def _build_ip_map(
    selected_domains: list[dict],
    target: FroxlorClient,
    preset_mapping: dict[str, str] | None = None,
) -> tuple[dict[int, int], list[dict], list[dict]]:
    source_ip_rows = collect_ip_mapping_candidates(selected_domains)
    if not source_ip_rows:
        return {}, [], []

    target_ips = target.listing("IpsAndPorts.listing")
    target_ip_rows = ip_view(target_ips)
    if not target_ip_rows:
        console.print("[yellow]No target IPs available via API, using Froxlor defaults.[/yellow]")
        return {}, source_ip_rows, []

    try:
        mapping = resolve_named_mapping(
            raw_mapping=preset_mapping or {},
            source_rows=source_ip_rows,
            source_value_getter=lambda row: as_int(pick(row, "id", default=0)),
            source_alias_getter=ip_aliases,
            target_rows=target_ip_rows,
            target_value_getter=lambda row: as_int(pick(row, "id", default=0)),
            target_alias_getter=ip_aliases,
            mapping_label="IP mapping",
        )
    except ValueError as exc:
        console.print(f"[red]Mapping/selection error:[/red] {exc}")
        raise SystemExit(1) from exc

    return mapping, source_ip_rows, target_ip_rows


def _build_php_setting_map(
    selected_domains: list[dict],
    source_settings: list[dict],
    target_settings: list[dict],
    preset_mapping: dict[str, str] | None = None,
) -> tuple[dict[int, int], list[dict]]:
    source_ids, source_rows, target_rows, default_map = collect_php_mapping_candidates(selected_domains, source_settings, target_settings)
    if not source_ids:
        return {}, []
    try:
        mapping = resolve_named_mapping(
            raw_mapping=preset_mapping or {},
            source_rows=source_rows,
            source_value_getter=lambda row: as_int(pick(row, "id", default=0)),
            source_alias_getter=php_setting_aliases,
            target_rows=target_rows,
            target_value_getter=lambda row: as_int(pick(row, "id", default=0)),
            target_alias_getter=php_setting_aliases,
            mapping_label="PHP mapping",
        )
    except ValueError as exc:
        console.print(f"[red]Mapping/selection error:[/red] {exc}")
        raise SystemExit(1) from exc

    for source_id in source_ids:
        if source_id not in mapping:
            mapping[source_id] = default_map[source_id]
    return mapping, source_rows


def _resolve_include_flag(cli_value: str | None) -> bool:
    if cli_value is not None:
        return cli_value == "yes"
    return True


def _print_migration_plan(rows: list[tuple[str, str]]) -> None:
    plan = Table(title="Migration plan")
    plan.add_column("Item")
    plan.add_column("Count", justify="right")
    for item, value in rows:
        plan.add_row(item, value)
    console.print(plan)


def _execute_migration(args: argparse.Namespace, migrator: Migrator, runner: TransferRunner, selection: Selection):
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Starting migration", total=1)
            last_progress_line: str | None = None

            def _on_progress(step: int, total: int, status: str) -> None:
                nonlocal last_progress_line
                progress.update(task_id, total=max(total, 1), completed=step, description=f"[cyan]{status}[/cyan]")
                runner.progress_event(step, max(total, 1), status)
                line = f"Progress {step}/{max(total, 1)}: {status}"
                if line != last_progress_line:
                    console.print(line)
                    last_progress_line = line

            migrator.set_progress_callback(_on_progress)
            context = migrator.execute(selection)
            progress.update(task_id, completed=progress.tasks[0].total, description="[green]Migration completed[/green]")
        return context
    except (MigrationError, FroxlorApiError, TransferError) as exc:
        console.print(f"[red]Migration failed:[/red] {exc}")
        console.print(f"Manifest: {runner.manifest_path}")
        raise SystemExit(1) from exc


def _print_migration_result(context, runner: TransferRunner) -> None:
    console.print("[green]Migration completed.[/green]")
    console.print(f"Target customer id: {context.target_customer_id}")
    if context.source_to_target_db:
        db_table = Table(title="Database mapping")
        db_table.add_column("Source DB")
        db_table.add_column("Target DB")
        for source_db, target_db in sorted(context.source_to_target_db.items()):
            db_table.add_row(source_db, target_db)
        console.print(db_table)
    console.print(f"Manifest: {runner.manifest_path}")


def _run_headless(args: argparse.Namespace, config) -> None:
    dry_run = _resolve_dry_run(args, config)
    source, target = build_clients(config)

    console.print("[bold]Froxlor Migrator[/bold]")
    console.print(f"Mode: {'[yellow]dry-run[/yellow]' if dry_run else '[green]apply[/green]'}")
    if args.debug:
        console.print("Debug: [green]enabled[/green]")

    php_mapping_arg, ip_mapping_arg = _parse_mapping_args(args)

    try:
        customers = source.list_customers()
    except FroxlorApiError as exc:
        console.print(f"[red]API error while listing customers:[/red] {exc}")
        raise SystemExit(1) from exc

    selected_customer = _resolve_source_customer(args, customer_view(customers))
    customer_id = as_int(pick(selected_customer, "customerid", "id", default=0))
    customer_login = str(pick(selected_customer, "loginname", "login", default=""))

    try:
        resources = discover_customer_resources(source, customer_id, customer_login)
        source_php_settings = source.list_php_settings()
        target_php_settings = target.list_php_settings()
    except FroxlorApiError as exc:
        console.print(f"[red]API discovery error:[/red] {exc}")
        raise SystemExit(1) from exc

    migrate_whole_customer = _resolve_mode(args)

    target_customer = None
    if not migrate_whole_customer:
        target_customer = _resolve_target_customer(args, target)

    selected_domains = _select_domains(args, resources["domains"], migrate_whole_customer, config.paths.source_web_root)
    sel = _select_resources(args, resources, migrate_whole_customer, selected_domains, resources["subdomains"])

    include_certificates = not args.skip_certificates
    include_domain_zones = not args.skip_dns_zones
    include_password_sync = not args.skip_password_sync
    include_forwarders = not args.skip_forwarders
    include_sender_aliases = not args.skip_sender_aliases

    if include_domain_zones:
        sel["domain_zones"], zone_errors = fetch_domain_zones(source, sel["domain_names"])
        for error in zone_errors:
            console.print(f"[yellow]Skipping DNS zone for {error}[/yellow]")
    else:
        sel["domain_zones"] = []

    try:
        php_setting_map, source_selected_php_settings = _build_php_setting_map(
            sel["domains"] + sel["subdomains"],
            source_php_settings,
            target_php_settings,
            preset_mapping=php_mapping_arg,
        )
        ip_mapping, source_ip_rows, target_ip_rows = _build_ip_map(
            sel["domains"],
            target,
            preset_mapping=ip_mapping_arg,
        )
    except FroxlorApiError as exc:
        console.print(f"[red]IP discovery/mapping error:[/red] {exc}")
        raise SystemExit(1) from exc

    include_files = _resolve_include_flag(args.include_files)
    include_databases = _resolve_include_flag(args.include_databases)
    include_mail = _resolve_include_flag(args.include_mail)

    include_subdomains = not args.skip_subdomains
    validate_database_names = not args.skip_database_name_validation

    _print_migration_plan(
        plan_rows(
            selected_domains=sel["domains"],
            selected_subdomains=sel["subdomains"],
            selected_databases=sel["databases"],
            selected_mailboxes=sel["mailboxes"],
            selected_forwarders=sel["forwarders"],
            selected_sender_aliases=sel["sender_aliases"],
            selected_ftps=sel["ftps"],
            selected_ssh_keys=sel["ssh_keys"],
            selected_data_dumps=sel["data_dumps"],
            selected_dir_protections=sel["dir_protections"],
            selected_dir_options=sel["dir_options"],
            selected_domain_zones=sel["domain_zones"],
            migrate_whole_customer=migrate_whole_customer,
            php_setting_map=php_setting_map,
            ip_mapping=ip_mapping,
            include_files=include_files,
            include_databases=include_databases,
            include_mail=include_mail,
            include_certificates=include_certificates,
            include_domain_zones=include_domain_zones,
            include_password_sync=include_password_sync,
            include_forwarders=include_forwarders,
            include_sender_aliases=include_sender_aliases,
            include_subdomains=include_subdomains,
            validate_database_names=validate_database_names,
            debug=args.debug,
            dry_run=dry_run,
        )
    )

    php_mapping_tokens = build_php_mapping_tokens(php_setting_map, source_selected_php_settings, target_php_settings)
    ip_mapping_tokens = build_ip_mapping_tokens(ip_mapping, source_ip_rows, target_ip_rows)
    replay_command = build_replay_command(
        config_path=args.config,
        apply=args.apply,
        debug=args.debug,
        migrate_whole_customer=migrate_whole_customer,
        selected_customer=selected_customer,
        target_customer=target_customer,
        selected_domains=sel["domains"],
        selected_subdomains=sel["subdomains"],
        selected_databases=sel["databases"],
        selected_mailboxes=sel["mailboxes"],
        selected_ftps=sel["ftps"],
        php_mapping_tokens=php_mapping_tokens,
        ip_mapping_tokens=ip_mapping_tokens,
        include_files=include_files,
        include_databases=include_databases,
        include_mail=include_mail,
        include_certificates=include_certificates,
        include_domain_zones=include_domain_zones,
        include_password_sync=include_password_sync,
        include_forwarders=include_forwarders,
        include_sender_aliases=include_sender_aliases,
        skip_subdomains=args.skip_subdomains,
        skip_database_name_validation=args.skip_database_name_validation,
    )
    console.print("[bold]Replay command (same selection, non-interactive):[/bold]")
    console.print(replay_command)

    manifest_name = slugify(f"{pick(selected_customer, 'loginname', 'login', default='customer')}-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
    runner = TransferRunner(config=config, dry_run=dry_run, manifest_name=manifest_name, debug=args.debug)
    migrator = Migrator(config=config, source=source, target=target, runner=runner)

    selection = build_selection(
        customer=selected_customer,
        target_customer=target_customer,
        domains=sel["domains"],
        subdomains=sel["subdomains"],
        databases=sel["databases"],
        mailboxes=sel["mailboxes"],
        email_forwarders=sel["forwarders"],
        email_senders=sel["sender_aliases"],
        ftp_accounts=sel["ftps"],
        ssh_keys=sel["ssh_keys"],
        data_dumps=sel["data_dumps"],
        dir_protections=sel["dir_protections"],
        dir_options=sel["dir_options"],
        domain_zones=sel["domain_zones"],
        include_files=include_files,
        include_databases=include_databases,
        include_mail=include_mail,
        include_subdomains=include_subdomains,
        validate_database_names=validate_database_names,
        php_setting_map=php_setting_map,
        ip_mapping=ip_mapping,
        include_certificates=include_certificates,
        include_domain_zones=include_domain_zones,
        include_password_sync=include_password_sync,
        include_forwarders=include_forwarders,
        include_sender_aliases=include_sender_aliases,
    )

    context = _execute_migration(args, migrator, runner, selection)
    _print_migration_result(context, runner)


def run_app() -> None:
    args = _parse_args()

    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )

    config = _load_config_or_exit(args.config)

    if args.domain_only and args.whole_customer:
        console.print("[red]Use only one of --domain-only or --whole-customer.[/red]")
        raise SystemExit(1)

    if args.non_interactive:
        _run_headless(args, config)
        return

    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        console.print("[red]Interactive wizard requires a TTY. Use --non-interactive for scripted runs.[/red]")
        raise SystemExit(2)

    from .wizard import run_wizard

    run_wizard(config=config, args=args)
