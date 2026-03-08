from __future__ import annotations

import argparse
from datetime import datetime

from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table

from .api import FroxlorApiError, FroxlorClient
from .config import load_config
from .migrate import MigrationError, Migrator, Selection
from .transfer import TransferError, TransferRunner
from .util import as_int, parse_multi_select, pick, slugify

console = Console()


def _render_table(title: str, rows: list[dict], cols: list[tuple[str, str]]) -> None:
    table = Table(title=title)
    table.add_column("#", justify="right")
    for _, label in cols:
        table.add_column(label)
    for idx, row in enumerate(rows, start=1):
        values = [str(idx)]
        for key, _ in cols:
            values.append(str(row.get(key, "")))
        table.add_row(*values)
    console.print(table)


def _choose_rows(title: str, rows: list[dict], cols: list[tuple[str, str]], multi: bool = False, allow_empty: bool = False) -> list[dict]:
    if not rows:
        return []
    _render_table(title, rows, cols)

    if multi:
        while True:
            default = "none" if allow_empty else "all"
            raw = Prompt.ask(f"Select entries (e.g. 1,2,5-7, {default})", default=default)
            if allow_empty and raw == "none":
                return []
            try:
                indices = parse_multi_select(raw, len(rows))
                return [rows[i] for i in indices]
            except ValueError:
                console.print("[red]Invalid selection[/red]")

    while True:
        if allow_empty:
            default = "new"
            prompt_text = f"Select entry number (or {default} to create new)"
            raw = Prompt.ask(prompt_text, default=default)
        else:
            prompt_text = "Select entry number"
            raw = Prompt.ask(prompt_text)

        if allow_empty and raw == "new":
            return []

        try:
            idx = int(raw)
            if 1 <= idx <= len(rows):
                return [rows[idx - 1]]
        except ValueError:
            pass
        console.print("[red]Invalid selection[/red]")


def _customer_view(customers: list[dict]) -> list[dict]:
    view = []
    for item in customers:
        view.append({
            "id": as_int(pick(item, "customerid", "id", default=0)),
            "login": pick(item, "loginname", "login", default=""),
            "name": pick(item, "name", "company", default=""),
            "email": pick(item, "email", default=""),
            "_raw": item,
        })
    return view


def _domain_view(domains: list[dict]) -> list[dict]:
    view = []
    for item in domains:
        view.append({
            "domain": pick(item, "domain", "domainname", default=""),
            "docroot": pick(item, "documentroot", default=""),
            "ssl": pick(item, "sslenabled", default=""),
            "php": as_int(pick(item, "phpsettingid", default=0)),
            "_raw": item,
        })
    return view


def _db_view(dbs: list[dict]) -> list[dict]:
    view = []
    for item in dbs:
        view.append({
            "dbname": pick(item, "databasename", "dbname", default=""),
            "description": pick(item, "description", default=""),
            "server": pick(item, "mysql_server", "dbserver", default=""),
            "_raw": item,
        })
    return view


def _subdomain_view(rows: list[dict]) -> list[dict]:
    view = []
    for item in rows:
        view.append({
            "domain": pick(item, "domain", "domainname", default=""),
            "path": pick(item, "path", "documentroot", default=""),
            "ssl": pick(item, "sslenabled", "ssl_enabled", default=""),
            "_raw": item,
        })
    return view


def _ftp_view(rows: list[dict]) -> list[dict]:
    view = []
    for item in rows:
        view.append({
            "username": pick(item, "username", "ftpuser", default=""),
            "path": pick(item, "path", default=""),
            "login": pick(item, "login_enabled", default=""),
            "_raw": item,
        })
    return view


def _mail_view(emails: list[dict], selected_domains: set[str]) -> list[dict]:
    view = []
    for item in emails:
        email = str(pick(item, "email_full", "email", "emailaddr", default="")).lower()
        domain = email.split("@", 1)[1] if "@" in email else ""
        if selected_domains and domain not in selected_domains:
            continue
        view.append({"email": email, "domain": domain, "_raw": item})
    return view


def _php_settings_view(settings: list[dict]) -> list[dict]:
    view = []
    for item in settings:
        view.append({
            "id": as_int(pick(item, "id", default=0)),
            "description": pick(item, "description", default=""),
            "binary": pick(item, "binary", default=""),
            "_raw": item,
        })
    return view


def _ip_view(ip_rows: list[dict]) -> list[dict]:
    view = []
    for item in ip_rows:
        view.append({
            "id": as_int(pick(item, "id", default=0)),
            "ip": str(pick(item, "ip", default="")),
            "port": as_int(pick(item, "port", default=0)),
            "ssl": as_int(pick(item, "ssl", default=0)),
            "_raw": item,
        })
    return view


def _domain_in_source_root(domain: dict, source_root: str) -> bool:
    docroot = str(pick(domain, "documentroot", default="")).strip()
    root = source_root.rstrip("/")
    return bool(docroot.startswith(root + "/") or docroot == root)


def _build_ip_map(selected_domains: list[dict], target: FroxlorClient) -> dict[int, int]:
    source_ips: dict[int, dict] = {}
    for domain in selected_domains:
        for ip in pick(domain, "ipsandports", default=[]) or []:
            ip_id = as_int(pick(ip, "id", default=0))
            if ip_id > 0 and ip_id not in source_ips:
                source_ips[ip_id] = ip

    if not source_ips:
        return {}

    target_ips = target.listing("IpsAndPorts.listing")
    target_ip_rows = _ip_view(target_ips)
    if not target_ip_rows:
        console.print("[yellow]No target IPs available via API, using Froxlor defaults.[/yellow]")
        return {}

    _render_table(
        "Target IP/Port entries",
        target_ip_rows,
        cols=[("id", "ID"), ("ip", "IP"), ("port", "Port"), ("ssl", "SSL")],
    )

    valid_target_ids = {row["id"] for row in target_ip_rows}
    mapping: dict[int, int] = {}
    for source_id, source_ip in sorted(source_ips.items()):
        source_label = f"{pick(source_ip, 'ip', default='?')}:{pick(source_ip, 'port', default='?')} ssl={pick(source_ip, 'ssl', default=0)}"
        raw = Prompt.ask(
            f"Map source IP id {source_id} ({source_label}) to target id (empty=use Froxlor default)",
            default="",
            show_default=False,
        ).strip()
        if not raw:
            continue
        try:
            target_id = int(raw)
        except ValueError:
            console.print(f"[yellow]Invalid IP id '{raw}', using Froxlor default for source id {source_id}[/yellow]")
            continue
        if target_id not in valid_target_ids:
            console.print(f"[yellow]Target IP id {target_id} not found, using Froxlor default for source id {source_id}[/yellow]")
            continue
        mapping[source_id] = target_id

    return mapping


def _build_php_setting_map(selected_domains: list[dict], target_settings: list[dict]) -> dict[int, int]:
    source_ids = sorted({as_int(pick(item, "phpsettingid", default=0)) for item in selected_domains if as_int(pick(item, "phpsettingid", default=0)) > 0})
    if not source_ids:
        return {}

    target_rows = _php_settings_view(target_settings)
    if not target_rows:
        raise ValueError("No target PHP settings found")

    _render_table(
        "Target PHP settings",
        target_rows,
        cols=[("id", "ID"), ("description", "Description"), ("binary", "Binary")],
    )
    valid_target_ids = {row["id"] for row in target_rows}
    default_target_id = target_rows[0]["id"]
    mapping: dict[int, int] = {}

    for source_id in source_ids:
        guessed = source_id if source_id in valid_target_ids else default_target_id
        while True:
            raw = Prompt.ask(f"Map source PHP setting id {source_id} to target id", default=str(guessed))
            try:
                chosen = int(raw)
            except ValueError:
                console.print("[red]Please enter a numeric target PHP setting id[/red]")
                continue
            if chosen not in valid_target_ids:
                console.print("[red]Target PHP setting id not found[/red]")
                continue
            mapping[source_id] = chosen
            break
    return mapping


def run_app() -> None:
    parser = argparse.ArgumentParser(description="Froxlor full migration helper")
    parser.add_argument("--config", default="config.toml", help="Path to config TOML")
    parser.add_argument("--apply", action="store_true", help="Execute changes (default is dry-run)")
    parser.add_argument("--skip-subdomains", action="store_true", help="Skip subdomain creation/update on target")
    parser.add_argument(
        "--skip-database-name-validation",
        action="store_true",
        help="Allow source/target database names to differ after Mysqls.add",
    )
    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except Exception as exc:
        console.print(f"[red]Config error:[/red] {exc}")
        return

    dry_run = not args.apply
    if args.apply:
        dry_run = False
    elif config.behavior.dry_run_default:
        dry_run = True

    source = FroxlorClient(
        api_url=config.source.api_url,
        api_key=config.source.api_key,
        api_secret=config.source.api_secret,
        timeout_seconds=config.source.timeout_seconds,
    )
    target = FroxlorClient(
        api_url=config.target.api_url,
        api_key=config.target.api_key,
        api_secret=config.target.api_secret,
        timeout_seconds=config.target.timeout_seconds,
    )

    console.print("[bold]Froxlor Migrator[/bold]")
    console.print(f"Mode: {'[yellow]dry-run[/yellow]' if dry_run else '[green]apply[/green]'}")

    try:
        customers = source.list_customers()
    except FroxlorApiError as exc:
        console.print(f"[red]API error while listing customers:[/red] {exc}")
        return

    customer_rows = _customer_view(customers)
    selected_customer_row = _choose_rows(
        "Source customers",
        customer_rows,
        cols=[("id", "ID"), ("login", "Login"), ("name", "Name"), ("email", "Email")],
        multi=False,
    )
    if not selected_customer_row:
        console.print("[yellow]No customer selected.[/yellow]")
        return
    selected_customer = selected_customer_row[0]["_raw"]
    customer_id = as_int(pick(selected_customer, "customerid", "id", default=0))
    customer_login = str(pick(selected_customer, "loginname", "login", default=""))

    try:
        domains = source.list_domains(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        subdomains = source.list_subdomains(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        dbs = source.list_mysqls(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        emails = source.list_emails(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        ftps = source.list_ftps(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        forwarders = source.list_email_forwarders(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        sender_aliases = source.list_email_senders(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        dir_protections = source.list_dir_protections(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        dir_options = source.list_dir_options(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        ssh_keys = source.list_ssh_keys(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        data_dumps = source.list_data_dumps(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        target_php_settings = target.list_php_settings()
    except FroxlorApiError as exc:
        console.print(f"[red]API discovery error:[/red] {exc}")
        return

    migrate_whole_customer = Confirm.ask(
        "Migrate whole customer (all domains, webspace, databases, mailboxes and settings)",
        default=True,
    )

    # For domain-only migration, let user select target customer
    target_customer = None
    if not migrate_whole_customer:
        try:
            target_customers = target.list_customers()
            target_customer_rows = _customer_view(target_customers)

            if target_customer_rows:
                selected_target_row = _choose_rows(
                    "Target customers",
                    target_customer_rows,
                    cols=[("id", "ID"), ("login", "Login"), ("name", "Name"), ("email", "Email")],
                    multi=False,
                    allow_empty=True,
                )

                if selected_target_row:
                    target_customer = selected_target_row[0]["_raw"]
                    console.print(f"[green]Using existing target customer: {pick(target_customer, 'loginname', 'login', default='unknown')}[/green]")
                else:
                    console.print("[yellow]New customer will be created from source customer data.[/yellow]")
            else:
                console.print("[yellow]No customers found on target system. New customer will be created from source customer data.[/yellow]")
        except FroxlorApiError as exc:
            console.print(f"[red]API error while listing target customers:[/red] {exc}")
            return

    if migrate_whole_customer:
        selected_domains = [d for d in domains if _domain_in_source_root(d, config.paths.source_web_root)]
        skipped = len(domains) - len(selected_domains)
        if skipped > 0:
            console.print(f"[yellow]Skipped {skipped} domain(s) outside source_web_root {config.paths.source_web_root}[/yellow]")
    else:
        selected_domain_rows = _choose_rows(
            "Source domains",
            _domain_view(domains),
            cols=[
                ("domain", "Domain"),
                ("docroot", "Documentroot"),
                ("php", "PHP setting"),
                ("ssl", "SSL"),
            ],
            multi=True,
        )
        selected_domains = [x["_raw"] for x in selected_domain_rows]

    selected_domain_names = {str(pick(domain, "domain", "domainname", default="")).lower() for domain in selected_domains}
    selected_subdomains = [
        item for item in subdomains if str(pick(item, "domain", "domainname", default="")).split(".", 1)[-1].lower() in selected_domain_names
    ]

    if migrate_whole_customer:
        selected_databases = dbs
        selected_mailboxes = emails
        selected_ftps = ftps
        selected_dir_protections = dir_protections
        selected_dir_options = dir_options
        selected_ssh_keys = ssh_keys
        selected_data_dumps = data_dumps
    else:
        if dbs:
            selected_db_rows = _choose_rows(
                "Customer databases (separate selection, optional - press Enter for none)",
                _db_view(dbs),
                cols=[("dbname", "DB Name"), ("description", "Description"), ("server", "Server")],
                multi=True,
                allow_empty=True,
            )
            selected_databases = [x["_raw"] for x in selected_db_rows]
        else:
            selected_databases = []
            console.print("[yellow]No databases found for this customer.[/yellow]")

        selected_mail_rows = _choose_rows(
            "Mailboxes for selected domains",
            _mail_view(emails, selected_domain_names),
            cols=[("email", "Mailbox"), ("domain", "Domain")],
            multi=True,
        )
        selected_mailboxes = [x["_raw"] for x in selected_mail_rows]
        selected_subdomain_rows = _choose_rows(
            "Subdomains",
            _subdomain_view(selected_subdomains),
            cols=[("domain", "Subdomain"), ("path", "Path"), ("ssl", "SSL")],
            multi=True,
        )
        selected_subdomains = [x["_raw"] for x in selected_subdomain_rows]
        if ftps:
            selected_ftp_rows = _choose_rows(
                "FTP accounts (optional - press Enter for none)",
                _ftp_view(ftps),
                cols=[("username", "Username"), ("path", "Path"), ("login", "Login")],
                multi=True,
                allow_empty=True,
            )
            selected_ftps = [x["_raw"] for x in selected_ftp_rows]
        else:
            selected_ftps = []
            console.print("[yellow]No FTP accounts found for this customer.[/yellow]")
        selected_dir_protections = dir_protections
        selected_dir_options = dir_options
        selected_ssh_keys = ssh_keys
        selected_data_dumps = data_dumps

    mailbox_names = {str(pick(item, "email_full", "email", "emailaddr", default="")).strip().lower() for item in selected_mailboxes}
    selected_forwarders = [item for item in forwarders if str(pick(item, "email", "emailaddr", default="")).strip().lower() in mailbox_names]
    selected_sender_aliases = [item for item in sender_aliases if str(pick(item, "email", "emailaddr", default="")).strip().lower() in mailbox_names]

    selected_domain_zones: list[dict] = []
    for domain_name in sorted(selected_domain_names):
        try:
            selected_domain_zones.extend(source.list_domain_zones(domainname=domain_name))
        except FroxlorApiError:
            continue

    try:
        php_setting_map = _build_php_setting_map(selected_domains, target_php_settings)
        ip_mapping = _build_ip_map(selected_domains, target)
    except ValueError as exc:
        console.print(f"[red]PHP setting mapping error:[/red] {exc}")
        return
    except FroxlorApiError as exc:
        console.print(f"[red]IP discovery/mapping error:[/red] {exc}")
        return

    include_files = Confirm.ask("Include website files", default=True)
    include_databases = Confirm.ask("Include database schema+data", default=True)
    include_mail = Confirm.ask("Include mailbox content via doveadm backup", default=True)

    plan = Table(title="Migration plan")
    plan.add_column("Item")
    plan.add_column("Count", justify="right")
    plan.add_row("Customer", "1")
    plan.add_row("Domains", str(len(selected_domains)))
    plan.add_row("Subdomains", str(0 if args.skip_subdomains else len(selected_subdomains)))
    plan.add_row("Databases", str(len(selected_databases) if include_databases else 0))
    plan.add_row("Mailboxes", str(len(selected_mailboxes) if include_mail else 0))
    plan.add_row("Mail forwarders", str(len(selected_forwarders)))
    plan.add_row("Sender aliases", str(len(selected_sender_aliases)))
    plan.add_row("FTP accounts", str(len(selected_ftps)))
    plan.add_row("SSH keys", str(len(selected_ssh_keys)))
    plan.add_row("Data dumps", str(len(selected_data_dumps)))
    plan.add_row("Dir protections", str(len(selected_dir_protections)))
    plan.add_row("Dir options", str(len(selected_dir_options)))
    plan.add_row("Domain zone records", str(len(selected_domain_zones)))
    plan.add_row("Whole customer mode", "yes" if migrate_whole_customer else "no")
    plan.add_row("PHP mappings", str(len(php_setting_map)))
    plan.add_row("Mapped IP entries", str(len(ip_mapping)))
    plan.add_row("Files transfer", "yes" if include_files else "no")
    plan.add_row("Validate DB names", "no" if args.skip_database_name_validation else "yes")
    plan.add_row("Dry-run", "yes" if dry_run else "no")
    console.print(plan)

    if not Confirm.ask("Start migration", default=True):
        console.print("[yellow]Cancelled.[/yellow]")
        return

    manifest_name = slugify(f"{pick(selected_customer, 'loginname', 'login', default='customer')}-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
    runner = TransferRunner(config=config, dry_run=dry_run, manifest_name=manifest_name)
    migrator = Migrator(config=config, source=source, target=target, runner=runner)

    selection = Selection(
        customer=selected_customer,
        target_customer=target_customer,
        domains=selected_domains,
        subdomains=selected_subdomains,
        databases=selected_databases,
        mailboxes=selected_mailboxes,
        email_forwarders=selected_forwarders,
        email_senders=selected_sender_aliases,
        ftp_accounts=selected_ftps,
        ssh_keys=selected_ssh_keys,
        data_dumps=selected_data_dumps,
        dir_protections=selected_dir_protections,
        dir_options=selected_dir_options,
        domain_zones=selected_domain_zones,
        include_files=include_files,
        include_databases=include_databases,
        include_mail=include_mail,
        include_subdomains=not args.skip_subdomains,
        validate_database_names=not args.skip_database_name_validation,
        php_setting_map=php_setting_map,
        ip_mapping=ip_mapping,
    )

    try:
        context = migrator.execute(selection)
    except (MigrationError, FroxlorApiError, TransferError) as exc:
        console.print(f"[red]Migration failed:[/red] {exc}")
        console.print(f"Manifest: {runner.manifest_path}")
        return

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
