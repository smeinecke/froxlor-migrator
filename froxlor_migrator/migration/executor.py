from __future__ import annotations

from collections.abc import Callable

from ..util import as_int, pick, relative_customer_path
from .accounts import MigratorAccountOps
from .core import MigratorCore
from .domains import MigratorDomainOps
from .types import MigrationContext, MigrationError, Selection


class _Progress:
    """Step counter bound to the migrator's progress callback."""

    def __init__(self, emit: Callable[[int, int, str], None], total: int) -> None:
        self.step = 0
        self.total = total
        self._emit = emit

    def status(self, status: str) -> None:
        self._emit(self.step, self.total, status)

    def advance(self, status: str) -> None:
        self.step += 1
        self._emit(self.step, self.total, status)


class Migrator(MigratorCore, MigratorDomainOps, MigratorAccountOps):
    def _count_steps(self, selection: Selection) -> int:
        if self.runner.dry_run:
            return 2  # preflight + dry-run completion
        total_steps = 10
        for flag in (
            selection.include_certificates,
            selection.include_domain_zones,
            selection.include_letsencrypt_flags,
            selection.include_password_sync,
            selection.include_forwarders,
            selection.include_sender_aliases,
            selection.include_subdomains,
        ):
            if flag:
                total_steps += 1
        if selection.include_databases and selection.databases:
            total_steps += 2 + len(selection.databases)
        if selection.mailboxes:
            total_steps += 1
        if selection.include_files:
            total_steps += len(selection.domains)
            if selection.include_subdomains:
                total_steps += sum(1 for sub in selection.subdomains if str(pick(sub, "path", default="")).strip())
        if selection.include_mail and selection.mailboxes:
            total_steps += 1
        return total_steps

    def execute(self, selection: Selection) -> MigrationContext:
        progress = _Progress(self._emit_progress, self._count_steps(selection))
        progress.status("Running preflight checks")
        self.preflight(selection)
        progress.advance("Preflight checks")
        if self.runner.dry_run:
            target_customer_id = as_int(pick(selection.target_customer or {}, "customerid", "id", default=0))
            progress.advance("Dry-run completed")
            return MigrationContext(target_customer_id=target_customer_id, source_to_target_db={})

        progress.status("Synchronizing customer")
        target_customer_id = self._ensure_target_customer(selection.customer, selection.target_customer, selection.php_setting_map)
        progress.advance("Customer synchronized")
        customer_login = str(pick(selection.customer, "loginname", "login", default="")).strip()
        progress.status("Preparing IP mapping")
        ip_value_mapping = self._build_ip_value_mapping(selection.domains, selection.ip_mapping)
        progress.advance("IP mapping prepared")

        target_login = ""
        if selection.target_customer:
            target_login = self._customer_login(selection.target_customer)
        target_login = target_login or customer_login

        self._sync_web_config(selection, progress, target_customer_id, customer_login, target_login, ip_value_mapping)
        db_map = self._sync_databases(selection, progress, target_customer_id)
        transferable_mailboxes = self._sync_mail_objects(selection, progress, target_customer_id, customer_login, target_login)
        self._transfer_web_files(selection, progress, customer_login, target_login)
        self._transfer_mail_content(selection, progress, transferable_mailboxes)
        return MigrationContext(target_customer_id=target_customer_id, source_to_target_db=db_map)

    def _sync_web_config(
        self,
        selection: Selection,
        progress: _Progress,
        target_customer_id: int,
        customer_login: str,
        target_login: str,
        ip_value_mapping: dict[str, str],
    ) -> None:
        progress.status("Synchronizing domains")
        self._ensure_domains(
            target_customer_id,
            selection.domains,
            selection.php_setting_map,
            selection.ip_mapping,
            ip_value_mapping,
            customer_login,
            target_login,
        )
        progress.advance("Domains synchronized")
        progress.status("Synchronizing domain redirects")
        self._sync_domain_redirects(selection.domains)
        progress.advance("Domain redirects synchronized")
        if selection.include_subdomains:
            progress.status("Synchronizing subdomains")
            self._ensure_subdomains(
                target_customer_id,
                selection.subdomains,
                selection.php_setting_map,
                customer_login,
                target_login,
            )
            progress.advance("Subdomains synchronized")
        if selection.include_certificates:
            progress.status("Synchronizing certificates")
            self._migrate_domain_certificates(selection.domains)
            progress.advance("Certificates synchronized")
        progress.status("Synchronizing FTP accounts")
        self._ensure_ftp_accounts(target_customer_id, selection.ftp_accounts, customer_login, target_login)
        progress.advance("FTP accounts synchronized")
        progress.status("Synchronizing SSH keys")
        self._ensure_ssh_keys(target_customer_id, selection.ssh_keys)
        progress.advance("SSH keys synchronized")
        progress.status("Synchronizing data dumps")
        self._ensure_data_dumps(target_customer_id, selection.data_dumps, customer_login)
        progress.advance("Data dumps synchronized")
        progress.status("Synchronizing directory options")
        self._ensure_dir_options(target_customer_id, selection.dir_options, customer_login, target_login)
        progress.advance("Directory options synchronized")
        progress.status("Synchronizing directory protections")
        self._ensure_dir_protections(target_customer_id, selection.dir_protections, customer_login, target_login)
        progress.advance("Directory protections synchronized")
        if selection.include_domain_zones:
            progress.status("Synchronizing domain zones")
            self._ensure_domain_zones(selection.domain_zones, ip_value_mapping)
            progress.advance("Domain zones synchronized")
        if selection.include_letsencrypt_flags:
            progress.status("Synchronizing Let's Encrypt flags")
            try:
                self._enable_letsencrypt_after_dns(selection.domains)
                progress.advance("Let's Encrypt flags synchronized")
            except Exception as exc:  # non-fatal by design for post-sync ACME edge-cases
                self.runner.debug_event(
                    "letsencrypt_flag_sync_non_fatal_error",
                    error=str(exc)[:500],
                )
                summary = str(exc).splitlines()[0].strip()[:120]
                progress.advance(f"Let's Encrypt flags skipped (non-fatal): {summary}")

    def _sync_databases(self, selection: Selection, progress: _Progress, target_customer_id: int) -> dict[str, str]:
        db_map: dict[str, str] = {}
        if not (selection.include_databases and selection.databases):
            return db_map
        # Keep the SSH tunnel open for the duration of database-related operations
        # to avoid reconnecting on every single query.
        with self._target_mysql_connect_kwargs():
            progress.status("Synchronizing MySQL prefix")
            self._sync_target_mysql_prefix_setting()
            progress.advance("MySQL prefix synchronized")

            known_databases = {
                str(pick(item, "databasename", "dbname", "database", default="")): as_int(pick(item, "customerid", default=0))
                for item in self.target.list_mysqls()
                if str(pick(item, "databasename", "dbname", "database", default=""))
            }
            for source_db in selection.databases:
                source_name = str(pick(source_db, "databasename", "dbname", "database", default=""))
                target_name = self._create_database_on_target(target_customer_id, source_db, known_databases)
                if target_name is None:
                    progress.advance(f"Database skipped (already exists): {source_name}")
                    continue
                if selection.validate_database_names and source_name != target_name:
                    raise MigrationError(
                        f"Database name mismatch: source={source_name!r} target={target_name!r}; preserving identical DB logins requires matching names"
                    )
                known_databases[target_name] = target_customer_id
                db_map[source_name] = target_name
                progress.status(f"Transferring database: {source_name}")
                self._transfer_database_with_defaults(source_name, target_name)
                progress.advance(f"Database migrated: {source_name}")
            progress.status("Synchronizing database login hashes")
            self._sync_database_login_hashes(db_map)
            progress.advance("Database login hashes synchronized")
        return db_map

    def _sync_mail_objects(
        self,
        selection: Selection,
        progress: _Progress,
        target_customer_id: int,
        customer_login: str,
        target_login: str,
    ) -> list[str]:
        transferable_mailboxes: list[str] = []
        if selection.mailboxes:
            progress.status("Synchronizing mailbox objects")
            transferable_mailboxes = self._ensure_mailboxes(target_customer_id, selection.mailboxes)
            progress.advance("Mailboxes synchronized")
        if selection.include_forwarders:
            progress.status("Synchronizing mail forwarders")
            self._ensure_email_forwarders(target_customer_id, selection.email_forwarders)
            progress.advance("Mail forwarders synchronized")
        if selection.include_sender_aliases:
            progress.status("Synchronizing sender aliases")
            self._ensure_email_sender_aliases(target_customer_id, selection.email_senders)
            progress.advance("Sender aliases synchronized")
        if selection.include_password_sync:
            progress.status("Synchronizing password hashes")
            self._sync_password_hashes(
                target_customer_id,
                selection.customer,
                selection.ftp_accounts,
                selection.mailboxes,
                selection.dir_protections,
                customer_login,
                target_login,
            )
            progress.advance("Password hashes synchronized")
        return transferable_mailboxes

    def _transfer_web_files(self, selection: Selection, progress: _Progress, customer_login: str, target_login: str) -> None:
        if not selection.include_files:
            return
        transferred_docroots: set[tuple[str, str]] = set()
        for domain in selection.domains:
            domain_name = self._domain_name(domain)
            if as_int(pick(domain, "aliasdomain", "isaliasdomain", default=0)) > 0:
                self.runner.debug_event(
                    "file_transfer_skipped",
                    domain=domain_name,
                    reason="alias domain shares its target domain's docroot",
                )
                progress.advance(f"Files skipped (alias domain): {domain_name}")
                continue
            source_docroot = self._resolve_source_docroot(domain, customer_login)
            target_docroot = self._resolve_target_docroot(domain, customer_login, target_login, source_docroot)
            pair = (source_docroot, target_docroot)
            if pair in transferred_docroots:
                self.runner.debug_event("file_transfer_skipped", domain=domain_name, reason="docroot already transferred")
                progress.advance(f"Files skipped (duplicate docroot): {domain_name}")
                continue
            transferred_docroots.add(pair)
            progress.status(f"Transferring domain data: {domain_name}")
            self.runner.transfer_files(source_docroot, target_docroot)
            self._fix_transferred_docroot_ownership(target_docroot, target_login)
            progress.advance(f"Files transferred: {domain_name}")

        if not selection.include_subdomains:
            return
        target_root = self.config.paths.target_web_root.rstrip("/")
        for sub in selection.subdomains:
            sub_name = self._domain_name(sub)
            sub_path = str(pick(sub, "path", default="")).strip()
            if not sub_path:
                continue
            # "" means the path *is* the customer root — don't fall
            # back to the raw path or the absolute source path would
            # be nested under the target docroot.
            relative_sub_path = relative_customer_path(sub_path, customer_login)
            source_path = self._resolve_source_docroot({**sub, "documentroot": sub_path}, customer_login)
            # The target record always stores the path relative to the
            # target customer's documentroot, so transfer under the
            # target login regardless of how the source stored it.
            target_path = f"{target_root}/{target_login}/{relative_sub_path}"
            pair = (source_path, target_path)
            if pair in transferred_docroots:
                progress.advance(f"Files skipped (duplicate path): {sub_name}")
                continue
            transferred_docroots.add(pair)
            progress.status(f"Transferring subdomain data: {sub_name}")
            self.runner.transfer_files(source_path, target_path)
            self._fix_transferred_docroot_ownership(target_path, target_login)
            progress.advance(f"Files transferred: {sub_name}")

    def _transfer_mail_content(self, selection: Selection, progress: _Progress, transferable_mailboxes: list[str]) -> None:
        if not (selection.include_mail and selection.mailboxes):
            return
        progress.status("Transferring mailbox content")
        for mailbox in transferable_mailboxes:
            progress.status(f"Transferring mailbox content: {mailbox}")
            self.runner.transfer_mailbox(mailbox)
        progress.advance("Mailbox content transferred")
