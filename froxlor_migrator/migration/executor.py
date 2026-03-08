from __future__ import annotations

from ..util import as_int, pick
from .accounts import MigratorAccountOps
from .core import MigratorCore
from .domains import MigratorDomainOps
from .types import MigrationContext, MigrationError, Selection


class Migrator(MigratorCore, MigratorDomainOps, MigratorAccountOps):
    def execute(self, selection: Selection) -> MigrationContext:
        self.preflight(selection)
        if self.runner.dry_run:
            target_customer_id = as_int(pick(selection.target_customer or {}, "customerid", "id", default=0))
            return MigrationContext(target_customer_id=target_customer_id, source_to_target_db={})

        target_customer_id = self._ensure_target_customer(selection.customer, selection.target_customer)
        customer_login = str(pick(selection.customer, "loginname", "login", default="")).strip()
        ip_value_mapping = self._build_ip_value_mapping(selection.domains, selection.ip_mapping)

        target_customer_login = None
        if selection.target_customer:
            target_customer_login = self._customer_login(selection.target_customer)

        self._ensure_domains(
            target_customer_id,
            selection.domains,
            selection.php_setting_map,
            selection.ip_mapping,
            ip_value_mapping,
            customer_login,
        )
        self._sync_domain_redirects(selection.domains)
        if selection.include_subdomains:
            self._ensure_subdomains(target_customer_id, selection.subdomains, selection.php_setting_map)
        self._migrate_domain_certificates(selection.domains)
        self._ensure_ftp_accounts(target_customer_id, selection.ftp_accounts, customer_login)
        self._ensure_ssh_keys(target_customer_id, selection.ssh_keys)
        self._ensure_data_dumps(target_customer_id, selection.data_dumps)
        self._ensure_dir_options(target_customer_id, selection.dir_options, customer_login)
        self._ensure_dir_protections(target_customer_id, selection.dir_protections, customer_login)
        self._ensure_domain_zones(selection.domain_zones, ip_value_mapping)
        self._enable_letsencrypt_after_dns(selection.domains)

        db_map: dict[str, str] = {}
        if selection.include_databases and selection.databases:
            self._sync_target_mysql_prefix_setting()
            known_before = {
                str(pick(item, "databasename", "dbname", "database", default=""))
                for item in self.target.list_mysqls()
                if str(pick(item, "databasename", "dbname", "database", default=""))
            }
            for source_db in selection.databases:
                source_name = str(pick(source_db, "databasename", "dbname", "database", default=""))
                target_name = self._create_database_on_target(target_customer_id, source_db, known_before, customer_login)
                if selection.validate_database_names and source_name != target_name:
                    raise MigrationError(
                        f"Database name mismatch: source={source_name!r} target={target_name!r}; preserving identical DB logins requires matching names"
                    )
                known_before.add(target_name)
                db_map[source_name] = target_name
                self._transfer_database_with_defaults(source_name, target_name)
            self._sync_database_login_hashes(db_map)

        transferable_mailboxes: list[str] = []
        if selection.mailboxes:
            transferable_mailboxes = self._ensure_mailboxes(target_customer_id, selection.mailboxes)
        self._ensure_email_forwarders(target_customer_id, selection.email_forwarders)
        self._ensure_email_sender_aliases(target_customer_id, selection.email_senders)
        self._sync_password_hashes(
            target_customer_id,
            selection.customer,
            selection.ftp_accounts,
            selection.mailboxes,
            selection.dir_protections,
            customer_login,
        )

        if selection.include_files:
            for domain in selection.domains:
                source_docroot = self._resolve_source_docroot(domain, customer_login)
                target_docroot = self._resolve_target_docroot(domain, customer_login, source_docroot)
                self.runner.transfer_files(source_docroot, target_docroot)
                self._fix_transferred_docroot_ownership(target_docroot, customer_login, target_customer_login)

        if selection.include_mail and transferable_mailboxes:
            for mailbox in transferable_mailboxes:
                self.runner.transfer_mailbox(mailbox)

        return MigrationContext(target_customer_id=target_customer_id, source_to_target_db=db_map)
