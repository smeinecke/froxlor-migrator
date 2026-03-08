from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class MigrationError(RuntimeError):
    pass


ResourceRow = dict[str, Any]


@dataclass
class Selection:
    customer: ResourceRow
    target_customer: ResourceRow | None
    domains: list[ResourceRow]
    subdomains: list[ResourceRow]
    databases: list[ResourceRow]
    mailboxes: list[ResourceRow]
    email_forwarders: list[ResourceRow]
    email_senders: list[ResourceRow]
    ftp_accounts: list[ResourceRow]
    ssh_keys: list[ResourceRow]
    data_dumps: list[ResourceRow]
    dir_protections: list[ResourceRow]
    dir_options: list[ResourceRow]
    domain_zones: list[ResourceRow]
    include_files: bool
    include_databases: bool
    include_mail: bool
    include_subdomains: bool
    validate_database_names: bool
    php_setting_map: dict[int, int]
    ip_mapping: dict[int, int]


@dataclass
class MigrationContext:
    target_customer_id: int
    source_to_target_db: dict[str, str]
