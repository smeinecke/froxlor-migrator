from __future__ import annotations

import json
import re
import shlex
import subprocess
from dataclasses import dataclass
from typing import Any

from .api import FroxlorClient
from .config import AppConfig
from .transfer import TransferRunner
from .util import as_bool, as_int, pick, random_password


class MigrationError(RuntimeError):
    pass


@dataclass
class Selection:
    customer: dict[str, Any]
    target_customer: dict[str, Any] | None
    domains: list[dict[str, Any]]
    subdomains: list[dict[str, Any]]
    databases: list[dict[str, Any]]
    mailboxes: list[dict[str, Any]]
    email_forwarders: list[dict[str, Any]]
    email_senders: list[dict[str, Any]]
    ftp_accounts: list[dict[str, Any]]
    ssh_keys: list[dict[str, Any]]
    data_dumps: list[dict[str, Any]]
    dir_protections: list[dict[str, Any]]
    dir_options: list[dict[str, Any]]
    domain_zones: list[dict[str, Any]]
    include_files: bool
    include_databases: bool
    include_mail: bool
    php_setting_map: dict[int, int]
    ip_mapping: dict[int, int]


@dataclass
class MigrationContext:
    target_customer_id: int
    source_to_target_db: dict[str, str]


class Migrator:
    def __init__(
        self,
        config: AppConfig,
        source: FroxlorClient,
        target: FroxlorClient,
        runner: TransferRunner,
    ) -> None:
        self.config = config
        self.source = source
        self.target = target
        self.runner = runner

    def preflight(self, selection: Selection) -> None:
        self.source.test_connection()
        self.target.test_connection()
        needs_ssh = (
            selection.include_files
            or selection.include_databases
            or selection.include_mail
            or any(as_int(pick(domain, "dkim", default=0)) == 1 for domain in selection.domains)
        )
        for command in self.runner.preflight_commands(
            include_ssh=needs_ssh,
            include_database_tools=selection.include_databases or needs_ssh,
            include_mail_tools=selection.include_mail,
        ):
            self.runner.run(command)

    def _find_target_customer(self, source_customer: dict[str, Any]) -> dict[str, Any] | None:
        source_login = str(pick(source_customer, "loginname", "login", default=""))
        source_email = str(pick(source_customer, "email", default="")).strip().lower()

        # Debug: Print target server info
        if hasattr(self.target, 'api_url'):
            print(f"DEBUG: Searching for customer on target server: {self.target.api_url}")
        print(f"DEBUG: Looking for source_login={source_login}, source_email={source_email}")

        for customer in self.target.list_customers():
            customer_login = str(pick(customer, "loginname", "login", default=""))
            customer_email = str(pick(customer, "email", default="")).strip().lower()

            if source_login and customer_login == source_login:
                print(f"DEBUG: Found existing customer by login: {customer_login}")
                return customer
            if source_email and customer_email and source_email == customer_email:
                print(f"DEBUG: Found existing customer by email: {customer_email}")
                return customer

        print(f"DEBUG: No existing customer found for login={source_login}, email={source_email}")
        return None

    def _customer_payload(self, source_customer: dict[str, Any]) -> dict[str, Any]:
        def _as_id_list(value: Any, fallback: list[int]) -> list[int]:
            if isinstance(value, list):
                result = [as_int(x) for x in value if as_int(x) > 0]
                return result or fallback
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return fallback
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, list):
                    result = [as_int(x) for x in parsed if as_int(x) > 0]
                    return result or fallback
                if text.isdigit() and as_int(text) > 0:
                    return [as_int(text)]
            numeric = as_int(value)
            if numeric > 0:
                return [numeric]
            return fallback

        return {
            "email": str(pick(source_customer, "email", default="migration@example.invalid")),
            "name": str(pick(source_customer, "name", "lastname", default="Migrated")),
            "firstname": str(pick(source_customer, "firstname", default="Customer")),
            "company": str(pick(source_customer, "company", default="")),
            "street": str(pick(source_customer, "street", default="")),
            "zipcode": str(pick(source_customer, "zipcode", default="")),
            "city": str(pick(source_customer, "city", default="")),
            "phone": str(pick(source_customer, "phone", default="")),
            "fax": str(pick(source_customer, "fax", default="")),
            "customernumber": str(pick(source_customer, "customernumber", default="")),
            "def_language": str(pick(source_customer, "def_language", default="en")),
            "gui_access": bool(as_int(pick(source_customer, "gui_access", default=1))),
            "api_allowed": bool(as_int(pick(source_customer, "api_allowed", default=1))),
            "shell_allowed": bool(as_int(pick(source_customer, "shell_allowed", default=0))),
            "gender": as_int(pick(source_customer, "gender", default=0)),
            "custom_notes": str(pick(source_customer, "custom_notes", default="")),
            "custom_notes_show": bool(as_int(pick(source_customer, "custom_notes_show", default=0))),
            "sendpassword": False,
            "deactivated": bool(as_int(pick(source_customer, "deactivated", default=0))),
            "diskspace": as_int(pick(source_customer, "diskspace", default=-1024)),
            "diskspace_ul": bool(as_int(pick(source_customer, "diskspace_ul", default=1))),
            "traffic": as_int(pick(source_customer, "traffic", default=-1048576)),
            "traffic_ul": bool(as_int(pick(source_customer, "traffic_ul", default=1))),
            "subdomains": as_int(pick(source_customer, "subdomains", default=-1)),
            "subdomains_ul": bool(as_int(pick(source_customer, "subdomains_ul", default=1))),
            "emails": as_int(pick(source_customer, "emails", default=-1)),
            "emails_ul": bool(as_int(pick(source_customer, "emails_ul", default=1))),
            "email_accounts": as_int(pick(source_customer, "email_accounts", default=-1)),
            "email_accounts_ul": bool(as_int(pick(source_customer, "email_accounts_ul", default=1))),
            "email_forwarders": as_int(pick(source_customer, "email_forwarders", default=-1)),
            "email_forwarders_ul": bool(as_int(pick(source_customer, "email_forwarders_ul", default=1))),
            "email_quota": as_int(pick(source_customer, "email_quota", default=-1)),
            "email_quota_ul": bool(as_int(pick(source_customer, "email_quota_ul", default=1))),
            "email_imap": bool(as_int(pick(source_customer, "imap", "email_imap", default=0))),
            "email_pop3": bool(as_int(pick(source_customer, "pop3", "email_pop3", default=0))),
            "ftps": as_int(pick(source_customer, "ftps", default=-1)),
            "ftps_ul": bool(as_int(pick(source_customer, "ftps_ul", default=1))),
            "mysqls": as_int(pick(source_customer, "mysqls", default=-1)),
            "mysqls_ul": bool(as_int(pick(source_customer, "mysqls_ul", default=1))),
            "createstdsubdomain": bool(as_int(pick(source_customer, "createstdsubdomain", default=1))),
            "phpenabled": bool(as_int(pick(source_customer, "phpenabled", default=1))),
            "allowed_phpconfigs": _as_id_list(pick(source_customer, "allowed_phpconfigs", default=[]), [1]),
            "perlenabled": bool(as_int(pick(source_customer, "perlenabled", default=0))),
            "dnsenabled": bool(as_int(pick(source_customer, "dnsenabled", default=0))),
            "logviewenabled": bool(as_int(pick(source_customer, "logviewenabled", default=0))),
            "store_defaultindex": bool(as_int(pick(source_customer, "store_defaultindex", default=0))),
            "theme": str(pick(source_customer, "theme", default="")),
            "allowed_mysqlserver": _as_id_list(pick(source_customer, "allowed_mysqlserver", default=[]), [0]),
            "type_2fa": as_int(pick(source_customer, "type_2fa", default=0)),
            "data_2fa": str(pick(source_customer, "data_2fa", default="")),
        }

    def _ensure_target_customer(self, source_customer: dict[str, Any], target_customer: dict[str, Any] | None = None) -> int:
        # If we already have a target customer (domain-only migration), use it
        if target_customer:
            customer_id = as_int(pick(target_customer, "customerid", "id", default=0))
            if not customer_id:
                raise MigrationError("Could not resolve pre-selected target customer id")
            return customer_id

        existing = self._find_target_customer(source_customer)
        payload = self._customer_payload(source_customer)
        if existing:
            customer_id = as_int(pick(existing, "customerid", "id", default=0))
            if not customer_id:
                raise MigrationError("Could not resolve existing target customer id")
            self.target.call(
                "Customers.update",
                {
                    "id": customer_id,
                    "loginname": str(pick(existing, "loginname", "login", default="")),
                    **payload,
                },
            )
            return customer_id

        data = self.target.call(
            "Customers.add",
            {
                **{k: v for k, v in payload.items() if k not in {"deactivated", "theme"}},
                "new_loginname": str(pick(source_customer, "loginname", "login", default="")),
            },
        )
        customer_id = as_int(pick(data or {}, "customerid", "id", default=0))
        if customer_id:
            return customer_id
        existing = self._find_target_customer(source_customer)
        if existing:
            return as_int(pick(existing, "customerid", "id", default=0))
        raise MigrationError("Failed to create target customer")

    def _domain_name(self, domain: dict[str, Any]) -> str:
        return str(pick(domain, "domain", "domainname", default=""))

    def _database_name(self, db: dict[str, Any]) -> str:
        return str(pick(db, "databasename", "dbname", "database", default=""))

    def _email_name(self, email: dict[str, Any]) -> str:
        return str(pick(email, "email_full", "email", "emailaddr", default=""))

    def _target_domain_set(self) -> set[str]:
        return {self._domain_name(item) for item in self.target.list_domains() if self._domain_name(item)}

    def _get_target_domain(self, domain_name: str) -> dict[str, Any] | None:
        for domain in self.target.list_domains():
            if self._domain_name(domain) == domain_name:
                return domain
        return None

    def _ssh_prefix(self) -> str:
        ssh = self.config.commands.ssh
        options = []
        if not self.config.ssh.strict_host_key_checking:
            options.append("-o StrictHostKeyChecking=no")
            options.append("-o UserKnownHostsFile=/dev/null")
        options.append(f"-p {self.config.ssh.port}")
        return f"{ssh} {' '.join(options)} -l {shlex.quote(self.config.ssh.user)} {shlex.quote(self.config.ssh.host)}"

    def _sql_utf8_literal(self, value: str) -> str:
        if value == "":
            return "''"
        return f"CONVERT(0x{value.encode('utf-8').hex()} USING utf8mb4)"

    def _sql_string_literal(self, value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    def _run_source_mysql_query(self, sql: str, database: str) -> list[list[str]]:
        if self.runner.dry_run:
            return []
        command = [
            *shlex.split(self.config.commands.mysql),
            *self.config.mysql.source_dump_args,
            database,
            "-N",
            "-B",
            "-e",
            sql,
        ]
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode != 0:
            raise MigrationError(f"Source panel SQL query failed: {completed.stderr.strip()[:400]}")
        rows: list[list[str]] = []
        for line in completed.stdout.splitlines():
            rows.append(line.split("\t"))
        return rows

    def _run_source_panel_query(self, sql: str) -> list[list[str]]:
        return self._run_source_mysql_query(sql, self.config.mysql.source_panel_database)

    def _exec_target_mysql_sql(self, sql: str, database: str) -> None:
        mysql = shlex.quote(self.config.commands.mysql)
        target_args = " ".join(shlex.quote(arg) for arg in self.config.mysql.target_import_args)
        db_name = shlex.quote(database)
        remote_cmd = f"{mysql} {target_args} {db_name} -e {shlex.quote(sql)}"
        self.runner.run(f"{self._ssh_prefix()} {shlex.quote(remote_cmd)}")

    def _exec_target_panel_sql(self, sql: str) -> None:
        self._exec_target_mysql_sql(sql, self.config.mysql.target_panel_database)

    def _sync_dkim_keys_db(self, domain_name: str, dkim_pubkey: str, dkim_privkey: str) -> None:
        update_sql = (
            "UPDATE panel_domains "
            f"SET dkim=1, dkim_pubkey={self._sql_utf8_literal(dkim_pubkey)}, "
            f"dkim_privkey={self._sql_utf8_literal(dkim_privkey)} "
            f"WHERE domain={self._sql_utf8_literal(domain_name)};"
        )
        self._exec_target_panel_sql(update_sql)

    def _ensure_target_mysql_prefix_dbname(self) -> None:
        sql = "UPDATE panel_settings SET value='DBNAME' WHERE settinggroup='customer' AND varname='mysqlprefix';"
        self._exec_target_panel_sql(sql)

    def _load_source_domain_redirects(self, domains: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
        domain_names = sorted({self._domain_name(row).lower() for row in domains if self._domain_name(row)})
        if not domain_names:
            return []
        domain_sql = ", ".join(self._sql_utf8_literal(name) for name in domain_names)
        rows = self._run_source_panel_query(
            "SELECT d.domain, a.domain, COALESCE(drc.rid, 1) "
            "FROM panel_domains d "
            "JOIN panel_domains a ON a.id = d.aliasdomain "
            "LEFT JOIN domain_redirect_codes drc ON drc.did = d.id "
            f"WHERE d.domain IN ({domain_sql}) AND d.aliasdomain IS NOT NULL;"
        )
        redirects: list[tuple[str, str, int]] = []
        for row in rows:
            if len(row) < 3:
                continue
            source_domain = str(row[0]).strip().lower()
            target_domain = str(row[1]).strip().lower()
            redirect_code = as_int(row[2], default=1)
            if source_domain and target_domain:
                redirects.append((source_domain, target_domain, redirect_code))
        return redirects

    def _sync_domain_redirects(self, domains: list[dict[str, Any]]) -> None:
        redirects = self._load_source_domain_redirects(domains)
        if not redirects:
            return
        statements: list[str] = []
        for domain_name, alias_name, redirect_code in redirects:
            statements.append(
                "UPDATE panel_domains d "
                "JOIN panel_domains a ON a.domain="
                f"{self._sql_utf8_literal(alias_name)} "
                f"SET d.aliasdomain=a.id WHERE d.domain={self._sql_utf8_literal(domain_name)};"
            )
            statements.append(
                "INSERT INTO domain_redirect_codes (did, rid) "
                "SELECT d.id, "
                f"{redirect_code} FROM panel_domains d "
                f"WHERE d.domain={self._sql_utf8_literal(domain_name)} "
                "ON DUPLICATE KEY UPDATE rid=VALUES(rid);"
            )
        self._exec_target_panel_sql(" ".join(statements))

    def _target_database_set(self) -> set[str]:
        return {self._database_name(item) for item in self.target.list_mysqls() if self._database_name(item)}

    def _target_mail_set(self) -> set[str]:
        return {self._email_name(item) for item in self.target.list_emails() if self._email_name(item)}

    def _migrate_domain_certificates(self, domains: list[dict[str, Any]]) -> None:
        source_certs = self.source.listing("Certificates.listing")
        target_certs = self.target.listing("Certificates.listing")
        source_by_domain = {str(pick(cert, "domainname", "domain", default="")).lower(): cert for cert in source_certs}
        target_by_domain = {str(pick(cert, "domainname", "domain", default="")).lower(): cert for cert in target_certs}

        for domain in domains:
            domain_name = self._domain_name(domain).lower()
            if not domain_name:
                continue
            source_cert = source_by_domain.get(domain_name)
            if not source_cert:
                continue

            cert_payload = {
                "domainname": domain_name,
                "ssl_cert_file": str(pick(source_cert, "ssl_cert_file", default="")),
                "ssl_key_file": str(pick(source_cert, "ssl_key_file", default="")),
                "ssl_ca_file": str(pick(source_cert, "ssl_ca_file", default="")),
                "ssl_cert_chainfile": str(pick(source_cert, "ssl_cert_chainfile", default="")),
            }
            if not cert_payload["ssl_cert_file"] or not cert_payload["ssl_key_file"]:
                continue

            existing_target_cert = target_by_domain.get(domain_name)
            if existing_target_cert:
                self.target.call("Certificates.update", cert_payload)
            else:
                self.target.call("Certificates.add", cert_payload)

            refreshed_target = {str(pick(cert, "domainname", "domain", default="")).lower(): cert for cert in self.target.listing("Certificates.listing")}
            target_cert = refreshed_target.get(domain_name)
            if not target_cert:
                raise MigrationError(f"Certificate migration failed for domain: {domain_name}")

            for field in ("ssl_cert_file", "ssl_key_file", "ssl_ca_file", "ssl_cert_chainfile"):
                expected = cert_payload[field]
                actual = str(pick(target_cert, field, default=""))
                if expected != actual:
                    raise MigrationError(f"Certificate mismatch for {domain_name}: {field} expected={len(expected)}B actual={len(actual)}B")

    def _build_ip_value_mapping(self, domains: list[dict[str, Any]], ip_mapping: dict[int, int]) -> dict[str, str]:
        if not ip_mapping:
            return {}

        source_ip_by_id: dict[int, str] = {}
        for domain in domains:
            for ip_row in pick(domain, "ipsandports", default=[]) or []:
                source_ip_id = as_int(pick(ip_row, "id", default=0))
                source_ip = str(pick(ip_row, "ip", default="")).strip().lower()
                if source_ip_id > 0 and source_ip:
                    source_ip_by_id[source_ip_id] = source_ip

        target_ip_by_id = {
            as_int(pick(row, "id", default=0)): str(pick(row, "ip", default="")).strip().lower()
            for row in self.target.listing("IpsAndPorts.listing")
            if as_int(pick(row, "id", default=0)) > 0 and str(pick(row, "ip", default="")).strip()
        }

        mapping: dict[str, str] = {}
        for source_ip_id, target_ip_id in ip_mapping.items():
            source_ip = source_ip_by_id.get(as_int(source_ip_id), "").strip().lower()
            target_ip = target_ip_by_id.get(as_int(target_ip_id), "").strip().lower()
            if not source_ip or not target_ip or source_ip == target_ip:
                continue
            mapping[source_ip] = target_ip
        return mapping

    def _replace_ip_tokens(self, value: str, replacements: dict[str, str]) -> str:
        if not value or not replacements:
            return value
        parts = re.split(r"(\s+)", value)
        for index, part in enumerate(parts):
            token = part.strip()
            if not token:
                continue
            replacement = replacements.get(token.lower())
            if replacement:
                parts[index] = replacement
        return "".join(parts)

    def _ensure_domains(
        self,
        target_customer_id: int,
        domains: list[dict[str, Any]],
        php_setting_map: dict[int, int],
        ip_mapping: dict[int, int],
        ip_value_mapping: dict[str, str],
        customer_login: str,
    ) -> None:
        existing_domains = self._target_domain_set()
        for domain in domains:
            domain_name = self._domain_name(domain)
            if not domain_name:
                continue

            source_docroot = self._resolve_source_docroot(domain, customer_login)
            target_docroot = self._resolve_target_docroot(domain, customer_login, source_docroot)
            source_php_setting = as_int(pick(domain, "phpsettingid", default=0))
            mapped_php_setting = php_setting_map.get(source_php_setting, source_php_setting)
            mapped_ip_ids: list[int] = []
            mapped_ssl_ip_ids: list[int] = []
            for ip_row in pick(domain, "ipsandports", default=[]) or []:
                source_ip_id = as_int(pick(ip_row, "id", default=0))
                target_ip_id = ip_mapping.get(source_ip_id)
                if target_ip_id:
                    mapped_ip_ids.append(target_ip_id)
                    if as_int(pick(ip_row, "ssl", default=0)) == 1:
                        mapped_ssl_ip_ids.append(target_ip_id)

            base_payload = {
                "customerid": target_customer_id,
                "documentroot": target_docroot,
                "isemaildomain": bool(as_int(pick(domain, "isemaildomain", default=0))),
                "email_only": bool(as_int(pick(domain, "email_only", default=0))),
                "phpenabled": bool(as_int(pick(domain, "phpenabled", default=1))),
                "sslenabled": bool(as_int(pick(domain, "sslenabled", "ssl_enabled", default=1))),
                "letsencrypt": bool(as_int(pick(domain, "letsencrypt", default=0))),
                "specialsettings": str(pick(domain, "specialsettings", default="")),
                "ssl_specialsettings": str(pick(domain, "ssl_specialsettings", default="")),
                "include_specialsettings": bool(as_int(pick(domain, "include_specialsettings", default=0))),
                "ssl_redirect": bool(as_int(pick(domain, "ssl_redirect", default=0))),
                "openbasedir": bool(as_int(pick(domain, "openbasedir", default=1))),
                "openbasedir_path": str(pick(domain, "openbasedir_path", default="0")),
                "notryfiles": bool(as_int(pick(domain, "notryfiles", default=0))),
                "writeaccesslog": bool(as_int(pick(domain, "writeaccesslog", default=1))),
                "writeerrorlog": bool(as_int(pick(domain, "writeerrorlog", default=1))),
                "http2": bool(as_int(pick(domain, "http2", default=0))),
                "http3": bool(as_int(pick(domain, "http3", default=0))),
                "hsts_maxage": as_int(pick(domain, "hsts", "hsts_maxage", default=0)),
                "hsts_sub": bool(as_int(pick(domain, "hsts_sub", default=0))),
                "hsts_preload": bool(as_int(pick(domain, "hsts_preload", default=0))),
                "ocsp_stapling": bool(as_int(pick(domain, "ocsp_stapling", default=0))),
                "override_tls": bool(as_int(pick(domain, "override_tls", default=0))),
                "ssl_protocols": str(pick(domain, "ssl_protocols", default="")),
                "ssl_cipher_list": str(pick(domain, "ssl_cipher_list", default="")),
                "tlsv13_cipher_list": str(pick(domain, "tlsv13_cipher_list", default="")),
                "honorcipherorder": bool(as_int(pick(domain, "ssl_honorcipherorder", "honorcipherorder", default=0))),
                "sessiontickets": bool(as_int(pick(domain, "ssl_sessiontickets", "sessiontickets", default=1))),
                "description": str(pick(domain, "description", default="")),
                "selectserveralias": as_int(pick(domain, "wwwserveralias", "selectserveralias", default=0)),
                "subcanemaildomain": as_int(pick(domain, "subcanemaildomain", default=0)),
                "speciallogfile": bool(as_int(pick(domain, "speciallogfile", default=0))),
                "alias": as_int(pick(domain, "alias", default=0)),
                "registration_date": str(pick(domain, "registration_date", default="")),
                "termination_date": str(pick(domain, "termination_date", default="")),
                "caneditdomain": bool(as_int(pick(domain, "caneditdomain", default=0))),
                "isbinddomain": bool(as_int(pick(domain, "isbinddomain", default=0))),
                "zonefile": self._replace_ip_tokens(str(pick(domain, "zonefile", default="")), ip_value_mapping),
                "dkim": bool(as_int(pick(domain, "dkim", default=0))),
                "specialsettingsforsubdomains": bool(as_int(pick(domain, "specialsettingsforsubdomains", default=0))),
                "phpsettingsforsubdomains": bool(as_int(pick(domain, "phpsettingsforsubdomains", default=0))),
                "mod_fcgid_starter": as_int(pick(domain, "mod_fcgid_starter", default=-1)),
                "mod_fcgid_maxrequests": as_int(pick(domain, "mod_fcgid_maxrequests", default=-1)),
                "dont_use_default_ssl_ipandport_if_empty": bool(as_int(pick(domain, "dont_use_default_ssl_ipandport_if_empty", default=0))),
                "deactivated": bool(as_int(pick(domain, "deactivated", default=0))),
            }
            if mapped_php_setting > 0:
                base_payload["phpsettingid"] = mapped_php_setting
            if mapped_ip_ids:
                base_payload["ipandport"] = [{"id": target_ip_id} for target_ip_id in sorted(set(mapped_ip_ids))]
            if mapped_ssl_ip_ids:
                base_payload["ssl_ipandport"] = [{"id": target_ip_id} for target_ip_id in sorted(set(mapped_ssl_ip_ids))]

            if domain_name in existing_domains:
                if self.config.behavior.domain_exists == "fail":
                    raise MigrationError(f"Target domain already exists: {domain_name}")
                if self.config.behavior.domain_exists == "skip":
                    continue
                existing = self._get_target_domain(domain_name)
                if not existing:
                    raise MigrationError(f"Target domain lookup failed: {domain_name}")
                domain_id = as_int(pick(existing, "id", default=0))
            else:
                self.target.call("Domains.add", {"domain": domain_name, **base_payload})
                existing_domains.add(domain_name)
                created = self._get_target_domain(domain_name)
                if not created:
                    raise MigrationError(f"Could not find created target domain: {domain_name}")
                domain_id = as_int(pick(created, "id", default=0))

            self.target.call(
                "Domains.update",
                {
                    "id": domain_id,
                    "domainname": domain_name,
                    **base_payload,
                },
            )

            target_domain = self._get_target_domain(domain_name)
            if not target_domain:
                raise MigrationError(f"Could not reload target domain after update: {domain_name}")

            comparisons: list[tuple[str, Any, Any]] = [
                ("documentroot", target_docroot, pick(target_domain, "documentroot", default="")),
                (
                    "specialsettings",
                    base_payload["specialsettings"],
                    pick(target_domain, "specialsettings", default=""),
                ),
                (
                    "ssl_specialsettings",
                    base_payload["ssl_specialsettings"],
                    pick(target_domain, "ssl_specialsettings", default=""),
                ),
                (
                    "ssl_redirect",
                    int(bool(base_payload["ssl_redirect"])),
                    as_int(pick(target_domain, "ssl_redirect", default=0)),
                ),
                (
                    "sslenabled",
                    int(bool(base_payload["sslenabled"])),
                    as_int(pick(target_domain, "ssl_enabled", default=0)),
                ),
                (
                    "letsencrypt",
                    int(bool(base_payload["letsencrypt"])),
                    as_int(pick(target_domain, "letsencrypt", default=0)),
                ),
                (
                    "openbasedir",
                    int(bool(base_payload["openbasedir"])),
                    as_int(pick(target_domain, "openbasedir", default=0)),
                ),
                (
                    "openbasedir_path",
                    str(base_payload["openbasedir_path"]),
                    str(pick(target_domain, "openbasedir_path", default="")),
                ),
                (
                    "phpsettingid",
                    as_int(base_payload.get("phpsettingid", 0)),
                    as_int(pick(target_domain, "phpsettingid", default=0)),
                ),
                (
                    "isemaildomain",
                    int(bool(base_payload["isemaildomain"])),
                    as_int(pick(target_domain, "isemaildomain", default=0)),
                ),
                (
                    "email_only",
                    int(bool(base_payload["email_only"])),
                    as_int(pick(target_domain, "email_only", default=0)),
                ),
                (
                    "alias",
                    as_int(base_payload["alias"]),
                    as_int(pick(target_domain, "alias", default=0)),
                ),
                (
                    "speciallogfile",
                    int(bool(base_payload["speciallogfile"])),
                    as_int(pick(target_domain, "speciallogfile", default=0)),
                ),
                (
                    "caneditdomain",
                    int(bool(base_payload["caneditdomain"])),
                    as_int(pick(target_domain, "caneditdomain", default=0)),
                ),
                (
                    "isbinddomain",
                    int(bool(base_payload["isbinddomain"])),
                    as_int(pick(target_domain, "isbinddomain", default=0)),
                ),
                (
                    "notryfiles",
                    int(bool(base_payload["notryfiles"])),
                    as_int(pick(target_domain, "notryfiles", default=0)),
                ),
                (
                    "hsts_maxage",
                    as_int(base_payload["hsts_maxage"]),
                    as_int(pick(target_domain, "hsts", default=0)),
                ),
                (
                    "hsts_sub",
                    int(bool(base_payload["hsts_sub"])),
                    as_int(pick(target_domain, "hsts_sub", default=0)),
                ),
                (
                    "hsts_preload",
                    int(bool(base_payload["hsts_preload"])),
                    as_int(pick(target_domain, "hsts_preload", default=0)),
                ),
                (
                    "http2",
                    int(bool(base_payload["http2"])),
                    as_int(pick(target_domain, "http2", default=0)),
                ),
                (
                    "http3",
                    int(bool(base_payload["http3"])),
                    as_int(pick(target_domain, "http3", default=0)),
                ),
                (
                    "ocsp_stapling",
                    int(bool(base_payload["ocsp_stapling"])),
                    as_int(pick(target_domain, "ocsp_stapling", default=0)),
                ),
                (
                    "dkim",
                    int(bool(base_payload["dkim"])),
                    as_int(pick(target_domain, "dkim", default=0)),
                ),
                (
                    "honorcipherorder",
                    int(bool(base_payload["honorcipherorder"])),
                    as_int(pick(target_domain, "ssl_honorcipherorder", default=0)),
                ),
                (
                    "sessiontickets",
                    int(bool(base_payload["sessiontickets"])),
                    as_int(pick(target_domain, "ssl_sessiontickets", default=0)),
                ),
                (
                    "override_tls",
                    int(bool(base_payload["override_tls"])),
                    as_int(pick(target_domain, "override_tls", default=0)),
                ),
                (
                    "specialsettingsforsubdomains",
                    int(bool(base_payload["specialsettingsforsubdomains"])),
                    as_int(pick(target_domain, "specialsettingsforsubdomains", default=0)),
                ),
                (
                    "phpsettingsforsubdomains",
                    int(bool(base_payload["phpsettingsforsubdomains"])),
                    as_int(pick(target_domain, "phpsettingsforsubdomains", default=0)),
                ),
                (
                    "mod_fcgid_starter",
                    as_int(base_payload["mod_fcgid_starter"]),
                    as_int(pick(target_domain, "mod_fcgid_starter", default=-1)),
                ),
                (
                    "mod_fcgid_maxrequests",
                    as_int(base_payload["mod_fcgid_maxrequests"]),
                    as_int(pick(target_domain, "mod_fcgid_maxrequests", default=-1)),
                ),
                (
                    "deactivated",
                    int(bool(base_payload["deactivated"])),
                    as_int(pick(target_domain, "deactivated", default=0)),
                ),
            ]
            for field_name, expected, actual in comparisons:
                if str(expected) != str(actual):
                    raise MigrationError(f"Domain setting mismatch after migration for {domain_name}: {field_name} expected={expected!r} actual={actual!r}")

            source_dkim_public = str(pick(domain, "dkim_pubkey", default=""))
            source_dkim_private = str(pick(domain, "dkim_privkey", default=""))
            target_dkim_public = str(pick(target_domain, "dkim_pubkey", default=""))
            if source_dkim_public and source_dkim_public != target_dkim_public:
                if not source_dkim_private:
                    raise MigrationError(f"DKIM key mismatch for {domain_name} and source private key is empty")
                self._sync_dkim_keys_db(domain_name, source_dkim_public, source_dkim_private)
                if self.runner.dry_run:
                    continue
                target_domain = self._get_target_domain(domain_name)
                if not target_domain:
                    raise MigrationError(f"Could not reload target domain after DKIM DB sync: {domain_name}")
                target_dkim_public = str(pick(target_domain, "dkim_pubkey", default=""))
                if source_dkim_public != target_dkim_public:
                    raise MigrationError(f"DKIM public key mismatch for {domain_name} after DB sync fallback")

            if mapped_ip_ids:
                actual_ip_ids = {as_int(pick(item, "id", default=0)) for item in pick(target_domain, "ipsandports", default=[]) or []}
                missing_ip_ids = {ip_id for ip_id in mapped_ip_ids if ip_id not in actual_ip_ids}
                if missing_ip_ids:
                    raise MigrationError(f"Domain IP mapping mismatch after migration for {domain_name}: missing target IP ids {sorted(missing_ip_ids)}")

    def _create_database_on_target(
        self,
        target_customer_id: int,
        source_db: dict[str, Any],
        known_before: set[str],
        customer_login: str,
    ) -> str:
        src_name = self._database_name(source_db)
        if not src_name:
            raise MigrationError("Source database has no name")
        if src_name in known_before:
            if self.config.behavior.database_exists == "fail":
                raise MigrationError(f"Target database already exists: {src_name}")
            return src_name

        payload = {
            "customerid": target_customer_id,
            "mysql_password": random_password(24),
            "description": str(pick(source_db, "description", default=f"Migrated from {src_name}")),
            "custom_suffix": src_name[len(customer_login) + 1 :] if src_name.startswith(f"{customer_login}_") else src_name,
            "sendinfomail": False,
        }
        self.target.call("Mysqls.add", payload)

        after = self._target_database_set()
        new_entries = sorted(after - known_before)
        if src_name in after:
            return src_name
        if len(new_entries) == 1:
            return new_entries[0]
        raise MigrationError(f"Could not detect created target database for source: {src_name}")

    def _ensure_subdomains(
        self,
        target_customer_id: int,
        subdomains: list[dict[str, Any]],
        php_setting_map: dict[int, int],
    ) -> None:
        if not subdomains:
            return

        target_rows = self.target.list_subdomains(customerid=target_customer_id)
        target_by_name = {str(pick(row, "domain", "domainname", default="")).strip().lower(): row for row in target_rows}
        target_domain_names = {
            str(pick(row, "domain", "domainname", default="")).strip().lower() for row in self.target.list_domains(customerid=target_customer_id)
        }

        for row in subdomains:
            full_name = str(pick(row, "domain", "domainname", default="")).strip().lower()
            if not full_name:
                continue

            parent_domain = str(pick(row, "parentdomain", "maindomain", default="")).strip().lower()
            sub_part = str(pick(row, "subdomain", default="")).strip()
            if not parent_domain and "." in full_name:
                parts = full_name.split(".", 1)
                sub_part = parts[0]
                parent_domain = parts[1]
            if not sub_part or not parent_domain:
                raise MigrationError(f"Cannot resolve subdomain components for {full_name}")
            if parent_domain not in target_domain_names:
                continue

            source_php_setting = as_int(pick(row, "phpsettingid", default=0))
            mapped_php_setting = php_setting_map.get(source_php_setting, source_php_setting)

            payload = {
                "domainname": full_name,
                "alias": as_int(pick(row, "alias", default=0)),
                "path": str(pick(row, "path", default="")),
                "url": str(pick(row, "url", default="")),
                "selectserveralias": as_int(pick(row, "wwwserveralias", "selectserveralias", default=0)),
                "isemaildomain": bool(as_int(pick(row, "isemaildomain", default=0))),
                "openbasedir_path": as_int(pick(row, "openbasedir_path", default=0)),
                "redirectcode": as_int(pick(row, "redirectcode", default=0)),
                "speciallogfile": bool(as_int(pick(row, "speciallogfile", default=0))),
                "sslenabled": bool(as_int(pick(row, "sslenabled", "ssl_enabled", default=1))),
                "ssl_redirect": bool(as_int(pick(row, "ssl_redirect", default=0))),
                "letsencrypt": bool(as_int(pick(row, "letsencrypt", default=0))),
                "http2": bool(as_int(pick(row, "http2", default=0))),
                "http3": bool(as_int(pick(row, "http3", default=0))),
                "hsts_maxage": as_int(pick(row, "hsts", "hsts_maxage", default=0)),
                "hsts_sub": bool(as_int(pick(row, "hsts_sub", default=0))),
                "hsts_preload": bool(as_int(pick(row, "hsts_preload", default=0))),
                "customerid": target_customer_id,
            }
            if mapped_php_setting > 0:
                payload["phpsettingid"] = mapped_php_setting

            target_row = target_by_name.get(full_name)
            if target_row:
                sub_id = as_int(pick(target_row, "id", default=0))
                self.target.call("SubDomains.update", {"id": sub_id, **payload})
            else:
                self.target.call(
                    "SubDomains.add",
                    {
                        "subdomain": sub_part,
                        "domain": parent_domain,
                        **payload,
                    },
                )

            refreshed = self.target.list_subdomains(customerid=target_customer_id)
            target_by_name = {str(pick(item, "domain", "domainname", default="")).strip().lower(): item for item in refreshed}

    def _ensure_email_forwarders(self, target_customer_id: int, forwarders: list[dict[str, Any]]) -> None:
        if not forwarders:
            return
        target_rows = self.target.list_email_forwarders(customerid=target_customer_id)
        existing = {
            (
                str(pick(row, "email", "emailaddr", default="")).strip().lower(),
                str(pick(row, "destination", default="")).strip().lower(),
            )
            for row in target_rows
        }
        for row in forwarders:
            emailaddr = str(pick(row, "email", "emailaddr", default="")).strip().lower()
            destination = str(pick(row, "destination", default="")).strip().lower()
            if not emailaddr or not destination:
                continue
            key = (emailaddr, destination)
            if key in existing:
                continue
            self.target.call(
                "EmailForwarders.add",
                {
                    "emailaddr": emailaddr,
                    "destination": destination,
                    "customerid": target_customer_id,
                },
            )
            existing.add(key)

    def _ensure_email_sender_aliases(self, target_customer_id: int, sender_aliases: list[dict[str, Any]]) -> None:
        if not sender_aliases:
            return
        target_rows = self.target.list_email_senders(customerid=target_customer_id)
        existing = {
            (
                str(pick(row, "email", "emailaddr", default="")).strip().lower(),
                str(pick(row, "allowed_sender", default="")).strip().lower(),
            )
            for row in target_rows
        }
        for row in sender_aliases:
            emailaddr = str(pick(row, "email", "emailaddr", default="")).strip().lower()
            allowed_sender = str(pick(row, "allowed_sender", default="")).strip().lower()
            if not emailaddr or not allowed_sender:
                continue
            key = (emailaddr, allowed_sender)
            if key in existing:
                continue
            self.target.call(
                "EmailSender.add",
                {
                    "emailaddr": emailaddr,
                    "allowed_sender": allowed_sender,
                    "customerid": target_customer_id,
                },
            )
            existing.add(key)

    def _ensure_ftp_accounts(self, target_customer_id: int, ftp_accounts: list[dict[str, Any]], customer_login: str) -> None:
        if not ftp_accounts:
            return
        target_rows = self.target.list_ftps(customerid=target_customer_id)
        by_username = {str(pick(row, "username", "ftpuser", default="")).strip().lower(): row for row in target_rows}
        for row in ftp_accounts:
            username = str(pick(row, "username", "ftpuser", default="")).strip().lower()
            if not username:
                continue
            ftp_path = str(pick(row, "path", default="")).strip().strip("/")
            if not ftp_path:
                homedir = str(pick(row, "homedir", default="")).strip()
                marker = f"/{customer_login.strip('/')}/"
                if marker in homedir:
                    ftp_path = homedir.split(marker, 1)[1].strip("/")
            if not ftp_path:
                ftp_path = customer_login
            payload = {
                "path": ftp_path,
                "ftp_description": str(pick(row, "description", "ftp_description", default="")),
                "shell": str(pick(row, "shell", default="/bin/false")),
                "login_enabled": as_bool(pick(row, "login_enabled", default=1), default=True),
                "customerid": target_customer_id,
            }
            existing = by_username.get(username)
            if existing:
                self.target.call(
                    "Ftps.update",
                    {
                        "id": as_int(pick(existing, "id", default=0)),
                        "username": username,
                        **payload,
                    },
                )
                continue

            add_payload = {
                **payload,
                "ftp_password": random_password(24),
                "ftp_username": username.split("@", 1)[0],
                "sendinfomail": False,
            }
            if "@" in username:
                add_payload["ftp_domain"] = username.split("@", 1)[1]
            self.target.call("Ftps.add", add_payload)
            refreshed = self.target.list_ftps(customerid=target_customer_id)
            by_username = {str(pick(item, "username", "ftpuser", default="")).strip().lower(): item for item in refreshed}

    def _ensure_ssh_keys(self, target_customer_id: int, ssh_keys: list[dict[str, Any]]) -> None:
        if not ssh_keys:
            return
        target_ftp_names = {str(pick(item, "username", "ftpuser", default="")).strip().lower() for item in self.target.list_ftps(customerid=target_customer_id)}
        target_rows = self.target.list_ssh_keys(customerid=target_customer_id)
        existing = {
            (
                str(pick(row, "username", "ftpuser", default="")).strip().lower(),
                str(pick(row, "ssh_pubkey", default="")).strip(),
            ): row
            for row in target_rows
        }
        for row in ssh_keys:
            ftp_user = str(pick(row, "username", "ftpuser", default="")).strip().lower()
            ssh_pubkey = str(pick(row, "ssh_pubkey", default="")).strip()
            description = str(pick(row, "description", default="")).strip()
            if not ftp_user or not ssh_pubkey:
                continue
            if ftp_user not in target_ftp_names:
                raise MigrationError(f"Could not map SSH key FTP user on target: {ftp_user}")
            key = (ftp_user, ssh_pubkey)
            existing_row = existing.get(key)
            if existing_row:
                existing_description = str(pick(existing_row, "description", default="")).strip()
                if existing_description != description:
                    self.target.call(
                        "SshKeys.update",
                        {
                            "id": as_int(pick(existing_row, "id", default=0)),
                            "customerid": target_customer_id,
                            "description": description,
                        },
                    )
                continue
            self.target.call(
                "SshKeys.add",
                {
                    "ftpuser": ftp_user,
                    "customerid": target_customer_id,
                    "ssh_pubkey": ssh_pubkey,
                    "description": description,
                },
            )
            refreshed = self.target.list_ssh_keys(customerid=target_customer_id)
            existing = {
                (
                    str(pick(item, "username", "ftpuser", default="")).strip().lower(),
                    str(pick(item, "ssh_pubkey", default="")).strip(),
                ): item
                for item in refreshed
            }

    def _ensure_data_dumps(self, target_customer_id: int, data_dumps: list[dict[str, Any]]) -> None:
        if not data_dumps:
            return
        target_rows = self.target.list_data_dumps(customerid=target_customer_id)
        existing = {
            (
                str(pick(row, "path", default="")).strip(),
                as_int(pick(row, "dump_dbs", default=0)),
                as_int(pick(row, "dump_mail", default=0)),
                as_int(pick(row, "dump_web", default=0)),
                str(pick(row, "pgp_public_key", default="")).strip(),
            )
            for row in target_rows
        }
        for row in data_dumps:
            path = str(pick(row, "path", default="")).strip()
            if not path:
                continue
            payload = {
                "customerid": target_customer_id,
                "path": path,
                "pgp_public_key": str(pick(row, "pgp_public_key", default="")).strip(),
                "dump_dbs": as_bool(pick(row, "dump_dbs", default=0), default=False),
                "dump_mail": as_bool(pick(row, "dump_mail", default=0), default=False),
                "dump_web": as_bool(pick(row, "dump_web", default=0), default=False),
            }
            key = (
                payload["path"],
                int(bool(payload["dump_dbs"])),
                int(bool(payload["dump_mail"])),
                int(bool(payload["dump_web"])),
                payload["pgp_public_key"],
            )
            if key in existing:
                continue
            try:
                self.target.call("DataDump.add", payload)
            except Exception as exc:
                message = str(exc).lower()
                if "405" in message or "cannot access this resource" in message:
                    return
                raise
            existing.add(key)

    def _relative_customer_path(self, path: str, customer_login: str) -> str:
        cleaned = path.strip().strip("/")
        if not cleaned:
            return ""
        marker = f"/{customer_login.strip('/')}/"
        lowered = cleaned.lower()
        if marker.lower() in f"/{lowered}/":
            original = cleaned
            while marker in f"/{original}/":
                if marker in original:
                    original = original.split(marker, 1)[1].strip("/")
                else:
                    break
            cleaned = original
        if cleaned.startswith(customer_login.strip("/") + "/"):
            cleaned = cleaned[len(customer_login.strip("/")) + 1 :]
        return cleaned

    def _ensure_dir_options(self, target_customer_id: int, dir_options: list[dict[str, Any]], customer_login: str) -> None:
        if not dir_options:
            return
        target_rows = self.target.list_dir_options(customerid=target_customer_id)
        by_path = {self._relative_customer_path(str(pick(row, "path", default="")), customer_login).lower(): row for row in target_rows}
        for row in dir_options:
            path = self._relative_customer_path(str(pick(row, "path", default="")), customer_login)
            if not path:
                continue
            payload = {
                "customerid": target_customer_id,
                "path": path,
                "options_indexes": as_bool(pick(row, "options_indexes", default=0), default=False),
                "options_cgi": as_bool(pick(row, "options_cgi", default=0), default=False),
                "error404path": str(pick(row, "error404path", default="")),
                "error403path": str(pick(row, "error403path", default="")),
                "error500path": str(pick(row, "error500path", default="")),
                "error401path": str(pick(row, "error401path", default="")),
            }
            existing = by_path.get(path.lower())
            if existing:
                self.target.call(
                    "DirOptions.update",
                    {
                        "id": as_int(pick(existing, "id", default=0)),
                        **payload,
                    },
                )
            else:
                self.target.call("DirOptions.add", payload)
            refreshed = self.target.list_dir_options(customerid=target_customer_id)
            by_path = {self._relative_customer_path(str(pick(item, "path", default="")), customer_login).lower(): item for item in refreshed}

    def _ensure_dir_protections(self, target_customer_id: int, dir_protections: list[dict[str, Any]], customer_login: str) -> None:
        if not dir_protections:
            return
        target_rows = self.target.list_dir_protections(customerid=target_customer_id)
        existing = {
            (
                self._relative_customer_path(str(pick(row, "path", default="")), customer_login).lower(),
                str(pick(row, "username", default="")).strip().lower(),
            ): row
            for row in target_rows
        }
        for row in dir_protections:
            path = self._relative_customer_path(str(pick(row, "path", default="")), customer_login)
            username = str(pick(row, "username", default="")).strip().lower()
            if not path or not username:
                continue
            authname = str(pick(row, "authname", default="Restricted Area")).strip() or "Restricted Area"
            key = (path.lower(), username)
            target_row = existing.get(key)
            payload = {
                "customerid": target_customer_id,
                "path": path,
                "username": username,
                "directory_authname": authname,
                "directory_password": random_password(24),
            }
            if target_row:
                self.target.call(
                    "DirProtections.update",
                    {
                        "id": as_int(pick(target_row, "id", default=0)),
                        "username": username,
                        "customerid": target_customer_id,
                        "directory_authname": authname,
                        "directory_password": payload["directory_password"],
                    },
                )
            else:
                self.target.call("DirProtections.add", payload)
            refreshed = self.target.list_dir_protections(customerid=target_customer_id)
            existing = {
                (
                    self._relative_customer_path(str(pick(item, "path", default="")), customer_login).lower(),
                    str(pick(item, "username", default="")).strip().lower(),
                ): item
                for item in refreshed
            }

    def _load_source_mail_password_hashes(self, mailboxes: list[dict[str, Any]]) -> dict[str, tuple[str, str]]:
        emails = {
            str(pick(item, "email_full", "email", "emailaddr", default="")).strip().lower()
            for item in mailboxes
            if str(pick(item, "email_full", "email", "emailaddr", default="")).strip().lower()
        }
        if not emails:
            return {}
        email_list_sql = ", ".join(self._sql_utf8_literal(email) for email in sorted(emails))
        rows = self._run_source_panel_query(f"SELECT email, password, password_enc FROM mail_users WHERE email IN ({email_list_sql});")
        out: dict[str, tuple[str, str]] = {}
        for row in rows:
            if len(row) < 3:
                continue
            out[row[0].strip().lower()] = (row[1], row[2])
        return out

    def _load_source_database_user_hashes(self, source_db_names: list[str]) -> dict[str, tuple[str, str]]:
        db_users = [name.strip() for name in source_db_names if name.strip()]
        if not db_users:
            return {}
        user_literals = ", ".join(self._sql_utf8_literal(name) for name in sorted(set(db_users)))
        rows = self._run_source_mysql_query(
            f"SELECT User, plugin, authentication_string FROM mysql.user WHERE User IN ({user_literals});",
            "mysql",
        )
        out: dict[str, tuple[str, str]] = {}
        for row in rows:
            if len(row) < 3:
                continue
            out[row[0].strip()] = (row[1], row[2])
        return out

    def _sync_customer_password_hash(self, source_customer: dict[str, Any], target_customer_id: int) -> None:
        password_hash = str(pick(source_customer, "password", default="")).strip()
        if not password_hash:
            return
        sql = f"UPDATE panel_customers SET password={self._sql_utf8_literal(password_hash)} WHERE customerid={target_customer_id};"
        self._exec_target_panel_sql(sql)

    def _sync_customer_2fa_settings(self, source_customer: dict[str, Any], target_customer_id: int) -> None:
        type_2fa = as_int(pick(source_customer, "type_2fa", default=0))
        data_2fa = str(pick(source_customer, "data_2fa", default="")).strip()
        sql = f"UPDATE panel_customers SET type_2fa={type_2fa}, data_2fa={self._sql_utf8_literal(data_2fa)} WHERE customerid={target_customer_id};"
        self._exec_target_panel_sql(sql)

    def _sync_ftp_password_hashes(self, target_customer_id: int, ftp_accounts: list[dict[str, Any]]) -> None:
        statements: list[str] = []
        for row in ftp_accounts:
            username = str(pick(row, "username", "ftpuser", default="")).strip().lower()
            password_hash = str(pick(row, "password", default="")).strip()
            if not username:
                continue
            if not password_hash:
                raise MigrationError(f"Source FTP account has empty password hash: {username}")
            statements.append(
                "UPDATE ftp_users "
                f"SET password={self._sql_utf8_literal(password_hash)} "
                f"WHERE customerid={target_customer_id} AND username={self._sql_utf8_literal(username)};"
            )
        if statements:
            self._exec_target_panel_sql(" ".join(statements))

    def _sync_mail_password_hashes(self, target_customer_id: int, mailboxes: list[dict[str, Any]]) -> None:
        source_hashes = self._load_source_mail_password_hashes(mailboxes)
        statements: list[str] = []
        for mailbox in mailboxes:
            emailaddr = str(pick(mailbox, "email_full", "email", "emailaddr", default="")).strip().lower()
            if not emailaddr:
                continue
            if emailaddr not in source_hashes:
                raise MigrationError(f"Source mailbox login hash missing in mail_users table: {emailaddr}")
            password_hash, password_enc = source_hashes[emailaddr]
            if not password_hash and not password_enc:
                raise MigrationError(f"Source mailbox login hash empty for: {emailaddr}")
            statements.append(
                "UPDATE mail_users "
                f"SET password={self._sql_utf8_literal(password_hash)}, "
                f"password_enc={self._sql_utf8_literal(password_enc)} "
                f"WHERE customerid={target_customer_id} AND email={self._sql_utf8_literal(emailaddr)};"
            )
        if statements:
            self._exec_target_panel_sql(" ".join(statements))

    def _sync_dir_protection_password_hashes(
        self,
        target_customer_id: int,
        dir_protections: list[dict[str, Any]],
        customer_login: str,
    ) -> None:
        target_rows = self.target.list_dir_protections(customerid=target_customer_id)
        target_by_key = {
            (
                self._relative_customer_path(str(pick(row, "path", default="")), customer_login).lower(),
                str(pick(row, "username", default="")).strip().lower(),
            ): str(pick(row, "path", default="")).strip()
            for row in target_rows
        }
        statements: list[str] = []
        for row in dir_protections:
            path = self._relative_customer_path(str(pick(row, "path", default="")), customer_login)
            username = str(pick(row, "username", default="")).strip().lower()
            password_hash = str(pick(row, "password", default="")).strip()
            if not path or not username or not password_hash:
                continue
            target_path = target_by_key.get((path.lower(), username), "")
            if not target_path:
                continue
            statements.append(
                "UPDATE panel_htpasswds "
                f"SET password={self._sql_utf8_literal(password_hash)} "
                f"WHERE customerid={target_customer_id} "
                f"AND path={self._sql_utf8_literal(target_path)} "
                f"AND username={self._sql_utf8_literal(username)};"
            )
        if statements:
            self._exec_target_panel_sql(" ".join(statements))

    def _sync_database_login_hashes(self, source_to_target_db: dict[str, str]) -> None:
        if not source_to_target_db:
            return
        source_hashes = self._load_source_database_user_hashes(list(source_to_target_db.keys()))
        statements: list[str] = []
        for source_db, target_db in source_to_target_db.items():
            auth = source_hashes.get(source_db)
            if not auth:
                raise MigrationError(f"Source DB login hash missing in mysql.user for database user: {source_db}")
            plugin, auth_hash = auth
            if not auth_hash:
                raise MigrationError(f"Source DB login hash empty for database user: {source_db}")
            if not re.fullmatch(r"[A-Za-z0-9_]+", plugin):
                raise MigrationError(f"Unsupported SQL auth plugin name for database user {source_db}: {plugin!r}")
            for host in ["%", "localhost", "target-db", "127.0.0.1", self.config.ssh.host]:
                statements.append(
                    "ALTER USER IF EXISTS "
                    f"{self._sql_string_literal(target_db)}@{self._sql_string_literal(host)} "
                    f"IDENTIFIED VIA {plugin} USING {self._sql_string_literal(auth_hash)};"
                )
        if not statements:
            return
        self._exec_target_mysql_sql(" ".join(statements), "mysql")

    def _sync_password_hashes(
        self,
        target_customer_id: int,
        source_customer: dict[str, Any],
        ftp_accounts: list[dict[str, Any]],
        mailboxes: list[dict[str, Any]],
        dir_protections: list[dict[str, Any]],
        customer_login: str,
    ) -> None:
        self._sync_customer_password_hash(source_customer, target_customer_id)
        self._sync_customer_2fa_settings(source_customer, target_customer_id)
        self._sync_ftp_password_hashes(target_customer_id, ftp_accounts)
        self._sync_mail_password_hashes(target_customer_id, mailboxes)
        self._sync_dir_protection_password_hashes(target_customer_id, dir_protections, customer_login)

    def _is_custom_zone_record(self, row: dict[str, Any]) -> bool:
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
        if record_type in {"SOA", "NS"}:
            return False
        return True

    def _ensure_domain_zones(self, domain_zones: list[dict[str, Any]], ip_value_mapping: dict[str, str]) -> None:
        if not domain_zones:
            return
        by_domain: dict[str, list[dict[str, Any]]] = {}
        for row in domain_zones:
            domainname = str(pick(row, "domainname", default="")).strip().lower()
            if not domainname:
                continue
            by_domain.setdefault(domainname, []).append(row)

        for domainname, rows in by_domain.items():
            target_rows = self.target.list_domain_zones(domainname=domainname)
            existing = {
                (
                    str(pick(item, "record", default="")).strip().lower(),
                    str(pick(item, "type", default="")).strip().upper(),
                    as_int(pick(item, "prio", default=0)),
                    str(pick(item, "content", default="")).strip().lower(),
                    as_int(pick(item, "ttl", default=18000)),
                )
                for item in target_rows
            }
            for row in rows:
                if not self._is_custom_zone_record(row):
                    continue
                record_type = str(pick(row, "type", default="")).strip().upper()
                content = str(pick(row, "content", default="")).strip()
                if record_type in {"A", "AAAA"}:
                    content = self._replace_ip_tokens(content, ip_value_mapping)
                key = (
                    str(pick(row, "record", default="")).strip().lower(),
                    record_type,
                    as_int(pick(row, "prio", default=0)),
                    content.lower(),
                    as_int(pick(row, "ttl", default=18000)),
                )
                if key in existing:
                    continue
                self.target.call(
                    "DomainZones.add",
                    {
                        "domainname": domainname,
                        "record": key[0],
                        "type": key[1],
                        "prio": key[2],
                        "content": key[3],
                        "ttl": key[4],
                    },
                )
                existing.add(key)

    def _ensure_mailboxes(self, target_customer_id: int, mailboxes: list[dict[str, Any]]) -> list[str]:
        existing = self._target_mail_set()
        transferable: list[str] = []

        for mailbox_row in mailboxes:
            mailbox = str(pick(mailbox_row, "email_full", "email", "emailaddr", default="")).strip().lower()
            if not mailbox or "@" not in mailbox:
                continue
            local, domain = mailbox.split("@", 1)

            email_payload = {
                "emailaddr": mailbox,
                "customerid": target_customer_id,
                "spam_tag_level": as_int(pick(mailbox_row, "spam_tag_level", default=7)),
                "rewrite_subject": bool(as_int(pick(mailbox_row, "rewrite_subject", default=1))),
                "spam_kill_level": as_int(pick(mailbox_row, "spam_kill_level", default=14)),
                "bypass_spam": bool(as_int(pick(mailbox_row, "bypass_spam", default=0))),
                "policy_greylist": bool(as_int(pick(mailbox_row, "policy_greylist", default=1))),
                "iscatchall": bool(as_int(pick(mailbox_row, "iscatchall", default=0))),
                "description": str(pick(mailbox_row, "description", default="")),
            }

            if mailbox in existing:
                if self.config.behavior.mailbox_exists == "fail":
                    raise MigrationError(f"Target mailbox already exists: {mailbox}")
                if self.config.behavior.mailbox_exists == "skip":
                    continue
            else:
                self.target.call(
                    "Emails.add",
                    {
                        "email_part": local,
                        "domain": domain,
                        "customerid": target_customer_id,
                        "description": str(pick(mailbox_row, "description", default="")),
                        "spam_tag_level": email_payload["spam_tag_level"],
                        "rewrite_subject": email_payload["rewrite_subject"],
                        "spam_kill_level": email_payload["spam_kill_level"],
                        "bypass_spam": email_payload["bypass_spam"],
                        "policy_greylist": email_payload["policy_greylist"],
                        "iscatchall": email_payload["iscatchall"],
                    },
                )
                self.target.call(
                    "EmailAccounts.add",
                    {
                        "emailaddr": mailbox,
                        "customerid": target_customer_id,
                        "email_password": random_password(24),
                        "alternative_email": str(pick(mailbox_row, "alternative_email", default="")),
                        "email_quota": as_int(pick(mailbox_row, "quota", default=0)),
                        "sendinfomail": False,
                    },
                )

            self.target.call("Emails.update", email_payload)
            self.target.call(
                "EmailAccounts.update",
                {
                    "emailaddr": mailbox,
                    "customerid": target_customer_id,
                    "alternative_email": str(pick(mailbox_row, "alternative_email", default="")),
                    "email_quota": as_int(pick(mailbox_row, "quota", default=0)),
                    "deactivated": bool(as_int(pick(mailbox_row, "deactivated", default=0))),
                },
            )

            refreshed_mailboxes = self.target.list_emails(customerid=target_customer_id)
            target_mailbox = None
            for row in refreshed_mailboxes:
                candidate = str(pick(row, "email_full", "email", "emailaddr", default="")).strip().lower()
                if candidate == mailbox:
                    target_mailbox = row
                    break
            if not target_mailbox:
                raise MigrationError(f"Mailbox verification failed: could not reload {mailbox}")

            rspamd_checks: list[tuple[str, int, int]] = [
                (
                    "spam_tag_level",
                    as_int(email_payload["spam_tag_level"]),
                    as_int(pick(target_mailbox, "spam_tag_level", default=0)),
                ),
                (
                    "rewrite_subject",
                    int(bool(email_payload["rewrite_subject"])),
                    as_int(pick(target_mailbox, "rewrite_subject", default=0)),
                ),
                (
                    "spam_kill_level",
                    as_int(email_payload["spam_kill_level"]),
                    as_int(pick(target_mailbox, "spam_kill_level", default=0)),
                ),
                (
                    "bypass_spam",
                    int(bool(email_payload["bypass_spam"])),
                    as_int(pick(target_mailbox, "bypass_spam", default=0)),
                ),
                (
                    "policy_greylist",
                    int(bool(email_payload["policy_greylist"])),
                    as_int(pick(target_mailbox, "policy_greylist", default=0)),
                ),
                (
                    "iscatchall",
                    int(bool(email_payload["iscatchall"])),
                    as_int(pick(target_mailbox, "iscatchall", default=0)),
                ),
            ]
            for field_name, expected, actual in rspamd_checks:
                if expected != actual:
                    raise MigrationError(f"Mailbox setting mismatch after migration for {mailbox}: {field_name} expected={expected!r} actual={actual!r}")

            transferable.append(mailbox)
            existing.add(mailbox)
        return transferable

    def _resolve_source_docroot(self, source_domain: dict[str, Any], customer_login: str) -> str:
        documentroot = str(pick(source_domain, "documentroot", default="")).strip()
        panel_root = self.config.paths.source_web_root.rstrip("/")
        transfer_root = self.config.paths.source_transfer_root.rstrip("/")
        if documentroot.startswith("/"):
            if documentroot.startswith(panel_root + "/"):
                suffix = documentroot[len(panel_root) :]
                return transfer_root + suffix
            return documentroot
        documentroot = documentroot.lstrip("/")
        return f"{transfer_root}/{customer_login}/{documentroot}"

    def _resolve_target_docroot(self, source_domain: dict[str, Any], customer_login: str, source_docroot: str) -> str:
        source_root = self.config.paths.source_transfer_root.rstrip("/")
        target_root = self.config.paths.target_web_root.rstrip("/")
        if source_docroot.startswith(source_root + "/"):
            suffix = source_docroot[len(source_root) :]
            return target_root + suffix
        documentroot = str(pick(source_domain, "documentroot", default="")).strip().lstrip("/")
        return f"{target_root}/{customer_login}/{documentroot}"

    def execute(self, selection: Selection) -> MigrationContext:
        self.preflight(selection)

        target_customer_id = self._ensure_target_customer(selection.customer, selection.target_customer)
        customer_login = str(pick(selection.customer, "loginname", "login", default="")).strip()
        ip_value_mapping = self._build_ip_value_mapping(selection.domains, selection.ip_mapping)

        # Check if username changed between source and target for permission fixing
        target_customer_login = None
        if selection.target_customer:
            target_customer_login = str(pick(selection.target_customer, "loginname", "login", default="")).strip()

        self._ensure_domains(
            target_customer_id,
            selection.domains,
            selection.php_setting_map,
            selection.ip_mapping,
            ip_value_mapping,
            customer_login,
        )
        self._sync_domain_redirects(selection.domains)
        self._ensure_subdomains(target_customer_id, selection.subdomains, selection.php_setting_map)
        self._migrate_domain_certificates(selection.domains)
        self._ensure_ftp_accounts(target_customer_id, selection.ftp_accounts, customer_login)
        self._ensure_ssh_keys(target_customer_id, selection.ssh_keys)
        self._ensure_data_dumps(target_customer_id, selection.data_dumps)
        self._ensure_dir_options(target_customer_id, selection.dir_options, customer_login)
        self._ensure_dir_protections(target_customer_id, selection.dir_protections, customer_login)
        self._ensure_domain_zones(selection.domain_zones, ip_value_mapping)

        db_map: dict[str, str] = {}
        if selection.include_databases and selection.databases:
            self._ensure_target_mysql_prefix_dbname()
            known_before = self._target_database_set()
            for source_db in selection.databases:
                source_name = self._database_name(source_db)
                target_name = self._create_database_on_target(target_customer_id, source_db, known_before, customer_login)
                if source_name != target_name:
                    raise MigrationError(
                        f"Database name mismatch: source={source_name!r} target={target_name!r}; preserving identical DB logins requires matching names"
                    )
                known_before.add(target_name)
                db_map[source_name] = target_name
                self.runner.transfer_database(source_name, target_name)
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

                # Fix permissions if username changed
                if target_customer_login and target_customer_login != customer_login:
                    if not self.runner.dry_run:
                        chown_cmd = (
                            f"find {shlex.quote(target_docroot)} -user {shlex.quote(customer_login)} "
                            f"-exec chown -h {shlex.quote(target_customer_login)} {{}} +"
                        )
                        self.runner.run(f"{self._ssh_prefix()} '{chown_cmd}'")

        if selection.include_mail and transferable_mailboxes:
            for mailbox in transferable_mailboxes:
                self.runner.transfer_mailbox(mailbox)

        return MigrationContext(target_customer_id=target_customer_id, source_to_target_db=db_map)
