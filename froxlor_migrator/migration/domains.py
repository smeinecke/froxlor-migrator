from __future__ import annotations

import re
import shlex
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any

from ..api import FroxlorApiError
from ..mysql_driver import query as mysql_query
from ..util import as_int, pick, random_password
from .types import MigrationError, ResourceRow


class MigratorDomainOps:
    if TYPE_CHECKING:
        from ..api import FroxlorClient
        from ..config import AppConfig
        from ..transfer import TransferRunner
        from .types import ResourceRow

        config: AppConfig
        runner: TransferRunner
        source: FroxlorClient
        target: FroxlorClient

        def _domain_name(self, domain: ResourceRow) -> str: ...
        def _exec_target_mysql_sql(self, sql: str, database: str) -> None: ...
        def _exec_target_panel_sql(self, sql: str) -> None: ...
        def _get_target_domain(self, domain_name: str) -> ResourceRow | None: ...
        def _run_source_panel_query(self, sql: str) -> list[list[str]]: ...
        def _run_target_panel_query(self, sql: str) -> list[list[str]]: ...
        def _sql_string_literal(self, value: str) -> str: ...
        def _sql_utf8_literal(self, value: str) -> str: ...
        def _sync_dkim_keys_db(self, domain_name: str, dkim_pubkey: str, dkim_privkey: str) -> None: ...
        def _target_mysql_access_hosts(self) -> list[str]: ...
        def _target_mysql_connect_kwargs(self) -> AbstractContextManager[dict[str, Any]]: ...

    def _load_source_domain_redirects(self, domains: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
        domain_names = sorted({
            str(pick(row, "domain", "domainname", default="")).lower() for row in domains if str(pick(row, "domain", "domainname", default=""))
        })
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

    def _migrate_domain_certificates(self, domains: list[dict[str, Any]]) -> None:
        source_certs = self.source.listing("Certificates.listing")
        target_certs = self.target.listing("Certificates.listing")
        source_by_domain = {str(pick(cert, "domainname", "domain", default="")).lower(): cert for cert in source_certs}
        target_by_domain = {str(pick(cert, "domainname", "domain", default="")).lower(): cert for cert in target_certs}

        for domain in domains:
            domain_name = self._domain_name(domain)
            if not domain_name:
                continue
            if bool(as_int(pick(domain, "letsencrypt", default=0))):
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
        missing_source_ids = [as_int(source_id) for source_id in ip_mapping if as_int(source_id) > 0 and as_int(source_id) not in source_ip_by_id]
        if missing_source_ids:
            for row in self.source.listing("IpsAndPorts.listing"):
                source_ip_id = as_int(pick(row, "id", default=0))
                if source_ip_id not in missing_source_ids:
                    continue
                source_ip = str(pick(row, "ip", default="")).strip().lower()
                if source_ip:
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

    def _normalize_domain_setting_for_compare(self, value: Any) -> str:
        text = str(value or "")
        text = re.sub(r"\\(?=[^\s])", "", text)
        return text.strip()

    def _mapped_domain_ip_ids(self, domain: ResourceRow, ip_mapping: dict[int, int]) -> tuple[list[int], list[int]]:
        mapped_ip_ids: list[int] = []
        mapped_ssl_ip_ids: list[int] = []
        for ip_row in pick(domain, "ipsandports", default=[]) or []:
            source_ip_id = as_int(pick(ip_row, "id", default=0))
            target_ip_id = ip_mapping.get(source_ip_id)
            if not target_ip_id:
                continue
            mapped_ip_ids.append(target_ip_id)
            if as_int(pick(ip_row, "ssl", default=0)) == 1:
                mapped_ssl_ip_ids.append(target_ip_id)
        return mapped_ip_ids, mapped_ssl_ip_ids

    def _domain_payload(
        self,
        target_customer_id: int,
        domain: ResourceRow,
        customer_login: str,
        php_setting_map: dict[int, int],
        ip_mapping: dict[int, int],
        ip_value_mapping: dict[str, str],
    ) -> tuple[str, str, dict[str, Any], list[int]]:
        domain_name = self._domain_name(domain)
        source_docroot = self._resolve_source_docroot(domain, customer_login)
        target_docroot = self._resolve_target_docroot(domain, customer_login, source_docroot)
        source_php_setting = as_int(pick(domain, "phpsettingid", default=0))
        mapped_php_setting = php_setting_map.get(source_php_setting, source_php_setting)
        source_server_alias = as_int(pick(domain, "wwwserveralias", "selectserveralias", default=0))
        mapped_ip_ids, mapped_ssl_ip_ids = self._mapped_domain_ip_ids(domain, ip_mapping)

        payload = {
            "customerid": target_customer_id,
            "loginname": customer_login,
            "adminid": as_int(pick(domain, "adminid", default=0)),
            "is_stdsubdomain": bool(as_int(pick(domain, "is_stdsubdomain", default=0))),
            "documentroot": target_docroot,
            "isemaildomain": bool(as_int(pick(domain, "isemaildomain", default=0))),
            "email_only": bool(as_int(pick(domain, "email_only", default=0))),
            "phpenabled": bool(as_int(pick(domain, "phpenabled", default=1))),
            "sslenabled": bool(as_int(pick(domain, "sslenabled", "ssl_enabled", default=1))),
            "letsencrypt": False,
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
            "selectserveralias": source_server_alias,
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
            payload["phpsettingid"] = mapped_php_setting
        if mapped_ip_ids:
            payload["ipandport"] = [{"id": target_ip_id} for target_ip_id in sorted(set(mapped_ip_ids))]
        if mapped_ssl_ip_ids:
            payload["ssl_ipandport"] = [{"id": target_ip_id} for target_ip_id in sorted(set(mapped_ssl_ip_ids))]
        return domain_name, target_docroot, payload, mapped_ip_ids

    def _domain_comparisons(self, target_docroot: str, payload: dict[str, Any], target_domain: ResourceRow) -> list[tuple[str, Any, Any]]:
        return [
            ("documentroot", target_docroot, pick(target_domain, "documentroot", default="")),
            ("specialsettings", payload["specialsettings"], pick(target_domain, "specialsettings", default="")),
            ("ssl_specialsettings", payload["ssl_specialsettings"], pick(target_domain, "ssl_specialsettings", default="")),
            ("ssl_redirect", int(bool(payload["ssl_redirect"])), as_int(pick(target_domain, "ssl_redirect", default=0))),
            ("sslenabled", int(bool(payload["sslenabled"])), as_int(pick(target_domain, "ssl_enabled", default=0))),
            ("letsencrypt", int(bool(payload["letsencrypt"])), as_int(pick(target_domain, "letsencrypt", default=0))),
            ("openbasedir", int(bool(payload["openbasedir"])), as_int(pick(target_domain, "openbasedir", default=0))),
            ("openbasedir_path", str(payload["openbasedir_path"]), str(pick(target_domain, "openbasedir_path", default=""))),
            ("phpsettingid", as_int(payload.get("phpsettingid", 0)), as_int(pick(target_domain, "phpsettingid", default=0))),
            ("isemaildomain", int(bool(payload["isemaildomain"])), as_int(pick(target_domain, "isemaildomain", default=0))),
            ("email_only", int(bool(payload["email_only"])), as_int(pick(target_domain, "email_only", default=0))),
            ("alias", as_int(payload["alias"]), as_int(pick(target_domain, "alias", default=0))),
            ("speciallogfile", int(bool(payload["speciallogfile"])), as_int(pick(target_domain, "speciallogfile", default=0))),
            ("caneditdomain", int(bool(payload["caneditdomain"])), as_int(pick(target_domain, "caneditdomain", default=0))),
            ("isbinddomain", int(bool(payload["isbinddomain"])), as_int(pick(target_domain, "isbinddomain", default=0))),
            ("notryfiles", int(bool(payload["notryfiles"])), as_int(pick(target_domain, "notryfiles", default=0))),
            ("hsts_maxage", as_int(payload["hsts_maxage"]), as_int(pick(target_domain, "hsts", default=0))),
            ("hsts_sub", int(bool(payload["hsts_sub"])), as_int(pick(target_domain, "hsts_sub", default=0))),
            ("hsts_preload", int(bool(payload["hsts_preload"])), as_int(pick(target_domain, "hsts_preload", default=0))),
            ("http2", int(bool(payload["http2"])), as_int(pick(target_domain, "http2", default=0))),
            ("http3", int(bool(payload["http3"])), as_int(pick(target_domain, "http3", default=0))),
            ("ocsp_stapling", int(bool(payload["ocsp_stapling"])), as_int(pick(target_domain, "ocsp_stapling", default=0))),
            ("dkim", int(bool(payload["dkim"])), as_int(pick(target_domain, "dkim", default=0))),
            ("honorcipherorder", int(bool(payload["honorcipherorder"])), as_int(pick(target_domain, "ssl_honorcipherorder", default=0))),
            ("sessiontickets", int(bool(payload["sessiontickets"])), as_int(pick(target_domain, "ssl_sessiontickets", default=0))),
            ("override_tls", int(bool(payload["override_tls"])), as_int(pick(target_domain, "override_tls", default=0))),
            (
                "specialsettingsforsubdomains",
                int(bool(payload["specialsettingsforsubdomains"])),
                as_int(pick(target_domain, "specialsettingsforsubdomains", default=0)),
            ),
            (
                "phpsettingsforsubdomains",
                int(bool(payload["phpsettingsforsubdomains"])),
                as_int(pick(target_domain, "phpsettingsforsubdomains", default=0)),
            ),
            ("mod_fcgid_starter", as_int(payload["mod_fcgid_starter"]), as_int(pick(target_domain, "mod_fcgid_starter", default=-1))),
            ("mod_fcgid_maxrequests", as_int(payload["mod_fcgid_maxrequests"]), as_int(pick(target_domain, "mod_fcgid_maxrequests", default=-1))),
            ("deactivated", int(bool(payload["deactivated"])), as_int(pick(target_domain, "deactivated", default=0))),
            (
                "selectserveralias",
                as_int(payload["selectserveralias"]),
                as_int(pick(target_domain, "wwwserveralias", "selectserveralias", default=0)),
            ),
        ]

    def _verify_domain_settings(self, domain_name: str, target_docroot: str, payload: dict[str, Any], target_domain: ResourceRow) -> None:
        for field_name, expected, actual in self._domain_comparisons(target_docroot, payload, target_domain):
            if field_name in {"specialsettings", "ssl_specialsettings"}:
                expected_cmp = self._normalize_domain_setting_for_compare(expected)
                actual_cmp = self._normalize_domain_setting_for_compare(actual)
            else:
                expected_cmp = str(expected)
                actual_cmp = str(actual)
            if expected_cmp != actual_cmp:
                raise MigrationError(f"Domain setting mismatch after migration for {domain_name}: {field_name} expected={expected!r} actual={actual!r}")

    def _ensure_domains(
        self,
        target_customer_id: int,
        domains: list[ResourceRow],
        php_setting_map: dict[int, int],
        ip_mapping: dict[int, int],
        ip_value_mapping: dict[str, str],
        customer_login: str,
    ) -> None:
        existing_domains = {self._domain_name(item) for item in self.target.list_domains() if self._domain_name(item)}
        for domain in domains:
            domain_name, target_docroot, base_payload, mapped_ip_ids = self._domain_payload(
                target_customer_id,
                domain,
                customer_login,
                php_setting_map,
                ip_mapping,
                ip_value_mapping,
            )
            if not domain_name:
                continue

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
                try:
                    self.target.call("Domains.add", {"domain": domain_name, **base_payload})
                except FroxlorApiError as exc:
                    message = str(exc).lower()
                    if "let's encrypt" in message or "letsencrypt" in message:
                        self.target.call("Domains.add", {"domain": domain_name, **{**base_payload, "letsencrypt": False}})
                    else:
                        raise
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
            self._verify_domain_settings(domain_name, target_docroot, base_payload, target_domain)

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

    def _enable_letsencrypt_after_dns(self, domains: list[dict[str, Any]]) -> None:
        for domain in domains:
            if not bool(as_int(pick(domain, "letsencrypt", default=0))):
                continue
            domain_name = str(pick(domain, "domain", "domainname", default=""))
            if not domain_name:
                continue
            target_domain = self._get_target_domain(domain_name)
            if not target_domain:
                raise MigrationError(f"Could not find target domain for Let's Encrypt enablement: {domain_name}")
            domain_id = as_int(pick(target_domain, "id", default=0))
            self.target.call(
                "Domains.update",
                {
                    "id": domain_id,
                    "domainname": domain_name,
                    "letsencrypt": True,
                },
            )

    def _create_database_on_target(
        self,
        target_customer_id: int,
        source_db: dict[str, Any],
        known_before: set[str],
    ) -> str:
        src_name = str(pick(source_db, "databasename", "dbname", "database", default=""))
        if not src_name:
            raise MigrationError("Source database has no name")
        if src_name in known_before:
            if self.config.behavior.database_exists == "fail":
                raise MigrationError(f"Target database already exists: {src_name}")
            return src_name

        description = str(pick(source_db, "description", default=f"Migrated from {src_name}"))
        self._recreate_database_like_froxlor(target_customer_id, src_name, description)
        return src_name

    def _recreate_database_like_froxlor(self, target_customer_id: int, src_name: str, description: str) -> None:
        metadata_rows = self._run_target_panel_query(
            f"SELECT loginname, allowed_mysqlserver, mysql_lastaccountnumber FROM panel_customers WHERE customerid={target_customer_id} LIMIT 1;"
        )
        if not metadata_rows or len(metadata_rows[0]) < 3:
            raise MigrationError(f"Could not load target customer metadata for database fallback: {target_customer_id}")
        customer_login = str(metadata_rows[0][0] or "").strip()
        allowed_mysqlserver_raw = str(metadata_rows[0][1] or "").strip()
        current_last_accountnumber = as_int(metadata_rows[0][2], default=0)

        dbserver = self._default_mysql_server_from_allowed(allowed_mysqlserver_raw)
        mysql_access_hosts = self._target_mysql_access_hosts()
        database_password = random_password(24)
        existing_panel_rows = self._run_target_panel_query(
            f"SELECT COUNT(*) FROM panel_databases WHERE customerid={target_customer_id} AND databasename={self._sql_utf8_literal(src_name)};"
        )
        panel_row_exists = bool(existing_panel_rows and existing_panel_rows[0] and as_int(existing_panel_rows[0][0], default=0) > 0)

        # Keep fallback idempotent across retries if database was physically created in a previous partial run.
        self._exec_target_mysql_sql(f"CREATE DATABASE IF NOT EXISTS `{src_name}`", "mysql")
        for host in mysql_access_hosts:
            self._exec_target_mysql_sql(
                "CREATE USER IF NOT EXISTS "
                f"{self._sql_string_literal(src_name)}@{self._sql_string_literal(host)} "
                f"IDENTIFIED BY {self._sql_string_literal(database_password)};",
                "mysql",
            )
            self._exec_target_mysql_sql(
                f"GRANT ALL ON `{src_name}`.* TO {self._sql_string_literal(src_name)}@{self._sql_string_literal(host)};",
                "mysql",
            )
            if customer_login and self._target_mysql_user_exists(customer_login, host):
                self._exec_target_mysql_sql(
                    f"GRANT ALL ON `{src_name}`.* TO {self._sql_string_literal(customer_login)}@{self._sql_string_literal(host)};",
                    "mysql",
                )
        self._exec_target_mysql_sql("FLUSH PRIVILEGES;", "mysql")

        if not panel_row_exists:
            self._exec_target_panel_sql(
                "INSERT INTO panel_databases (customerid, databasename, description, dbserver) "
                f"VALUES ({target_customer_id}, {self._sql_utf8_literal(src_name)}, "
                f"{self._sql_utf8_literal(description)}, {dbserver});"
            )
            target_prefix = self._target_mysql_prefix_setting()
            fallback_last = self._fallback_last_account_number(src_name, customer_login, target_prefix)
            self._exec_target_panel_sql(
                "UPDATE panel_customers "
                "SET mysqls_used=mysqls_used+1, "
                f"mysql_lastaccountnumber=GREATEST(mysql_lastaccountnumber+1, {fallback_last}, {current_last_accountnumber + 1}) "
                f"WHERE customerid={target_customer_id};"
            )

    def _default_mysql_server_from_allowed(self, allowed_mysqlserver_raw: str) -> int:
        allowed: list[int] = []
        text = allowed_mysqlserver_raw.strip()
        if text:
            if text.startswith("["):
                for token in re.findall(r"\d+", text):
                    number = as_int(token, default=-1)
                    if number >= 0:
                        allowed.append(number)
            else:
                number = as_int(text, default=-1)
                if number >= 0:
                    allowed.append(number)
        if not allowed:
            return 0
        allowed = sorted(set(allowed))
        if len(allowed) == 1 and allowed[0] != 0:
            return allowed[0]
        return allowed[0]

    def _target_mysql_access_hosts(self) -> list[str]:
        rows = self._run_target_panel_query("SELECT value FROM panel_settings WHERE settinggroup='system' AND varname='mysql_access_host' LIMIT 1;")
        raw = str(rows[0][0] if rows and rows[0] else "").strip()
        hosts = [item.strip() for item in raw.split(",") if item.strip()]
        if not hosts:
            return ["localhost"]
        return hosts

    def _target_mysql_prefix_setting(self) -> str:
        rows = self._run_target_panel_query("SELECT value FROM panel_settings WHERE settinggroup='customer' AND varname='mysqlprefix' LIMIT 1;")
        if not rows or not rows[0]:
            return ""
        return str(rows[0][0]).strip()

    def _fallback_last_account_number(self, database_name: str, customer_login: str, mysql_prefix: str) -> int:
        prefix = mysql_prefix.strip()
        login = customer_login.strip()
        if not prefix or prefix.upper() in {"DBNAME", "RANDOM"}:
            return 0
        pattern = f"{re.escape(login)}{re.escape(prefix)}(\\d+)$"
        match = re.fullmatch(pattern, database_name)
        if not match:
            return 0
        return as_int(match.group(1), default=0)

    def _target_mysql_user_exists(self, username: str, host: str) -> bool:
        rows = self._run_target_panel_query(
            f"SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user={self._sql_string_literal(username)} AND host={self._sql_string_literal(host)});"
        )
        if not rows or not rows[0]:
            return False
        return as_int(rows[0][0], default=0) == 1

    def _target_database_exists_physical(self, db_name: str) -> bool:
        if self.runner.dry_run:
            return False
        if not db_name.strip():
            return False
        try:
            with self._target_mysql_connect_kwargs() as connect_kwargs:
                rows = mysql_query(
                    connect_kwargs,
                    "mysql",
                    f"SHOW DATABASES LIKE {self._sql_string_literal(db_name)};",
                )
        except Exception:
            return False
        return any(row and row[0].strip() == db_name for row in rows)

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

    def _fix_transferred_docroot_ownership(self, target_docroot: str, source_login: str, target_login: str | None) -> None:
        if not target_login or target_login == source_login or self.runner.dry_run:
            return
        chown_cmd = f"find {shlex.quote(target_docroot)} -user {shlex.quote(source_login)} -exec chown -h {shlex.quote(target_login)} {{}} +"
        self.runner.run_remote(chown_cmd)
