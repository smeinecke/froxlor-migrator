from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..util import as_bool, as_int, pick, random_password
from .types import MigrationError, ResourceRow


class MigratorAccountOps:
    if TYPE_CHECKING:
        from ..api import FroxlorClient
        from ..config import AppConfig

        config: AppConfig
        target: FroxlorClient

        def _mailbox_address(self, mailbox: ResourceRow) -> str: ...
        def _relative_customer_path(self, path: str, customer_login: str) -> str: ...

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

    def _mailbox_payload(self, target_customer_id: int, mailbox_row: ResourceRow) -> dict[str, Any]:
        return {
            "emailaddr": self._mailbox_address(mailbox_row),
            "customerid": target_customer_id,
            "spam_tag_level": as_int(pick(mailbox_row, "spam_tag_level", default=7)),
            "rewrite_subject": bool(as_int(pick(mailbox_row, "rewrite_subject", default=1))),
            "spam_kill_level": as_int(pick(mailbox_row, "spam_kill_level", default=14)),
            "bypass_spam": bool(as_int(pick(mailbox_row, "bypass_spam", default=0))),
            "policy_greylist": bool(as_int(pick(mailbox_row, "policy_greylist", default=1))),
            "iscatchall": bool(as_int(pick(mailbox_row, "iscatchall", default=0))),
            "description": str(pick(mailbox_row, "description", default="")),
        }

    def _verify_mailbox_settings(self, mailbox: str, payload: dict[str, Any], target_mailbox: ResourceRow) -> None:
        rspamd_checks: list[tuple[str, int, int]] = [
            ("spam_tag_level", as_int(payload["spam_tag_level"]), as_int(pick(target_mailbox, "spam_tag_level", default=0))),
            ("rewrite_subject", int(bool(payload["rewrite_subject"])), as_int(pick(target_mailbox, "rewrite_subject", default=0))),
            ("spam_kill_level", as_int(payload["spam_kill_level"]), as_int(pick(target_mailbox, "spam_kill_level", default=0))),
            ("bypass_spam", int(bool(payload["bypass_spam"])), as_int(pick(target_mailbox, "bypass_spam", default=0))),
            ("policy_greylist", int(bool(payload["policy_greylist"])), as_int(pick(target_mailbox, "policy_greylist", default=0))),
            ("iscatchall", int(bool(payload["iscatchall"])), as_int(pick(target_mailbox, "iscatchall", default=0))),
        ]
        for field_name, expected, actual in rspamd_checks:
            if expected != actual:
                raise MigrationError(f"Mailbox setting mismatch after migration for {mailbox}: {field_name} expected={expected!r} actual={actual!r}")

    def _ensure_mailboxes(self, target_customer_id: int, mailboxes: list[dict[str, Any]]) -> list[str]:
        existing = {
            str(pick(item, "email_full", "email", "emailaddr", default=""))
            for item in self.target.list_emails(customerid=target_customer_id)
            if str(pick(item, "email_full", "email", "emailaddr", default=""))
        }
        transferable: list[str] = []

        for mailbox_row in mailboxes:
            mailbox = self._mailbox_address(mailbox_row)
            if not mailbox or "@" not in mailbox:
                continue
            local, domain = mailbox.split("@", 1)
            email_payload = self._mailbox_payload(target_customer_id, mailbox_row)

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
            self._verify_mailbox_settings(mailbox, email_payload, target_mailbox)

            transferable.append(mailbox)
            existing.add(mailbox)
        return transferable
