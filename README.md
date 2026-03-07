# Froxlor Migrator (Source-side TUI)

Python TUI utility to migrate selected Froxlor customer resources from one server to another.

What it does:

- Uses Froxlor API on source and target to manage customers/domains/databases/mail objects
- Migrates customer-level limits/settings and domain-level web/SSL settings
- Migrates mailbox-level anti-spam settings (`spam_tag_level`, `rewrite_subject`, `spam_kill_level`, `bypass_spam`, `policy_greylist`)
- Migrates subdomains, FTP accounts, SSH keys, email forwarders, allowed sender aliases, directory protections and directory options
- Migrates custom domain-zone records (custom-only; system/default DNS records are skipped)
- Migrates domain forwarding mappings (alias-domain redirects) including redirect-code ids via DB fallback
- After API object creation, synchronizes login password hashes for customer panel login, FTP logins, mailbox logins, directory protection users and database users
- Synchronizes customer 2FA settings (`type_2fa` and `data_2fa`) via DB fallback
- Migrates customer DataDump schedules when API access is available (skips gracefully when provider/API blocks `DataDump.*`)
- Enforces identical source->target database names for migrated databases to preserve matching DB login names
- Runs on the source (old) server
- Transfers payload data over SSH to target:
  - files via `tar -cf - --preserve-permissions --preserve-owner | pzstd -3 | ssh ... pzstd -d | tar -xf --preserve-permissions --preserve-owner` (uses pigz as fallback)
  - SQL via `mysqldump | ssh ... mysql`
  - mail via `sudo doveadm backup ... ssh ... sudo doveadm dsync-server ...`
- Databases are listed separately and selected manually
- Supports full-customer mode (all domains, files, databases, mailboxes, settings)
- Supports interactive source->target IP/port mapping for domains; if no mapping is provided, target Froxlor defaults are used
- Preserves original file ownership and permissions during transfer (relies on Froxlor creating matching users on target)
- Supports separate source panel docroot and local transfer path (`paths.source_web_root` vs `paths.source_transfer_root`)

## Install (uv)

```bash
uv sync
cp config.example.toml config.toml
```

Fill `config.toml` and/or export environment variables used in it.

## Requirements

The following tools must be installed on both source and target hosts:

- `tar` - for file archiving
- `pzstd` (recommended) or `pigz` (fallback) - for parallel compression during file transfers
- `mysqldump` and `mysql` - for database migration
- `doveadm` - for mail migration
- SSH access with appropriate permissions

## Run

Dry-run (default):

```bash
uv run python main.py --config config.toml
```

Apply mode:

```bash
uv run python main.py --config config.toml --apply
```

Post-migration parity verification:

```bash
uv run froxlor-migrator-verify --config config.toml
# or for specific customer(s)
uv run froxlor-migrator-verify --config config.toml --customer custalpha --customer custgamma
```

For local Docker testing where API docroots are inside containers but files are on host bind-mounts, set:

```toml
[paths]
source_web_root = "/data/customers"            # docroot seen in source Froxlor API
source_transfer_root = "./testing/data/source/customers"  # local path used for tar transfer
target_web_root = "/data/customers"
```

## Docker Testing

For local testing with Docker containers, see the `testing/README.md` for detailed setup instructions:

```bash
cd testing
cp .env.example .env
docker compose --profile bootstrap run --rm bootstrap
```

This provides:
- Source and target Froxlor instances with MariaDB
- Automated bootstrap with test data
- Full migration testing environment

## Notes

- Default behavior is conservative:
  - existing domains: fail
  - existing databases: fail
  - existing mailboxes: skip
- PHP setting ids used by selected domains are mapped interactively to target PHP setting ids.
- Domain IP/port assignments can be mapped interactively to target IP IDs.
- SSL options and custom certificates are migrated via API where available.
- DKIM enablement is migrated. If API migration leaves key mismatch, migrator performs a DB-level fallback update on target `panel_domains` and re-verifies key parity.
- Domain-zone migration intentionally skips system/default records and only syncs custom records.
- A migration manifest JSON is written to `output.manifest_dir`.
- For mail migration, `sudo doveadm` must work on both source and target.
- File ownership and permissions are preserved during transfer using tar's `--preserve-permissions --preserve-owner` flags.
- The migrator relies on Froxlor creating matching user accounts on the target system for proper ownership mapping.
