# Test Environment (Docker)

This folder provides a reproducible local test setup for the migrator:

- Source Froxlor + MariaDB
- Target Froxlor + MariaDB
- Seed script for source data:
  - 3 customers
  - 6 domains
  - WordPress files in one domain
  - static HTML in one domain
  - one mail-only domain with 2 mailboxes and one catchall
  - one empty domain with `letsencrypt=1` (LE DNS check disabled; exercises the migrator's deferred LE-enable path without real ACME)
  - one SSL domain with custom certificate, DKIM key material, rewrite/vhost settings
- one additional redirect domain with custom domain config
- one forwarding-domain fixture (`forward-demo.test` -> `secure-demo.test`) with explicit redirect code
  - one explicit subdomain fixture with dedicated settings
  - a `migrator_marker` row in the WordPress database, compared on the target after migration
- one FTP account fixture
- one SSH key fixture bound to FTP user `custgammaftp1`
- one directory-protection fixture and matching directory-options fixture
- one mail forwarder fixture
- custom DNS zone records on `secure-demo.test` (TXT + CNAME)
- one customer 2FA fixture (`custgamma`)
- one DataDump fixture (created when API endpoint is accessible)
- mailbox-level rspamd/spam settings test fixtures
- 2 PHP settings used across domains

The Froxlor containers are built locally from a Dockerfile that installs the latest stable Froxlor tarball (`https://files.froxlor.org/releases/froxlor-latest.tar.gz`).
For multi-version PHP tests, the image also enables the Sury PHP repository and installs multiple FPM runtimes (including 8.3 and 8.4).

The Froxlor web install wizard is automated via CLI (`froxlor:install`) using the command's example JSON template.

## 1) Start stack

```bash
cd testing
cp .env.example .env
docker compose up -d
```

Or run full automation through a compose bootstrap profile (installs containers, runs CLI wizard, creates API keys, seeds data, verifies fixtures):

```bash
docker compose --profile bootstrap run --rm bootstrap
```

Froxlor UIs:

- source: `http://127.0.0.1:8081`
- target: `http://127.0.0.1:8082`
- source SSH: `127.0.0.1:${SOURCE_SSH_PORT:-2221}`
- target SSH: `127.0.0.1:${TARGET_SSH_PORT:-2222}`

Run unattended setup for both source/target instances:

```bash
docker compose run --rm --profile bootstrap bootstrap install_wizard
```

To automate everything in one shot:

```bash
docker compose run --rm --profile bootstrap bootstrap
```

This bootstrap also generates `testing/ssh/id_ed25519` and installs the public key into both Froxlor containers for root SSH login (key-only auth).

During bootstrap we also run Froxlor service setup (`froxlor:config-services`) for postfix+dovecot+php-fpm and ensure named PHP setting profiles (`php8.3`, `php8.4`) on both source and target. Matching FPM daemon entries are created/updated as well, so profile names map to the corresponding runtime version.

## 2) Create API key/secret(s)

In source Froxlor UI (admin account):

- open user menu -> API keys
- create a new key
- put key and secret into `testing/.env` for seeding:
  - `SOURCE_API_KEY=...`
  - `SOURCE_API_SECRET=...`

If you used `bootstrap_all.sh` or compose `bootstrap`, keys are already generated and written to `testing/.env` for both source and target.

For running the migrator source->target, you also need a target API key in your root `config.toml`.

## 3) Seed source data

```bash
cd testing
docker compose run --rm --profile bootstrap bootstrap
```

Or run individual steps (still within Docker bootstrap container):

```bash
docker compose run --rm --profile bootstrap bootstrap seed_source
```

This creates the test customers/domains/mail objects (including SSL/cert/domain settings and mailbox spam settings) and writes web content under `testing/data/source/customers`.

## 4) Verify seed

```bash
cd testing
docker compose run --rm --profile bootstrap bootstrap verify_seed
```

`bootstrap_all.sh` runs this verification automatically after seeding.

## 4b) Run migration + target parity verification

```bash
cd testing
docker compose run --rm --profile bootstrap bootstrap migrate_and_verify
```

This performs the full migration flow for seeded test customers. Every apply
runs through `main.py --non-interactive` (the real CLI path: arg parsing,
selection, mappings, and confirmation all get covered — not a test-only
`Selection` shortcut). Multi-customer applies run as a *single* batch
invocation, so the batch path itself (shared plan table, per-customer
manifests, tolerant selectors, continue-on-failure, exit status) is covered
end to end:

1. a `--dry-run` batch apply via `--all-customers`, then asserts none of the four seed customers exist on the target (guards writes bypassing dry-run) and that the batch plan table lists every customer
2. `custbeta` is pre-created on the target so the apply exercises the existing-customer update path
3. a real batch apply of `custalpha`+`custbeta`+`custgamma` in one `main.py` invocation (files + databases + mailbox content via doveadm); `--ip-map` uses the named `ip:port:ssl=>ip:port:ssl` token form so `static-demo.test` rebinds from the source secondary IP:port to the target's. The "Batch result" table and per-customer manifest files are asserted.
4. a second batch apply after deliberately drifting a target DNS record — exercises all update/dedup paths plus the zone delete-and-re-add repair (`DomainZones.update` is a stub in Froxlor)
5. `verify_migration` for all three customers with `--ip-value-map` (zone record content is translated source-IP → target-IP before comparison), plus the mailbox probe assertion
6. a negative check: a migrated zone record is deleted on the target and `verify_migration` must fail — then a final apply + verify restores parity
7. byte-level file-content parity between source and target customer dirs, docroot ownership matching the customer's `panel_customers.guid`, and the `migrator_marker` database row on the target
8. batch failure isolation: with `mailbox_exists=fail`, a `custalpha`/`custbeta` batch yields one ok + one failed row (continue-on-failure) and exits non-zero; `custbeta`/`custgamma` yields all-failed; a `domain_exists=fail` config fails all three — the batch table still renders every row
9. `custepsilon` resources-only apply (`--domains none`): database + FTP account migrate while the domain and mailboxes stay absent
10. `custepsilon` selective apply: `--domains`/`--mailboxes`/`--ftp-accounts` subsets, `--include-files no`, `--include-databases no`, all `--skip-*` flags, an explicit `--php-map` and an `ip:port:ssl` `--ip-map` token — `assert_absent.py` proves exactly the flagged resources are missing on target (incl. tampered DB/FTP staying dropped, mail content not transferred, docroot marker file absent)
11. `custepsilon` full apply: everything migrates, the auto PHP map restores `php8.3` (update-path rewrite), the SSL ip:port binding is remapped, and the mail probe arrives. The stale target Maildir created by step 10's mailbox object is dropped first - `doveadm backup` onto a mailbox with a foreign GUID triggers a dovecot trash-delete that fails under the `maildir:` layout
12. an `--all-customers` real apply over all four customers, then `verify_migration` incl. `custepsilon`
13. verify-side `--skip-*` flags: after deleting `custepsilon`'s subdomain + zone record, `verify_migration --skip-subdomains --skip-domain-zones` passes while the unsuppressed run fails — then a repair apply restores parity
14. CLI error paths: unknown customer token, unmatched `--domains` token (single + batch), and `--domain-only` with a nonexistent `--target-customer` all exit non-zero
15. the printed batch replay command is executed verbatim and must succeed idempotently
16. a domain-only rename: `wp-demo.test` and `static-demo.test` migrate into the pre-created `custdelta` via `--domain-only --target-customer` — `assert_rename.py` verifies target ownership and docroot remapping under the new login

It also injects probe emails into source mailboxes `alerts@secure-demo.test` and `one@eps-demo.test`, asserting exact delivery through the migrator's own `doveadm backup | dsync-server` transfer — including a negative check that `--include-mail no` leaves the probe behind. Password-hash parity is applied for customer/FTP/mailbox/dir-protection/database logins after API object creation. The image ships `zstd`, so file transfer uses the `pzstd` compression codec rather than the uncompressed fallback.

## 5) Use with migrator

Point `config.toml` to:

- source API: `http://127.0.0.1:8081/api.php`
- target API: `http://127.0.0.1:8082/api.php`
- source web root: `/var/customers/webs`
- source transfer root: `./testing/data/source/customers` (or `/data/customers` when running inside `source-froxlor`)
- target web root: `/data/customers`

For local host-run tests, use SSH target `127.0.0.1` and `TARGET_SSH_PORT` with key `testing/ssh/id_ed25519`.

## Notes

- Mailbox object creation is seeded via Froxlor API.
- The test Froxlor image includes postfix+dovecot and starts both daemons, so `doveadm backup` is exercised with real mailbox payloads.
- All bootstrap scripts use `uv` which is automatically installed in the Docker bootstrap container.
- The Docker bootstrap container handles all Python dependencies and uv installation automatically.
