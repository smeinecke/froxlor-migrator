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
  - one empty domain
  - one SSL domain with custom certificate, DKIM key material, rewrite/vhost settings
- one additional redirect domain with custom domain config
- one forwarding-domain fixture (`forward-demo.test` -> `secure-demo.test`) with explicit redirect code
  - one explicit subdomain fixture with dedicated settings
- one FTP account fixture
- one SSH key fixture bound to FTP user `custgammaftp1`
- one directory-protection fixture and matching directory-options fixture
- one mail forwarder fixture
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
docker compose run --rm --profile bootstrap bootstrap
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

This performs a real apply migration (files + databases + mailbox content via doveadm) for seeded test customers and then verifies source/target parity. It also injects a probe email into source mailbox `alerts@secure-demo.test` and asserts that the exact probe reaches target after migration. Password-hash parity is applied for customer/FTP/mailbox/dir-protection/database logins after API object creation.

## 5) Use with migrator

Point `config.toml` to:

- source API: `http://127.0.0.1:8081/api.php`
- target API: `http://127.0.0.1:8082/api.php`
- source web root: `/data/customers`
- target web root: `/data/customers`

For local host-run tests, use SSH target `127.0.0.1` and `TARGET_SSH_PORT` with key `testing/ssh/id_ed25519`.

## Notes

- Mailbox object creation is seeded via Froxlor API.
- The test Froxlor image includes postfix+dovecot and starts both daemons, so `doveadm backup` is exercised with real mailbox payloads.
- All bootstrap scripts use `uv` which is automatically installed in the Docker bootstrap container.
- The Docker bootstrap container handles all Python dependencies and uv installation automatically.
