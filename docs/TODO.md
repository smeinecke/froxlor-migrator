# Known bugs & issues

Findings from code review (2026-10-01). Ordered by severity.

## High severity

- [x] **`database_exists="skip"` is ignored — existing DBs get overwritten.**
  `_create_database_on_target` returns `src_name` for both `"skip"` and `"update"`
  (`migration/domains.py:439-442`), then the executor unconditionally runs
  `_transfer_database_with_defaults` (`migration/executor.py:144-146`), restoring the
  source dump *into the existing database*. A "skip" policy causes data overwrite.
  Also `known_before` contains *all* customers' DBs, so this can clobber another
  customer's database. Additionally `validate_database_names` can never trigger
  (`executor.py:138`) since `target_name == src_name` always.

- [x] **File ownership fix is broken in every mode** (`migration/domains.py:731-735`).
  `find {docroot} -user {source_login}` runs on the *target*: GNU find exits 1 for
  unknown users → `run_remote(check=True)` → `TransferError` when the source login
  doesn't exist on target (exactly the case it was written for). Even if it didn't
  error, `tar -xp` as root restores *numeric* UIDs, so name-based `-user` matches
  nothing. And when `target_login == source_login` it early-returns, so transferred
  files keep stale source UIDs in the common case. Should be
  `chown -R {target_login}:{target_login} {docroot}` or find by numeric UID.

- [x] **`sudo` required by preflight but never used by remote commands.**
  Preflight requires remote sudo for non-root SSH users (`transfer.py:214`), but
  `transfer_files`' `mkdir -p`/`tar -xpf` into `/var/customers/webs`
  (`transfer.py:233`), the DB restore and `rm` (`migration/core.py:610-625`), the
  remote mysql CLI fallback (`core.py:481-499`), and the chown all run as the SSH
  user. Only `transfer_mailbox` uses sudo. Non-root setups pass preflight then fail
  on writes/chown.

- [x] **DNS zone record `content` lowercased before insert** (`migration/domains.py:684-708`).
  `key[3]` (`content.lower()`) is what gets sent to `DomainZones.add` — DKIM TXT
  records (`p=` base64) and other case-sensitive content get corrupted.

- [x] **Relative documentroots silently excluded in whole-customer mode**
  (`tui.py:440-443, 775`). `_domain_in_source_root` only accepts absolute docroots
  under `source_web_root`, but `_resolve_source_docroot` explicitly handles relative
  docroots (`{transfer_root}/{login}/{docroot}`, `migration/domains.py:719-720`) —
  Froxlor commonly stores documentroot relative to the customer homedir. If the API
  returns relative values, every domain is "skipped outside source_web_root" and
  whole-customer migrates zero domains. `verify_migration._expected_target_docroot`
  (lines 77-89) also lacks the relative→`{target_root}/{login}` mapping → false
  mismatches.

- [x] **Docroots use the *source* login when target customer is renamed**
  (`migration/domains.py:722-729`). `_resolve_target_docroot` embeds
  `customer_login` (source) in the target path `{target_root}/{source_login}/…`.
  With `--target-customer` pointing at a differently-named customer, docroots/files
  land outside the target customer's homedir. Same class: `_relative_customer_path`
  is applied to *target* rows using the source login (`migration/accounts.py:226,261`)
  → dedup keys never match → duplicate DirOptions/DirProtections on re-run.

- [x] **SQL splitter escape bug** (`mysql_driver.py:96-102`). Escape detection checks
  only the previous char: `'a\\'` (escaped backslash + closing quote) is parsed as an
  unterminated string → the rest of the script is swallowed into one statement →
  `execute()`/`import_sql_dump` corrupt any SQL containing `\\` inside quotes (e.g.
  `'C:\\'`). Also `--` starts a "comment" without MySQL's required trailing space
  (`a--b` truncates), and `DELIMITER` is matched mid-line instead of at line start.

- [x] **DB user host sets don't match** (`migration/domains.py:468-484` vs
  `migration/core.py:779`). Users are created for hosts from the
  `mysql_access_host` panel setting, but `_sync_database_login_hashes` ALTERs only
  `%/localhost/target-db/127.0.0.1/{ssh.host}`. If the setting is e.g. `web1,
  10.0.0.%`, created users keep their random passwords → migrated DB logins broken.

## Medium

- [x] Mailbox `existing` set isn't lowercased but `mailbox` is → mixed-case target
  mailboxes bypass the exists-check → duplicate `Emails.add`
  (`migration/accounts.py:330-348`).

- [x] `EmailAccounts.add` runs for *every* email row — forward-only addresses
  (acknowledged in `core.py:716`'s comment) become real mailboxes → changed
  semantics (`migration/accounts.py:365-375`).

- [x] Source-ID passthrough into target ID space: `adminid`, `allowed_phpconfigs`,
  `allowed_mysqlserver`, `hosting_plan_id`, `alias` (`migration/core.py:214,226,
  232,234`; `migration/domains.py:247`). `allowed_phpconfigs`/`phpsettingid` should
  be remapped via `php_setting_map`; `alias` is a source domain id sent to
  `Domains.add` before `_sync_domain_redirects` fixes it → possible API rejection.

- [x] PHP map only covers domains, not subdomains — `_build_php_setting_map`
  iterates `selected_domains` only (`tui.py:521`); `_ensure_subdomains` falls back
  to the raw source id for unmapped settings (`migration/domains.py:601-602`) →
  wrong/absent PHP config.

- [x] `phpsettingid` omitted from payload when mapped value is 0
  (`migration/domains.py:261-262`) but `_verify_domain_settings` expects 0
  (`domains.py:279`) → false `MigrationError` if Froxlor assigns a default.

- [x] `unicode_escape` decode mangles credentials (`froxlor_mysql.py:67-69,114,118`):
  non-ASCII passwords → mojibake; PHP single-quoted `\n` (literal backslash-n) →
  real newline → wrong password *and* newline injection into
  `mysql_defaults_content`.

- [x] `process.stderr.read()` blocks while the ssh process is alive
  (`migration/core.py:470`) — missing `ExitOnForwardFailure=yes` means a remote-side
  forward failure hangs forever instead of erroring.

- [x] API params logged at DEBUG include `ssl_key_file` (private keys),
  `new_customer_password`, `ftp_password`, `data_2fa` (`api.py:38-43`).

- [x] Verify/migrate inconsistency: `deactivated` and `theme` are stripped from
  `Customers.add` (`migration/core.py:263`) but compared by `_compare_customer`
  (`verify_migration.py:323-334`) → newly created customers with custom
  theme/deactivated=1 always FAIL verify. Also `new_customer_password` is sent on
  `Customers.update` — risks resetting the password to empty.

- [x] `_filter_customer_rows` asymmetry (`api.py:278-281`): rows *missing*
  `customerid` are dropped when filtering by id, but rows missing `loginname` pass
  when filtering by login. The post-filter on `list_email_forwarders`/
  `list_email_senders` can silently drop all rows if those API items lack
  `customerid` — forwarders lost without a trace.

- [x] CLI fallback returns `NULL` as the string `"NULL"` while the driver path
  returns `""` (`migration/core.py:543-544` vs `mysql_driver.py:25`) — e.g.
  `mysql_lastaccountnumber`, `loginname`, `allowed_mysqlserver` read as literal
  `"NULL"` downstream.

- [x] Dead code: `_target_database_exists_physical` (`migration/domains.py:554`),
  `import_sql_dump` (`mysql_driver.py:124`, tests-only), `behavior.parallel` config
  never used.

- [x] Non-interactive defaults inconsistent: mailboxes auto-select all candidates
  but databases/FTPs default to empty (`tui.py:876-877,894,944-945`).

## Low

- [x] `_ensure_data_dumps` uses `return` instead of `continue` on 405
  (`migration/accounts.py:217`).
- [x] `_is_custom_zone_record` drops *all* NS/SOA → delegated sub-zone NS records
  never migrate or verify (`migration/domains.py:654-657`,
  `verify_migration.py:496-509`).
- [x] `load_local_sql_*` `RuntimeError` escapes `run_app`'s except clause in
  `_transfer_database_with_defaults` (`tui.py:1133` only catches
  MigrationError/FroxlorApiError/TransferError).
- [x] `_default_mysql_server_from_allowed` dead branch (`migration/domains.py:517-519`)
  — both paths return `allowed[0]`.
- [x] `_ssh_prefix` lacks `BatchMode`/`ConnectTimeout` → password prompt can hang
  automation; `transfer_mailbox` embeds the raw mailbox inside the quoted remote
  string (`transfer.py:281` — `'` in an address breaks quoting).
- [x] Verify can't handle socket-only MySQL: `_target_connect_kwargs_via_ssh` strips
  `unix_socket` and tunnels TCP (`verify_migration.py:531-539`).
- [x] `_relative_customer_path` loop over-strips when a nested directory equals the
  login (`migration/core.py:80-85`).
- [x] `mailbox_exists="skip"` also excludes the mailbox from content transfer
  (`migration/accounts.py:348` vs `migration/executor.py:187-189`).
- [x] `api.call` retries mutating requests once on network errors → possible
  double-add on lost responses (`api.py:56-71`).
- [x] `bool("false") == True` if users quote TOML booleans (`config.py:137,159`).

---

# Second review pass (2026-10-01)

Fresh findings after the first round of fixes. All items unchecked; ordered by
severity/confidence.

## High

- [x] **`SshDriver.run` can deadlock on chatty remote stderr**
  (`ssh_driver.py:83-84`). `stdout.read()` is fully drained before
  `stderr.read()`; Paramiko stdout/stderr share one channel window, so a remote
  command writing enough stderr blocks the server while we block on stdout →
  hang forever. No timeouts exist anywhere in the SSH/command path, so this is
  unrecoverable without Ctrl-C. Fix via `channel.set_combine_stderr(True)` or
  concurrent stream draining, plus a command timeout.

- [x] **Remote-CLI MySQL fallback leaks query output to console/manifest**
  (`migration/core.py:473-492` + `transfer.py:303-325`).
  `_run_target_mysql_via_remote_cli` calls `runner.run_remote(cmd)` without
  `sensitive=True`. When the SSH tunnel fails for non-panel DBs (e.g. `mysql`
  during `_sync_database_login_hashes`), result events write raw stdout —
  including `mysql.user` / `mail_users` password hashes — to the terminal and
  into `manifests/*.json`.

- [x] **Socket discovery overrides explicit remote DB host**
  (`migration/core.py:388-400`). When target `sql_root` creds have no
  `unix_socket`, `_discover_remote_mysql_socket()` finds a local socket on the
  SSH host and *deletes* `host`/`port` — so a credentials file pointing at a
  separate DB server (e.g. `db.internal`) silently connects to the SSH host's
  local MySQL instead. Panel writes could hit the wrong server. Only apply
  socket discovery when host is localhost/127.0.0.1/empty.

- [x] **Dir-protection dedup rebuild keyed on the wrong login**
  (`migration/accounts.py:316-321`). Initial `existing` map uses `target_login`
  (line 282); the post-write rebuild uses `customer_login` (source). For renamed
  target customers every row after the first misses → duplicate
  `DirProtections.add` on re-run. Same class of bug the first pass fixed — one
  rebuild site was missed.

- [x] **File transfer loops over alias/empty-docroot domains**
  (`migration/executor.py:182-188`). No `aliasdomain`/`parentdomain` filter; an
  alias row's empty `documentroot` resolves to `{transfer_root}/{login}/` — the
  whole customer homedir — so every alias/empty-docroot domain triggers a full
  homedir tar + `chown -R` again. N extras → N duplicate full transfers. Dedupe
  by resolved source path and/or skip alias domains.

## Medium

- [x] **`mysqldump` drops routines and events** (`migration/core.py:594-599`).
  `--triggers` is on by default but `--routines`/`--events` are off → stored
  procedures, functions, and scheduled events are silently lost.

- [x] **Listing wrappers swallow `FroxlorApiError` → silent loss that verify
  can't see** (`api.py`: `list_domain_zones`, `list_data_dumps`,
  `list_email_forwarders`, `list_email_senders`). On error they return `[]`.
  Migration drops the data; verify calls the same wrappers and gets `[]` on both
  sides → reports OK on data it never saw. At minimum, verification must not
  treat a listing error as "empty".

- [x] **Zone dedup lowercases `content` but verify compares verbatim**
  (`migration/domains.py:662,678` vs `verify_migration.py:823`). An existing
  target record differing only in case is skipped by the migrator yet flagged by
  verify forever — re-running never repairs case-mangled records. Dedup should
  compare case-sensitively for content.

- [x] **Multi-level subdomains silently dropped everywhere**
  (`tui.py:823`, `migration/domains.py:573-580`, `verify_migration.py:847`).
  All three resolve the parent via `split(".", 1)` → `a.b.example.com` gets
  parent `b.example.com`, which isn't a main domain → filtered/skipped with no
  warning. Use the row's parent-domain field or suffix-match against known
  domains.

- [x] **N+1 listing refresh pattern** (resolved in round 7, `aac213b` +
  round 8 `d75492f`/`eadaedc`). `_get_target_domain` runs an unfiltered
  `Domains.listing` ~3× per domain (`core.py:284-288`, `domains.py:355-383`);
  `accounts.py:130,266,315,423` and `domains.py:620-621` re-list after every
  single add/update. O(N) full-panel listings per migration; also creates
  stale-read windows. Filter by customerid where supported or keep a refreshed
  snapshot.

- [x] **`hsts` compare asymmetry** (`migration/domains.py:284`): payload reads
  `pick(domain, "hsts", "hsts_maxage")` but the target side reads only
  `"hsts"` → false mismatch when the API returns `hsts_maxage`.

- [x] **`_find_target_customer` matches on email alone**
  (`migration/core.py:164-176`). A different customer sharing the email is
  treated as the same one → `Customers.update` overwrites quotas/settings/2FA on
  an unrelated account.

- [x] **`_compare_ftp` false-fails on derived paths**
  (`verify_migration.py:430-434`). The migrator derives `ftp_path` from
  `homedir`/`target_login` when `path` is empty (`accounts.py:94-101`); verify
  compares raw `path` → source `""` vs target `"web/site"` → FAIL.

- [x] **Verify assumes all optional phases ran** — unconditional
  password/2FA/FTP/dir-protection/mailbox hash checks
  (`verify_migration.py:353-360`, `_compare_ftp`, `_compare_dir_protection`).
  A `--skip-password-sync` migration can never verify clean; verify needs a flag
  or manifest awareness for which phases ran.

- [x] **Dead `Domains.add` retry** (`migration/domains.py:361-367`): retries a
  Let's Encrypt error with `letsencrypt: False`, but `base_payload` already has
  `letsencrypt=False` → identical retry that always re-throws.

## Low

- [x] `_load_source_database_user_hashes` ignores the `Host` column
  (`core.py:660-668`) — `(user,host)` duplicates collapse to an arbitrary last
  row, flattening per-host auth differences onto all target hosts.
- [x] `_filter_customer_rows` `int(row_customer_id)` (`api.py:305`) crashes on a
  non-numeric `customerid` value → listing aborts.
- [x] `_target_mysql_connect_kwargs` yields `{}` on credential-resolution
  failure (`core.py:376-386`) — test seam that masks real credential errors
  behind a later, misleading pymysql connect error.
- [x] Manifest writes are non-atomic and O(n²) (`transfer.py:61`) — every event
  rewrites the whole JSON file; a crash mid-write corrupts it. Append-JSONL or
  tmp+rename.
- [x] `transfer_files` uses `tar -cvf` (`transfer.py:244`) — the per-file list
  is buffered whole in memory by `capture_output` and dumped to the terminal
  after completion.
- [x] Path-only LRU caches go stale for the process lifetime
  (`transfer.py:297` `read_remote_file`, `froxlor_mysql.py:152-157`
  `_read_userdata_file`); `@cached` on a method also pins `self` alive.
- [x] PHP array extraction regexes truncate on `];`/`],` inside nested
  structures (`froxlor_mysql.py:122-129`); multiple `sql_root` entries pick the
  highest-scored rather than index 0. — resolved in round 7 (`4377f5f`,
  depth-aware `_php_bracket_span` scanner).
- [x] `EmailAccounts.update` runs unconditionally when `has_account`
  (`accounts.py:411-421`) — fails if the target mailbox has no mail account.
- [x] `int(data.get("status", 200))` (`api.py:117`) crashes on a non-numeric
  status string.
- [x] `_is_idempotent_command` substring heuristic (`api.py:25-28`) could
  retry a mutating command that happens to contain `.list`/`.get` — prefer an
  explicit allowlist.
- [x] Subdomain content outside the parent's docroot is never transferred —
  only domain docroots are tarred (`executor.py:182-188`). Document or cover.

## Verified non-issues (checked, no action needed)

- `parse_multi_select` `ValueError` — caught in `_choose_rows`, reprompts.
- `ALTER USER IF EXISTS` — valid MariaDB ≥10.1.3.
- `_relative_customer_path` index math — nested login dirs preserved correctly.
- Tunnel refcount/`ExitStack` — self-heals on mid-setup failure.
- `include_letsencrypt_flags` default `True` — intentional, non-fatal wrapped.
- `run()` local subprocess — `capture_output` drains both pipes; no deadlock.
- `transfer_mailbox` nested `shlex.quote` — quoting is correct.
- `IDENTIFIED VIA ... USING` / `IDENTIFIED BY PASSWORD` — MariaDB syntax, and
  the README explicitly scopes the tool to MariaDB panels.

---

# Third pass (2026-10)

Review of TUI internals, PHP credential parsing, SSH transport draining,
mailbox/SSH-key scoping, certificate writes, and self-review of round-2
changes.

## Fixed this pass

- [x] **`resolve_subdomain_parts` blind chop** (`util.py`): a `parentdomain`
  hint present in `known_domains` but *not* an actual suffix of the subdomain
  name (stale/inconsistent API row) produced a garbage label via
  `name[:-len(hint)]` — e.g. `sub.other.com` + hint `example.com` yielded
  `("su", "example.com")`. Now requires `name.endswith("." + hint)`.

- [x] **`_php_unescape` mangled `\"` in single-quoted PHP strings**
  (`froxlor_mysql.py`): single-quoted literals only honour `\\` and `\'`;
  unescaping `\"` corrupted credentials like `'pa\"ss'`. Double-quoted
  semantics unchanged.

- [x] **`Certificates.update` called without `id`** (`domains.py`): Froxlor's
  update endpoint keys on the certificate id; the call only passed
  `domainname` + cert fields. Now passes `id` from the existing target row.

- [x] **`_ensure_mailboxes` stale `existing_rows`** (`accounts.py`): after
  `Emails.add`, the row map wasn't updated — a duplicate mailbox row in the
  selection hit `existing_rows[mailbox]` → `KeyError`. The refreshed target
  row is now stored back, and `transferable` is deduplicated so a duplicated
  source row can't double-trigger `doveadm`.

- [x] **`mailbox_exists=skip` queued dsync for forward-only targets**
  (`accounts.py`): a skipped target mailbox with no mail account was still
  appended to `transferable` → `transfer_mailbox` fails on dsync. Now gated
  on the target row's account state.

- [x] **`list_email_senders` dropped per-mailbox rows lacking `email`**
  (`api.py`): the forwarders path injects `email`/`emailaddr`/`destination`
  into each row; the senders path did not — rows without an email key were
  silently unselectable in the TUI and unmatched in `_ensure_email_sender_aliases`.
  Now normalizes like forwarders and skips rows without `allowed_sender`.

- [x] **TUI silently swallowed zone-listing errors** (`tui.py`):
  `except FroxlorApiError: continue` while collecting `selected_domain_zones`
  dropped a domain's whole zone with no trace. Now prints a warning.

- [x] **SSH keys not scoped to selected FTP accounts** (`tui.py`):
  `selected_ssh_keys = ssh_keys` ignored the FTP selection, then
  `_ensure_ssh_keys` raised `MigrationError` for keys whose FTP user wasn't
  migrated. Keys are now filtered via `_filter_ssh_keys_for_ftps` (mirrors
  the forwarder/mailbox scoping).

- [x] **`SshDriver.run` truncated late-arriving output** (`ssh_driver.py`):
  the post-exit drain stopped when recv buffers were momentarily empty —
  exit status arrives before the last data packets, so trailing
  stdout/stderr was lost. Now drains until `eof_received`/closed
  (deadline still enforced).

- [x] **`run_remote` printed stderr verbatim under `sensitive=True`**
  (`transfer.py`): stderr of sensitive commands (e.g. mysql error echoes
  containing SQL fragments) reached the console; now gated like stdout.

- [x] **Duplicated `_is_custom_zone_record`** — identical copies in
  `domains.py` and `verify_migration.py` consolidated into
  `util.is_custom_zone_record` (drift risk).

## Still open / deferred

- [x] **N+1 listing refresh pattern** — `list_*` called per item across
  accounts/domains after each write (`accounts.py`, `domains.py`,
  `core.py:284`). O(n) full-panel API listings per migration; needs a
  snapshot/refresh design. — resolved in round 7 (`aac213b`).

- [x] **PHP array extraction regexes truncate on `];`/`],` inside nested
  structures** (`froxlor_mysql.py` `_extract_php_array_body` /
  `_extract_first_sql_root_entry`); multiple `sql_root` entries pick the
  highest-scored rather than index 0. Needs a real `userdata.inc.php`
  fixture set before rewriting. — resolved in round 7 (`4377f5f`).

- [x] **Local `TransferRunner.run` has no timeout** (`transfer.py:90`) —
  fixed: `[behavior] local_command_timeout_seconds` (0 = disabled by
  default since long transfers are legitimate) plus a per-call `timeout`
  kwarg; expiry raises `TransferError`.

- [x] **Progress accounting drifts from `total_steps`** (`executor.py`) —
  fixed: subdomain-path transfers counted in `total_steps`, and
  alias/duplicate-docroot skips now `_advance` instead of drifting.

- [x] **`Debug`/`logger.debug` lines log raw command strings**
  (`ssh_driver.py`, `transfer.py`) — fixed: `SshDriver.run` and
  `run_remote` accept `sensitive=True` which redacts the command in debug
  logs, the manifest `command` event, and `check` error messages.

## Verified non-issues (round 3)

- `_php_unescape` indexed-`sql_root` regex ambiguity — the direct-regex
  paths can't distinguish quote types; single-quoted semantics is the
  correct default for `userdata.inc.php` (Froxlor emits single quotes).
- `run_remote` command logging — SQL is transported via `write_remote_file`
  (mode 0600), so `command` strings contain only file paths, not queries.
- `TransferError` embedding the command — same reasoning; no secrets in
  remote-CLI command strings.
- `parse_multi_select("all,junk")` — `all` wins, junk ignored; harmless.
- `open_ssh_tunnel` select loop — paramiko `Channel.fileno()` works with
  `select`; EOF/close handled.
- `_replace_ip_tokens` — whitespace-tokenized replacement can't corrupt
  `ip4:`/`include:` mechanisms inside quoted TXT payloads.
- `mysqldump`/`DELIMITER` splitter — handles `DELIMITER ` directive at line
  start, doubled quotes, backslash runs, `-- `#`/`/* */` comments.
- `Certificates.listing` unfiltered in verify — keyed by domain name;
  cross-customer rows can't collide since domain names are unique panel-wide.

## Dead code / dedup sweep (round 3b)

Fixed:

- [x] `_customer_email` (`core.py`) — dead since login-only matching.
- [x] `CommandResult.started_at`/`finished_at` — written on every command,
  never read anywhere.
- [x] Row-key extraction copy-pasted across `verify_migration.py`,
  `core.py`, `accounts.py`, `tui.py` with subtly different normalization —
  consolidated into `util.domain_name`/`mailbox_address`/`ftp_username`/
  `ssh_key_identity`/`data_dump_key` (verify's copies also lacked
  `.strip()` — real inconsistency, now fixed).
- [x] `_run_target_mysql_query`/`_exec_target_mysql_sql` duplicated the
  tunnel→remote-CLI fallback scaffold — now `_with_target_mysql(...)`.
- [x] `_build_php_mapping_tokens`/`_build_ip_mapping_tokens` shared loop —
  now `_build_mapping_tokens(token_getter=...)`.
- [x] `_ensure_email_forwarders`/`_ensure_email_sender_aliases` identical
  bodies — now `_ensure_mail_attribute_rows`.
- [x] `domain_name` shadowed by loop locals in both `verify_migration.main()`
  and `tui.run_app()` → `UnboundLocalError` traps; renamed to
  `redirect_domain`/`zone_domain`.

Vulture clean (remaining hits are known false positives: `daemon_threads`
socketserver attr, `run_app` entry point).

## Integration-test findings (round 4 — docker-compose testbed)

All surfaced by `tests/test_integration_compose.py` running a real
source→target migration against Froxlor 2.3.x containers. Fixed:

- [x] **`Ftps.listing` strips `password`** — hash sync read API rows that
  never carry it → `Source FTP account has empty password hash`. Now
  `_load_source_ftp_password_hashes` queries `ftp_users` on the source
  panel DB (same pattern as mailbox hashes); missing rows are skipped
  with a debug event, genuinely empty hashes still raise.

- [x] **`Domains.listing`/`get` strip `dkim_privkey`** — DKIM-enabled
  domains with a pubkey drift always hit "source private key is empty".
  `_load_source_dkim_private_key` queries `panel_domains` lazily on
  mismatch.

- [x] **`EmailSender.add` rejects admin API keys for customer-owned
  domains** (`validateLocalDomainOwnership` compares against the admin
  caller's empty `customerid`) — fallback `INSERT IGNORE` into
  `mail_sender_aliases` when the target mailbox exists and has a mail
  account; forward-only/absent mailboxes still raise.

- [x] **`Customers.listing` never returns `data_2fa`** (unset in listing
  and get) — seeded/verified via the panel DB; migrator 2FA sync was
  already DB-based.

- [x] **`chown` on target fails when the customer system user doesn't
  exist yet** — Froxlor creates system users via async cron tasks after
  `Customers.add`. `_fix_transferred_docroot_ownership` probes `id -u`
  first and emits a debug event instead of aborting; the panel's own
  cron chowns the homedir when it provisions the user.

- [x] **FTP `path` field does not exist in listings** — `ftp_users` only
  has absolute `homedir`. The migrator's `target_login` fallback would
  have created a nested `docroot/<login>` dir for main accounts; now "/"
  (docroot). `_compare_ftp` derives the target path from `homedir` the
  same way.

- [x] **`DataDump.listing` returns `panel_tasks` rows with config nested
  in decoded `data` JSON** — `path`/`dump_*`/`pgp_public_key` top-level
  picks were all empty, so dumps were silently skipped and verify keyed
  on `('',0,0,0,'')`. `data_dump_key`, `_ensure_data_dumps`, the seed's
  `ensure_data_dump`/summary, and `verify_seed` now read `data.destdir`
  (relativized by the customer login marker) and nested flags.

- [x] **Compose testbed coverage gaps** — `system.dnsenabled` and
  `system.exportenabled` are now enabled in bootstrap so zone and
  data-dump paths are exercised; `seed_source.sh` resolves the built
  image via `docker compose images -q` (project-name independent).

### Round 5 (post-integration review)

- [x] **`listing()` trusted `count` as a global total** — several
  endpoints (`Ftps.listing` at least) return `count` = current page
  size, so the loop broke after the first full page and silently
  truncated large listings. Now continues while a full page is returned
  and stops on a short/empty one.

- [x] **`domain_redirect_codes` unique key is `(rid, did)`, not `did`** —
  `ON DUPLICATE KEY UPDATE` never fires when the redirect code changed,
  leaving stale rows that `pexecute_first` reads arbitrarily. Sync now
  deletes all redirect rows for migrated domains then inserts the
  desired ones (mirrors `Domain::updateRedirectOfDomain`).

- [x] **PHP credential regexes rejected the opposite quote inside
  values** — `'pa"ss'` / `"ro'ot"` in `userdata.inc.php` failed to parse.
  The value literal now captures the opening quote and requires the
  matching close (named backref `(?P<q>…)(?!(?P=q))…(?P=q)`).

- [x] **`SubDomains.add` resolves `path` against the target customer
  docroot** — absolute source paths embed the source login and were
  passed verbatim, so renamed customers got
  `…/<target>/<source-web-root>/…`. The record payload and the
  file-transfer destination both relativize via
  `util.relative_customer_path` (promoted from `MigratorCore`).

- [x] **Verify compared `dir_protection`/`dir_option`/`subdomain` paths
  verbatim** — absolute paths embed the customer login; keys and
  `_compare_dir_protection.path` now normalize through
  `relative_customer_path`.

- [x] **`Ftps.listing` has no `path` field** — `_ensure_ftp_accounts`
  and verify's `_relative_ftp_path` now share
  `relative_customer_path(path or homedir, login)`; root accounts use
  `/` instead of a nested `docroot/<login>`.

- [x] **`Customers.get`/`listing` strip `password`/`data_2fa` —
  customer auth sync was a silent no-op** — `_sync_customer_password_hash`
  and `_sync_customer_2fa_settings` now load from source
  `panel_customers`; `type_2fa > 0` without a readable secret raises
  `MigrationError` instead of writing a secretless flag. Verify gained
  `_load_customer_secrets` for real password/2FA comparison.

- [x] **`run_app()` returned exit 0 on every failure** — automation
  could not detect errors; error paths now raise `SystemExit(1)` and
  `--source-customer`/`--target-customer` selector errors are caught
  (`ValueError` → clean exit instead of traceback).

- [x] **Manifest `result` event logged raw commands even with
  `sensitive=True`** — redaction now covers the result event's command
  field too.

- [x] **Verify opened a fresh SSH session + tunnel per target panel
  query** — `_target_panel_session` lazily shares one tunnel per run for
  redirects/FTP hashes/customer secrets.

- [x] **`preflight` `needs_ssh` missed the panel-DB writers** — redirect
  sync (unconditional when domains are selected), sender-alias fallback,
  and password/2FA/FTP-hash sync all need SSH; covered now.

- [x] **`listing()` could loop forever** on endpoints ignoring
  `sql_offset` — an identical full page now breaks pagination.

- [x] **`pytest tests/test_integration_compose.py` standalone fails the
  75% coverage gate** (code runs inside containers) — use
  `make test-integration` (`--no-cov`) or the CI compose job.

### Round 6 (post-review cleanup)

- [x] **`_extract_credentials` ignored the captured quote type** —
  `_php_unescape` always ran in single-quote mode, so `\n`/`\"` in
  double-quoted `userdata.inc.php` values were never unescaped. The
  `double_quoted` flag is now derived from the actual opening quote.

- [x] **`_ensure_target_customer` skipped `Customers.update` on the
  add-failed-but-exists path** — settings drifted silently on
  re-migration. The found customer now receives the update payload.

- [x] **`_ensure_data_dumps` nested the absolute `destdir` under the
  docroot when `panel_tasks.data` lacked `loginname`** — falls back to
  the customer login; skips with a debug event when no owner can be
  determined. `data_dump_key` accepts a login fallback so dedup stays
  consistent.

- [x] **Subdomain file transfer nested the full absolute source path**
  when the path equaled the customer root (`relative_customer_path` → `""`
  then fell back to `sub_path.lstrip("/")`). The fallback is removed;
  `""` maps to the target docroot.

- [x] **`_compare_subdomain` compared `path` verbatim** — source listings
  can return absolute login-embedded paths while the migrator writes the
  relative form, producing false mismatches. Both sides normalize via
  `relative_customer_path` now.

- [x] **Committed files failed `ruff format --check`** — CI runs the
  format gate; repo is now format-clean (added to the verify loop).

### Verified non-issues this pass

- `Customers.update` ignores unknown params (`getParam` only reads known
  keys) — `type_2fa`/`data_2fa` in the payload are harmless.
- `openbasedir_path` is an int enum (0/1/2), not a filesystem path —
  verbatim compare is correct.
- Verify only matches same-login customers, so single-login
  normalization of dir-protection/dir-option keys is consistent.
- `transfer_files` already runs under `bash -o pipefail`.

### Still open / deferred (all resolved in round 7)

- [x] **N+1 listing refreshes** — every ensure-* re-lists target rows
  per item; needs a snapshot/cache design pass. → `aac213b`
- [x] **PHP `userdata.inc.php` nested-array regexes** — best-effort;
  needs real fixtures before a rewrite. → `4377f5f` (`_php_bracket_span`)
- [x] **Local `run()` timeout** — `[behavior] local_command_timeout_seconds`
  exists (0=disabled); tar/doveadm on huge trees may exceed an hour —
  needs per-call policy, not a blunt global default. → `663b64a`

### Round 7 (structural debt — all three deferred items resolved)

- [x] **N+1 listing refreshes** (`aac213b`) — ensure-* operations now
  merge `add`/`update` response rows into their local indexes instead of
  re-listing the whole target collection per item; mailbox reloads use
  `Emails.get` with a list-scan fallback, and `_get_target_domain` uses
  `Domains.get(domainname)` single-row fetches.

- [x] **Per-call timeout policy** (`663b64a`) — command probes get a
  hard ~30s cap; tar/doveadm/mysqldump transfers use
  `[behavior] transfer_timeout_seconds` (0 = unlimited, preserving
  multi-hour migrations of large trees).

- [x] **PHP `userdata.inc.php` scanner** (`4377f5f`) — replaced the
  non-greedy `(.*?)\];` body regexes with `_php_bracket_span`, a
  depth- and quote-aware scanner that survives `]`/`];` inside string
  values and arbitrary nested arrays. `$sql`/`$sql_root` extraction
  shares the same scanner.

- [x] **Xenon gate now passes at `-b D -m B -a B`** — all rank-E/F
  blocks refactored instead of weakening the gate:
  - `mysql_driver._iter_mysql_statements` F → `_MysqlScriptScanner`
    class, one method per lexer state (`7916975`).
  - `api.list_email_forwarders`/`list_email_senders` E/D → shared
    `_forwarder_rows_from_payload`/`_sender_rows_from_payload`
    normalizers (`0862be3`).
  - `Migrator.execute` F → `_Progress` reporter + `_sync_web_config`,
    `_sync_databases`, `_sync_mail_objects`, `_transfer_files_and_mail`
    phase methods (`fff7e5f`).
  - `tui.run_app` F → extracted `_parse_args`, `_resolve_source_customer`,
    `_discover_customer_resources`, `_resolve_mode`,
    `_resolve_target_customer`, `_select_domains`, `_select_resources`,
    `_resolve_mappings`, `_resolve_includes`, `_print_migration_plan`,
    `_execute_migration`, `_print_migration_result`; `_build_replay_command`
    now uses per-flag emitter helpers (`f9736c7`).
  - `verify_migration.main` F → `_Report` emitter +
    `_load_customer_resources` (lazy loaders keep `--skip-*` from hitting
    the API) + per-resource `_verify_*` helpers (`e72f318`).

### Watch items (complexity hotspots kept at rank C)

`_build_php_setting_map` (C19), `TransferRunner.run_remote` (C19),
`SshDriver.run` (C16), `_build_ip_map`/`_print_migration_plan` (C16),
`_choose_rows`/`preflight_commands`/`run` (C15). These are interactive
prompts or retry/exec plumbing where further splitting would add
indirection without real clarity; revisit only if they grow.

### Round 8 (validation pass — edge cases + remaining N+1 spots)

- [x] **Sender-alias SQL fallback scoped to the real bug** (`2051562`) —
  `_add_sender_alias_sql` used to run on any `FroxlorApiError` from
  `EmailSender.add`, silently bypassing legitimate rejections (feature
  disabled → 405, `senderdomainexternal` policy, malformed addresses).
  It now verifies the sender's domain exists in target `panel_domains`
  (the actual `validateLocalDomainOwnership` admin-caller conflict)
  before writing `mail_sender_aliases`, and re-raises the original API
  error for everything else.

- [x] **Dialect-aware `ALTER USER` for DB login hashes** (`d2abd9d`) —
  the sync emitted MariaDB-only `IDENTIFIED VIA ... USING` /
  `IDENTIFIED BY PASSWORD` unconditionally; MySQL 8 removed both in
  favor of `IDENTIFIED WITH <plugin> AS '<hash>'`. Target `VERSION()`
  is probed once per run (cached) and the matching syntax emitted.

- [x] **Certificate listing N+1** (`d75492f`) —
  `_migrate_domain_certificates` re-listed `Certificates.listing` after
  every write; `Certificates.add`/`update` return the `get` row, so
  the response is merged (single-row `Certificates.get` fallback for
  older APIs/stubs). DKIM-sync and IP-mapping verify moved into
  `_sync_domain_dkim`/`_verify_domain_ip_mapping` helpers.

- [x] **Verify parity + per-customer cert listings** (`eadaedc`) —
  `_relative_ftp_path` now delegates to `relative_customer_path` so an
  absolute FTP `path`/`homedir` embedding the login normalizes exactly
  like the migrator's write (was a false-mismatch); `data_dump_key`
  gets the login fallback matching the migrator's key derivation;
  `Certificates.listing` results are shared across all customers via a
  lazy `cert_cache` instead of two full listings per customer.

- [x] **Secret-handling + MySQL driver edge cases** (`53f9bea`) —
  `_redact_params` recurses into list values; SSH `TimeoutError`
  honors `sensitive`; `mysql_driver.query` decodes bytes cells instead
  of rendering `b'...'` reprs; `mysql_defaults_content` quotes values
  containing `#`/`;`/whitespace/quotes and writes control chars as
  option-file escapes; `_extract_credentials` split into
  regex/scanner variants for the complexity gate.

#### Xenon after this pass

`-b D -m B -a B` passes; `-b C` is now also clean — all blocks ≤ B.

#### Still open / watch items

- `remote mysql CLI` TSV fallback in `_run_target_mysql_query` splits on
  `\t`/newlines — cell values containing those would misparse; only used
  when the SSH-tunnel path fails, and queried fields are scalar.
- `_select_rows_by_tokens` treats a whitespace-only selector as "all" —
  surprising but consistent with "no filter".
- `_build_replay_command` emits `uv run python main.py` — assumes the
  source-checkout layout rather than the installed console script.
- Subdomain `phpsettingid` unmapped → `0` (inherit), while main domains
  fall back to the source id — asymmetric but harmless; revisit if a
  target rejects `0`.

### Round 9 — integration-test feature audit + coverage gaps

Audited the docker testbed end-to-end (seed → apply → verify) against the
full migrator surface. Gaps found and closed (`e190794`):

- **`--include-mail` was never passed** — `Selection.include_mail` was
  always False, so `TransferRunner.transfer_mailbox` (the
  `doveadm backup | ssh doveadm dsync-server` path) had zero e2e
  coverage; the mail probe was pushed by a manual doveadm call that
  bypassed the migrator entirely. The probe message now flows through
  the real `transfer_mailbox` code path.
- **No custom DNS zone records seeded** — zone sync and `verify_zone`
  compared two empty sets. Seeded TXT + CNAME fixtures on
  `secure-demo.test` (requires `system.bind_enable` + per-domain
  `isbinddomain`, now set in bootstrap), plus a `verify_dns_zones`
  seed-side check.
- **No idempotent re-run** — the apply now runs twice, with a
  deliberately drifted target zone record in between, exercising every
  update/dedup path.

The new coverage immediately caught three real production bugs
(`1bf52d8`, `3e2626e`):

- `Certificates.update` was called with the `domain_ssl_settings` row id
  where Froxlor expects the **domain** id — the row id silently aliased
  an unrelated domain on re-runs (412 in the test, silent corruption
  risk in production). Now passes `domainid`.
- `DomainZones.listing` rows carry `domain_id`, not `domainname` —
  `_ensure_domain_zones` grouped every record under `""` and skipped
  them: zone sync was a complete silent no-op. Now resolves names via a
  lazy `domain_id → domain` map.
- `DomainZones.update` is an unconditional 303 stub in Froxlor ("delete
  it and re-add it") — and `api.call()` only treated `status >= 400` as
  an error, so the throw slipped through as success. Near-match drift
  now uses `DomainZones.delete` + `add`; `call()` treats any non-2xx
  status as an error.

#### Remaining e2e coverage gaps (accepted/documented)

- Domain-only mode / pre-selected `target_customer` rename path —
  unit-tested only (verify can't compare renamed customers).
- `ip_mapping` — both panels share one IP; nothing to map against.
- Real ACME issuance — `letsencrypt=1` propagation is covered with
  `le_domain_dnscheck` disabled; no real DNS/ACME on `.test` domains.

### Round 10 — Integration hardening, cont. (`6c84ea3`, `115b5ca`, `5702f54`, `ec77a73`)

Extended coverage: `--use-ssl` at install + `letsencrypt=1` fixture on
`empty-demo.test` (deferred LE-enable path exercised for real), dry-run
write-guard, pre-created existing customer, negative verify on a deleted
zone record, byte-level web-content parity, docroot ownership vs.
`panel_customers.guid`, and a `migrator_marker` DB row compared on the
target.

New bugs caught by the stronger assertions:

- `_fix_transferred_docroot_ownership` silently skipped when the
  customer's system user didn't exist yet (Froxlor's CREATE_HOME cron
  creates it async) — leaving docroots `root:root` permanently. Now
  falls back to a numeric `chown guid:guid` from `panel_customers.guid`
  (the exact uid/gid Froxlor will assign).
- `load_config` silently ignored unknown keys — the test harness shipped
  dead `target_owner_user`/`target_owner_group`/`parallel` keys for
  months. `load_config` now rejects unknown keys per section; the test
  config was cleaned accordingly.

### Round 11 — Real-CLI e2e + ip-map + rename (`98e3386`, `d8e10e4`, `003a866`, `bc0236b`, `506aed2`, `0d51cb8`)

The harness previously built `Selection` objects directly — bypassing
the real CLI front-end. `run_migration_apply.py` now invokes
`main.py --non-interactive` so arg parsing, selection, mappings, and
confirmation get real coverage. New e2e coverage:

- `custalpha` applies with `--ip-map` (secondary ip:port fixtures on
  both panels; `static-demo.test` is bound to the source secondary IP
  with an A record containing it). `verify_migration --ip-value-map`
  translates record content before comparison.
- Domain-only rename: `wp-demo.test`/`static-demo.test` migrate into a
  pre-created `custdelta` via `--domain-only --target-customer`;
  `assert_rename.py` checks ownership reassignment and docroot
  remapping under the new login.

Bugs the new paths caught:

- `ipandport`/`ssl_ipandport` were sent as `[{"id": N}]` — Froxlor's
  `validateIpAddresses` `trim()`s each element (PHP 8 TypeError →
  HTTP 500). Now plain int lists. This path had never run end-to-end.
- `mysql --batch --raw` remote-CLI fallback parsed raw TSV — a literal
  tab/newline in a value silently corrupted rows. Now parses batch-mode
  escapes (`_parse_mysql_batch_output`).

Watch items resolved/closed:

- Replay command assumed source-checkout layout → emits the installed
  `froxlor-migrator` console script (`003a866`).
- Whitespace-only domain selector selecting "all" → by design (empty
  filter = no filter; required for scripting).
- Subdomain `phpsettingid → 0` → by design (Froxlor's 0 = "inherit
  parent", which resolves the mapped value automatically).

#### Remaining e2e coverage gaps (accepted/documented)

- Real ACME issuance — `letsencrypt=1` propagation is covered with
  `le_domain_dnscheck` disabled; no real DNS/ACME on `.test` domains.
