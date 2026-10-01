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

- [ ] **`SshDriver.run` can deadlock on chatty remote stderr**
  (`ssh_driver.py:83-84`). `stdout.read()` is fully drained before
  `stderr.read()`; Paramiko stdout/stderr share one channel window, so a remote
  command writing enough stderr blocks the server while we block on stdout →
  hang forever. No timeouts exist anywhere in the SSH/command path, so this is
  unrecoverable without Ctrl-C. Fix via `channel.set_combine_stderr(True)` or
  concurrent stream draining, plus a command timeout.

- [ ] **Remote-CLI MySQL fallback leaks query output to console/manifest**
  (`migration/core.py:473-492` + `transfer.py:303-325`).
  `_run_target_mysql_via_remote_cli` calls `runner.run_remote(cmd)` without
  `sensitive=True`. When the SSH tunnel fails for non-panel DBs (e.g. `mysql`
  during `_sync_database_login_hashes`), result events write raw stdout —
  including `mysql.user` / `mail_users` password hashes — to the terminal and
  into `manifests/*.json`.

- [ ] **Socket discovery overrides explicit remote DB host**
  (`migration/core.py:388-400`). When target `sql_root` creds have no
  `unix_socket`, `_discover_remote_mysql_socket()` finds a local socket on the
  SSH host and *deletes* `host`/`port` — so a credentials file pointing at a
  separate DB server (e.g. `db.internal`) silently connects to the SSH host's
  local MySQL instead. Panel writes could hit the wrong server. Only apply
  socket discovery when host is localhost/127.0.0.1/empty.

- [ ] **Dir-protection dedup rebuild keyed on the wrong login**
  (`migration/accounts.py:316-321`). Initial `existing` map uses `target_login`
  (line 282); the post-write rebuild uses `customer_login` (source). For renamed
  target customers every row after the first misses → duplicate
  `DirProtections.add` on re-run. Same class of bug the first pass fixed — one
  rebuild site was missed.

- [ ] **File transfer loops over alias/empty-docroot domains**
  (`migration/executor.py:182-188`). No `aliasdomain`/`parentdomain` filter; an
  alias row's empty `documentroot` resolves to `{transfer_root}/{login}/` — the
  whole customer homedir — so every alias/empty-docroot domain triggers a full
  homedir tar + `chown -R` again. N extras → N duplicate full transfers. Dedupe
  by resolved source path and/or skip alias domains.

## Medium

- [ ] **`mysqldump` drops routines and events** (`migration/core.py:594-599`).
  `--triggers` is on by default but `--routines`/`--events` are off → stored
  procedures, functions, and scheduled events are silently lost.

- [ ] **Listing wrappers swallow `FroxlorApiError` → silent loss that verify
  can't see** (`api.py`: `list_domain_zones`, `list_data_dumps`,
  `list_email_forwarders`, `list_email_senders`). On error they return `[]`.
  Migration drops the data; verify calls the same wrappers and gets `[]` on both
  sides → reports OK on data it never saw. At minimum, verification must not
  treat a listing error as "empty".

- [ ] **Zone dedup lowercases `content` but verify compares verbatim**
  (`migration/domains.py:662,678` vs `verify_migration.py:823`). An existing
  target record differing only in case is skipped by the migrator yet flagged by
  verify forever — re-running never repairs case-mangled records. Dedup should
  compare case-sensitively for content.

- [ ] **Multi-level subdomains silently dropped everywhere**
  (`tui.py:823`, `migration/domains.py:573-580`, `verify_migration.py:847`).
  All three resolve the parent via `split(".", 1)` → `a.b.example.com` gets
  parent `b.example.com`, which isn't a main domain → filtered/skipped with no
  warning. Use the row's parent-domain field or suffix-match against known
  domains.

- [ ] **N+1 listing refresh pattern**. `_get_target_domain` runs an unfiltered
  `Domains.listing` ~3× per domain (`core.py:284-288`, `domains.py:355-383`);
  `accounts.py:130,266,315,423` and `domains.py:620-621` re-list after every
  single add/update. O(N) full-panel listings per migration; also creates
  stale-read windows. Filter by customerid where supported or keep a refreshed
  snapshot.

- [ ] **`hsts` compare asymmetry** (`migration/domains.py:284`): payload reads
  `pick(domain, "hsts", "hsts_maxage")` but the target side reads only
  `"hsts"` → false mismatch when the API returns `hsts_maxage`.

- [ ] **`_find_target_customer` matches on email alone**
  (`migration/core.py:164-176`). A different customer sharing the email is
  treated as the same one → `Customers.update` overwrites quotas/settings/2FA on
  an unrelated account.

- [ ] **`_compare_ftp` false-fails on derived paths**
  (`verify_migration.py:430-434`). The migrator derives `ftp_path` from
  `homedir`/`target_login` when `path` is empty (`accounts.py:94-101`); verify
  compares raw `path` → source `""` vs target `"web/site"` → FAIL.

- [ ] **Verify assumes all optional phases ran** — unconditional
  password/2FA/FTP/dir-protection/mailbox hash checks
  (`verify_migration.py:353-360`, `_compare_ftp`, `_compare_dir_protection`).
  A `--skip-password-sync` migration can never verify clean; verify needs a flag
  or manifest awareness for which phases ran.

- [ ] **Dead `Domains.add` retry** (`migration/domains.py:361-367`): retries a
  Let's Encrypt error with `letsencrypt: False`, but `base_payload` already has
  `letsencrypt=False` → identical retry that always re-throws.

## Low

- [ ] `_load_source_database_user_hashes` ignores the `Host` column
  (`core.py:660-668`) — `(user,host)` duplicates collapse to an arbitrary last
  row, flattening per-host auth differences onto all target hosts.
- [ ] `_filter_customer_rows` `int(row_customer_id)` (`api.py:305`) crashes on a
  non-numeric `customerid` value → listing aborts.
- [ ] `_target_mysql_connect_kwargs` yields `{}` on credential-resolution
  failure (`core.py:376-386`) — test seam that masks real credential errors
  behind a later, misleading pymysql connect error.
- [ ] Manifest writes are non-atomic and O(n²) (`transfer.py:61`) — every event
  rewrites the whole JSON file; a crash mid-write corrupts it. Append-JSONL or
  tmp+rename.
- [ ] `transfer_files` uses `tar -cvf` (`transfer.py:244`) — the per-file list
  is buffered whole in memory by `capture_output` and dumped to the terminal
  after completion.
- [ ] `IDENTIFIED VIA ... USING` / `IDENTIFIED BY PASSWORD` are MariaDB-only
  (`core.py:784-795`). README scopes to MariaDB, but a MySQL 8 target would
  fail on every statement — document or engine-detect.
- [ ] Path-only LRU caches go stale for the process lifetime
  (`transfer.py:297` `read_remote_file`, `froxlor_mysql.py:152-157`
  `_read_userdata_file`); `@cached` on a method also pins `self` alive.
- [ ] PHP array extraction regexes truncate on `];`/`],` inside nested
  structures (`froxlor_mysql.py:122-129`); multiple `sql_root` entries pick the
  highest-scored rather than index 0.
- [ ] `EmailAccounts.update` runs unconditionally when `has_account`
  (`accounts.py:411-421`) — fails if the target mailbox has no mail account.
- [ ] `int(data.get("status", 200))` (`api.py:117`) crashes on a non-numeric
  status string.
- [ ] `_is_idempotent_command` substring heuristic (`api.py:25-28`) could
  retry a mutating command that happens to contain `.list`/`.get` — prefer an
  explicit allowlist.
- [ ] Subdomain content outside the parent's docroot is never transferred —
  only domain docroots are tarred (`executor.py:182-188`). Document or cover.

## Verified non-issues (checked, no action needed)

- `parse_multi_select` `ValueError` — caught in `_choose_rows`, reprompts.
- `ALTER USER IF EXISTS` — valid MariaDB ≥10.1.3.
- `_relative_customer_path` index math — nested login dirs preserved correctly.
- Tunnel refcount/`ExitStack` — self-heals on mid-setup failure.
- `include_letsencrypt_flags` default `True` — intentional, non-fatal wrapped.
- `run()` local subprocess — `capture_output` drains both pipes; no deadlock.
- `transfer_mailbox` nested `shlex.quote` — quoting is correct.
