# Known bugs & issues

Findings from code review (2026-10-01). Ordered by severity.

## High severity

- [ ] **`database_exists="skip"` is ignored — existing DBs get overwritten.**
  `_create_database_on_target` returns `src_name` for both `"skip"` and `"update"`
  (`migration/domains.py:439-442`), then the executor unconditionally runs
  `_transfer_database_with_defaults` (`migration/executor.py:144-146`), restoring the
  source dump *into the existing database*. A "skip" policy causes data overwrite.
  Also `known_before` contains *all* customers' DBs, so this can clobber another
  customer's database. Additionally `validate_database_names` can never trigger
  (`executor.py:138`) since `target_name == src_name` always.

- [ ] **File ownership fix is broken in every mode** (`migration/domains.py:731-735`).
  `find {docroot} -user {source_login}` runs on the *target*: GNU find exits 1 for
  unknown users → `run_remote(check=True)` → `TransferError` when the source login
  doesn't exist on target (exactly the case it was written for). Even if it didn't
  error, `tar -xp` as root restores *numeric* UIDs, so name-based `-user` matches
  nothing. And when `target_login == source_login` it early-returns, so transferred
  files keep stale source UIDs in the common case. Should be
  `chown -R {target_login}:{target_login} {docroot}` or find by numeric UID.

- [ ] **`sudo` required by preflight but never used by remote commands.**
  Preflight requires remote sudo for non-root SSH users (`transfer.py:214`), but
  `transfer_files`' `mkdir -p`/`tar -xpf` into `/var/customers/webs`
  (`transfer.py:233`), the DB restore and `rm` (`migration/core.py:610-625`), the
  remote mysql CLI fallback (`core.py:481-499`), and the chown all run as the SSH
  user. Only `transfer_mailbox` uses sudo. Non-root setups pass preflight then fail
  on writes/chown.

- [ ] **DNS zone record `content` lowercased before insert** (`migration/domains.py:684-708`).
  `key[3]` (`content.lower()`) is what gets sent to `DomainZones.add` — DKIM TXT
  records (`p=` base64) and other case-sensitive content get corrupted.

- [ ] **Relative documentroots silently excluded in whole-customer mode**
  (`tui.py:440-443, 775`). `_domain_in_source_root` only accepts absolute docroots
  under `source_web_root`, but `_resolve_source_docroot` explicitly handles relative
  docroots (`{transfer_root}/{login}/{docroot}`, `migration/domains.py:719-720`) —
  Froxlor commonly stores documentroot relative to the customer homedir. If the API
  returns relative values, every domain is "skipped outside source_web_root" and
  whole-customer migrates zero domains. `verify_migration._expected_target_docroot`
  (lines 77-89) also lacks the relative→`{target_root}/{login}` mapping → false
  mismatches.

- [ ] **Docroots use the *source* login when target customer is renamed**
  (`migration/domains.py:722-729`). `_resolve_target_docroot` embeds
  `customer_login` (source) in the target path `{target_root}/{source_login}/…`.
  With `--target-customer` pointing at a differently-named customer, docroots/files
  land outside the target customer's homedir. Same class: `_relative_customer_path`
  is applied to *target* rows using the source login (`migration/accounts.py:226,261`)
  → dedup keys never match → duplicate DirOptions/DirProtections on re-run.

- [ ] **SQL splitter escape bug** (`mysql_driver.py:96-102`). Escape detection checks
  only the previous char: `'a\\'` (escaped backslash + closing quote) is parsed as an
  unterminated string → the rest of the script is swallowed into one statement →
  `execute()`/`import_sql_dump` corrupt any SQL containing `\\` inside quotes (e.g.
  `'C:\\'`). Also `--` starts a "comment" without MySQL's required trailing space
  (`a--b` truncates), and `DELIMITER` is matched mid-line instead of at line start.

- [ ] **DB user host sets don't match** (`migration/domains.py:468-484` vs
  `migration/core.py:779`). Users are created for hosts from the
  `mysql_access_host` panel setting, but `_sync_database_login_hashes` ALTERs only
  `%/localhost/target-db/127.0.0.1/{ssh.host}`. If the setting is e.g. `web1,
  10.0.0.%`, created users keep their random passwords → migrated DB logins broken.

## Medium

- [ ] Mailbox `existing` set isn't lowercased but `mailbox` is → mixed-case target
  mailboxes bypass the exists-check → duplicate `Emails.add`
  (`migration/accounts.py:330-348`).

- [ ] `EmailAccounts.add` runs for *every* email row — forward-only addresses
  (acknowledged in `core.py:716`'s comment) become real mailboxes → changed
  semantics (`migration/accounts.py:365-375`).

- [ ] Source-ID passthrough into target ID space: `adminid`, `allowed_phpconfigs`,
  `allowed_mysqlserver`, `hosting_plan_id`, `alias` (`migration/core.py:214,226,
  232,234`; `migration/domains.py:247`). `allowed_phpconfigs`/`phpsettingid` should
  be remapped via `php_setting_map`; `alias` is a source domain id sent to
  `Domains.add` before `_sync_domain_redirects` fixes it → possible API rejection.

- [ ] PHP map only covers domains, not subdomains — `_build_php_setting_map`
  iterates `selected_domains` only (`tui.py:521`); `_ensure_subdomains` falls back
  to the raw source id for unmapped settings (`migration/domains.py:601-602`) →
  wrong/absent PHP config.

- [ ] `phpsettingid` omitted from payload when mapped value is 0
  (`migration/domains.py:261-262`) but `_verify_domain_settings` expects 0
  (`domains.py:279`) → false `MigrationError` if Froxlor assigns a default.

- [ ] `unicode_escape` decode mangles credentials (`froxlor_mysql.py:67-69,114,118`):
  non-ASCII passwords → mojibake; PHP single-quoted `\n` (literal backslash-n) →
  real newline → wrong password *and* newline injection into
  `mysql_defaults_content`.

- [ ] `process.stderr.read()` blocks while the ssh process is alive
  (`migration/core.py:470`) — missing `ExitOnForwardFailure=yes` means a remote-side
  forward failure hangs forever instead of erroring.

- [ ] API params logged at DEBUG include `ssl_key_file` (private keys),
  `new_customer_password`, `ftp_password`, `data_2fa` (`api.py:38-43`).

- [ ] Verify/migrate inconsistency: `deactivated` and `theme` are stripped from
  `Customers.add` (`migration/core.py:263`) but compared by `_compare_customer`
  (`verify_migration.py:323-334`) → newly created customers with custom
  theme/deactivated=1 always FAIL verify. Also `new_customer_password` is sent on
  `Customers.update` — risks resetting the password to empty.

- [ ] `_filter_customer_rows` asymmetry (`api.py:278-281`): rows *missing*
  `customerid` are dropped when filtering by id, but rows missing `loginname` pass
  when filtering by login. The post-filter on `list_email_forwarders`/
  `list_email_senders` can silently drop all rows if those API items lack
  `customerid` — forwarders lost without a trace.

- [ ] CLI fallback returns `NULL` as the string `"NULL"` while the driver path
  returns `""` (`migration/core.py:543-544` vs `mysql_driver.py:25`) — e.g.
  `mysql_lastaccountnumber`, `loginname`, `allowed_mysqlserver` read as literal
  `"NULL"` downstream.

- [ ] Dead code: `_target_database_exists_physical` (`migration/domains.py:554`),
  `import_sql_dump` (`mysql_driver.py:124`, tests-only), `behavior.parallel` config
  never used.

- [ ] Non-interactive defaults inconsistent: mailboxes auto-select all candidates
  but databases/FTPs default to empty (`tui.py:876-877,894,944-945`).

## Low

- [ ] `_ensure_data_dumps` uses `return` instead of `continue` on 405
  (`migration/accounts.py:217`).
- [ ] `_is_custom_zone_record` drops *all* NS/SOA → delegated sub-zone NS records
  never migrate or verify (`migration/domains.py:654-657`,
  `verify_migration.py:496-509`).
- [ ] `load_local_sql_*` `RuntimeError` escapes `run_app`'s except clause in
  `_transfer_database_with_defaults` (`tui.py:1133` only catches
  MigrationError/FroxlorApiError/TransferError).
- [ ] `_default_mysql_server_from_allowed` dead branch (`migration/domains.py:517-519`)
  — both paths return `allowed[0]`.
- [ ] `_ssh_prefix` lacks `BatchMode`/`ConnectTimeout` → password prompt can hang
  automation; `transfer_mailbox` embeds the raw mailbox inside the quoted remote
  string (`transfer.py:281` — `'` in an address breaks quoting).
- [ ] Verify can't handle socket-only MySQL: `_target_connect_kwargs_via_ssh` strips
  `unix_socket` and tunnels TCP (`verify_migration.py:531-539`).
- [ ] `_relative_customer_path` loop over-strips when a nested directory equals the
  login (`migration/core.py:80-85`).
- [ ] `mailbox_exists="skip"` also excludes the mailbox from content transfer
  (`migration/accounts.py:348` vs `migration/executor.py:187-189`).
- [ ] `api.call` retries mutating requests once on network errors → possible
  double-add on lost responses (`api.py:56-71`).
- [ ] `bool("false") == True` if users quote TOML booleans (`config.py:137,159`).
