#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTING_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$TESTING_DIR/.." && pwd)"

if [[ ! -f "$TESTING_DIR/.env" ]]; then
	echo "Create $TESTING_DIR/.env first (copy from .env.example)."
	exit 1
fi

set -a
source "$TESTING_DIR/.env"
set +a

MAILBOX_PROBE="alerts@secure-demo.test"
PROBE_SUBJECT="MIGRATOR-PROBE-$(date +%s)"
EPS_MAILBOX_PROBE="one@eps-demo.test"
EPS_PROBE_SUBJECT="MIGRATOR-PROBE-EPS-$(date +%s)"
EPS_DOMAIN="eps-demo.test"
EPS_SUBDOMAIN="eps-sub.eps-demo.test"
EPS_DB="${EPS_DB_NAME:-custepsilon_main}"
EPS_FTP="custepsilonftp1"
EPS_SSL_IP="${TARGET_SECONDARY_IP:-10.66.77.2}"

wait_api() {
	local url="$1"
	for _ in $(seq 1 60); do
		local code
		code="$(curl -s -o /dev/null -w "%{http_code}" "$url" || true)"
		if [[ "$code" != "000" ]] && [[ "$code" -lt 500 ]]; then
			return 0
		fi
		sleep 2
	done
	echo "API endpoint not ready: $url" >&2
	exit 1
}

refresh_froxlor_runtime() {
	local service="$1"
	docker compose exec -T "$service" sh -lc "/var/www/html/bin/froxlor-cli froxlor:cron --force >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service php8.2-fpm restart >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service dovecot restart >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service postfix restart >/dev/null 2>&1 || postfix start >/dev/null 2>&1 || true"
}

seed_mail_probe() {
	local mailbox="${1:-$MAILBOX_PROBE}" subject="${2:-$PROBE_SUBJECT}"
	docker compose exec -T source-froxlor sh -lc "doveadm mailbox create -u '$mailbox' INBOX >/dev/null 2>&1 || true"
	docker compose exec -T source-froxlor sh -lc "doveadm expunge -u '$mailbox' mailbox INBOX ALL >/dev/null 2>&1 || true"
	printf 'From: migration-probe@example.test\nTo: %s\nSubject: %s\n\nmail body for migration probe\n' "$mailbox" "$subject" |
		docker compose exec -T source-froxlor sh -lc "doveadm save -u '$mailbox' -m INBOX"
	if ! docker compose exec -T source-froxlor sh -lc "test -n \"\$(doveadm search -u '$mailbox' mailbox INBOX HEADER Subject '$subject')\""; then
		echo "Failed to seed source mailbox probe message for $mailbox" >&2
		exit 1
	fi
}

verify_mail_probe() {
	# $1 = mailbox, $2 = subject - probe must exist on target.
	local mailbox="${1:-$MAILBOX_PROBE}" subject="${2:-$PROBE_SUBJECT}"
	if docker compose exec -T target-froxlor sh -lc "test -n \"\$(doveadm search -u '$mailbox' mailbox INBOX HEADER Subject '$subject')\""; then
		echo "Mail probe transferred to target: $mailbox / $subject"
	else
		echo "Mail probe message missing on target for $mailbox" >&2
		exit 1
	fi
}

verify_mail_probe_absent() {
	# $1 = mailbox, $2 = subject - probe must NOT exist on target (proves
	# --include-mail no skipped the doveadm content transfer).
	local mailbox="$1" subject="$2"
	if docker compose exec -T target-froxlor sh -lc "test -n \"\$(doveadm search -u '$mailbox' mailbox INBOX HEADER Subject '$subject' 2>/dev/null)\""; then
		echo "Mail probe unexpectedly present on target for $mailbox" >&2
		exit 1
	else
		echo "Mail content correctly absent on target: $mailbox"
	fi
}

assert_customers_absent() {
	# After a --dry-run apply the target must still have none of the seed
	# customers - guards against a write path that bypasses dry_run.
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests python3 /workspace/testing/bootstrap/assert_target_clean.py \
		--api-url '${TARGET_API_URL}' --api-key '${TARGET_API_KEY}' --api-secret '${TARGET_API_SECRET}' \
		--absent custalpha --absent custbeta --absent custgamma --absent custepsilon"
}

assert_target() {
	# Generic presence/absence checks on the target panel - the args are
	# assert_absent.py flags (--customer, --domain-absent, --mailbox-present, ...)
	local remote_args=""
	if (($#)); then printf -v remote_args '%q ' "$@"; fi
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests python3 /workspace/testing/bootstrap/assert_absent.py \
		--api-url '${TARGET_API_URL}' --api-key '${TARGET_API_KEY}' --api-secret '${TARGET_API_SECRET}' $remote_args"
}

assert_manifests() {
	# $* = --customer login (repeatable) - newest manifest per login must be
	# valid JSON with at least one event.
	local remote_args=""
	if (($#)); then printf -v remote_args '%q ' "$@"; fi
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project python3 /workspace/testing/bootstrap/assert_manifests.py \
		--dir /tmp/manifests $remote_args"
}

precreate_target_customer() {
	local login="${1:-custbeta}"
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace \
		TARGET_API_URL='${TARGET_API_URL}' TARGET_API_KEY='${TARGET_API_KEY}' TARGET_API_SECRET='${TARGET_API_SECRET}' \
		TARGET_DB_ROOT_USER='${TARGET_DB_ROOT_USER:-root}' TARGET_DB_ROOT_PASSWORD='${TARGET_DB_ROOT_PASSWORD:-target-root}' \
		TARGET_API_MYSQL_HOST='${TARGET_API_MYSQL_HOST:-target-db}' TARGET_API_MYSQL_PORT='${TARGET_API_MYSQL_PORT:-3306}' \
		uv run --no-project --with requests --with pymysql python3 /workspace/testing/bootstrap/precreate_target_customer.py '$login'"
}

verify_target_web_content() {
	# Byte-level content parity + ownership on the target data dir. Static dirs
	# that exist on source must exist on target; files must be identical.
	local src_root="$TESTING_DIR/data/source/customers"
	local dst_root="$TESTING_DIR/data/target/customers"
	local rel
	for rel in \
		"custalpha/wp-demo.test/index.php" \
		"custalpha/wp-demo.test/wp-config.php" \
		"custalpha/wp-demo.test/wp-includes/version.php" \
		"custalpha/static-demo.test/index.html" \
		"custbeta/mail-demo.test" \
		"custbeta/empty-demo.test" \
		"custgamma/secure-demo.test/index.html" \
		"custgamma/secure-demo.test/app/index.html" \
		"custgamma/secure-demo.test/protected/index.html" \
		"custgamma/redirect-demo.test/index.html" \
		"custgamma/forward-demo.test/index.html"; do
		if [ -f "$src_root/$rel" ]; then
			cmp -s "$src_root/$rel" "$dst_root/$rel" || {
				echo "target file content mismatch: $rel" >&2
				exit 1
			}
		elif [ -d "$src_root/$rel" ]; then
			[ -d "$dst_root/$rel" ] || {
				echo "target directory missing: $rel" >&2
				exit 1
			}
		else
			echo "source fixture missing: $rel" >&2
			exit 1
		fi
	done
	# Ownership must equal the customer's numeric guid from panel_customers -
	# the value Froxlor's CREATE_HOME cron will give the system user. The user
	# itself may not exist yet, so compare uid/gid numerically.
	local guid owner
	guid="$(docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -N -e \
		\"SELECT guid FROM panel_customers WHERE loginname='custalpha'\"")"
	owner="$(docker compose exec -T target-froxlor stat -c '%u:%g' /data/customers/custalpha/wp-demo.test/index.php)"
	[ -n "$guid" ] && [ "$owner" = "$guid:$guid" ] || {
		echo "target ownership not applied: $owner (expected $guid:$guid)" >&2
		exit 1
	}
	echo "Target web content parity OK"
}

verify_db_marker() {
	local db="${1:-custalpha_wpdemo}"
	local out
	out="$(docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' -N -e \
		\"SELECT note FROM ${db}.migrator_marker WHERE id=1\"")"
	[ "$out" = "seeded-for-parity" ] || {
		echo "db marker row missing on target: '$out' (${db})" >&2
		exit 1
	}
	echo "Database marker parity OK (${db})"
}

verify_db_absent() {
	# $1 = database name - the MariaDB schema itself must not exist on target.
	local db="$1" out
	out="$(docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' -N -e \
		\"SHOW DATABASES LIKE '${db}'\"")"
	[ -z "$out" ] || {
		echo "database unexpectedly present on target: $db" >&2
		exit 1
	}
	echo "Database correctly absent on target: $db"
}

drift_target_zone_record() {
	# Drift the seeded TXT record on the target so the second migration run has
	# to repair it via the near-match path (same record/type/prio/ttl key,
	# different content) - Froxlor's DomainZones.update is a stub, so the
	# migrator repairs via delete+re-add.
	docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -e \
		\"UPDATE domain_dns_entries e JOIN panel_domains d ON d.id=e.domain_id \
		 SET e.content='drifted-before-second-run' \
		 WHERE d.domain='secure-demo.test' AND e.record='migrator-test' AND e.type='TXT';\""
}

tamper_epsilon_db_ftp() {
	# Drop the already-migrated custepsilon DB + FTP rows on target so the
	# selective apply below can prove --include-databases no / --ftp-accounts
	# none do not recreate them. Froxlor names extra FTP accounts
	# <login>ftp<N> from ftp_lastaccountnumber - reset it so the full apply
	# recreates the deleted account under its source name (verify compares
	# usernames).
	docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -e \
		\"DELETE FROM panel_databases WHERE databasename='${EPS_DB}'; DROP DATABASE IF EXISTS ${EPS_DB};\""
	docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -e \
		\"DELETE FROM ftp_users WHERE username='${EPS_FTP}'; \
		UPDATE panel_customers SET ftp_lastaccountnumber=0 WHERE loginname='custepsilon';\""
}

tamper_epsilon_for_verify() {
	# Remove custepsilon's subdomain row + zone record on target: the
	# verify-side --skip-* flags must hide exactly these diffs.
	docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -e \
		\"DELETE d FROM panel_domains d JOIN panel_customers c ON c.customerid=d.customerid \
		 WHERE d.domain='${EPS_SUBDOMAIN}' AND c.loginname='custepsilon';\""
	docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -e \
		\"DELETE e FROM domain_dns_entries e JOIN panel_domains d ON d.id=e.domain_id \
		 WHERE d.domain='${EPS_DOMAIN}' AND e.record='migrator-test' AND e.type='TXT';\""
}

resolve_ip_ids() {
	# Numeric panel_ipsandports ids for the secondary IP fixtures on each side.
	SRC_IP2_ID="$(docker compose exec -T source-db sh -lc \
		"MYSQL_PWD='${SOURCE_DB_ROOT_PASSWORD:-source-root}' mariadb -u'${SOURCE_DB_ROOT_USER:-root}' '${SOURCE_DB_NAME:-froxlor}' -N -e \
		\"SELECT id FROM panel_ipsandports WHERE ip='${SOURCE_SECONDARY_IP:-10.66.77.1}' AND port=80\"" | tr -d '[:space:]')"
	DST_IP2_ID="$(docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -N -e \
		\"SELECT id FROM panel_ipsandports WHERE ip='${TARGET_SECONDARY_IP:-10.66.77.2}' AND port=80\"" | tr -d '[:space:]')"
	# ip+port=443 already pins the seeded SSL row ('ssl' is a MariaDB reserved
	# word and would need quoting that does not survive the nested shells).
	SRC_SSL_IP_ID="$(docker compose exec -T source-db sh -lc \
		"MYSQL_PWD='${SOURCE_DB_ROOT_PASSWORD:-source-root}' mariadb -u'${SOURCE_DB_ROOT_USER:-root}' '${SOURCE_DB_NAME:-froxlor}' -N -e \
		\"SELECT id FROM panel_ipsandports WHERE ip='${SOURCE_SECONDARY_IP:-10.66.77.1}' AND port=443\"" | tr -d '[:space:]')"
	DST_SSL_IP_ID="$(docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -N -e \
		\"SELECT id FROM panel_ipsandports WHERE ip='${TARGET_SECONDARY_IP:-10.66.77.2}' AND port=443\"" | tr -d '[:space:]')"
	[ -n "$SRC_IP2_ID" ] && [ -n "$DST_IP2_ID" ] && [ -n "$SRC_SSL_IP_ID" ] && [ -n "$DST_SSL_IP_ID" ] || {
		echo "secondary IP fixture missing (src='$SRC_IP2_ID' dst='$DST_IP2_ID' srcssl='$SRC_SSL_IP_ID' dstssl='$DST_SSL_IP_ID')" >&2
		exit 1
	}
	# Named ip:port:ssl token form - exercises alias resolution, not just
	# numeric ids. The SSL map covers the ssl_ipandport binding on eps-demo.test.
	IP_MAP_ARG="${SOURCE_SECONDARY_IP:-10.66.77.1}:80:0=>${TARGET_SECONDARY_IP:-10.66.77.2}:80:0"
	SSL_IP_MAP_ARG="${SOURCE_SECONDARY_IP:-10.66.77.1}:443:1=>${TARGET_SECONDARY_IP:-10.66.77.2}:443:1"
	IP_MAP_ALL_ARG="${IP_MAP_ARG},${SSL_IP_MAP_ARG}"
	IP_VALUE_MAP_ARG="${SOURCE_SECONDARY_IP:-10.66.77.1}=>${TARGET_SECONDARY_IP:-10.66.77.2}"
}

CONTAINER_CONFIG="/workspace/testing/.tmp/bootstrap-migration-config.toml"
CONTAINER_FAIL_CONFIG="/workspace/testing/.tmp/bootstrap-migration-config-fail.toml"
CONTAINER_FAIL_DOMAIN_CONFIG="/workspace/testing/.tmp/bootstrap-migration-config-fail-domain.toml"
APPLY_LOG="$TESTING_DIR/.tmp/last-apply.log"

run_cli() {
	# $1 = config path inside the container; rest = run_migration_apply.py args.
	# COLUMNS widened so Rich tables/replay commands never wrap mid-token.
	# %q quoting keeps '=>' mapping tokens literal for the remote shell.
	local cfg="$1" remote_args=""
	shift
	if (($#)); then printf -v remote_args '%q ' "$@"; fi
	docker compose exec -T -e COLUMNS=500 source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests --with pymysql --with paramiko --with rich /workspace/testing/bootstrap/run_migration_apply.py \
		--config '$cfg' $remote_args"
}

run_apply() {
	# Single batch invocation for the three main customers - exercises the real
	# batch path (shared plan table, per-customer manifests, tolerant ip-map,
	# continue-on-failure). $* forwards --dry-run/--expect-fail/--extra-arg=...
	run_cli "$CONTAINER_CONFIG" \
		--batch --include-mail \
		--ip-map "$IP_MAP_ARG" \
		--customer custalpha --customer custbeta --customer custgamma "$@"
}

run_apply_all() {
	# --all-customers batch: every source customer in one invocation.
	run_cli "$CONTAINER_CONFIG" \
		--all-customers --include-mail \
		--ip-map "$IP_MAP_ALL_ARG" "$@"
}

run_apply_eps() {
	run_cli "$CONTAINER_CONFIG" --customer custepsilon "$@"
}

capture_apply() {
	# Run a migrator command, stream output live AND capture it to APPLY_LOG
	# for table/replay assertions.
	"$@" 2>&1 | tee "$APPLY_LOG"
}

assert_batch_table() {
	# $1 = log file, rest = "login:status" specs that must each appear in the
	# batch result table.
	local log="$1"
	shift
	grep -q "Batch result" "$log" || {
		echo "batch result table missing in $log" >&2
		exit 1
	}
	local spec login status
	for spec in "$@"; do
		login="${spec%%:*}"
		status="${spec##*:}"
		grep -qE "\b${login}\b.*\b${status}\b" "$log" || {
			echo "batch table missing row ${login}:${status}" >&2
			exit 1
		}
	done
}

run_verify() {
	local remote_args=""
	if (($#)); then printf -v remote_args '%q ' "$@"; fi
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests --with pymysql --with paramiko --with rich python3 -m froxlor_migrator.verify_migration \
		--config '$CONTAINER_CONFIG' \
		--ip-value-map '$IP_VALUE_MAP_ARG' \
		--customer custalpha \
		--customer custbeta \
		--customer custgamma $remote_args"
}

run_verify_eps() {
	# $* = extra verify flags (e.g. --skip-subdomains --skip-domain-zones)
	local remote_args=""
	if (($#)); then printf -v remote_args '%q ' "$@"; fi
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests --with pymysql --with paramiko --with rich python3 -m froxlor_migrator.verify_migration \
		--config '$CONTAINER_CONFIG' \
		--ip-value-map '$IP_VALUE_MAP_ARG' \
		--customer custepsilon $remote_args"
}

rename_customer_apply() {
	# Domain-only apply into a pre-created, differently-named target customer -
	# exercises docroot/login remap (custalpha resources land under custdelta).
	run_cli "$CONTAINER_CONFIG" \
		--customer custalpha \
		--domain-only --target-customer custdelta \
		--ip-map "$IP_MAP_ARG" \
		--extra-arg=--domains --extra-arg='wp-demo.test,static-demo.test' \
		--extra-arg=--subdomains --extra-arg=none \
		--extra-arg=--databases --extra-arg=none \
		--extra-arg=--mailboxes --extra-arg=none \
		--extra-arg=--ftp-accounts --extra-arg=none
}

assert_rename() {
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests python3 /workspace/testing/bootstrap/assert_rename.py \
		--api-url '${TARGET_API_URL}' --api-key '${TARGET_API_KEY}' --api-secret '${TARGET_API_SECRET}' \
		--login custdelta --domain wp-demo.test --domain static-demo.test \
		--data-root /workspace/testing/data/target/customers"
}

tamper_target_zone_record() {
	# Delete the migrated TXT record on the target - the negative verify
	# below must detect this and exit non-zero.
	docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -e \
		\"DELETE e FROM domain_dns_entries e JOIN panel_domains d ON d.id=e.domain_id \
		 WHERE d.domain='secure-demo.test' AND e.record='migrator-test' AND e.type='TXT';\""
}

if [[ "${BOOTSTRAP_IN_DOCKER:-0}" == "1" ]]; then
	SOURCE_API_URL="${SOURCE_API_URL/127.0.0.1/host.docker.internal}"
	TARGET_API_URL="${TARGET_API_URL/127.0.0.1/host.docker.internal}"
fi

mkdir -p "$TESTING_DIR/.tmp"
TMP_CONFIG="$TESTING_DIR/.tmp/bootstrap-migration-config.toml"
FAIL_CONFIG="$TESTING_DIR/.tmp/bootstrap-migration-config-fail.toml"
FAIL_DOMAIN_CONFIG="$TESTING_DIR/.tmp/bootstrap-migration-config-fail-domain.toml"
cleanup() {
	rm -f "$TMP_CONFIG" "$FAIL_CONFIG" "$FAIL_DOMAIN_CONFIG" "$APPLY_LOG"
}
trap cleanup EXIT

cat >"$TMP_CONFIG" <<EOF
[source]
api_url = "${SOURCE_API_URL}"
api_key = "${SOURCE_API_KEY}"
api_secret = "${SOURCE_API_SECRET}"

[target]
api_url = "${TARGET_API_URL}"
api_key = "${TARGET_API_KEY}"
api_secret = "${TARGET_API_SECRET}"

[ssh]
host = "host.docker.internal"
user = "root"
port = ${TARGET_SSH_PORT:-2222}
strict_host_key_checking = false

[paths]
# Seeded documentroots live under /data/customers, so the panel-visible root
# and the transfer root coincide in the testbed.
source_web_root = "/data/customers"
source_transfer_root = "/data/customers"
target_web_root = "/data/customers"

[mysql]
source_panel_database = "${SOURCE_DB_NAME:-froxlor}"
target_panel_database = "${TARGET_DB_NAME:-froxlor}"

[commands]
ssh = "ssh -i $TESTING_DIR/ssh/id_ed25519 -o IdentitiesOnly=yes"
sudo = "sudo"
tar = "tar"
mysqldump = "mysqldump"
mysql = "mysql"
doveadm = "doveadm"

[behavior]
# Default dry-run so omitting --apply is safe; real runs pass --apply.
dry_run_default = true
domain_exists = "update"
database_exists = "skip"
mailbox_exists = "update"

[output]
manifest_dir = "/tmp/manifests"
EOF

# Second/third config variants for the behavior-failure paths: mailbox_exists=
# fail makes custbeta/custgamma fail on their existing mailboxes while
# custalpha (no mailboxes) still succeeds; domain_exists=fail fails everyone.
sed 's/mailbox_exists = "update"/mailbox_exists = "fail"/' "$TMP_CONFIG" >"$FAIL_CONFIG"
sed 's/domain_exists = "update"/domain_exists = "fail"/' "$TMP_CONFIG" >"$FAIL_DOMAIN_CONFIG"

refresh_froxlor_runtime source-froxlor
refresh_froxlor_runtime target-froxlor

wait_api "${SOURCE_API_URL}"
wait_api "${TARGET_API_URL}"

resolve_ip_ids
seed_mail_probe

# 1) Batch dry-run over --all-customers: nothing may be written, and the plan
#    table must list every seeded customer - proves the all-customers selector
#    and the batch planning path produce the full plan without side effects.
capture_apply run_apply_all --dry-run
grep -q "Batch migration plan" "$APPLY_LOG" || {
	echo "batch plan table missing in dry-run output" >&2
	exit 1
}
for login in custalpha custbeta custgamma custepsilon; do
	grep -q "$login" "$APPLY_LOG" || {
		echo "batch dry-run plan missing $login" >&2
		exit 1
	}
done
assert_customers_absent

# 2) Pre-create custbeta on the target so the real apply exercises the
#    existing-customer update path instead of the create path.
precreate_target_customer

# 3) Real batch apply: ONE main.py invocation for all three customers.
capture_apply run_apply
assert_batch_table "$APPLY_LOG" custalpha:ok custbeta:ok custgamma:ok
assert_manifests --customer custalpha --customer custbeta --customer custgamma

# Stash the printed batch replay command - it is executed verbatim later to
# prove the printed command reproduces the run.
REPLAY_CMD="$(grep -A1 "Replay command" "$APPLY_LOG" | tail -n1 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
[ -n "$REPLAY_CMD" ] || {
	echo "no replay command found in batch apply output" >&2
	exit 1
}
REPLAY_CMD="${REPLAY_CMD/#froxlor-migrator/python3 \/workspace\/main.py}"

# Second run exercises the update/dedup paths for every resource type
# (domain_exists=update, mailbox_exists=update); the drifted zone record
# additionally forces the DomainZones delete+re-add repair path.
drift_target_zone_record
run_apply

run_verify

verify_mail_probe

# Negative check: delete a migrated zone record on the target and require
# verify_migration to fail - then restore via a final apply+verify so the
# testbed ends consistent.
tamper_target_zone_record
if run_verify; then
	echo "verify_migration did not detect the deleted zone record" >&2
	exit 1
else
	echo "Negative verify passed: drift detected as expected"
fi
run_apply
run_verify
verify_target_web_content
verify_db_marker

# 4) Batch failure isolation. mailbox_exists=fail fails customers that already
#    have mailboxes on target: custbeta+custgamma fail while custalpha (no
#    mailboxes) still completes - the batch must continue after a per-customer
#    failure and exit non-zero.
capture_apply run_cli "$CONTAINER_FAIL_CONFIG" \
	--batch --include-mail --ip-map "$IP_MAP_ARG" \
	--customer custalpha --customer custbeta \
	--expect-fail
assert_batch_table "$APPLY_LOG" custalpha:ok custbeta:failed

# All-fail variant: both mailbox-owning customers fail; batch still completes
# the table and exits non-zero.
capture_apply run_cli "$CONTAINER_FAIL_CONFIG" \
	--batch --include-mail \
	--customer custbeta --customer custgamma \
	--expect-fail
assert_batch_table "$APPLY_LOG" custbeta:failed custgamma:failed

# domain_exists=fail: every customer's first domain exists -> all rows fail.
capture_apply run_cli "$CONTAINER_FAIL_DOMAIN_CONFIG" \
	--batch \
	--customer custalpha --customer custbeta --customer custgamma \
	--expect-fail
assert_batch_table "$APPLY_LOG" custalpha:failed custbeta:failed custgamma:failed

# 5) custepsilon: resources-only apply (--domains none) migrates the customer
#    record + databases + FTP + misc rows without touching any domain or
#    mailbox. Mail-linked objects (forwarders, sender aliases) depend on
#    mailboxes, so they must be skipped explicitly - that also exercises the
#    corresponding --skip-* flags.
run_apply_eps \
	--extra-arg=--domains --extra-arg=none \
	--extra-arg=--subdomains --extra-arg=none \
	--extra-arg=--mailboxes --extra-arg=none \
	--extra-arg=--skip-forwarders \
	--extra-arg=--skip-sender-aliases
assert_target --customer custepsilon \
	--domain-absent "$EPS_DOMAIN" \
	--db-present "$EPS_DB" \
	--ftp-present "$EPS_FTP" \
	--mailbox-absent "$EPS_MAILBOX_PROBE"

# 6) Tamper the migrated custepsilon DB + FTP on target, then run a selective
#    apply: only eps-demo.test + one mailbox migrate; every other selector is
#    constrained and every --skip-* flag set. The tampered resources must stay
#    absent, proving the flags/selectors are honored end to end.
tamper_epsilon_db_ftp
seed_mail_probe "$EPS_MAILBOX_PROBE" "$EPS_PROBE_SUBJECT"
run_apply_eps \
	--extra-arg=--domains --extra-arg="$EPS_DOMAIN" \
	--extra-arg=--mailboxes --extra-arg="$EPS_MAILBOX_PROBE" \
	--extra-arg=--ftp-accounts --extra-arg=none \
	--extra-arg=--include-files --extra-arg=no \
	--extra-arg=--include-databases --extra-arg=no \
	--extra-arg=--skip-subdomains \
	--extra-arg=--skip-certificates \
	--extra-arg=--skip-dns-zones \
	--extra-arg=--skip-forwarders \
	--extra-arg=--skip-sender-aliases \
	--extra-arg=--skip-password-sync \
	--extra-arg=--skip-database-name-validation \
	--php-map 'php8.3=>php8.4' \
	--ip-map "$SSL_IP_MAP_ARG"
assert_target --customer custepsilon \
	--domain-present "$EPS_DOMAIN" \
	--subdomain-absent "$EPS_SUBDOMAIN" \
	--mailbox-present "$EPS_MAILBOX_PROBE" \
	--mailbox-absent two@eps-demo.test \
	--db-absent "$EPS_DB" \
	--ftp-absent "$EPS_FTP" \
	--zone-absent "$EPS_DOMAIN":migrator-test:TXT \
	--cert-absent "$EPS_DOMAIN" \
	--forwarder-absent "$EPS_MAILBOX_PROBE" \
	--alias-absent "two@eps-demo.test:$EPS_MAILBOX_PROBE" \
	--php-map-expect "$EPS_DOMAIN":php8.4 \
	--ssl-ip-expect "$EPS_DOMAIN":"$EPS_SSL_IP"
verify_db_absent "$EPS_DB"
verify_mail_probe_absent "$EPS_MAILBOX_PROBE" "$EPS_PROBE_SUBJECT"
if docker compose exec -T target-froxlor test -f "/data/customers/custepsilon/${EPS_DOMAIN}/migrator-marker.txt"; then
	echo "marker file transferred despite --include-files no" >&2
	exit 1
else
	echo "File transfer correctly skipped (--include-files no)"
fi

# 7) Full apply for custepsilon: every selector open, mail content on, SSL
#    ip-map preserved, and no --php-map so the auto-map restores php8.3 -
#    proving the update path rewrites phpsettingid on an existing domain.
#    The selective apply already created the probe mailbox object, so dovecot
#    materialized a Maildir with a different GUID. dsync resolves such
#    conflicts by trash-deleting INBOX, but under the maildir: layout that
#    rename targets a child of itself (EINVAL) - drop the stale Maildir so
#    the content sync starts clean.
docker compose exec -T target-froxlor sh -lc \
	"rm -rf '/var/customers/mail/custepsilon/${EPS_DOMAIN}/${EPS_MAILBOX_PROBE%%@*}'"
run_apply_eps --include-mail --ip-map "$SSL_IP_MAP_ARG"
assert_target --customer custepsilon \
	--domain-present "$EPS_DOMAIN" \
	--subdomain-present "$EPS_SUBDOMAIN" \
	--mailbox-present "$EPS_MAILBOX_PROBE" \
	--mailbox-present two@eps-demo.test \
	--db-present "$EPS_DB" \
	--ftp-present "$EPS_FTP" \
	--zone-present "$EPS_DOMAIN":migrator-test:TXT \
	--cert-present "$EPS_DOMAIN" \
	--forwarder-present "$EPS_MAILBOX_PROBE":two@eps-demo.test \
	--alias-present "two@eps-demo.test:$EPS_MAILBOX_PROBE" \
	--php-map-expect "$EPS_DOMAIN":php8.3 \
	--ssl-ip-expect "$EPS_DOMAIN":"$EPS_SSL_IP"
verify_db_marker "$EPS_DB"
verify_mail_probe "$EPS_MAILBOX_PROBE" "$EPS_PROBE_SUBJECT"
# Byte-level file parity for custepsilon too (marker + index.html).
for rel in "custepsilon/${EPS_DOMAIN}/index.html" "custepsilon/${EPS_DOMAIN}/migrator-marker.txt"; do
	cmp -s "$TESTING_DIR/data/source/customers/$rel" "$TESTING_DIR/data/target/customers/$rel" || {
		echo "target file content mismatch: $rel" >&2
		exit 1
	}
done

# 8) --all-customers real apply: all four customers re-run idempotently in one
#    invocation; both ip-map tokens are used (tolerant per-customer matching).
capture_apply run_apply_all
assert_batch_table "$APPLY_LOG" custalpha:ok custbeta:ok custgamma:ok custepsilon:ok
run_verify
run_verify_eps

# 9) Verify-side --skip-* flags: tamper custepsilon's subdomain + zone record.
#    Verify with skips must pass, without must fail, then a repair apply
#    restores parity.
tamper_epsilon_for_verify
run_verify_eps --skip-subdomains --skip-domain-zones
if run_verify_eps; then
	echo "verify_migration did not detect the deleted subdomain/zone record" >&2
	exit 1
else
	echo "Verify-side skip flags honored; unsuppressed verify failed as expected"
fi
run_apply_eps --include-mail --ip-map "$SSL_IP_MAP_ARG"
run_verify_eps

# 10) CLI error paths - every case must exit non-zero (--expect-fail inverts).
run_cli "$CONTAINER_CONFIG" --expect-fail --customer nosuchuser
run_cli "$CONTAINER_CONFIG" --expect-fail --customer custalpha \
	--extra-arg=--domains --extra-arg=nosuchdom.test
run_cli "$CONTAINER_CONFIG" --expect-fail --batch \
	--customer custalpha --customer custbeta \
	--extra-arg=--domains --extra-arg="$EPS_DOMAIN"
run_cli "$CONTAINER_CONFIG" --expect-fail --customer custalpha \
	--domain-only --target-customer nosuchtarget

# 11) Replay command: execute the batch replay line printed in step 3 - it must
#     reproduce the migration idempotently and exit 0. The line is already
#     shell-quoted for display ('a=>b' tokens), so eval re-parses it exactly as
#     a user paste would - %q-quoting instead would keep the quotes literal.
docker compose exec -T source-froxlor sh -lc \
	"cd /workspace && eval \"PYTHONPATH=/workspace uv run --no-project --with requests --with pymysql --with paramiko --with rich ${REPLAY_CMD}\"" || {
	echo "replay command failed: $REPLAY_CMD" >&2
	exit 1
}
echo "Replay command executed successfully"

# Rename path: domain-only apply of custalpha's domains into a pre-created,
# differently-named target customer - exercises docroot/login remap. Runs last
# because it moves wp-demo.test/static-demo.test away from custalpha.
precreate_target_customer custdelta
rename_customer_apply
assert_rename

echo "Migration + parity verification succeeded"
