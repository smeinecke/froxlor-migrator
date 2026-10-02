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
	docker compose exec -T source-froxlor sh -lc "doveadm mailbox create -u '$MAILBOX_PROBE' INBOX >/dev/null 2>&1 || true"
	docker compose exec -T source-froxlor sh -lc "doveadm expunge -u '$MAILBOX_PROBE' mailbox INBOX ALL >/dev/null 2>&1 || true"
	printf 'From: migration-probe@example.test\nTo: %s\nSubject: %s\n\nmail body for migration probe\n' "$MAILBOX_PROBE" "$PROBE_SUBJECT" |
		docker compose exec -T source-froxlor sh -lc "doveadm save -u '$MAILBOX_PROBE' -m INBOX"
	if ! docker compose exec -T source-froxlor sh -lc "test -n \"\$(doveadm search -u '$MAILBOX_PROBE' mailbox INBOX HEADER Subject '$PROBE_SUBJECT')\""; then
		echo "Failed to seed source mailbox probe message" >&2
		exit 1
	fi
}

verify_mail_probe_target() {
	if docker compose exec -T target-froxlor sh -lc "test -n \"\$(doveadm search -u '$MAILBOX_PROBE' mailbox INBOX HEADER Subject '$PROBE_SUBJECT')\""; then
		echo "Mail probe transferred to target: $MAILBOX_PROBE / $PROBE_SUBJECT"
	else
		echo "Mail probe message missing on target" >&2
		exit 1
	fi
}

assert_customers_absent() {
	# After a --dry-run apply the target must still have none of the seed
	# customers — guards against a write path that bypasses dry_run.
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests python3 /workspace/testing/bootstrap/assert_target_clean.py \
		--api-url '${TARGET_API_URL}' --api-key '${TARGET_API_KEY}' --api-secret '${TARGET_API_SECRET}' \
		--absent custalpha --absent custbeta --absent custgamma"
}

precreate_target_customer() {
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace \
		TARGET_API_URL='${TARGET_API_URL}' TARGET_API_KEY='${TARGET_API_KEY}' TARGET_API_SECRET='${TARGET_API_SECRET}' \
		TARGET_DB_ROOT_USER='${TARGET_DB_ROOT_USER:-root}' TARGET_DB_ROOT_PASSWORD='${TARGET_DB_ROOT_PASSWORD:-target-root}' \
		TARGET_API_MYSQL_HOST='${TARGET_API_MYSQL_HOST:-target-db}' TARGET_API_MYSQL_PORT='${TARGET_API_MYSQL_PORT:-3306}' \
		uv run --no-project --with requests --with pymysql python3 /workspace/testing/bootstrap/precreate_target_customer.py custbeta"
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
	# Ownership must equal the customer's numeric guid from panel_customers —
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
	local out
	out="$(docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' -N -e \
		\"SELECT note FROM custalpha_wpdemo.migrator_marker WHERE id=1\"")"
	[ "$out" = "seeded-for-parity" ] || {
		echo "db marker row missing on target: '$out'" >&2
		exit 1
	}
	echo "Database marker parity OK"
}

drift_target_zone_record() {
	# Drift the seeded TXT record on the target so the second migration run has
	# to repair it via the near-match path (same record/type/prio/ttl key,
	# different content) — Froxlor's DomainZones.update is a stub, so the
	# migrator repairs via delete+re-add.
	docker compose exec -T target-db sh -lc \
		"MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -e \
		\"UPDATE domain_dns_entries e JOIN panel_domains d ON d.id=e.domain_id \
		 SET e.content='drifted-before-second-run' \
		 WHERE d.domain='secure-demo.test' AND e.record='migrator-test' AND e.type='TXT';\""
}

run_apply() {
	# Extra args are passed through to run_migration_apply.py (e.g. --dry-run).
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests --with pymysql --with paramiko /workspace/testing/bootstrap/run_migration_apply.py \
		--config /workspace/testing/.tmp/bootstrap-migration-config.toml \
		--include-mail \
		--customer custalpha \
		--customer custbeta \
		--customer custgamma $*"
}

run_verify() {
	docker compose exec -T source-froxlor sh -lc \
		"PYTHONPATH=/workspace uv run --no-project --with requests --with pymysql --with paramiko python3 -m froxlor_migrator.verify_migration \
		--config /workspace/testing/.tmp/bootstrap-migration-config.toml \
		--customer custalpha \
		--customer custbeta \
		--customer custgamma"
}

tamper_target_zone_record() {
	# Delete the migrated TXT record on the target — the negative verify
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
cleanup() {
	rm -f "$TMP_CONFIG"
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
source_web_root = "/var/customers/webs"
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
dry_run_default = false
domain_exists = "update"
database_exists = "skip"
mailbox_exists = "update"

[output]
manifest_dir = "/tmp/manifests"
EOF

refresh_froxlor_runtime source-froxlor
refresh_froxlor_runtime target-froxlor

wait_api "${SOURCE_API_URL}"
wait_api "${TARGET_API_URL}"

seed_mail_probe

# 1) Dry-run must not write anything to the target.
run_apply --dry-run
assert_customers_absent

# 2) Pre-create custbeta on the target so the real apply exercises the
#    existing-customer update path instead of the create path.
precreate_target_customer

run_apply

# Second run exercises the update/dedup paths for every resource type
# (domain_exists=update, mailbox_exists=update); the drifted zone record
# additionally forces the DomainZones delete+re-add repair path.
drift_target_zone_record
run_apply

run_verify

verify_mail_probe_target

# Negative check: delete a migrated zone record on the target and require
# verify_migration to fail — then restore via a final apply+verify so the
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

echo "Migration + parity verification succeeded"
