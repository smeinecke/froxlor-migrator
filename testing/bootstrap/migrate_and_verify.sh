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

migrate_mail_probe_real() {
	docker compose exec -T source-froxlor sh -lc \
		"doveadm backup -u '$MAILBOX_PROBE' ssh -i /tmp/id_ed25519 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p '${TARGET_SSH_PORT:-2222}' -l root host.docker.internal 'sudo doveadm dsync-server -u $MAILBOX_PROBE'"
}

MYSQL_SOURCE_HOST="127.0.0.1"
MYSQL_TARGET_HOST="127.0.0.1"
MYSQL_TARGET_PORT="${TARGET_DB_PORT:-33062}"
if [[ "${BOOTSTRAP_IN_DOCKER:-0}" == "1" ]]; then
	SOURCE_API_URL="${SOURCE_API_URL/127.0.0.1/host.docker.internal}"
	TARGET_API_URL="${TARGET_API_URL/127.0.0.1/host.docker.internal}"
	MYSQL_SOURCE_HOST="host.docker.internal"
	MYSQL_TARGET_HOST="host.docker.internal"
fi

TMP_CONFIG="$(mktemp)"
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
source_web_root = "/data/customers"
source_transfer_root = "$TESTING_DIR/data/source/customers"
target_web_root = "/data/customers"
target_owner_user = "www-data"
target_owner_group = "www-data"

[mysql]
source_dump_args = ["-h${MYSQL_SOURCE_HOST}", "-P${SOURCE_DB_PORT:-33061}", "-u${SOURCE_DB_ROOT_USER:-root}", "-p${SOURCE_DB_ROOT_PASSWORD:-source-root}"]
target_import_args = ["-h${MYSQL_TARGET_HOST}", "-P${MYSQL_TARGET_PORT}", "-u${TARGET_DB_ROOT_USER:-root}", "-p${TARGET_DB_ROOT_PASSWORD:-target-root}"]
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
parallel = 1

[output]
manifest_dir = "$REPO_DIR/manifests"
EOF

refresh_froxlor_runtime source-froxlor
refresh_froxlor_runtime target-froxlor

wait_api "${SOURCE_API_URL}"
wait_api "${TARGET_API_URL}"

seed_mail_probe

python3 "$SCRIPT_DIR/run_migration_apply.py" \
	--config "$TMP_CONFIG" \
	--customer custalpha \
	--customer custgamma

migrate_mail_probe_real

PYTHONPATH="$REPO_DIR" python3 -m froxlor_migrator.verify_migration --config "$TMP_CONFIG" --customer custalpha --customer custgamma

verify_mail_probe_target

echo "Migration + parity verification succeeded"
