#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${MYSQL_HOST:-}" && -n "${MYSQL_ROOT_PASSWORD:-}" ]]; then
	cat >/root/.my.cnf <<EOF
[client]
host=${MYSQL_HOST}
port=${MYSQL_PORT:-3306}
user=${MYSQL_ROOT_USER:-root}
password=${MYSQL_ROOT_PASSWORD}
EOF
	chmod 600 /root/.my.cnf

	mkdir -p /var/www
	cat >/var/www/.my.cnf <<EOF
[client]
host=${MYSQL_HOST}
port=${MYSQL_PORT:-3306}
user=${MYSQL_ROOT_USER:-root}
password=${MYSQL_ROOT_PASSWORD}
EOF
	chown www-data:www-data /var/www/.my.cnf
	chmod 600 /var/www/.my.cnf
fi

mkdir -p /var/run/sshd
ssh-keygen -A >/dev/null 2>&1 || true
if [[ -f /tmp/authorized_keys ]]; then
	mkdir -p /root/.ssh
	install -m 600 /tmp/authorized_keys /root/.ssh/authorized_keys
	chown -R root:root /root/.ssh
fi
cat >/etc/ssh/sshd_config.d/99-froxlor-test.conf <<EOF
PermitRootLogin prohibit-password
PasswordAuthentication no
PubkeyAuthentication yes
EOF
/usr/sbin/sshd

service cron start >/dev/null 2>&1 || true
service php8.2-fpm start >/dev/null 2>&1 || true
service php8.3-fpm start >/dev/null 2>&1 || true
service php8.4-fpm start >/dev/null 2>&1 || true
service dovecot start >/dev/null 2>&1 || true
service postfix start >/dev/null 2>&1 || postfix start >/dev/null 2>&1 || true

exec apachectl -D FOREGROUND
