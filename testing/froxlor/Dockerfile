FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN printf 'postfix postfix/main_mailer_type select No configuration\npostfix postfix/mailname string localhost\n' | debconf-set-selections

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && curl -fsSL https://packages.sury.org/php/apt.gpg | gpg --dearmor -o /usr/share/keyrings/sury-php.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/sury-php.gpg] https://packages.sury.org/php/ bookworm main" > /etc/apt/sources.list.d/sury-php.list

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        apache2 \
        ca-certificates \
        cron \
        curl \
        dovecot-imapd \
        dovecot-managesieved \
        dovecot-mysql \
        dovecot-pop3d \
        dovecot-sieve \
        libnss-extrausers \
        mariadb-client \
        openssh-server \
        postfix \
        postfix-mysql \
        php \
        php-bcmath \
        php-cli \
        php-curl \
        php-fpm \
        php-gd \
        php-gmp \
        php-gnupg \
        php-intl \
        php-mbstring \
        php-mysql \
        php-sqlite3 \
        php-xml \
        php-zip \
        php8.3-cli \
        php8.3-fpm \
        php8.4-cli \
        php8.4-fpm \
        sudo \
    && rm -rf /var/lib/apt/lists/*

RUN rm -rf /var/www/html/* \
    && curl -fsSL https://files.froxlor.org/releases/froxlor-latest.tar.gz -o /tmp/froxlor.tar.gz \
    && mkdir -p /tmp/froxlor \
    && tar -xzf /tmp/froxlor.tar.gz -C /tmp/froxlor \
    && if [ -d /tmp/froxlor/froxlor ]; then cp -a /tmp/froxlor/froxlor/. /var/www/html/; else cp -a /tmp/froxlor/. /var/www/html/; fi \
    && rm -rf /tmp/froxlor /tmp/froxlor.tar.gz

RUN a2enmod rewrite \
    && sed -ri 's!/var/www/html!/var/www/html!g' /etc/apache2/sites-available/000-default.conf \
    && sed -ri 's!AllowOverride None!AllowOverride All!g' /etc/apache2/apache2.conf \
    && groupadd -r froxlorlocal || true \
    && useradd -r -g froxlorlocal -s /usr/sbin/nologin -d /nonexistent froxlorlocal || true \
    && mkdir -p /data/customers /var/www/html/lib /var/www/html/cache \
    && chown -R www-data:www-data /var/www/html /data/customers

COPY start.sh /usr/local/bin/start.sh
RUN chmod +x /usr/local/bin/start.sh

EXPOSE 80 22

CMD ["/usr/local/bin/start.sh"]
