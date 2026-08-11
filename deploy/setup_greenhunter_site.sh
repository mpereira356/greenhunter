#!/usr/bin/env bash
set -euo pipefail

DOMAIN="greenhunter.com.br"
WWW_DOMAIN="www.greenhunter.com.br"
PROJECT_DIR="/home/viny/greenhunter"
NGINX_AVAILABLE="/etc/nginx/sites-available/${DOMAIN}"
NGINX_ENABLED="/etc/nginx/sites-enabled/${DOMAIN}"
BACKUP_DIR="/etc/nginx/greenhunter-backups/$(date +%Y%m%d_%H%M%S)"
SOURCE_CONF="${PROJECT_DIR}/deploy/nginx/${DOMAIN}.conf"
ENV_FILE="${PROJECT_DIR}/.env"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Execute como root: sudo bash ${PROJECT_DIR}/deploy/setup_greenhunter_site.sh"
  exit 1
fi

mkdir -p "${BACKUP_DIR}" /var/log/nginx

if [[ -f /etc/nginx/sites-available/default ]]; then
  cp /etc/nginx/sites-available/default "${BACKUP_DIR}/default.sites-available.bak"
fi

if [[ -L /etc/nginx/sites-enabled/default || -f /etc/nginx/sites-enabled/default ]]; then
  cp -a /etc/nginx/sites-enabled/default "${BACKUP_DIR}/default.sites-enabled.bak"
fi

if [[ -f "${NGINX_AVAILABLE}" ]]; then
  cp "${NGINX_AVAILABLE}" "${BACKUP_DIR}/${DOMAIN}.bak"
fi

if [[ -f "${ENV_FILE}" ]]; then
  cp "${ENV_FILE}" "${BACKUP_DIR}/greenhunter.env.bak"
fi

cp "${SOURCE_CONF}" "${NGINX_AVAILABLE}"
ln -sfn "${NGINX_AVAILABLE}" "${NGINX_ENABLED}"

nginx -t
systemctl enable nginx
systemctl reload nginx || systemctl restart nginx

apt-get update
apt-get install -y certbot python3-certbot-nginx

CERTBOT_DOMAINS=(-d "${DOMAIN}")
if getent hosts "${WWW_DOMAIN}" >/dev/null 2>&1; then
  CERTBOT_DOMAINS+=(-d "${WWW_DOMAIN}")
else
  echo "AVISO: ${WWW_DOMAIN} ainda nao resolve DNS. SSL sera emitido apenas para ${DOMAIN}."
  echo "Depois crie o DNS do www apontando para 191.252.193.10 e rode este script novamente."
fi

certbot --nginx \
  --non-interactive \
  --agree-tos \
  --register-unsafely-without-email \
  --redirect \
  --hsts \
  "${CERTBOT_DOMAINS[@]}"

set_env_value() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" "${ENV_FILE}"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "${ENV_FILE}"
  else
    printf '\n%s=%s\n' "${key}" "${value}" >> "${ENV_FILE}"
  fi
}

set_env_value "ALLOWED_HOSTS" "${DOMAIN},${WWW_DOMAIN},191.252.193.10,127.0.0.1,localhost"
set_env_value "SITE_URL" "https://${DOMAIN}"
set_env_value "PREFERRED_URL_SCHEME" "https"
set_env_value "SESSION_COOKIE_SECURE" "1"

nginx -t
systemctl reload nginx
systemctl restart greenhunter.service || {
  systemctl kill greenhunter.service || true
  sleep 3
  systemctl start greenhunter.service
}

echo "Deploy concluido."
echo "Backups em: ${BACKUP_DIR}"
