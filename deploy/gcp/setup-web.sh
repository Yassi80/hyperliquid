#!/usr/bin/env bash
# Serve the dashboard over HTTPS with a password, using Caddy. Run on the VM as root;
# web-setup.sh (on your Mac) does it for you:
#   printf '%s' "$PASSWORD" | sudo bash /opt/hlflows/deploy/gcp/setup-web.sh HOST USER
# HOST must resolve to this VM's IP (e.g. yourname.duckdns.org). Caddy gets a Let's Encrypt
# certificate for it automatically (needs ports 80 and 443 open). The password is read
# from stdin and only its bcrypt hash is stored. Idempotent: re-run to change it.
set -euo pipefail
HOST="${1:?usage: setup-web.sh HOST USER (password on stdin)}"
WEB_USER="${2:?usage: setup-web.sh HOST USER (password on stdin)}"
PASSWORD="$(cat)"
if [ "${#PASSWORD}" -lt 12 ]; then
  echo "refusing: use a password of at least 12 characters" >&2
  exit 1
fi

echo "== Caddy (official repository)"
if ! command -v caddy >/dev/null; then
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq debian-keyring debian-archive-keyring \
    apt-transport-https curl gpg >/dev/null
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
    | gpg --batch --yes --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
    > /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq caddy >/dev/null
fi
caddy version

# caddy hash-password reads one line from a pipe and strips its newline, so send one.
HASH="$(printf '%s\n' "$PASSWORD" | caddy hash-password)"
case "$HASH" in
  '$2'*) ;;
  *) echo "could not hash the password (got: ${HASH:0:20}...)" >&2; exit 1 ;;
esac

echo "== /etc/caddy/Caddyfile for https://$HOST"
install -d -m 755 -o hlflows -g hlflows /var/lib/hlflows-www
cat > /etc/caddy/Caddyfile <<CADDY
# Managed by hlflows deploy/gcp/setup-web.sh; re-run it to change host or password.
$HOST {
	basic_auth {
		$WEB_USER $HASH
	}
	root * /var/lib/hlflows-www
	file_server
	encode zstd gzip
	header {
		Cache-Control "no-cache"
		X-Robots-Tag "noindex, nofollow"
		Referrer-Policy "no-referrer"
		X-Content-Type-Options "nosniff"
		Strict-Transport-Security "max-age=31536000"
		-Server
	}
	# Access log to stderr, i.e. the journal (journalctl -u caddy), which is size-capped.
	log
}
CADDY
chmod 640 /etc/caddy/Caddyfile
chown root:caddy /etc/caddy/Caddyfile
# Validate as the user Caddy runs as: validating as root would create root-owned files
# (logs, storage) that the service then can't open.
sudo -u caddy caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
chown -R caddy:caddy /var/log/caddy 2>/dev/null || true   # repair files from older runs
systemctl enable caddy >/dev/null
systemctl restart caddy

echo "== done: https://$HOST (user: $WEB_USER)"
echo "   The first request can take ~30 s while Caddy obtains the certificate."
