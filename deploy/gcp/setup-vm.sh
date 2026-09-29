#!/usr/bin/env bash
# Provision or update hlflows on a Debian 12 VM. Run on the VM as root, from the unpacked
# code in /opt/hlflows (push.sh does this):  sudo bash /opt/hlflows/deploy/gcp/setup-vm.sh BUCKET
# Idempotent: safe to re-run on every deploy.
set -euo pipefail

BUCKET="${1:?usage: setup-vm.sh BUCKET}"
UV_VERSION=0.11.28
APP=/opt/hlflows                 # code (replaced on every deploy)
RUNTIME=/opt/hlflows-runtime     # Python + virtualenv (kept across deploys)
DATA=/var/lib/hlflows            # database, raw trades, archives
CONF=/etc/hlflows

echo "== swap (1 GB; the e2-micro has 1 GB of RAM)"
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 1G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "== packages"
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq sqlite3 curl ca-certificates >/dev/null

echo "== user and directories"
id hlflows >/dev/null 2>&1 || useradd --system --home-dir "$DATA" --shell /usr/sbin/nologin hlflows
install -d -o hlflows -g hlflows "$DATA" "$DATA/backup" "$DATA/.gcloud" "$RUNTIME" /var/cache/hlflows
install -d -o root -g root "$CONF"
chown -R hlflows:hlflows "$APP"

echo "== uv $UV_VERSION"
if ! command -v uv >/dev/null || [ "$(uv --version | awk '{print $2}')" != "$UV_VERSION" ]; then
  curl -LsSf "https://astral.sh/uv/$UV_VERSION/install.sh" \
    | env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
fi

echo "== Python environment"
sudo -u hlflows env \
  UV_PYTHON_INSTALL_DIR="$RUNTIME/python" \
  UV_PROJECT_ENVIRONMENT="$RUNTIME/venv" \
  UV_CACHE_DIR=/var/cache/hlflows/uv \
  uv sync --frozen --no-dev --no-editable --project "$APP"

echo "== config"
if [ ! -f "$CONF/hlflows.toml" ]; then
  sed -e "s#^db_path = .*#db_path = \"$DATA/hlflows.sqlite\"#" \
      -e "s#^data_dir = .*#data_dir = \"$DATA\"#" \
      "$APP/hlflows.example.toml" > "$CONF/hlflows.toml"
  echo "   wrote $CONF/hlflows.toml (edit it to add manual wallets etc.)"
fi
chown root:hlflows "$CONF/hlflows.toml"
chmod 640 "$CONF/hlflows.toml"   # may hold an API key (liquidation_provider)
echo "BUCKET=$BUCKET" > "$CONF/deploy.env"

echo "== hlflows command for interactive use (runs as the hlflows user)"
cat > /usr/local/bin/hlflows <<WRAPPER
#!/bin/sh
exec sudo -u hlflows env HLFLOWS_CONFIG=$CONF/hlflows.toml COLUMNS=\${COLUMNS:-160} $RUNTIME/venv/bin/hlflows "\$@"
WRAPPER
chmod 755 /usr/local/bin/hlflows

echo "== journald size cap"
install -d /etc/systemd/journald.conf.d
printf '[Journal]\nSystemMaxUse=200M\n' > /etc/systemd/journald.conf.d/hlflows.conf
systemctl restart systemd-journald

echo "== systemd units"
install -m 644 "$APP"/deploy/gcp/systemd/*.service "$APP"/deploy/gcp/systemd/*.timer /etc/systemd/system/
install -m 755 "$APP/deploy/gcp/sync.sh" "$APP/deploy/gcp/backup.sh" "$RUNTIME/"
systemctl daemon-reload
systemctl enable --now hlflows-sync.timer hlflows-backup.timer
systemctl enable hlflows-recorder.service
systemctl restart hlflows-recorder.service

echo "== done"
systemctl --no-pager --lines=5 status hlflows-recorder.service || true
