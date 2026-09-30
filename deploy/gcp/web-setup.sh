#!/usr/bin/env bash
# Set up (or change the password of) the dashboard website. Run on your Mac:
#   bash deploy/gcp/web-setup.sh
# Needs WEB_HOST and WEB_USER in deploy/gcp/gcp.env. Asks for the password without echoing
# it (or generates one), sends it to the VM over SSH on stdin, where only its hash is kept.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
# shellcheck source=/dev/null
source deploy/gcp/gcp.env
: "${WEB_HOST:?set WEB_HOST in deploy/gcp/gcp.env (e.g. yourname.duckdns.org)}"
: "${WEB_USER:?set WEB_USER in deploy/gcp/gcp.env}"

read -r -s -p "Dashboard password (empty = generate one): " PW; echo
if [ -z "$PW" ]; then
  PW="$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-20)"
  echo "Generated password: $PW"
  echo "Save it in your phone's password manager now; it isn't stored anywhere readable."
else
  read -r -s -p "Repeat it: " PW2; echo
  [ "$PW" = "$PW2" ] || { echo "passwords differ" >&2; exit 1; }
fi

printf '%s' "$PW" | gcloud compute ssh "$VM" --zone "$ZONE" --project "$PROJECT" -- \
  "sudo bash /opt/hlflows/deploy/gcp/setup-web.sh '$WEB_HOST' '$WEB_USER'"
