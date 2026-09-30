#!/usr/bin/env bash
# Pull a fresh copy of the recorded data from the VM to your Mac and build the HTML
# dashboard from it. Run from anywhere in the repo:
#
#   bash deploy/gcp/pull-and-dashboard.sh                    # data in ~/hlflows-data
#   bash deploy/gcp/pull-and-dashboard.sh ~/research/hl      # another folder
#   bash deploy/gcp/pull-and-dashboard.sh ~/hlflows-data --bars 400 --tf 1h,4h,1d
#
# Steps: take a fresh database backup on the VM (so the copy is current, not last night's),
# sync raw trades, archived positions and that backup from the bucket, write the
# dashboard to DEST/dashboard.html and open it. Later runs only download what changed,
# plus the database. Arguments after DEST go to `hlflows dashboard`.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
# shellcheck source=/dev/null
source deploy/gcp/gcp.env

DEST="$HOME/hlflows-data"
if [ $# -gt 0 ] && [ "${1#-}" = "$1" ]; then
  DEST="$1"
  shift
fi
mkdir -p "$DEST"
DEST="$(cd "$DEST" && pwd)"

echo "== fresh database backup on the VM"
gcloud compute ssh "$VM" --zone "$ZONE" --project "$PROJECT" -- \
  'sudo systemctl start hlflows-backup.service && journalctl -u hlflows-backup -n 1 --no-pager -o cat'

echo "== pulling data to $DEST"
bash deploy/gcp/pull-data.sh "$DEST"

echo "== building the dashboard"
HLFLOWS_CONFIG="$DEST/hlflows.toml" uv run hlflows dashboard --out "$DEST/dashboard.html" "$@"
open "$DEST/dashboard.html"
