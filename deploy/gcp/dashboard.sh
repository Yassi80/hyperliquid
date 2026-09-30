#!/usr/bin/env bash
# Build the dashboard on the VM from live data, copy it to your Mac and open it:
#   bash deploy/gcp/dashboard.sh                      # all coins; 15m, 1h, 4h, 1d; 200 bars each
#   bash deploy/gcp/dashboard.sh --bars 400 --tf 1h,4h
# Then pick coin / timeframe in the page, or bookmark e.g. ...dashboard.html#coin=ETH&tf=4h
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
# shellcheck source=/dev/null
source deploy/gcp/gcp.env
OUT=dashboards/hlflows-dashboard.html
mkdir -p dashboards

gcloud compute ssh "$VM" --zone "$ZONE" --project "$PROJECT" -- \
  hlflows dashboard --out /tmp/hlflows-dashboard.html "$@"
gcloud compute scp --zone "$ZONE" --project "$PROJECT" \
  "$VM:/tmp/hlflows-dashboard.html" "$OUT"
open "$OUT"
