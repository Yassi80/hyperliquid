#!/usr/bin/env bash
# Hourly (hlflows-sync.timer): archive old positions, copy Parquet to GCS, then prune
# local files past storage.local_retention_days. Pruning runs only if the copy succeeded.
set -euo pipefail
: "${BUCKET:?BUCKET not set (see /etc/hlflows/deploy.env)}"
DATA="${HLFLOWS_DATA:-/var/lib/hlflows}"
HLFLOWS="${HLFLOWS_BIN:-/opt/hlflows-runtime/venv/bin/hlflows}"

"$HLFLOWS" maintain
for dir in trades positions; do
  if [ -d "$DATA/$dir" ]; then
    gcloud storage rsync --recursive --exclude='.*\.tmp$' "$DATA/$dir" "gs://$BUCKET/$dir"
  fi
done
"$HLFLOWS" maintain --prune
