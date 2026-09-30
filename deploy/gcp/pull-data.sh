#!/usr/bin/env bash
# Copy the recorded data from GCS to your Mac for research:
#   bash deploy/gcp/pull-data.sh [DEST]   (default ./data)
# Raw trades and archived positions sync incrementally; the database is the latest
# nightly backup (up to a day old). Point storage.db_path / data_dir at DEST to query it.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
# shellcheck source=/dev/null
source deploy/gcp/gcp.env
DEST="${1:-./data}"
mkdir -p "$DEST"

for dir in trades positions; do
  # positions/ appears once positions are 14 days old and get archived; skip until then.
  if gcloud storage ls "gs://$BUCKET/$dir/" >/dev/null 2>&1; then
    gcloud storage rsync --recursive "gs://$BUCKET/$dir" "$DEST/$dir"
  else
    echo "(nothing in gs://$BUCKET/$dir yet, skipping)"
  fi
done
gcloud storage cp "gs://$BUCKET/backups/latest.sqlite.gz" "$DEST/hlflows.sqlite.gz"
rm -f "$DEST/hlflows.sqlite" "$DEST/hlflows.sqlite-wal" "$DEST/hlflows.sqlite-shm"
gunzip -f "$DEST/hlflows.sqlite.gz"

ABS="$(cd "$DEST" && pwd)"
if [ ! -f "$ABS/hlflows.toml" ]; then
  printf '[storage]\ndb_path = "%s/hlflows.sqlite"\ndata_dir = "%s"\n' "$ABS" "$ABS" \
    > "$ABS/hlflows.toml"
fi
echo "database: $ABS/hlflows.sqlite (latest backup in the bucket)"
echo "query it: HLFLOWS_CONFIG=$ABS/hlflows.toml uv run hlflows show --coin ETH"
