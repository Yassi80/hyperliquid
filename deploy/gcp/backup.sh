#!/usr/bin/env bash
# Daily (hlflows-backup.timer): consistent online copy of the database, compressed, to
# gs://BUCKET/backups/hlflows-YYYY-MM-DD.sqlite.gz and backups/latest.sqlite.gz.
# The bucket lifecycle rule deletes dated backups after 14 days.
set -euo pipefail
: "${BUCKET:?BUCKET not set (see /etc/hlflows/deploy.env)}"
WORK="${HLFLOWS_DATA:-/var/lib/hlflows}/backup"
HLFLOWS="${HLFLOWS_BIN:-/opt/hlflows-runtime/venv/bin/hlflows}"
DAY=$(date -u +%F)

"$HLFLOWS" backup "$WORK/hlflows.sqlite"
gzip -f "$WORK/hlflows.sqlite"
gcloud storage cp "$WORK/hlflows.sqlite.gz" "gs://$BUCKET/backups/hlflows-$DAY.sqlite.gz"
gcloud storage cp "$WORK/hlflows.sqlite.gz" "gs://$BUCKET/backups/latest.sqlite.gz"
rm -f "$WORK/hlflows.sqlite.gz"
