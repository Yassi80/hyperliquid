#!/usr/bin/env bash
# Deploy the committed code (git HEAD) to the VM and (re)provision it. Run on your Mac
# from the repo root:  bash deploy/gcp/push.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
# shellcheck source=/dev/null
source deploy/gcp/gcp.env

if [ -n "$(git status --porcelain)" ]; then
  echo "note: uncommitted changes are NOT deployed (deploying $(git rev-parse --short HEAD))"
fi
TARBALL=$(mktemp -t hlflows.XXXXXX).tar.gz
git archive --format=tar.gz -o "$TARBALL" HEAD

gcloud compute scp --project "$PROJECT" --zone "$ZONE" "$TARBALL" "$VM:/tmp/hlflows.tar.gz"
gcloud compute ssh --project "$PROJECT" --zone "$ZONE" "$VM" -- \
  "sudo rm -rf /opt/hlflows && sudo mkdir -p /opt/hlflows \
   && sudo tar -xzf /tmp/hlflows.tar.gz -C /opt/hlflows && rm /tmp/hlflows.tar.gz \
   && sudo bash /opt/hlflows/deploy/gcp/setup-vm.sh '$BUCKET'"
rm -f "$TARBALL"
