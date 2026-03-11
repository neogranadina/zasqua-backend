#!/usr/bin/env bash
# dry-run.sh — SSH into the Cabildos droplet and run the ingest script with --dry-run
#
# Prints what volumes would be processed without doing any actual work.
# Use this to verify every volume path resolves correctly from Dropbox
# before kicking off the full pipeline (PROC-01 verification).
#
# Usage:
#   bash scripts/iiif/dry-run.sh

set -euo pipefail

DROPLET_NAME="zasqua-cabildos"
SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes"

echo "Looking up IP for $DROPLET_NAME..."
IP=$(doctl compute droplet get "$DROPLET_NAME" --format PublicIPv4 --no-header 2>/dev/null || true)
if [[ -z "$IP" || "$IP" == "0.0.0.0" ]]; then
  echo "ERROR: Could not resolve IP for $DROPLET_NAME. Is the droplet running?" >&2
  exit 1
fi
echo "  IP: $IP"
echo ""

echo "Running dry-run on $DROPLET_NAME ($IP)..."
echo "── Output ──────────────────────────────────────────────────────────────────"

# shellcheck disable=SC2029
ssh $SSH_OPTS "root@$IP" \
  "python /root/zasqua/scripts/iiif/ingest_dropbox_volumes.py \
    --manifest /root/zasqua/volumes-cabildos.csv \
    --dropbox-root 'dropbox:/Archivos Comunes/Imagenes/Copia seguridad AHRB' \
    --work-dir /mnt/work \
    --base-url https://iiif.zasqua.org \
    --r2-remote r2:zasqua-iiif-tiles \
    --dry-run"

echo "────────────────────────────────────────────────────────────────────────────"
echo "Dry run complete. Review the volume paths above before proceeding."
