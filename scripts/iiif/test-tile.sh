#!/usr/bin/env bash
# test-tile.sh — SSH into the Cabildos droplet and tile one volume end-to-end
#
# Runs the ingest script with --limit 1 to process the first volume in the
# Cabildos manifest, then verifies that:
#   (a) progress.log has one entry
#   (b) tiles exist on R2 for that volume's slug
#
# Use this to confirm the full pipeline works before launching all 5 droplets
# (PROC-02 single-volume verification).
#
# Usage:
#   bash scripts/iiif/test-tile.sh

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

# Derive the slug from the first data row of the remote manifest CSV
echo "Reading first volume slug from remote manifest..."
FIRST_ROW=$(ssh $SSH_OPTS "root@$IP" "head -2 /root/zasqua/volumes-cabildos.csv | tail -1")
FOND=$(echo "$FIRST_ROW" | cut -d',' -f1)
VOLUME=$(echo "$FIRST_ROW" | cut -d',' -f2)
# derive_volume_slug: co-{fond.lower().replace('_','-')}-{volume}
SLUG="co-$(echo "$FOND" | tr '[:upper:]' '[:lower:]' | tr '_' '-')-${VOLUME}"
echo "  First volume: fond=$FOND volume=$VOLUME slug=$SLUG"
echo ""

echo "Tiling one volume on $DROPLET_NAME ($IP) with --limit 1..."
echo "── Tiling output ───────────────────────────────────────────────────────────"

# shellcheck disable=SC2029
ssh $SSH_OPTS "root@$IP" \
  "python3 /root/zasqua/scripts/iiif/ingest_dropbox_volumes.py \
    --manifest /root/zasqua/volumes-cabildos.csv \
    --dropbox-root 'dropbox:/Archivos Comunes/Imagenes/Copia seguridad AHRB' \
    --work-dir /mnt/work \
    --base-url https://iiif.zasqua.org \
    --r2-remote r2:zasqua-iiif-tiles \
    --progress /root/zasqua/progress.log \
    --errors-log /root/zasqua/errors.log \
    --limit 1"

echo "────────────────────────────────────────────────────────────────────────────"
echo ""

# Verify (a): progress.log has one entry
echo "Verifying progress.log..."
PROGRESS_COUNT=$(ssh $SSH_OPTS "root@$IP" "wc -l < /root/zasqua/progress.log 2>/dev/null || echo 0")
echo "  progress.log entries: $PROGRESS_COUNT"
if [[ "$PROGRESS_COUNT" -ge 1 ]]; then
  echo "  OK: at least one volume logged"
else
  echo "  WARNING: progress.log is empty — check for errors"
fi
echo ""

# Verify (b): tiles exist on R2 for the slug
echo "Verifying tiles on R2 for $SLUG..."
echo "── R2 tile listing (head -5) ───────────────────────────────────────────────"
ssh $SSH_OPTS "root@$IP" "rclone ls r2:zasqua-iiif-tiles/${SLUG}/ 2>/dev/null | head -5 || echo '  (no files found)'"
echo "────────────────────────────────────────────────────────────────────────────"
echo ""
echo "Test tile complete. Review output above to confirm tiles were uploaded."
