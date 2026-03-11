#!/usr/bin/env bash
# run-all.sh — Launch the full tiling pipeline on all 5 droplets in background
#
# SSHes into each fond's droplet and starts the ingest script with nohup so it
# continues running after the SSH session ends. Stdout and stderr are redirected
# to /root/zasqua/tiling-{fond}.log on each droplet.
#
# Use check-progress.sh to monitor progress after launching.
#
# Usage:
#   bash scripts/iiif/run-all.sh

set -euo pipefail

SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes"

declare -A DROPLETS=(
  [aht]=zasqua-aht
  [cabildos]=zasqua-cabildos
  [n1]=zasqua-n1
  [n2]=zasqua-n2
  [nvl]=zasqua-nvl
)

echo "Launching tiling pipeline on all 5 droplets..."
echo ""

for FOND in aht cabildos n1 n2 nvl; do
  DROPLET_NAME="${DROPLETS[$FOND]}"

  IP=$(doctl compute droplet get "$DROPLET_NAME" --format PublicIPv4 --no-header 2>/dev/null || true)
  if [[ -z "$IP" || "$IP" == "0.0.0.0" ]]; then
    echo "ERROR: Could not resolve IP for $DROPLET_NAME. Is the droplet running?" >&2
    exit 1
  fi

  # Guard: skip droplet if ingest is already running
  RUNNING=$(ssh $SSH_OPTS "root@$IP" "pgrep -f ingest_dropbox_volumes || true")
  if [[ -n "$RUNNING" ]]; then
    echo "  SKIPPED: ingest already running (PID: $RUNNING)"
    continue
  fi

  # shellcheck disable=SC2029
  ssh $SSH_OPTS "root@$IP" \
    "nohup python3 /root/zasqua/scripts/iiif/ingest_dropbox_volumes.py \
      --manifest /root/zasqua/volumes-${FOND}.csv \
      --dropbox-root 'dropbox:/Archivos Comunes/Imagenes/Copia seguridad AHRB' \
      --work-dir /mnt/work \
      --base-url https://iiif.zasqua.org \
      --r2-remote r2:zasqua-iiif-tiles \
      --workers 4 \
      --progress /root/zasqua/progress.log \
      --errors-log /root/zasqua/errors.log \
      > /root/zasqua/tiling-${FOND}.log 2>&1 &"

  echo "Launched on $DROPLET_NAME ($IP)"
done

echo ""
echo "All 5 droplets launched. Use check-progress.sh to monitor progress."
