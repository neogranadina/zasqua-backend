#!/usr/bin/env bash
# provision.sh — Create and verify five DigitalOcean droplets for AHRB tiling
#
# Prerequisites:
#   - doctl installed and authenticated (doctl auth init)
#   - At least one SSH key registered in your DigitalOcean account
#
# Usage:
#   ./provision.sh
#
# What it does:
#   1. Detects all SSH key IDs from your DO account
#   2. Creates five droplets (zasqua-aht, zasqua-n1, zasqua-n2, zasqua-nvl,
#      zasqua-cabildos) — skips any that already exist
#   3. Waits for each droplet to receive a public IP
#   4. Polls SSH on each droplet until it is ready (max 5 minutes per droplet)
#   5. Prints name and IP when each droplet is confirmed SSH-ready
#
# Droplet spec:
#   Size:   c-8
#   Region: nyc3
#   Image:  ubuntu-24-04-x64

set -euo pipefail

DROPLET_NAMES=(zasqua-aht zasqua-n1 zasqua-n2 zasqua-nvl zasqua-cabildos)
SIZE="c-8"
REGION="nyc3"
IMAGE="ubuntu-24-04-x64"
SSH_MAX_ATTEMPTS=60
SSH_SLEEP=5

# ── Detect SSH key IDs ────────────────────────────────────────────────────────

echo "Detecting SSH key IDs from DigitalOcean account..."
SSH_KEY_IDS=$(doctl compute ssh-key list --format ID --no-header | tr '\n' ',' | sed 's/,$//')

if [[ -z "$SSH_KEY_IDS" ]]; then
  echo "ERROR: No SSH keys found in your DigitalOcean account." >&2
  echo "       Register an SSH key first: doctl compute ssh-key import" >&2
  exit 1
fi

echo "Using SSH key IDs: $SSH_KEY_IDS"

# ── Get currently existing droplet names ─────────────────────────────────────

EXISTING=$(doctl compute droplet list --format Name --no-header 2>/dev/null || true)

# ── Create droplets (idempotent) ──────────────────────────────────────────────

for NAME in "${DROPLET_NAMES[@]}"; do
  if echo "$EXISTING" | grep -qx "$NAME"; then
    echo "SKIP: $NAME already exists"
  else
    echo "Creating droplet: $NAME"
    doctl compute droplet create "$NAME" \
      --size "$SIZE" \
      --region "$REGION" \
      --image "$IMAGE" \
      --ssh-keys "$SSH_KEY_IDS" \
      --wait=false \
      --no-header \
      --format ID > /dev/null
    echo "  -> create command issued for $NAME"
  fi
done

# ── Wait for public IPs ───────────────────────────────────────────────────────

echo ""
echo "Waiting for droplets to receive public IPs..."

declare -A DROPLET_IPS

for NAME in "${DROPLET_NAMES[@]}"; do
  echo -n "  $NAME IP: "
  while true; do
    IP=$(doctl compute droplet get "$NAME" --format PublicIPv4 --no-header 2>/dev/null || true)
    if [[ -n "$IP" && "$IP" != "0.0.0.0" ]]; then
      echo "$IP"
      DROPLET_IPS["$NAME"]="$IP"
      break
    fi
    sleep 5
  done
done

# ── Poll SSH readiness ────────────────────────────────────────────────────────

echo ""
echo "Waiting for SSH to become available on each droplet..."

for NAME in "${DROPLET_NAMES[@]}"; do
  IP="${DROPLET_IPS[$NAME]}"
  echo -n "  $NAME ($IP): "
  ATTEMPTS=0

  while true; do
    if ssh \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=5 \
        -o BatchMode=yes \
        "root@$IP" true 2>/dev/null; then
      echo "SSH ready"
      break
    fi

    ATTEMPTS=$(( ATTEMPTS + 1 ))
    if (( ATTEMPTS >= SSH_MAX_ATTEMPTS )); then
      echo ""
      echo "ERROR: $NAME ($IP) did not become SSH-ready after $((SSH_MAX_ATTEMPTS * SSH_SLEEP)) seconds." >&2
      exit 1
    fi

    echo -n "."
    sleep "$SSH_SLEEP"
  done
done

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "All droplets are SSH-ready:"
for NAME in "${DROPLET_NAMES[@]}"; do
  printf "  %-22s %s\n" "$NAME" "${DROPLET_IPS[$NAME]}"
done

echo ""
echo "Next step: run ./configure.sh <IP> for each droplet above."
