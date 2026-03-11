#!/usr/bin/env bash
# configure.sh — Configure a single DigitalOcean droplet for AHRB tiling
#
# Prerequisites:
#   - SSH access to the droplet as root (key already provisioned by provision.sh)
#   - ~/.config/rclone/rclone.conf on this machine with [dropbox] and [r2] sections
#   - rclone installed locally (for config parsing)
#
# Usage:
#   ./configure.sh <IP>
#
# What it does:
#   1. Validates the IP argument
#   2. Installs: libvips-tools, python3, python3-pip, python3-venv, rclone
#   3. Installs Pillow via pip3
#   4. Verifies libvips, Python, and rclone are functional
#   5. Extracts the [dropbox] and [r2] sections from local rclone.conf
#   6. Copies the filtered rclone config to the droplet
#   7. Verifies rclone can reach Dropbox and R2 from the droplet
#   8. Prints a success message when everything is confirmed
#
# Run once per droplet. Safe to re-run (idempotent).

set -euo pipefail

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=30 -o BatchMode=yes"
RCLONE_CONF="${HOME}/.config/rclone/rclone.conf"
DROPBOX_PATH="dropbox:/Archivos Comunes/Imagenes/Copia seguridad AHRB"
R2_PATH="r2:zasqua-iiif-tiles"

# ── Validate argument ─────────────────────────────────────────────────────────

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <IP>" >&2
  echo "  Configure a droplet for AHRB tiling." >&2
  exit 1
fi

IP="$1"
echo "Configuring droplet at $IP..."

# ── Install dependencies ──────────────────────────────────────────────────────

echo ""
echo "[1/7] Installing system dependencies..."
# shellcheck disable=SC2029
ssh $SSH_OPTS "root@$IP" \
  "apt-get update -qq && apt-get install -y -qq libvips-tools python3 python3-pip python3-venv rclone"

echo ""
echo "[2/7] Installing Pillow..."
# shellcheck disable=SC2029
ssh $SSH_OPTS "root@$IP" "pip3 install --quiet --break-system-packages Pillow"

# ── Verify dependencies ───────────────────────────────────────────────────────

echo ""
echo "[3/7] Verifying libvips..."
VIPS_VERSION=$(ssh $SSH_OPTS "root@$IP" "vips --version") || {
  echo "ERROR: libvips verification failed on $IP" >&2
  exit 1
}
echo "  $VIPS_VERSION"

echo ""
echo "[4/7] Verifying Python..."
PY_VERSION=$(ssh $SSH_OPTS "root@$IP" "python3 --version") || {
  echo "ERROR: Python verification failed on $IP" >&2
  exit 1
}
echo "  $PY_VERSION"

echo ""
echo "[5/7] Verifying rclone..."
RCLONE_VERSION=$(ssh $SSH_OPTS "root@$IP" "rclone version" | head -1) || {
  echo "ERROR: rclone verification failed on $IP" >&2
  exit 1
}
echo "  $RCLONE_VERSION"

# ── Extract and copy rclone config ───────────────────────────────────────────

echo ""
echo "[6/7] Copying rclone config ([dropbox] and [r2] sections)..."

if [[ ! -f "$RCLONE_CONF" ]]; then
  echo "ERROR: Local rclone config not found at $RCLONE_CONF" >&2
  exit 1
fi

TMPCONF=$(mktemp /tmp/rclone-filtered.XXXXXX.conf)
trap 'rm -f "$TMPCONF"' EXIT

# Extract [dropbox] and [r2] sections using awk.
# Matches the section header, then collects lines until the next [section] header.
awk '
  /^\[(dropbox|r2)\]/ { in_section = 1; print; next }
  in_section && /^\[/ { in_section = 0 }
  in_section { print }
' "$RCLONE_CONF" > "$TMPCONF"

if [[ ! -s "$TMPCONF" ]]; then
  echo "ERROR: Could not extract [dropbox] or [r2] sections from $RCLONE_CONF" >&2
  exit 1
fi

echo "  Extracted sections:"
grep '^\[' "$TMPCONF" | sed 's/^/    /'

# Create the config directory on the droplet and copy the filtered config
# shellcheck disable=SC2029
ssh $SSH_OPTS "root@$IP" "mkdir -p ~/.config/rclone"
scp -o StrictHostKeyChecking=no "$TMPCONF" "root@$IP:~/.config/rclone/rclone.conf"
echo "  rclone.conf copied to droplet"

# ── Verify rclone remotes from droplet ───────────────────────────────────────

echo ""
echo "[7/7] Verifying rclone remotes from droplet..."

echo "  Checking Dropbox..."
# shellcheck disable=SC2029
ssh $SSH_OPTS "root@$IP" "rclone lsd '$DROPBOX_PATH'" || {
  echo "ERROR: Dropbox remote check failed on $IP" >&2
  echo "       Make sure [dropbox] section in rclone.conf is valid and authorised." >&2
  exit 1
}

echo "  Checking R2..."
# shellcheck disable=SC2029
ssh $SSH_OPTS "root@$IP" "rclone lsd $R2_PATH" || {
  echo "ERROR: R2 remote check failed on $IP" >&2
  echo "       Make sure [r2] section in rclone.conf is valid and the bucket exists." >&2
  exit 1
}

# ── Done ──────────────────────────────────────────────────────────────────────

echo ""
echo "DONE: $IP configured and verified"
echo "  libvips:  $VIPS_VERSION"
echo "  Python:   $PY_VERSION"
echo "  rclone:   $RCLONE_VERSION"
echo "  Dropbox:  OK"
echo "  R2:       OK"
