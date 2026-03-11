#!/usr/bin/env bash
# check-progress.sh — Report tiling progress across all 5 droplets
#
# For each fond's droplet, reports:
#   - Number of completed volumes (from progress.log)
#   - Number of errors (from errors.log)
#   - Whether the ingest process is still running
#   - Last 3 lines of the tiling log
#   - Whether "Done --" completion signal appears in the log
#
# Usage:
#   bash scripts/iiif/check-progress.sh

set -euo pipefail

SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes"

declare -A DROPLETS=(
  [aht]=zasqua-aht
  [cabildos]=zasqua-cabildos
  [n1]=zasqua-n1
  [n2]=zasqua-n2
  [nvl]=zasqua-nvl
)

# Collect summary data
declare -a SUMMARY_FOND=()
declare -a SUMMARY_COMPLETED=()
declare -a SUMMARY_ERRORS=()
declare -a SUMMARY_STATUS=()
declare -a SUMMARY_DONE=()

for FOND in aht cabildos n1 n2 nvl; do
  DROPLET_NAME="${DROPLETS[$FOND]}"

  IP=$(doctl compute droplet get "$DROPLET_NAME" --format PublicIPv4 --no-header 2>/dev/null || true)
  if [[ -z "$IP" || "$IP" == "0.0.0.0" ]]; then
    echo "WARNING: Could not resolve IP for $DROPLET_NAME — skipping" >&2
    SUMMARY_FOND+=("$FOND")
    SUMMARY_COMPLETED+=("?")
    SUMMARY_ERRORS+=("?")
    SUMMARY_STATUS+=("UNREACHABLE")
    SUMMARY_DONE+=("?")
    continue
  fi

  echo "── $DROPLET_NAME ($IP) ──────────────────────────────────────────────────────"

  # Completed volumes
  COMPLETED=$(ssh $SSH_OPTS "root@$IP" "wc -l < /root/zasqua/progress.log 2>/dev/null || echo 0" 2>/dev/null || echo 0)
  COMPLETED=$(echo "$COMPLETED" | tr -d '[:space:]')
  echo "  Completed: $COMPLETED"

  # Errors
  ERRORS=$(ssh $SSH_OPTS "root@$IP" "wc -l < /root/zasqua/errors.log 2>/dev/null || echo 0" 2>/dev/null || echo 0)
  ERRORS=$(echo "$ERRORS" | tr -d '[:space:]')
  echo "  Errors: $ERRORS"

  # Running status
  STATUS=$(ssh $SSH_OPTS "root@$IP" "pgrep -f ingest_dropbox_volumes > /dev/null 2>&1 && echo RUNNING || echo STOPPED" 2>/dev/null || echo UNKNOWN)
  STATUS=$(echo "$STATUS" | tr -d '[:space:]')
  echo "  Status: $STATUS"

  # Completion signal
  DONE=$(ssh $SSH_OPTS "root@$IP" "grep -c '^Done --' /root/zasqua/tiling-${FOND}.log 2>/dev/null || echo 0" 2>/dev/null || echo 0)
  DONE=$(echo "$DONE" | tr -d '[:space:]')
  if [[ "$DONE" -ge 1 ]]; then
    DONE_LABEL="YES"
  else
    DONE_LABEL="no"
  fi
  echo "  Done signal: $DONE_LABEL"

  # Last 3 lines of tiling log
  echo "  Last log lines:"
  ssh $SSH_OPTS "root@$IP" "tail -3 /root/zasqua/tiling-${FOND}.log 2>/dev/null || echo '    (no log yet)'" 2>/dev/null | sed 's/^/    /'

  echo ""

  SUMMARY_FOND+=("$FOND")
  SUMMARY_COMPLETED+=("$COMPLETED")
  SUMMARY_ERRORS+=("$ERRORS")
  SUMMARY_STATUS+=("$STATUS")
  SUMMARY_DONE+=("$DONE_LABEL")
done

# Print summary table
echo "── Summary ─────────────────────────────────────────────────────────────────"
printf "%-12s %-10s %-8s %-10s %-6s\n" "Fond" "Completed" "Errors" "Status" "Done?"
printf "%-12s %-10s %-8s %-10s %-6s\n" "------------" "----------" "--------" "----------" "------"
for i in "${!SUMMARY_FOND[@]}"; do
  printf "%-12s %-10s %-8s %-10s %-6s\n" \
    "${SUMMARY_FOND[$i]}" \
    "${SUMMARY_COMPLETED[$i]}" \
    "${SUMMARY_ERRORS[$i]}" \
    "${SUMMARY_STATUS[$i]}" \
    "${SUMMARY_DONE[$i]}"
done
echo "────────────────────────────────────────────────────────────────────────────"
