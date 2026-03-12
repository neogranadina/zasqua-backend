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
  [n1b]=zasqua-n1b
  [n2]=zasqua-n2
  [nvl]=zasqua-nvl
)

declare -A EXPECTED=(
  [aht]=113
  [cabildos]=45
  [n1]=95
  [n1b]=73
  [n2]=157
  [nvl]=67
)

# Collect summary data
declare -a SUMMARY_FOND=()
declare -a SUMMARY_COMPLETED=()
declare -a SUMMARY_EXPECTED=()
declare -a SUMMARY_ERRORS=()
declare -a SUMMARY_STATUS=()
declare -a SUMMARY_DONE=()

for FOND in aht cabildos n1 n1b n2; do
  DROPLET_NAME="${DROPLETS[$FOND]}"

  IP=$(doctl compute droplet get "$DROPLET_NAME" --format PublicIPv4 --no-header 2>/dev/null || true)
  if [[ -z "$IP" || "$IP" == "0.0.0.0" ]]; then
    echo "WARNING: Could not resolve IP for $DROPLET_NAME — skipping" >&2
    SUMMARY_FOND+=("$FOND")
    SUMMARY_COMPLETED+=("?")
    SUMMARY_EXPECTED+=("${EXPECTED[$FOND]}")
    SUMMARY_ERRORS+=("?")
    SUMMARY_STATUS+=("UNREACHABLE")
    SUMMARY_DONE+=("?")
    continue
  fi

  TOTAL_EXPECTED="${EXPECTED[$FOND]}"
  echo "── $DROPLET_NAME ($IP) ──────────────────────────────────────────────────────"

  # Completed volumes
  COMPLETED=$(ssh $SSH_OPTS "root@$IP" "wc -l < /root/zasqua/progress.log 2>/dev/null || echo 0" 2>/dev/null || echo 0)
  COMPLETED=$(echo "$COMPLETED" | tr -d '[:space:]')
  echo "  Completed: $COMPLETED/$TOTAL_EXPECTED"

  # Errors
  ERRORS=$(ssh $SSH_OPTS "root@$IP" "wc -l < /root/zasqua/errors.log 2>/dev/null || echo 0" 2>/dev/null || echo 0)
  ERRORS=$(echo "$ERRORS" | tr -d '[:space:]')
  echo "  Errors: $ERRORS"

  # Running status
  STATUS=$(ssh $SSH_OPTS "root@$IP" "pgrep -f 'python3.*ingest_dropbox' > /dev/null 2>&1 && echo RUNNING || echo STOPPED" 2>/dev/null || echo UNKNOWN)
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

  # Current activity from tiling log
  echo "  Last activity:"
  ssh $SSH_OPTS "root@$IP" "tail -5 /root/zasqua/tiling-${FOND}.log 2>/dev/null || echo '    (no log yet)'" 2>/dev/null | sed 's/^/    /'

  # Disk usage
  DISK=$(ssh $SSH_OPTS "root@$IP" "du -sh /mnt/work 2>/dev/null | cut -f1 || echo '0'" 2>/dev/null || echo "?")
  DISK=$(echo "$DISK" | tr -d '[:space:]')
  echo "  Disk usage: $DISK"

  echo ""

  SUMMARY_FOND+=("$FOND")
  SUMMARY_COMPLETED+=("$COMPLETED")
  SUMMARY_EXPECTED+=("$TOTAL_EXPECTED")
  SUMMARY_ERRORS+=("$ERRORS")
  SUMMARY_STATUS+=("$STATUS")
  SUMMARY_DONE+=("$DONE_LABEL")
done

# Print summary table
echo "── Summary ─────────────────────────────────────────────────────────────────"
printf "%-12s %-14s %-8s %-10s %-6s\n" "Fond" "Progress" "Errors" "Status" "Done?"
printf "%-12s %-14s %-8s %-10s %-6s\n" "------------" "--------------" "--------" "----------" "------"
TOTAL_COMPLETED=0
TOTAL_EXPECTED=0
for i in "${!SUMMARY_FOND[@]}"; do
  PROGRESS_STR="${SUMMARY_COMPLETED[$i]}/${SUMMARY_EXPECTED[$i]}"
  printf "%-12s %-14s %-8s %-10s %-6s\n" \
    "${SUMMARY_FOND[$i]}" \
    "$PROGRESS_STR" \
    "${SUMMARY_ERRORS[$i]}" \
    "${SUMMARY_STATUS[$i]}" \
    "${SUMMARY_DONE[$i]}"
  if [[ "${SUMMARY_COMPLETED[$i]}" =~ ^[0-9]+$ ]]; then
    TOTAL_COMPLETED=$((TOTAL_COMPLETED + SUMMARY_COMPLETED[$i]))
  fi
  TOTAL_EXPECTED=$((TOTAL_EXPECTED + SUMMARY_EXPECTED[$i]))
done
printf "%-12s %-14s\n" "TOTAL" "$TOTAL_COMPLETED/$TOTAL_EXPECTED"
echo "────────────────────────────────────────────────────────────────────────────"
