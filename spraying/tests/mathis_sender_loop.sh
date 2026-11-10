#!/usr/bin/env bash
# Mathis per-loss SENDER loop (CP2 report tooling). Run on the SENDER node.
#
# Usage:
#   ./tests/mathis_sender_loop.sh <loss_percent> [reps]
# Example:
#   ./tests/mathis_sender_loop.sh 2        # 10 reps at 2% loss
#   ./tests/mathis_sender_loop.sh 0.1 10
#
# Before running, set the sender's loss for this p (done automatically here),
# and start the matching receiver loop on the receiver node:
#   ./tests/mathis_receiver_loop.sh [reps]
#
# Config via env (sensible defaults):
RECV_IP="${RECV_IP:-172.31.18.60}"
IFACE="${IFNAME:-ens5}"
PORT="${PORT:-15441}"
FILE="${MATHIS_FILE:-$(pwd)/tests/mathis.input}"
OUT="${OUT:-results.csv}"

P="$1"
REPS="${2:-10}"
if [ -z "$P" ]; then
  echo "Usage: $0 <loss_percent> [reps]" >&2
  exit 1
fi

TCSET="${TCSET:-$(command -v tcset)}"

# Create the CSV header once (only if the file doesn't exist yet).
if [ ! -f "$OUT" ]; then
  echo "loss_percent,rep,bytes,seconds,throughput_bps" > "$OUT"
fi

# Configure the sender link for this loss probability (rate/delay fixed).
sudo "$TCSET" "$IFACE" --rate 5Mbps --delay 20ms --loss "${P}%" --overwrite
echo "Sender link set: 5Mbps / 20ms / loss ${P}%"

for i in $(seq 1 "$REPS"); do
  line=$(server15441="$RECV_IP" serverport15441="$PORT" MATHIS_FILE="$FILE" \
         ./tests/mathis_send 2>/dev/null | grep '^MATHIS,')
  b=$(echo "$line"   | sed -n 's/.*bytes=\([0-9]*\).*/\1/p')
  s=$(echo "$line"   | sed -n 's/.*seconds=\([0-9.]*\).*/\1/p')
  bps=$(echo "$line" | sed -n 's/.*throughput_bps=\([0-9.]*\).*/\1/p')
  if [ -z "$b" ]; then
    echo "  p=${P}% rep=${i}: NO RESULT (transfer failed?) — rerun this rep"
  else
    echo "${P},${i},${b},${s},${bps}" >> "$OUT"
    echo "  p=${P}% rep=${i}: ${s}s  ${bps} bps"
  fi
  sleep 3   # pace: let the receiver finish and rebind before the next send
done
echo "Done p=${P}%. Rows appended to ${OUT}."
