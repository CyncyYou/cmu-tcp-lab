#!/usr/bin/env bash
# Mathis RECEIVER loop (CP2 report tooling). Run on the RECEIVER node.
#
# Serves [reps] back-to-back transfers (one per sender rep). Each receiver
# instance exits as soon as the full file (MATHIS_EXPECT bytes) arrives, then
# the loop starts the next one. Run this, then run the sender loop for the same
# rep count on the sender node.
#
# Usage:
#   ./tests/mathis_receiver_loop.sh [reps]
#
IFACE="${IFNAME:-ens5}"
PORT="${PORT:-15441}"
EXPECT="${MATHIS_EXPECT:-1000000}"
REPS="${1:-10}"

TCSET="${TCSET:-$(command -v tcset)}"

# Receiver link: fixed rate/delay, NO loss (loss is applied on the sender only).
sudo "$TCSET" "$IFACE" --rate 5Mbps --delay 20ms --overwrite
echo "Receiver link set: 5Mbps / 20ms / no loss"

for i in $(seq 1 "$REPS"); do
  echo "  waiting for transfer ${i}/${REPS}..."
  serverport15441="$PORT" MATHIS_OUT=/tmp/mathis_out.bin MATHIS_EXPECT="$EXPECT" \
    ./tests/mathis_recv 2>/dev/null
  # Brief pause so trailing/retransmitted packets from this transfer drain
  # before the next receiver instance binds, preventing it from racing ahead
  # of the sender and desynchronizing the rep pairing.
  sleep 2
done
echo "Receiver loop done (${REPS} transfers)."
