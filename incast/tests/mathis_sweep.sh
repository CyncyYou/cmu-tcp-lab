#!/usr/bin/env bash
# Mathis experiment sweep harness (CP2 report tooling). Runs on the SENDER node.
# For each loss p and each rep: configures the link, starts the receiver over
# SSH, runs the timed sender, and appends throughput to results.csv.
#
# EDIT for your AWS setup:
RECV_HOST="${RECV_HOST:-10.0.1.1}"
RECV_USER="${RECV_USER:-ubuntu}"
IFACE="${IFNAME:-eth0}"
PROJ="${PROJ:-/home/ubuntu/cmu-tcp/project-2_15-441}"
FILE="${MATHIS_FILE:-$PROJ/tests/mathis.input}"
PORT="${PORT:-15441}"
REPS="${REPS:-10}"
LOSSES=(0.1 0.2 0.5 1 2 5 10)   # percent
OUT="${OUT:-results.csv}"

# Full path to tcset on each node (pipx installs to ~/.local/bin, which is NOT
# on root's PATH under sudo). Resolve locally; override REMOTE_TCSET if the
# receiver's path differs.
TCSET="${TCSET:-$(command -v tcset)}"
REMOTE_TCSET="${REMOTE_TCSET:-/home/${RECV_USER}/.local/bin/tcset}"
if [ -z "$TCSET" ]; then
  echo "ERROR: tcset not found on PATH. Set TCSET=/full/path/to/tcset." >&2
  exit 1
fi

# SSH command used to reach the receiver. Set SSH_KEY to your .pem if the
# sender needs an explicit key (StrictHostKeyChecking=no avoids the first-use
# host-key prompt that would otherwise hang the non-interactive sweep).
SSH_KEY="${SSH_KEY:-}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [ -n "$SSH_KEY" ]; then
  SSH="ssh -i ${SSH_KEY} ${SSH_OPTS}"
else
  SSH="ssh ${SSH_OPTS}"
fi

export server15441="$RECV_HOST"
export serverport15441="$PORT"
export MATHIS_FILE="$FILE"

echo "loss_percent,rep,bytes,seconds,throughput_bps" > "$OUT"

for p in "${LOSSES[@]}"; do
  echo "=== loss = ${p}% ==="
  sudo "$TCSET" "$IFACE" --rate 5Mbps --delay 20ms --loss "${p}%" --overwrite
  $SSH "${RECV_USER}@${RECV_HOST}" \
    "sudo ${REMOTE_TCSET} ${IFACE} --rate 5Mbps --delay 20ms --overwrite" || true

  # Expected file size so the receiver can exit the moment it has everything.
  FSIZE=$(stat -c%s "$FILE" 2>/dev/null || wc -c < "$FILE")

  for rep in $(seq 1 "$REPS"); do
    # Start the receiver detached. Redirect stdin/stdout/stderr and use `ssh -n`
    # so the SSH call returns immediately instead of waiting on the backgrounded
    # process's file descriptors (which would hang the sweep).
    $SSH -n "${RECV_USER}@${RECV_HOST}" \
      "cd ${PROJ} && serverport15441=${PORT} MATHIS_OUT=/tmp/mathis_out.bin \
       MATHIS_EXPECT=${FSIZE} \
       setsid nohup ./tests/mathis_recv > /tmp/mathis_recv.log 2>&1 < /dev/null &" \
      || true
    sleep 2  # give the listener time to bind

    # Run the timed sender; discard its cwnd stderr spam, capture the summary.
    line=$(cd "$PROJ" && ./tests/mathis_send 2>/dev/null | grep '^MATHIS,')
    bytes=$(echo "$line"   | sed -n 's/.*bytes=\([0-9]*\).*/\1/p')
    seconds=$(echo "$line" | sed -n 's/.*seconds=\([0-9.]*\).*/\1/p')
    bps=$(echo "$line"     | sed -n 's/.*throughput_bps=\([0-9.]*\).*/\1/p')
    echo "${p},${rep},${bytes},${seconds},${bps}" >> "$OUT"
    echo "  rep ${rep}: ${seconds}s  ${bps} bps"

    ssh_target="${RECV_USER}@${RECV_HOST}"
    $SSH "$ssh_target" "pkill -f mathis_recv" || true
    sleep 1
  done
done
echo "Done. Wrote $OUT"
