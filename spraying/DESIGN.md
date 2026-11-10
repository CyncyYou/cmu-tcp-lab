# Spraying Design Notes (extra credit)

Reference for the report. Describes the changes made in `spraying/` (created
from `baseline/`), the reasoning, and the measured results.

Changes are in `spraying/src/backend.c` and `spraying/inc/cmu_tcp.h`.

---

## 1. Problem

The switch sprays one flow's packets across several paths with different delays
(local test: 4 paths at +0/+1.5/+3/+4.5 ms), so packets routinely arrive **out
of order**. The local test also drops **5 consecutive packets** at the 1 MB
mark to exercise real loss recovery.

Baseline behaviour (loss-based Reno, fixed 3 s RTO): local score **~16-22**.
- **62 of 67 retransmissions unnecessary.** Reordering makes the receiver emit
  duplicate ACKs; a packet overtaken by k others yields k duplicates. Reno's
  3-duplicate-ACK fast retransmit fires constantly on packets that already
  arrived, and halves cwnd each time.
- **Goodput ~2.3 / 9.8 Mbps.** The repeated spurious backoffs crush throughput.
- **Repair time ~3000 ms.** When the 5 genuine losses happen, recovery waits
  for the fixed 3 s timeout.

### Why Reno struggles (report question 2)

Reno cannot tell reordering from loss: both look like duplicate ACKs. On a spray
fabric reordering is the norm, so Reno misreads it as constant loss -- spurious
fast retransmits (bad precision) and constant cwnd backoff (bad goodput) -- and
its one fixed, long timeout makes the real losses slow to repair.

---

## 2. Design

Three changes, matching the three scored terms (goodput / precision / repair).

### 2.1 Ignore duplicate ACKs entirely (precision + goodput)

`on_dup_ack()` is now a no-op. Reordering, not loss, is what produces duplicate
ACKs here, so we neither fast-retransmit nor back off on them. This immediately
fixes precision (no reordering-driven resends) and goodput (cwnd is not
repeatedly halved). Real losses are handled solely by the timer below.

### 2.2 Short, adaptive retransmission timeout (repair time)

`timer_expired()` replaces the fixed 3 s RTO with one derived from the measured
**minimum RTT** (the fastest path, a good base-RTT estimate):

```
RTO = clamp(3 * min_rtt, 50 ms, 500 ms)
```

- The 3x multiple and 50 ms floor keep the RTO safely longer than the SLOWEST
  path, so a merely-reordered packet never times out (which would be an
  unnecessary retransmit). This matters most on S2 (larger delay spread).
- Still far shorter than 3 s, so a genuine loss is repaired quickly.

RTT is sampled once per round from a non-retransmitted segment
(`min_rtt_us`, `rtt_pending`, `rtt_seq`, `rtt_send_us`; added to the socket
struct), with **Karn's algorithm** (a retransmission clears `rtt_pending`, so
retransmitted segments are never timed). The internal `TIMEOUT` poll was
shortened from 3 s to 5 ms so the short RTO can actually fire.

### 2.3 Repair a burst of holes in one round trip (repair time)

`on_timeout()` resends the **leading run** of outstanding segments
(`retransmit_leading_run(sock, 5)`), not just the single segment at the ACK
point. Spray losses come in consecutive bursts (the local test loses 5 in a
row); repairing one hole per RTO would need one timeout per lost packet and make
the median repair time grow with the burst. Resending the leading run repairs
the whole burst in one round trip. The bound (5) matches the burst without
resending the large in-flight tail (which would be delivered anyway and would
wreck precision). The receiver's existing out-of-order buffer discards any
resent copy that was merely in flight.

`on_timeout()` also keeps cwnd essentially intact (trims only ~12.5%, floored at
4*MSS): spray loss is not congestion, so collapsing the window would needlessly
crater goodput.

---

## 3. Results (local test)

| Metric            | Baseline      | This design   |
|-------------------|---------------|---------------|
| **Score**         | ~16-22        | **~84**       |
| goodput           | 2.3 / 9.8 Mbps| 9.3 / 9.8 Mbps (95%) |
| retransmissions   | 63 (58 unnec) | 5 (0 unnec)   |
| precision         | ~0.08         | 1.0           |
| repair time (med) | ~3000 ms      | ~110 ms       |

Verified with `./labtest.py run spraying` (this design, stable at 83.8-83.9 over
repeated runs) and `--build baseline` (~22). Comfortably above the 60 threshold
that earns the full extra-credit points on this scenario.

### Score breakdown

`score = 50*min(1,G/0.6Cavail) + 25*min(1,precision/0.8) + 25*min(1,2R*/Rmedian)`
- Goodput term: **50/50** (95% >> 60% target).
- Precision term: **25/25** (1.0 >> 0.8 target).
- Repair term: ~**9/25** -- repair 110 ms vs the 2R* ~= 78 ms target. This is the
  only sub-maxed term and the reason the score is ~84 not higher.

---

## 4. Where it falls short

The repair-time term is the weak point: ~110 ms median vs the ~78 ms target. The
repair is gated by one RTO (~50 ms) plus a round trip for the resend. Pushing
the RTO lower (tried 25-40 ms) does shave repair time but starts causing
**spurious timeouts on reordering** -- a slow-path packet occasionally arrives
just after the RTO, triggering a false retransmit that drops precision (seen as
run-to-run variance, e.g. a 75 instead of 85). The 50 ms floor was chosen to
make precision rock-stable at the cost of ~1 point of repair term.

A design that reacts faster *without* false positives would need per-packet loss
inference (RACK/SACK style: declare a segment lost only when a later-sent one is
ACKed and a reordering window has elapsed) rather than a single timer. That is a
larger change and was not attempted.

For the harder Gradescope fabrics:
- **S1 (random loss):** the no-dup-ACK + short-RTO design should handle sparse
  random loss well; each loss is repaired by the timer.
- **S2 (larger delay spread, sprayed ACKs):** the min-RTT-based RTO with the 3x
  margin and 50 ms floor is meant to stay above the wider reordering; the main
  risk is repair time rising with the larger spread.
- **S3 (link flap, a whole stretch dropped):** the leading-run retransmit (5)
  repairs a burst per RTO; a long flap may need several RTOs, so repair time
  could suffer there.

---

## 5. File / function index

- `spraying/inc/cmu_tcp.h` -- added `min_rtt_us`, `rtt_pending`, `rtt_seq`,
  `rtt_send_us` to `cmu_socket_t`.
- `spraying/src/cmu_tcp.c` -- initialises those fields.
- `spraying/src/backend.c`:
  - `now_us()` -- microsecond clock for RTT.
  - `on_dup_ack()` -- no-op (reordering is not loss).
  - `on_timeout()` -- bounded leading-run retransmit; keep cwnd high.
  - `retransmit_leading_run()` -- resend up to N segments from the ACK point.
  - `timer_expired()` -- adaptive RTO (3x min_rtt, [50 ms, 500 ms]).
  - `handle_message()` new-ACK branch -- RTT sampling (min_rtt, Karn).
  - `single_send()` -- starts RTT timing on send.
  - `check_for_data()` `TIMEOUT` case -- 5 ms poll instead of 3 s.

Tuning constants: RTO multiple/bounds in `timer_expired()`; leading-run length
(5) and cwnd trim in `on_timeout()`.
