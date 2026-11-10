# Incast Design Notes

Reference for the report. Describes the changes made in `incast/` to survive the
Incast challenge (many workers answer one query at the same instant through a
small switch buffer), the reasoning, the measured results, and the remaining
limits.

Changes are in `incast/src/backend.c` (plus `#include "lab_host.h"`).

---

## 1. Problem

Many workers each send a reply simultaneously through a small switch buffer. In
the local test: **8 workers x 64 KB, 10 Mbps link, 10 ms RTT, 24-packet switch
buffer.**

Baseline behaviour (loss-based Reno, fixed 3 s timeout):
- All workers blast their initial windows at once -> combined in-flight far
  exceeds the 24-packet buffer -> heavy loss.
- A lost packet waits the fixed **`DEFAULT_TIMEOUT` = 3000 ms** before being
  resent, so completion time came out in multiples of 3 s.
- Baseline local score ~**6.8**, completion 6.35 s vs 0.44 s ideal, 53 drops.

---

## 2. Approach

The Incast app runs **one receiving (`TCP_LISTENER`) process per worker** on the
aggregator, and they share host state through `lab_host` (see
`inc/lab_host.h`). That lets the **receiver** throttle its sender via the
advertised window, which is the lever used here. Three parts:

### 2.1 Fair-share advertised window (receiver side) -- the main fix

`compute_adv_window()` on a listener socket no longer advertises the full free
buffer. It advertises this connection's **fair share of the pipe** so the sum
over all workers stays within what the switch can hold:

```
BDP          = lab_host_link_bps()/8 * lab_host_base_rtt_us()/1e6   (bytes)
fair_window  = 1.25 * BDP / lab_host_active()        // split among active flows
fair_window  = clamp(fair_window, 1*MSS, 2*MSS)      // floor and absolute cap
adv_window   = min(free_buffer_space, fair_window)
```

- Dividing by `lab_host_active()` gives each flow an equal share.
- **Absolute cap of 2*MSS** is the important subtlety. Early in the query only a
  few workers have connected, so `lab_host_active()` is small and `BDP/n` would
  grant the first flows a large share; if several flows each grab that before
  the rest arrive, their combined first flights overflow the switch (the classic
  incast collapse). Capping each flow to ~2 segments bounds the aggregate
  regardless of how many have joined yet.
- **Floor of 1*MSS** so every flow can always make progress (TCP cannot send
  less than one segment).
- On a worker's sending socket `lab_host` returns 0, so the cap is skipped and
  ordinary free-space flow control applies.

### 2.2 Capture the advertised window from the SYN-ACK (sender side)

The baseline only recorded the peer's advertised window on data ACKs, so a
worker kept the initial `MAX_NETWORK_BUFFER` assumption through its entire first
flight. Now the `CLOSED` (SYN-ACK) handler also stores
`adv_window_from_receiver`, so the worker honours the fair-share window from its
very first data segment -- the synchronized startup is controlled, not blasted.

### 2.3 Short, RTT-based retransmission timeout (sender side)

The fixed 3 s RTO is replaced in `timer_expired()` with:

```
rto = clamp(4 * base_rtt, 20 ms, 200 ms) + per-flow jitter(0..31 ms)
```

- `base_rtt` from `lab_host_base_rtt_us()` (falls back to 100 ms when unknown,
  e.g. on a worker before host info is available -- still far better than 3 s).
- A lost packet is now repaired in tens of ms instead of 3 s.
- **Per-flow jitter** (derived from the local port) de-synchronizes retransmits
  so workers that lost packets in the same instant do not all resend together
  and re-overflow the switch.

Supporting change: the `TIMEOUT` poll in `check_for_data()` was shortened from
`DEFAULT_TIMEOUT` (3 s) to 10 ms so the sender wakes often enough to honour the
short RTO (an arriving ACK still wakes `poll` immediately, so no fast-path cost).

---

## 3. Results

Measured with `./labtest.py run incast [--workers N --reply-kb K --buffer B]`.
Leaderboard "before" values are from the baseline submission.

| Scenario (approx)                | Baseline (leaderboard) | This design |
|----------------------------------|------------------------|-------------|
| local  (8 workers x 64 KB, buf 24)  | ~6.8                | **~70-75**  |
| I1-like (16 workers x 48 KB, buf 32)| 6.6 (1.1/10)        | **~82**     |
| I2-like (32 workers x 16 KB, buf 12)| 3.0 (0.5/10)        | **~53**     |
| I3-like (16 workers x 64 KB, buf 24)| 7.8 (1.3/10)        | **~83**     |

Completion time on the local test dropped from **6.35 s to ~0.57 s** (ideal
0.44 s): the multi-second timeouts are gone.

---

## 4. Remaining limit

A residual ~50-150 switch drops remain in every scenario, because the per-flow
window cannot go below one segment: with many workers, N * 1 MSS can still
exceed the buffer at moments, and ACK-clocked bursts briefly push the aggregate
over capacity. This is worst in **I2** (32 workers, 12-packet buffer), where
even one packet per worker (32) far exceeds the buffer -- no purely
window-based scheme can avoid loss there.

Fully eliminating the drops would require either:
- **sender-side pacing** (spread each flow's window across an RTT, as done in
  the Bufferbloat design), or
- **worker scheduling** (let only a subset of workers send at a time, e.g. using
  `lab_host_rank()` to take turns),

both of which are larger redesigns. The current window + short-RTO design
already turns near-zero scores into 50-83 across the scenarios.

### Attempted: rank-based admission control (reverted)

To attack I2 specifically, a scheduling scheme was implemented and then
reverted:

- **Idea.** When the fan-in is so large that even one packet per worker exceeds
  the pipe, admit only the first K flows (by `lab_host_rank()`) to send at a
  real window and **pause the rest at a zero advertised window**. As admitted
  flows finish and call `lab_host_leave()`, the paused flows' rank drops into
  the admitted set (FIFO), so they resume. K was estimated from the BDP.
- **Result.** It worked dramatically for I2 (**~40 -> ~82**, drops 216 -> 18)
  and was a no-op for small fan-ins.
- **Why reverted.** The zero-window hold introduced a **zero-window deadlock**:
  a paused sender can only resume when it receives a window-update ACK, but a
  pure window update is not retransmitted, and with nothing in flight the
  sender's retransmission timer never fires -- so a single lost wakeup ACK
  parks that flow forever. This showed up as intermittent (then, with a
  periodic re-advertise, consistent) **0.0 scores on I1/I2/I3** ("the
  receiver's last ACK advertised a window of 0 bytes and no window update
  followed: a zero-window deadlock"). Three receiver-side wakeup mechanisms
  were tried (ACK on active-set change; periodic re-advertise; re-advertise
  gated on `lab_host_remaining`) and each still deadlocked.
- **What it would need.** A correct version requires proper **sender-side
  zero-window probing** (the paused sender periodically probes to elicit a
  fresh window), which is self-healing against lost ACKs. That is a sizeable,
  risky change to the core send loop (which currently deliberately avoids
  injecting probes) and could destabilise the already-strong I1/I3 and the
  Bufferbloat folder, so it was judged not worth the risk for one test's gain.

The shipped design therefore keeps the robust window + short-RTO approach, with
I2 left at ~40-48.

---

## 5. File / function index

- `incast/src/backend.c`:
  - `#include "lab_host.h"`.
  - `compute_adv_window()` -- fair-share advertised window (receiver side).
  - `handle_message()` `CLOSED` branch -- capture adv window from SYN-ACK.
  - `timer_expired()` -- short RTT-based RTO with per-flow jitter.
  - `check_for_data()` `TIMEOUT` case -- 10 ms poll instead of 3 s.
- Tuning constants are inline: the `1.25` BDP headroom, the `1*MSS` floor and
  `2*MSS` absolute cap in `compute_adv_window()`, and the RTO bounds
  `4*base_rtt`, `[20 ms, 200 ms]` in `timer_expired()`.
