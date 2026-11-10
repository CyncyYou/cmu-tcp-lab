# Bufferbloat Design Notes

Reference for the report. Describes the congestion-control changes made in
`bufferbloat/` relative to the unchanged `baseline/` (Checkpoint 2) code, the
reasoning behind each, the measured results, and the approaches that were tried
and rejected.

All changes are in `bufferbloat/src/backend.c` and `bufferbloat/inc/cmu_tcp.h`.
`baseline/` is left untouched so it provides the "before" numbers.

---

## 1. Problem

Local test network: **4 Mbps link, 20 ms RTT, 30-packet router buffer.** A bulk
upload (`gdrive`) shares the bottleneck with a latency-sensitive game flow
(`gamer`).

The baseline uses loss-based TCP Reno. It only backs off **after** the router
buffer overflows and drops a packet. By then the 30-packet queue is full, so:

- Every game packet waits behind a full queue -> high latency.
- After a drop, Reno halves cwnd and recovers slowly -> the link also goes
  under-utilised.

Baseline result: **score ~27**, goodput 2.19 Mbps (59% of available), median
lag ~53 ms, p95 ~159 ms, **28 drops**.

This is classic **bufferbloat**: a large standing queue that hurts latency
without helping throughput.

---

## 2. Approach: TCP Vegas (delay-based congestion control)

Instead of waiting for loss, react to the **queue forming**, observed as RTT
inflation. Keep only a small, roughly constant number of packets queued at the
bottleneck. This simultaneously:

- keeps the game flow's latency low (short queue), and
- avoids the loss-driven cwnd collapse that wasted goodput.

Three mechanisms work together.

### 2.1 RTT sampling (microsecond resolution)

- Once per round trip, one in-flight segment is "timed": its end sequence
  number and send time are recorded. When a cumulative ACK first covers that
  sequence, the elapsed time is one RTT sample.
- `base_rtt` = the minimum RTT ever seen = propagation delay with an empty
  queue (measured ~20.8 ms on the local test).
- **Microseconds, not milliseconds.** At a 20 ms RTT, a 1–2 packet standing
  queue adds only a few hundred microseconds to a few milliseconds. Millisecond
  resolution cannot distinguish 20 ms from 22 ms, so the controller could not
  "see" a small queue and over-inflated cwnd. Switching RTT timing to
  microseconds (`now_us()`) fixed this.
- **Karn's algorithm:** RTT samples are discarded across a retransmission
  (fast-retransmit or timeout), because a retransmitted segment's ACK is
  ambiguous.

Relevant socket fields (in `cmu_tcp.h`): `base_rtt_ms`, `rtt_sample_ms`
(both hold **microseconds** despite the `_ms` name), `rtt_pending`, `rtt_seq`,
`rtt_send_ms`, `round_end_seq`, `vegas_inited`.

### 2.2 Delay-based control law (`vegas_update`, once per RTT)

Estimated bytes queued at the bottleneck:

```
diff = (expected_throughput - actual_throughput) * base_rtt
     = cwnd * (rtt - base_rtt) / rtt
```

- **Slow start:** grow cwnd by one MSS per round until `diff` exceeds `GAMMA`
  (~1 MSS), then switch to congestion avoidance (so we don't overshoot the BDP
  by much on startup).
- **Congestion avoidance:** steer `diff` toward a small target queue
  (`target = MSS/2`). Instead of a fixed +/- MSS step (which sets up a limit
  cycle that spikes tail latency), correct cwnd by a **damped fraction** of the
  queue error:
  ```
  err  = diff - target
  step = err / 4            // gentle gain -> less ringing
  step = clamp(step, -MSS/2, +MSS/2)   // a noisy sample can't swing cwnd far
  cwnd = cwnd - step
  ```
- cwnd is floored at one MSS.

The control law runs exactly **once per RTT** (`round_end_seq` marks the round
boundary; when `last_ack_received` passes it, a round completed).

Loss handling (3 duplicate ACKs -> fast retransmit; retransmission timeout ->
back to slow start) is **retained from the baseline as a safety net**, but with
Vegas keeping the queue short it rarely fires (0 drops in the test).

### 2.3 Packet pacing

Even when average cwnd is correct, releasing a whole window at once bursts the
bottleneck queue. After each segment is sent, sleep for an inter-packet gap so
the window is spread across one RTT:

```
gap = base_rtt_us * MSS / cwnd * 7/8   // 7/8 leaves headroom so goodput stays full
gap = min(gap, 5 ms)                   // never stall too long on one packet
```

Paced off `base_rtt` (empty-queue time), which keeps goodput near the link rate.

---

## 3. Results

Local test (`./labtest.py run bufferbloat`):

| Metric            | Baseline      | This design   |
|-------------------|---------------|---------------|
| **Score**         | ~26–27        | **~74**       |
| gdrive goodput    | 2.19 Mbps 59% | 3.74 Mbps 96% |
| gamer lag p20     | ~20–21 ms     | ~18 ms        |
| gamer lag median  | ~53–57 ms     | ~26 ms        |
| gamer lag p95     | ~159 ms       | ~37 ms        |
| router drops      | 28            | **0**         |

Verified by running `./labtest.py run bufferbloat` (this design) and
`./labtest.py run bufferbloat --build baseline` (confirmed baseline unchanged
at ~26). Results saved under `results/bufferbloat/`.

### How the score breaks down

`score = 100 * link_term * lag_term` (a **product** — both must be high).

- `link_term = min(1, goodput / (0.95 * c_avail))` ≈ **0.99** (goodput is fine).
- `lag_term  = avg over {p20,p50,p95} of min(1, target/lag)`, where
  `target ≈ 18.4 ms` (= empty-queue min 12.8 ms + an allowance of **2 queued
  packets**, `BB_QUEUE_PKTS = 2`). This is **~0.74** and is the limiting factor:
  p50 (~26 ms) and p95 (~37 ms) still exceed the 18.4 ms full-marks target.

So the remaining loss is tail latency, not throughput.

---

## 4. Why the score plateaus at ~74 (structural limit)

The lag spikes are **steady and uniform** across the whole transfer (~36–40 ms
peak in every 0.5 s bucket), not a startup transient. Root cause: a delay-based
controller that samples **once per RTT** reacts one RTT **after** the queue
starts building, so the queue can grow for a full RTT (several packets at
4 Mbps/20 ms) before cwnd responds. That residual peak is what keeps p95 above
the 18.4 ms target.

The scoring is a product of goodput and latency, and at this operating point
they trade off directly: pushing the queue lower drops goodput faster than the
latency term rises.

---

## 5. Approaches tried and rejected

Documented so the report can discuss the design search, not just the final
answer.

1. **Fixed +/- MSS Vegas step with an [alpha, beta] hold band.**
   Worked (score ~68) but set up a limit cycle: cwnd overshot, queue built, next
   sample pulled it back, repeat. Replaced with the damped proportional law
   (section 2.2), which reduced ringing.

2. **Tightening the queue target toward zero (MSS/4, MSS/8).**
   Lowered latency but started costing goodput (link_term < 1). Net score rose
   only slightly, then fell. Settled on `target = MSS/2` as the knee.

3. **Hard cwnd ceiling from a measured delivery rate (BBR-like
   `cwnd <= rate * base_rtt + MSS`).**
   **Failed badly (score ~19).** Created a downward feedback spiral: a low cwnd
   lowered the measured delivery rate, which lowered the ceiling, which lowered
   cwnd further. Goodput collapsed to 25%. Reverted.

4. **Pacing off the *current* RTT instead of base RTT (self-correcting gap).**
   Lowered lag (p95 ~33 ms) but over-throttled goodput to 77% (score ~60),
   because current RTT > base inflates the gap on top of cwnd. Reverted to
   base-RTT pacing.

The final design (sections 2.1–2.3) is the best of these: strong goodput, zero
drops, and the lowest latency achievable without sacrificing throughput under a
once-per-RTT controller.

---

## 6. Possible next steps (not implemented)

To push past ~74 would require reacting to the queue **within** an RTT rather
than once per RTT, e.g.:

- per-ACK queue estimation / per-ACK cwnd adjustment, or
- a proper BBR-style design with explicit bandwidth probing and a min-RTT
  windowing phase.

Both are substantially larger redesigns than the Vegas controller here.

---

## 7. File / function index

- `bufferbloat/inc/cmu_tcp.h` — added Vegas state fields to `cmu_socket_t`.
- `bufferbloat/src/cmu_tcp.c` — initialises those fields in `cmu_socket()`.
- `bufferbloat/src/backend.c`:
  - `now_us()` — microsecond monotonic clock for RTT.
  - `vegas_update()` — the once-per-RTT delay-based control law.
  - `handle_message()` (ESTABLISHED, new-ACK branch) — RTT sampling + per-round
    `vegas_update()` call.
  - `single_send()` — starts RTT timing on send; packet pacing after each send.
  - `on_dup_ack()` / `on_timeout()` — Karn's algorithm (discard RTT sample on
    retransmit); kept as loss safety net.

Tuning constants (top of `backend.c`): `VEGAS_ALPHA`, `VEGAS_BETA`,
`VEGAS_GAMMA`; queue `target` and gain are in `vegas_update()`; pacing fraction
(7/8) and cap (5 ms) are in `single_send()`.

---

## 8. Hidden-test results (B1/B2/B3) and the base_rtt fix

The autograder runs three bufferbloat scenarios. Scoring per test is
`weight * min(1, raw_score/60)`.

| Test | Scenario | Result |
|------|----------|--------|
| B1 Slow DSL     | slower uplink, longer RTT                  | 72 (**10/10**) |
| B2 Wi-Fi drop   | uplink rate drops several-fold mid-upload  | 24 (3.99/10)   |
| B3 Route change | propagation delay jumps up mid-upload      | 62 (**10/10**) |

### B3 was a real bug, now fixed

Initially B3 scored **7.3 (1.2/10)**: goodput collapsed to 10%. Root cause:
`base_rtt` was a permanent running minimum, so after the propagation delay
increased, every RTT looked hugely inflated -> Vegas read a huge queue -> cwnd
collapsed and never recovered.

**Fix (section 2.1, windowed minimum):** `base_rtt` can drop instantly (an
emptier queue) but may only **rise** after a short observation window (~5 base
RTTs) confirms the old minimum is stale. This lets base_rtt track a genuine
propagation increase within a handful of RTTs. B3 went **7.3 -> 62 (10/10)**
with 0 drops and no regression to B1.

Relevant fields: `base_rtt_stamp_us`, `win_min_us`, `win_start_us` in
`cmu_tcp.h`; the windowed-min logic is in the RTT-sampling block of
`handle_message()`.

### B2 remains the weak spot (a tried-and-reverted fix)

B2 (mid-stream rate drop) scores 24 because, after the rate falls, the old cwnd
holds several BDPs of data and the damped CA law drains it only slowly, so a
large standing queue persists for many RTTs (lag 3-6x target). Goodput is fine
(96%); latency is the whole problem.

**Attempted fix:** a fast multiplicative drain -- when the estimated queue is
far above target, jump `cwnd <- cwnd*base/rtt` (= the current BDP) in one step.
This fixed B2 locally (~23 -> ~71) **but regressed B3**: a propagation-delay
increase also shows a large transient queue before base_rtt adapts, so the drain
misfired and cut cwnd when the pipe had actually grown. Several gates were tried
to tell the two apart using only RTT:

| Variant                         | B2 (local repro) | B3 (local repro) |
|---------------------------------|------------------|------------------|
| base_rtt fix only (**shipped**) | ~23              | ~49 (real: 62)   |
| + fast drain, ungated           | ~71              | ~44              |
| + 2-round persistence gate      | ~72              | ~44              |
| + win_min distinguisher         | ~31              | ~54              |

No RTT-only threshold cleanly separates "rate dropped" (cut cwnd) from
"propagation rose" (do not cut); a throughput signal would, but an earlier
delivery-rate estimator caused a feedback collapse (goodput 25%). Because **B3
is already 10/10 and sits just above the 60 full-marks threshold**, risking it
to gain on B2 was not worth it, so the fast drain was **reverted**. B2 is left
at 24. Distinguishing the two scenarios robustly is the main avenue for a future
B2 improvement.

