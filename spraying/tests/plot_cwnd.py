#!/usr/bin/env python3
"""Plot the sender's congestion window over time for the CP2 report.

Input lines: CWND,<ms>,<cwnd>,<ssthresh>,<stage>[,<event>]
stage in {SLOW_START, CONGESTION_AVOIDANCE, FAST_RECOVERY};
event in {NEW_ACK, DUP, DUP3, TIMEOUT} (optional, for markers).

Usage: python3 plot_cwnd.py cwnd.csv [out.png]
"""

import sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

MSS = 1375
STAGE_COLORS = {"SLOW_START": "#d6eaf8",
                "CONGESTION_AVOIDANCE": "#d5f5e3",
                "FAST_RECOVERY": "#fdebd0"}
STAGE_LABEL = {"SLOW_START": "Slow start",
               "CONGESTION_AVOIDANCE": "Congestion avoidance",
               "FAST_RECOVERY": "Fast recovery"}


def parse(path):
    t, cwnd, ssth, stage, event = [], [], [], [], []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line.startswith("CWND,"):
                continue
            p = line.split(",")
            if len(p) < 5:
                continue
            t.append(int(p[1])); cwnd.append(int(p[2])); ssth.append(int(p[3]))
            stage.append(p[4]); event.append(p[5] if len(p) >= 6 else "")
    if not t:
        raise SystemExit("No CWND lines in " + path)
    t0 = t[0]
    t = [(x - t0) / 1000.0 for x in t]
    return t, cwnd, ssth, stage, event


def shade(ax, t, stage):
    s = 0
    for i in range(1, len(stage) + 1):
        if i == len(stage) or stage[i] != stage[s]:
            ax.axvspan(t[s], t[i] if i < len(t) else t[-1],
                       color=STAGE_COLORS.get(stage[s], "#fff"), alpha=0.6, lw=0)
            s = i


def main():
    if len(sys.argv) < 2:
        print(__doc__); raise SystemExit(1)
    path = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) >= 3 else "cwnd.png"
    t, cwnd, ssth, stage, event = parse(path)

    fig, ax = plt.subplots(figsize=(11, 6))
    shade(ax, t, stage)
    ax.plot(t, cwnd, color="#1f3b73", lw=1.6, label="cwnd", zorder=3)
    ax.plot(t, ssth, color="#7f8c8d", lw=1.2, ls="--", label="ssthresh", zorder=2)

    d3t = [t[i] for i in range(len(event)) if event[i] == "DUP3"]
    d3y = [cwnd[i] for i in range(len(event)) if event[i] == "DUP3"]
    tot = [t[i] for i in range(len(event)) if event[i] == "TIMEOUT"]
    toy = [cwnd[i] for i in range(len(event)) if event[i] == "TIMEOUT"]
    if d3t:
        ax.scatter(d3t, d3y, marker="v", s=70, color="#c0392b", zorder=5,
                   label="3 dup-ACKs (fast retransmit)")
    if tot:
        ax.scatter(tot, toy, marker="X", s=90, color="#000", zorder=5,
                   label="Timeout")

    ax.set_xlabel("Time (s)"); ax.set_ylabel("cwnd (bytes)")
    ax.set_title("CMU-TCP (Reno) congestion window over time")
    secax = ax.secondary_yaxis("right",
                               functions=(lambda b: b/MSS, lambda m: m*MSS))
    secax.set_ylabel("cwnd (MSS)")
    h, _ = ax.get_legend_handles_labels()
    patches = [Patch(facecolor=STAGE_COLORS[s], label=STAGE_LABEL[s], alpha=0.6)
               for s in STAGE_COLORS]
    ax.legend(handles=h + patches, loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3); fig.tight_layout(); fig.savefig(out, dpi=150)

    print("Wrote", out)
    print(f"Summary: {len(d3t)} fast-retransmit events, {len(tot)} timeouts, "
          f"duration {t[-1]:.2f}s, peak cwnd {max(cwnd)} bytes "
          f"({max(cwnd)/MSS:.1f} MSS).")


if __name__ == "__main__":
    main()
