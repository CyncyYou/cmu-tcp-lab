#!/usr/bin/env python3
"""Mathis model analysis (CP2 report).

Reads results.csv (loss_percent,rep,bytes,seconds,throughput_bps), plots
throughput vs 1/sqrt(p) with a regression line, and reports the fitted
constant C and the Pearson correlation coefficient.

Usage: python3 mathis_analyze.py results.csv [out.png]
"""

import csv
import math
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def read_rows(path):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                lp = float(r["loss_percent"]); bps = float(r["throughput_bps"])
            except (KeyError, ValueError):
                continue
            if lp > 0 and bps > 0:
                rows.append((lp, bps))
    if not rows:
        raise SystemExit("No valid rows in " + path)
    return rows


def pearson(xs, ys):
    n = len(xs); mx = sum(xs)/n; my = sum(ys)/n
    sxy = sum((x-mx)*(y-my) for x, y in zip(xs, ys))
    sxx = sum((x-mx)**2 for x in xs); syy = sum((y-my)**2 for y in ys)
    return float("nan") if sxx == 0 or syy == 0 else sxy/math.sqrt(sxx*syy)


def linfit(xs, ys):
    n = len(xs); mx = sum(xs)/n; my = sum(ys)/n
    sxx = sum((x-mx)**2 for x in xs)
    sxy = sum((x-mx)*(y-my) for x, y in zip(xs, ys))
    a = sxy/sxx
    return a, my - a*mx


def slope_origin(xs, ys):
    return sum(x*y for x, y in zip(xs, ys)) / sum(x*x for x in xs)


def main():
    if len(sys.argv) < 2:
        print(__doc__); raise SystemExit(1)
    path = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) >= 3 else "mathis.png"
    rows = read_rows(path)

    xs, ys = [], []
    for lp, bps in rows:
        xs.append(1.0/math.sqrt(lp/100.0)); ys.append(bps/1e6)

    groups = defaultdict(list)
    for lp, bps in rows:
        groups[lp].append(bps/1e6)
    mx = [1.0/math.sqrt(lp/100.0) for lp in sorted(groups)]
    my = [sum(groups[lp])/len(groups[lp]) for lp in sorted(groups)]

    a, b = linfit(xs, ys)
    c = slope_origin(xs, ys)
    r = pearson(xs, ys)

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(xs, ys, s=18, alpha=0.35, color="#4a90d9", label="runs")
    ax.scatter(mx, my, s=70, color="#1f3b73", zorder=4, label="mean per loss")
    xl = [0] + sorted(xs)
    ax.plot(xl, [a*x+b for x in xl], color="#c0392b", lw=1.8,
            label=f"OLS: y={a:.3f}x+{b:.3f}")
    ax.plot(xl, [c*x for x in xl], color="#27ae60", ls="--", lw=1.5,
            label=f"origin: y={c:.3f}x")
    ax.set_xlabel(r"$1/\sqrt{p}$"); ax.set_ylabel("Throughput (Mbps)")
    ax.set_title(r"Mathis model: throughput vs $1/\sqrt{p}$")
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3)
    fig.tight_layout(); fig.savefig(out, dpi=150)

    print(f"Wrote {out}")
    print(f"OLS fit: slope a={a:.4f} Mbps, intercept b={b:.4f} Mbps")
    print(f"Through-origin fit: C={c:.4f} Mbps")
    print(f"Pearson r (1/sqrt(p) vs throughput) = {r:.4f}")
    for lp in sorted(groups):
        v = groups[lp]
        print(f"  p={lp:>5}%  1/sqrt(p)={1/math.sqrt(lp/100):7.2f}  "
              f"mean={sum(v)/len(v):.3f} Mbps  (n={len(v)})")


if __name__ == "__main__":
    main()
