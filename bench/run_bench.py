"""Reproducible benchmark for the tracking core.

    python bench/run_bench.py            # prints a markdown report
    python bench/run_bench.py --out results.md

Three questions, all answered against simulator ground truth:
  1. Throughput  - how many pings/sec can one tracker process apply?
  2. ETA quality - how good are "bus is ~5 min away" alerts, per ETA mode,
                   in clean traffic, heavy traffic, and with breakdowns?
  3. Faults      - are duplicates / reordering / dropouts all handled?
"""

from __future__ import annotations

import argparse
import platform
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fleetops.simulator import SimConfig, simulate  # noqa: E402
from fleetops.tracker import Tracker  # noqa: E402

LEAD_TARGET = 300.0     # alert should fire ~5 min before arrival
LATE_LEAD = 120.0       # < 2 min warning = parent can't make it out the door
SEEDS = [1, 2, 3, 4, 5]


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


# --------------------------------------------------------------- throughput
def bench_throughput(n_buses=300, seed=99):
    cfg = SimConfig(seed=seed, n_buses=n_buses, duplicate_prob=0.05, reorder_prob=0.05, dropout_prob=0.2)
    r = simulate(cfg)
    t = Tracker(r.routes, r.frame, r.bus_route)
    lat_us = []
    start = time.perf_counter()
    for p in r.pings:
        t0 = time.perf_counter_ns()
        t.ingest(p)
        lat_us.append((time.perf_counter_ns() - t0) / 1000)
    wall = time.perf_counter() - start
    return {
        "buses": n_buses, "pings": len(r.pings), "wall_s": wall,
        "pings_per_s": len(r.pings) / wall,
        "p50_us": pct(lat_us, 50), "p99_us": pct(lat_us, 99),
        "counters": t.counters,
    }


# -------------------------------------------------------------- ETA quality
SCENARIOS = {
    "clean":      dict(traffic_prob=0.0),
    "traffic":    dict(traffic_prob=0.4),
    "breakdowns": dict(traffic_prob=0.15, breakdown_prob=0.5),
}


APPROACH = ("BUS_APPROACHING", "APPROACHING_AFTER_DELAY")


def eval_eta(scenario: dict, **tracker_kw):
    """Score what each parent was last told before the bus actually arrived.

    For every (trip, stop) that got an alert: take the parent's LAST message
    sent before arrival. If it is an "approaching" alert, its lead time counts
    (early = more than 10 min before arrival). If it is a "delayed" update, the
    parent was correctly told not to go out yet, so it does not count as early.
    """
    leads, eta_err_5min, early, scored, false_bd = [], [], 0, 0, 0
    for seed in SEEDS:
        r = simulate(SimConfig(seed=seed, n_buses=30, **scenario))
        t = Tracker(r.routes, r.frame, r.bus_route, **tracker_kw)
        first_ping: dict[str, float] = {}
        for p in r.pings:
            first_ping.setdefault(p.bus_id, p.ts)
            t.ingest(p)
            for stop_id, eta in t.etas(p.bus_id).items():
                actual = r.truth_arrival[(p.bus_id, stop_id)]
                if 240 <= actual - p.ts <= 360:
                    eta_err_5min.append(eta - actual)
        false_bd += sum(1 for i in t.incidents.values()
                        if i.type == "BREAKDOWN_SUSPECTED" and i.bus_id not in r.breakdowns)
        last: dict[tuple[str, str], object] = {}
        for n in t.notifications:
            actual = r.truth_arrival[(n.bus_id, n.stop_id)]
            if n.sent_ts <= actual and actual - first_ping[n.bus_id] > LEAD_TARGET + 60:
                last[(n.bus_id, n.stop_id)] = n
        for (bus, stop), n in last.items():
            scored += 1
            if n.kind in APPROACH:
                lead = r.truth_arrival[(bus, stop)] - n.sent_ts
                leads.append(lead)
                early += lead > 2 * LEAD_TARGET
    return {
        "alerts": scored,
        "lead_p50": pct(leads, 50),
        "late_pct": 100 * sum(l < LATE_LEAD for l in leads) / max(1, scored),
        "early_pct": 100 * early / max(1, scored),
        "mae_5min": statistics.mean(abs(e) for e in eta_err_5min),
        "bias_5min": statistics.mean(eta_err_5min),
        "false_bd": false_bd,
    }


# ------------------------------------------------------------------- faults
def eval_faults():
    rows = []
    for seed in SEEDS:
        cfg = SimConfig(seed=seed, n_buses=30, duplicate_prob=0.1, reorder_prob=0.1, dropout_prob=0.3)
        r = simulate(cfg)
        t = Tracker(r.routes, r.frame, r.bus_route)
        for p in r.pings:
            t.ingest(p)
            t.check_stale(p.ts)
        keys = [(n.bus_id, n.stop_id) for n in t.notifications]
        rows.append({
            "pings": len(r.pings), **t.counters,
            "dup_alerts": len(keys) - len(set(keys)),
            "dropouts": len(r.dropouts),
            "gps_lost_opened": len(t.incidents),
            "gps_lost_closed": sum(i.closed_ts is not None for i in t.incidents.values()),
            "finished": sum(s["finished"] for s in t.snapshot()),
            "buses": cfg.n_buses,
        })
    return {k: sum(r[k] for r in rows) for k in rows[0]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    args = ap.parse_args()

    lines = [f"# FleetOps tracking benchmark",
             f"",
             f"Machine: {platform.platform()}, Python {platform.python_version()}",
             f"Seeds: {SEEDS}. Synthetic data only.",
             ""]

    tp = bench_throughput()
    lines += ["## 1. Throughput (single tracker process, in-memory)", "",
              f"{tp['pings']:,} pings from {tp['buses']} buses (5% dup, 5% reorder, 20% of trips with a dropout)", "",
              "| pings/sec | p50 per ping | p99 per ping |", "|---|---|---|",
              f"| {tp['pings_per_s']:,.0f} | {tp['p50_us']:.1f} µs | {tp['p99_us']:.1f} µs |", ""]

    lines += ["## 2. Parent alert quality (target: alert ~5 min before arrival)", "",
              "Scored on the LAST message each parent got before the bus arrived. `lead` = real arrival − "
              "alert time. `late` = under 2 min warning. `early` = last message was an alert sent over "
              "10 min early (a 'delayed' update correctly cancels it). ETA MAE scored on predictions "
              "made 4–6 min out. `false breakdowns` = breakdown incidents for buses that never broke down.", "",
              "| scenario | method | scored stops | median lead | late % | early % | ETA MAE (5 min out) | ETA bias | false breakdowns |",
              "|---|---|---|---|---|---|---|---|---|"]
    off = float("inf")      # breakdown detection disabled
    variants = [
        ("naive", dict(eta_mode="naive", require_warmup=False, breakdown_after_s=off)),
        ("moving, no warmup", dict(eta_mode="moving", require_warmup=False, breakdown_after_s=off)),
        ("moving + warmup", dict(eta_mode="moving", require_warmup=True, breakdown_after_s=off)),
        ("+ breakdown updates", dict(eta_mode="moving", require_warmup=True)),
    ]
    for name, sc in SCENARIOS.items():
        for label, kw in variants:
            e = eval_eta(sc, **kw)
            lines.append(f"| {name} | {label} | {e['alerts']} | {e['lead_p50']:.0f} s | {e['late_pct']:.1f}% | "
                         f"{e['early_pct']:.1f}% | {e['mae_5min']:.0f} s | {e['bias_5min']:+.0f} s | {e['false_bd']} |")
    lines += ["", "ETA bias = mean(predicted - actual arrival). Positive = on average the ETA "
              "says the bus is later than it really is. Note an alert fires on the FIRST "
              "prediction under 5 min, so a noisy estimator can be late on average yet still alert early.", ""]

    f = eval_faults()
    lines += ["## 3. Fault handling (10% dup, 10% reorder, 30% of trips with a GPS dropout)", "",
              "| pings | accepted | duplicates dropped | out-of-order dropped | duplicate alerts | "
              "dropouts injected | GPS_LOST opened | closed on recovery | buses finished |",
              "|---|---|---|---|---|---|---|---|---|",
              f"| {f['pings']:,} | {f['accepted']:,} | {f['duplicate']:,} | {f['out_of_order']:,} | "
              f"{f['dup_alerts']} | {f['dropouts']} | {f['gps_lost_opened']} | {f['gps_lost_closed']} | "
              f"{f['finished']}/{f['buses']} |", ""]

    report = "\n".join(lines)
    print(report)
    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
