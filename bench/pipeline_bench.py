"""Pipeline benchmark: Kafka -> consumer -> tracker -> Postgres.

    docker compose up -d kafka postgres
    python bench/pipeline_bench.py --out bench/PIPELINE_RESULTS.md

    python bench/pipeline_bench.py --memory     # in-memory broker (no Kafka), for a quick check

Measures:
  1. Backlog throughput: N pings already in Kafka; how fast does one consumer
     process + persist + commit them?
  2. Crash recovery: kill the consumer between the DB commit and the offset
     commit every few batches. Count duplicate alerts with and without
     restoring tracker state from Postgres.
"""

from __future__ import annotations

import argparse
import os
import platform
import statistics
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fleetops import db  # noqa: E402
from fleetops.broker import KafkaBroker, MemoryBroker  # noqa: E402
from fleetops.consumer import Pipeline, SimulatedCrash  # noqa: E402
from fleetops.producer import day_pings, publish  # noqa: E402
from fleetops.simulator import SimConfig, simulate  # noqa: E402
from fleetops.tracker import Tracker  # noqa: E402


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def fresh_db(url):
    c = db.connect(url)
    with c.transaction():
        c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    db.migrate(c)
    return c


def make_broker(args):
    return MemoryBroker() if args.memory else KafkaBroker(args.kafka)


def consume(pipe, expected, *, batch, crash_every=0, make_pipe=None, deadline=600):
    """Run until `expected` messages committed. Returns dict of measurements."""
    committed = crashes = rejected = 0
    batch_s, n = [], 0
    t0 = time.perf_counter()
    t_end = time.time() + deadline
    while committed < expected and time.time() < t_end:
        msgs = pipe.consumer.poll(batch, 1000)
        if not msgs:
            continue
        n += 1
        b0 = time.perf_counter()
        try:
            r = pipe.process(msgs, crash_before_commit=bool(crash_every) and n % crash_every == 0)
        except SimulatedCrash:
            crashes += 1
            pipe.consumer.close()
            pipe = make_pipe()
            continue
        batch_s.append(time.perf_counter() - b0)
        committed += r.consumed
        rejected += r.alerts_rejected_by_db
    wall = time.perf_counter() - t0
    pipe.consumer.close()
    return {"committed": committed, "wall": wall, "batch_s": batch_s, "crashes": crashes, "rejected": rejected}


def bench_throughput(args):
    sim = simulate(SimConfig(seed=7, n_buses=args.buses, duplicate_prob=0.03, reorder_prob=0.03,
                             dropout_prob=0.2))
    conn = fresh_db(args.db)
    db.seed_routes(conn, sim.routes, sim.bus_route)
    broker = make_broker(args)
    topic, group = f"bench-{uuid.uuid4().hex[:8]}", f"bench-{uuid.uuid4().hex[:8]}"
    pings = day_pings(sim, 0)
    t0 = time.perf_counter()
    publish(pings, broker.producer(), topic=topic)
    produce_s = time.perf_counter() - t0
    pipe = Pipeline.from_db(broker.consumer(group, topic), broker.producer(), conn)
    m = consume(pipe, len(pings), batch=args.batch)
    alerts = conn.execute("SELECT count(*) AS n FROM notifications").fetchone()["n"]
    conn.close()
    return {"pings": len(pings), "buses": args.buses, "produce_rate": len(pings) / produce_s,
            "consume_rate": m["committed"] / m["wall"], "batch_p50": pct(m["batch_s"], 50),
            "batch_p99": pct(m["batch_s"], 99), "alerts": alerts, "batch": args.batch}


def bench_crash(args, restore: bool):
    sim = simulate(SimConfig(seed=8, n_buses=30, duplicate_prob=0.05, reorder_prob=0.05, dropout_prob=0.3))
    conn = fresh_db(args.db)
    db.seed_routes(conn, sim.routes, sim.bus_route)
    broker = make_broker(args)
    topic, group = f"crash-{uuid.uuid4().hex[:8]}", f"crash-{uuid.uuid4().hex[:8]}"
    pings = day_pings(sim, 0)
    publish(pings, broker.producer(), topic=topic)

    def make_pipe():
        return Pipeline.from_db(broker.consumer(group, topic), broker.producer(), conn, restore=restore)

    m = consume(make_pipe(), len(pings), batch=100, crash_every=4, make_pipe=make_pipe)
    rows = [(r["trip_id"], r["stop_id"]) for r in conn.execute("SELECT trip_id, stop_id FROM notifications")]
    ref = Tracker(sim.routes, sim.frame, sim.bus_route)
    for p in pings:
        ref.ingest(p)
    want = {(n.trip_id, n.stop_id) for n in ref.notifications}
    conn.close()
    return {"crashes": m["crashes"], "rejected": m["rejected"], "stored": len(rows),
            "dup_stored": len(rows) - len(set(rows)), "missing": len(want - set(rows)),
            "extra": len(set(rows) - want), "expected": len(want)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", default=os.getenv("FLEETOPS_KAFKA", "localhost:9094"))
    ap.add_argument("--db", default=os.getenv("FLEETOPS_DB", "postgresql://postgres:postgres@localhost:5432/fleetops_bench"))
    ap.add_argument("--memory", action="store_true", help="in-memory broker instead of Kafka")
    ap.add_argument("--buses", type=int, default=200)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--out")
    args = ap.parse_args()

    # The bench wipes its database, so it never uses the app's own 'fleetops' database.
    if args.db.rstrip("/").endswith("/fleetops"):
        sys.exit("refusing to run against the app database; use a separate one (default fleetops_bench)")
    admin = args.db.rsplit("/", 1)[0] + "/postgres"
    name = args.db.rsplit("/", 1)[1]
    with db.connect(admin) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            c.execute(f'CREATE DATABASE "{name}"')

    tp = bench_throughput(args)
    with_r = bench_crash(args, restore=True)
    without = bench_crash(args, restore=False)

    broker = "in-memory broker (NOT Kafka)" if args.memory else f"Kafka at {args.kafka}"
    lines = [
        "# FleetOps pipeline benchmark", "",
        f"Machine: {platform.platform()}, Python {platform.python_version()}. Broker: {broker}. "
        "Synthetic data only.", "",
        "## 1. Backlog throughput (one consumer, process + Postgres write + offset commit)", "",
        f"{tp['pings']:,} pings from {tp['buses']} buses already in the topic; batches of up to {tp['batch']}.", "",
        "| produce rate | consume rate (end to end) | batch p50 | batch p99 | alerts written |",
        "|---|---|---|---|---|",
        f"| {tp['produce_rate']:,.0f} msg/s | {tp['consume_rate']:,.0f} msg/s | {tp['batch_p50'] * 1000:.0f} ms | "
        f"{tp['batch_p99'] * 1000:.0f} ms | {tp['alerts']:,} |", "",
        "## 2. Crash recovery (consumer killed after the DB commit, before the offset commit, every 4th batch)", "",
        "| restart strategy | crashes | duplicate alerts stored | alert inserts blocked by DB | missing vs no-crash run | extra vs no-crash run |",
        "|---|---|---|---|---|---|",
        f"| restore tracker state from Postgres | {with_r['crashes']} | {with_r['dup_stored']} | {with_r['rejected']} | "
        f"{with_r['missing']} / {with_r['expected']} | {with_r['extra']} |",
        f"| start with an empty tracker | {without['crashes']} | {without['dup_stored']} | {without['rejected']} | "
        f"{without['missing']} / {without['expected']} | {without['extra']} |", "",
        "'Blocked by DB' = the tracker tried to alert again and only the UNIQUE(trip_id, stop_id) "
        "constraint stopped it. 'Missing'/'extra' compare against one uninterrupted run.", "",
    ]
    report = "\n".join(lines)
    print(report)
    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
