"""Publish simulated GPS pings to Kafka.

    python -m fleetops.producer            # uses env vars below

Env: FLEETOPS_KAFKA (localhost:9092), FLEETOPS_SEED (42), FLEETOPS_BUSES (12),
     FLEETOPS_SPEEDUP (10; 0 = as fast as possible), FLEETOPS_DAYS (0 = forever),
     FLEETOPS_FAULTS (1), FLEETOPS_POISON (0.001 = share of malformed messages)

Each "day" replays the same seeded morning with fresh trip ids, so the tracker
sees new trips (and the device's per-trip seq counter restarting).
"""

from __future__ import annotations

import json
import os
import random
import time

from .broker import PINGS_TOPIC, Producer
from .models import GpsPing
from .simulator import SimConfig, SimResult, simulate


def sim_config_from_env() -> SimConfig:
    cfg = SimConfig(seed=int(os.getenv("FLEETOPS_SEED", "42")), n_buses=int(os.getenv("FLEETOPS_BUSES", "12")))
    if os.getenv("FLEETOPS_FAULTS", "1") == "1":
        cfg.duplicate_prob, cfg.reorder_prob = 0.03, 0.03
        cfg.dropout_prob, cfg.breakdown_prob = 0.25, 0.15
    return cfg


def day_pings(sim: SimResult, day: int, *, start_ts: float | None = None, tag: str = "") -> list[GpsPing]:
    """The seeded morning as 'day N', with fresh ids so the tracker sees new trips.

    start_ts anchors the first ping at that event time; without it days are laid
    end to end after the simulator's own start time (used by tests). `tag`
    keeps ids unique across producer restarts.
    """
    span = sim.end_ts - sim.pings[0].ts + 600
    shift = (start_ts - sim.pings[0].ts) if start_ts is not None else day * span
    out = []
    for p in sim.pings:
        out.append(p.model_copy(update={
            "event_id": f"{tag}D{day}-{p.event_id}",
            "trip_id": f"{tag}D{day}-{p.trip_id}",
            "ts": round(p.ts + shift, 3),
        }))
    return out


# Virtual clock origin. Simulated time runs `speedup` times faster than real
# time from this fixed point, so it only ever moves forward — also across
# producer restarts. Anchoring a restart at plain wall-clock 'now' would put it
# behind pings already sent at 10x, and the consumer would (correctly) drop the
# new trip as stale; restarting at day 0 replayed the past, which it also dropped.
CLOCK_ORIGIN = 1_793_000_000.0      # 2026-10-26 UTC


def virtual_now(speedup: float, wall: float | None = None) -> float:
    wall = time.time() if wall is None else wall
    if speedup <= 0:
        return wall
    return CLOCK_ORIGIN + (wall - CLOCK_ORIGIN) * speedup


POISON = [
    b"{not json",
    json.dumps({"schema_version": 1, "bus_id": "BUS-101"}).encode(),            # missing fields
    json.dumps({"schema_version": 1, "event_id": "x", "bus_id": "BUS-101", "trip_id": "t", "seq": 0,
                "ts": 0, "lat": 999, "lon": 0, "speed_mps": 0, "heading": 0}).encode(),  # bad latitude
]


def publish(pings: list[GpsPing], producer: Producer, *, speedup: float = 0.0,
            poison_prob: float = 0.0, rng: random.Random | None = None, topic: str = PINGS_TOPIC) -> int:
    """Send pings in delivery order. With speedup > 0, pace them in scaled real time."""
    rng = rng or random.Random(0)
    sent = 0
    t0_sim, t0_wall = pings[0].ts, time.monotonic()
    for p in pings:
        if speedup > 0:
            wait = (p.ts - t0_sim) / speedup - (time.monotonic() - t0_wall)
            if wait > 0:
                time.sleep(wait)
        producer.send(topic, p.bus_id.encode(), p.model_dump_json().encode())
        sent += 1
        if poison_prob and rng.random() < poison_prob:
            producer.send(topic, p.bus_id.encode(), rng.choice(POISON))
            sent += 1
    producer.flush()
    return sent


def main() -> None:
    from .broker import KafkaBroker
    from .runtime import wait_for

    bootstrap = os.getenv("FLEETOPS_KAFKA", "localhost:9092")
    speedup = float(os.getenv("FLEETOPS_SPEEDUP", "10"))
    days = int(os.getenv("FLEETOPS_DAYS", "0"))
    poison = float(os.getenv("FLEETOPS_POISON", "0.001"))
    producer = wait_for(lambda: KafkaBroker(bootstrap).producer(), "kafka")
    sim = simulate(sim_config_from_env())
    tag = time.strftime("%y%m%d%H%M%S-", time.gmtime())   # unique per producer start
    day = 0
    while days == 0 or day < days:
        pings = day_pings(sim, day, start_ts=virtual_now(speedup), tag=tag)
        n = publish(pings, producer, speedup=speedup, poison_prob=poison, rng=random.Random(day))
        print(f"{tag}day {day}: published {n} messages", flush=True)
        day += 1


if __name__ == "__main__":
    main()
