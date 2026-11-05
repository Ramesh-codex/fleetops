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


def day_pings(sim: SimResult, day: int) -> list[GpsPing]:
    span = sim.end_ts - sim.pings[0].ts + 600
    out = []
    for p in sim.pings:
        out.append(p.model_copy(update={
            "event_id": f"D{day}-{p.event_id}",
            "trip_id": f"D{day}-{p.trip_id}",
            "ts": round(p.ts + day * span, 3),
        }))
    return out


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
    from .consumer import wait_for

    bootstrap = os.getenv("FLEETOPS_KAFKA", "localhost:9092")
    speedup = float(os.getenv("FLEETOPS_SPEEDUP", "10"))
    days = int(os.getenv("FLEETOPS_DAYS", "0"))
    poison = float(os.getenv("FLEETOPS_POISON", "0.001"))
    producer = wait_for(lambda: KafkaBroker(bootstrap).producer(), "kafka")
    sim = simulate(sim_config_from_env())
    day = 0
    while days == 0 or day < days:
        n = publish(day_pings(sim, day), producer, speedup=speedup, poison_prob=poison,
                    rng=random.Random(day))
        print(f"day {day}: published {n} messages", flush=True)
        day += 1


if __name__ == "__main__":
    main()
