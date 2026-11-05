"""Same guarantees as test_pipeline.py, but against a REAL Kafka broker.

Runs in CI (and on your machine with `docker compose up -d kafka postgres`).
Needs FLEETOPS_TEST_KAFKA (e.g. localhost:9094) and FLEETOPS_TEST_DB.
"""

import os
import time
import uuid

import pytest

from fleetops import db
from fleetops.broker import KafkaBroker
from fleetops.consumer import Pipeline, SimulatedCrash
from fleetops.producer import day_pings, publish
from fleetops.simulator import SimConfig, simulate
from fleetops.tracker import Tracker

KAFKA = os.getenv("FLEETOPS_TEST_KAFKA")
TEST_DB = os.getenv("FLEETOPS_TEST_DB")
pytestmark = pytest.mark.skipif(not (KAFKA and TEST_DB), reason="FLEETOPS_TEST_KAFKA / FLEETOPS_TEST_DB not set")

CFG = SimConfig(seed=41, n_buses=6, duplicate_prob=0.05, reorder_prob=0.05, dropout_prob=0.4)


@pytest.fixture()
def conn():
    c = db.connect(TEST_DB)
    with c.transaction():
        c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    db.migrate(c)
    yield c
    c.close()


def _consume_all(make_pipe, expected: int, crash_every: int = 0, deadline_s: float = 120):
    """Consume until `expected` messages are committed. Returns (crashes, rejected)."""
    pipe = make_pipe()
    crashes = rejected = batch = 0
    committed = 0
    t_end = time.time() + deadline_s
    while committed < expected and time.time() < t_end:
        batch += 1
        msgs = pipe.consumer.poll(100, 1000)
        if not msgs:
            continue
        try:
            r = pipe.process(msgs, crash_before_commit=bool(crash_every) and batch % crash_every == 0)
        except SimulatedCrash:
            crashes += 1
            pipe.consumer.close()           # dies without committing
            pipe = make_pipe()              # new process: rejoins the group, gets redelivery
            continue
        committed += r.consumed
        rejected += r.alerts_rejected_by_db
    pipe.consumer.close()
    return crashes, rejected, committed


def test_real_kafka_crash_recovery_no_duplicate_alerts(conn):
    sim = simulate(CFG)
    db.seed_routes(conn, sim.routes, sim.bus_route)
    topic, group = f"pings-{uuid.uuid4().hex[:8]}", f"g-{uuid.uuid4().hex[:8]}"
    broker = KafkaBroker(KAFKA)
    pings = day_pings(sim, 0)
    publish(pings, broker.producer(), topic=topic)

    def make_pipe():
        return Pipeline.from_db(broker.consumer(group, topic), broker.producer(), conn)

    crashes, rejected, committed = _consume_all(make_pipe, len(pings), crash_every=4)

    ref = Tracker(sim.routes, sim.frame, sim.bus_route)
    for p in pings:
        ref.ingest(p)
    want = {(n.trip_id, n.stop_id) for n in ref.notifications}
    got = [(r["trip_id"], r["stop_id"]) for r in conn.execute("SELECT trip_id, stop_id FROM notifications")]

    assert committed >= len(pings)
    assert crashes >= 1
    assert rejected == 0
    assert len(got) == len(set(got))
    assert set(got) == want
