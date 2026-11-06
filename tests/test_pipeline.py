"""Pipeline tests against a real PostgreSQL/PostGIS and the in-memory broker.

Needs a Postgres with PostGIS. Set FLEETOPS_TEST_DB, e.g.
    postgresql://postgres:postgres@localhost:5432/fleetops_test
Skipped when it isn't set or reachable.
"""

import os
import random

import psycopg
import pytest

from fleetops import db
from fleetops.broker import DLQ_TOPIC, PINGS_TOPIC, MemoryBroker
from fleetops.consumer import Pipeline, SimulatedCrash
from fleetops.producer import POISON, day_pings, publish
from fleetops.simulator import SimConfig, simulate
from fleetops.tracker import Tracker

TEST_DB = os.getenv("FLEETOPS_TEST_DB")
pytestmark = pytest.mark.skipif(not TEST_DB, reason="FLEETOPS_TEST_DB not set")

CFG = SimConfig(seed=31, n_buses=6, duplicate_prob=0.05, reorder_prob=0.05, dropout_prob=0.4, breakdown_prob=0.2)


@pytest.fixture()
def conn():
    try:
        c = db.connect(TEST_DB)
    except psycopg.OperationalError as e:
        pytest.skip(f"test database unreachable: {e}")
    with c.transaction():
        c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    db.migrate(c)
    yield c
    c.close()


@pytest.fixture()
def sim(conn):
    s = simulate(CFG)
    db.seed_routes(conn, s.routes, s.bus_route)
    return s


def reference(pings, sim):
    """What a single uninterrupted in-memory tracker produces for the same input."""
    t = Tracker(sim.routes, sim.frame, sim.bus_route)
    for p in pings:
        t.ingest(p)
        t.check_stale(p.ts)
    return {(n.trip_id, n.stop_id) for n in t.notifications}, {i.incident_id for i in t.incidents.values()}


def drain(pipe, **kw):
    totals = {"alerts": 0, "rejected": 0, "dead": 0}
    while True:
        r = pipe.run_once(max_records=200, **kw)
        if r.consumed == 0:
            return totals
        totals["alerts"] += r.alerts
        totals["rejected"] += r.alerts_rejected_by_db
        totals["dead"] += r.dead


def db_alerts(conn):
    return {(r["trip_id"], r["stop_id"]) for r in conn.execute("SELECT trip_id, stop_id FROM notifications")}


# ----------------------------------------------------------------- schema

def test_migrate_is_idempotent(conn):
    assert db.migrate(conn) == []


def test_routes_roundtrip(conn, sim):
    routes, bus_route = db.load_routes(conn)
    assert bus_route == sim.bus_route
    for rid, r in sim.routes.items():
        got = routes[rid]
        assert [s.stop_id for s in got.stops] == [s.stop_id for s in r.stops]
        for a, b in zip(got.stops, r.stops):
            assert a.lat == pytest.approx(b.lat, abs=1e-9) and a.dist_m == pytest.approx(b.dist_m)


def test_postgis_stops_near(conn, sim):
    s = next(iter(sim.routes.values())).stops[3]
    near = db.q_stops_near(conn, s.lat, s.lon, 50)
    assert near[0]["stop_id"] == s.stop_id and near[0]["distance_m"] < 1


# --------------------------------------------------------------- pipeline

def test_pipeline_matches_in_memory_tracker(conn, sim):
    b = MemoryBroker()
    pings = day_pings(sim, 0)
    publish(pings, b.producer())
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    totals = drain(pipe)
    ref_alerts, ref_incidents = reference(pings, sim)
    assert db_alerts(conn) == ref_alerts
    # Same GPS-loss incidents as a single ordered stream (ids embed bus, trip and
    # last-ping time, so they are comparable across both runs).
    assert {r["incident_id"] for r in conn.execute("SELECT incident_id FROM incidents")} == ref_incidents
    assert totals["rejected"] == 0
    assert conn.execute("SELECT bool_and(finished) AS f FROM bus_state").fetchone()["f"]


def test_crash_before_offset_commit_sends_no_duplicate_alerts(conn, sim):
    b = MemoryBroker()
    pings = day_pings(sim, 0)
    publish(pings, b.producer())
    crashes, rejected, batch_no = 0, 0, 0
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    while True:
        batch_no += 1
        try:
            r = pipe.run_once(max_records=100, crash_before_commit=(batch_no % 3 == 0))
        except SimulatedCrash:
            crashes += 1
            # A new process: fresh consumer (resumes from last COMMITTED offset)
            # and a tracker rebuilt from Postgres.
            pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
            continue
        if r.consumed == 0:
            break
        rejected += r.alerts_rejected_by_db
    assert crashes >= 3
    assert rejected == 0                                    # state restore alone prevented re-alerts
    assert db_alerts(conn) == reference(pings, sim)[0]      # same alerts as a run with no crashes
    assert conn.execute("SELECT count(*) AS n FROM notifications").fetchone()["n"] == len(db_alerts(conn))


def test_crash_without_state_restore_is_caught_only_by_db_constraint(conn, sim):
    """The 'before' picture: restart with an empty tracker. The tracker re-alerts,
    and only the UNIQUE constraint keeps parents from getting duplicates."""
    b = MemoryBroker()
    publish(day_pings(sim, 0), b.producer())
    rejected, batch_no = 0, 0
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn, restore=False)
    while True:
        batch_no += 1
        try:
            r = pipe.run_once(max_records=100, crash_before_commit=(batch_no % 3 == 0))
        except SimulatedCrash:
            pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn, restore=False)
            continue
        if r.consumed == 0:
            break
        rejected += r.alerts_rejected_by_db
    assert rejected > 0
    n = conn.execute("SELECT count(*) AS n FROM notifications").fetchone()["n"]
    assert n == len(db_alerts(conn))                       # still no duplicates stored


def test_poison_messages_go_to_dead_letters_and_pipeline_continues(conn, sim):
    b = MemoryBroker()
    pings = day_pings(sim, 0)
    sent = publish(pings, b.producer(), poison_prob=0.02, rng=random.Random(5))
    n_poison = sent - len(pings)
    assert n_poison > 0
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    totals = drain(pipe)
    assert totals["dead"] == n_poison
    assert len(b.messages(DLQ_TOPIC)) == n_poison
    reasons = {r["reason"] for r in conn.execute("SELECT DISTINCT reason FROM dead_letters")}
    assert reasons <= {"invalid_json", "validation_error", "unsupported_schema"}
    assert db_alerts(conn) == reference(pings, sim)[0]


def test_unsupported_schema_version_is_dead_lettered(conn, sim):
    b = MemoryBroker()
    p = day_pings(sim, 0)[0].model_copy(update={"schema_version": 2})
    b.producer().send(PINGS_TOPIC, b"BUS-101", p.model_dump_json().encode())
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    assert pipe.run_once().dead == 1
    assert conn.execute("SELECT reason FROM dead_letters").fetchone()["reason"] == "unsupported_schema"


def test_second_day_new_trips_are_tracked(conn, sim):
    b = MemoryBroker()
    publish(day_pings(sim, 0) + day_pings(sim, 1), b.producer())
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    drain(pipe)
    trips = {r["trip_id"][:2] for r in conn.execute("SELECT DISTINCT trip_id FROM notifications")}
    assert trips == {"D0", "D1"}
    per_day = {d: conn.execute("SELECT count(*) AS n FROM notifications WHERE trip_id LIKE %s",
                               (d + "-%",)).fetchone()["n"] for d in ("D0", "D1")}
    assert per_day["D0"] == per_day["D1"]               # same morning replayed -> same alerts


def test_poison_payloads_are_all_rejected(conn, sim):
    b = MemoryBroker()
    for v in POISON:
        b.producer().send(PINGS_TOPIC, b"BUS-101", v)
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    assert pipe.run_once().dead == len(POISON)


def test_api_db_mode_serves_what_the_consumer_wrote(conn, sim, monkeypatch):
    from fastapi.testclient import TestClient

    from fleetops import api

    b = MemoryBroker()
    publish(day_pings(sim, 0), b.producer())
    drain(Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn))

    monkeypatch.setattr(api, "MODE", "db")
    monkeypatch.setenv("FLEETOPS_DB", TEST_DB)
    with TestClient(api.app) as c:
        h = c.get("/health").json()
        assert h["mode"] == "db" and h["sim_now"] > 0
        buses = c.get("/buses").json()["buses"]
        assert len(buses) == len(sim.bus_route) and all(x["finished"] for x in buses)
        assert len(c.get("/routes").json()) == len(sim.routes)
        n = c.get("/stats").json()["counters"]["notifications"]
        assert n == len(db_alerts(conn)) and len(c.get("/notifications?limit=1000").json()) == n
        s = next(iter(sim.routes.values())).stops[2]
        near = c.get(f"/stops/near?lat={s.lat}&lon={s.lon}&radius_m=100").json()
        assert near[0]["stop_id"] == s.stop_id
        assert c.post("/pings", json=day_pings(sim, 0)[0].model_dump()).status_code == 405
        assert c.get("/buses/NOPE").status_code == 404


def test_committed_batch_is_visible_to_other_connections(conn, sim):
    """Regression: an implicit outer transaction turned every save into an
    uncommitted savepoint, invisible to the API while Kafka offsets advanced."""
    b = MemoryBroker()
    publish(day_pings(sim, 0)[:300], b.producer())
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    pipe.run_once(max_records=300)
    with db.connect(TEST_DB) as other:
        n = other.execute("SELECT count(*) AS n FROM bus_state").fetchone()["n"]
    assert n == len(sim.bus_route)


def test_api_metrics_do_not_include_consumer_metrics(conn, sim):
    """Regression: in db mode the API imported startup helpers from consumer.py,
    which registered the consumer's metrics in the API process. Prometheus
    scrapes both, so Grafana showed a second, always-zero series per panel.
    Runs in a fresh interpreter so this test process's imports don't interfere."""
    import subprocess
    import sys

    code = (
        "from fastapi.testclient import TestClient\n"
        "from fleetops import api\n"
        "api.MODE = 'db'\n"
        "with TestClient(api.app) as c:\n"
        "    print('fleetops_consumer' in c.get('/metrics').text)\n"
    )
    env = {**os.environ, "FLEETOPS_DB": TEST_DB}
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, check=True)
    assert out.stdout.strip().splitlines()[-1] == "False"


def test_producer_restart_is_tracked_not_dropped(conn, sim):
    """Regression (seen live in Grafana): a restarted producer replayed day 0 with
    the same ids, so the consumer dropped 100% of its pings. With the virtual
    clock and per-start tag, a restart starts new trips that are accepted."""
    from fleetops.producer import virtual_now

    b = MemoryBroker()
    speedup, wall = 10.0, 1_800_000_000.0
    first = day_pings(sim, 0, start_ts=virtual_now(speedup, wall), tag="run1-")
    publish(first[: len(first) // 2], b.producer())
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    drain(pipe)
    # The real producer paces pings on the virtual clock, so the last one sent was
    # sent at wall time (its offset / speedup). Restart 60 real seconds after that.
    sent_until = wall + (first[len(first) // 2 - 1].ts - first[0].ts) / speedup
    restarted = day_pings(sim, 0, start_ts=virtual_now(speedup, sent_until + 60), tag="run2-")
    assert restarted[0].ts > first[len(first) // 2 - 1].ts      # virtual time moved forward
    publish(restarted, b.producer())
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    outcomes = {}
    while True:
        r = pipe.run_once(max_records=200)
        if r.consumed == 0:
            break
        for k, v in r.outcomes.items():
            outcomes[k] = outcomes.get(k, 0) + v
    assert outcomes.get("accepted", 0) > 0.85 * len(restarted)
    trips = {r["trip_id"].split("D0")[0] for r in conn.execute("SELECT DISTINCT trip_id FROM notifications")}
    assert "run2-" in trips


def test_virtual_clock_is_monotonic_and_faster():
    from fleetops.producer import virtual_now
    a, b = virtual_now(10, 1_800_000_000.0), virtual_now(10, 1_800_000_001.0)
    assert b - a == pytest.approx(10.0)
    assert virtual_now(0, 1_800_000_000.0) == 1_800_000_000.0
