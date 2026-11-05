"""PostgreSQL/PostGIS persistence.

Write path (consumer): save_batch() stores changed bus state, new alerts,
incidents and dead letters in ONE transaction.
Read path (API): small query functions returning plain dicts.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from .models import Incident, IncidentType, Notification, Route, Stop

MIGRATIONS = Path(__file__).parent / "migrations"
DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/fleetops"


def dsn() -> str:
    return os.getenv("FLEETOPS_DB", DEFAULT_DSN)


def connect(url: str | None = None) -> psycopg.Connection:
    # autocommit=True is deliberate. Without it, any plain read opens an
    # implicit transaction, and every later `with conn.transaction():` becomes
    # a SAVEPOINT inside it: the consumer thinks a batch is committed, commits
    # the Kafka offsets, and the rows are invisible to everyone else (and lost
    # on crash). With autocommit, each transaction() block is a real COMMIT.
    return psycopg.connect(url or dsn(), row_factory=dict_row, autocommit=True)


# ------------------------------------------------------------------ migrations
def migrate(conn: psycopg.Connection) -> list[str]:
    """Apply migrations/NNN_*.sql in order, once each. Returns names applied."""
    with conn.transaction():
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, "
                     "applied_at timestamptz NOT NULL DEFAULT now())")
        # Serialise concurrent migrators (e.g. API and consumer starting together).
        conn.execute("SELECT pg_advisory_xact_lock(7210001)")
        done = {r["name"] for r in conn.execute("SELECT name FROM schema_migrations")}
        applied = []
        for f in sorted(MIGRATIONS.glob("*.sql")):
            if f.name in done:
                continue
            conn.execute(f.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (f.name,))
            applied.append(f.name)
    return applied


# ---------------------------------------------------------------------- routes
def _linestring(path: list[tuple[float, float]]) -> str:
    return "LINESTRING(" + ", ".join(f"{lon} {lat}" for lat, lon in path) + ")"


def seed_routes(conn: psycopg.Connection, routes: dict[str, Route], bus_route: dict[str, str]) -> None:
    """Idempotently load routes, stops and buses."""
    with conn.transaction():
        for r in routes.values():
            conn.execute(
                "INSERT INTO routes (route_id, name, path) VALUES (%s, %s, ST_GeomFromText(%s, 4326)) "
                "ON CONFLICT (route_id) DO UPDATE SET name = EXCLUDED.name, path = EXCLUDED.path",
                (r.route_id, r.name, _linestring(r.path)))
            for i, s in enumerate(r.stops):
                conn.execute(
                    "INSERT INTO stops (stop_id, route_id, seq, name, dist_m, geom) "
                    "VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) "
                    "ON CONFLICT (stop_id) DO UPDATE SET name = EXCLUDED.name, dist_m = EXCLUDED.dist_m, "
                    "geom = EXCLUDED.geom",
                    (s.stop_id, r.route_id, i, s.name, s.dist_m, s.lon, s.lat))
        for bus, rid in bus_route.items():
            conn.execute("INSERT INTO buses (bus_id, route_id) VALUES (%s, %s) "
                         "ON CONFLICT (bus_id) DO UPDATE SET route_id = EXCLUDED.route_id", (bus, rid))


def load_routes(conn: psycopg.Connection) -> tuple[dict[str, Route], dict[str, str]]:
    routes: dict[str, Route] = {}
    for r in conn.execute("SELECT route_id, name, ST_AsGeoJSON(path)::json AS path FROM routes ORDER BY route_id"):
        path = [(lat, lon) for lon, lat in r["path"]["coordinates"]]
        routes[r["route_id"]] = Route(route_id=r["route_id"], name=r["name"], path=path, stops=[])
    for s in conn.execute("SELECT stop_id, route_id, name, dist_m, ST_Y(geom) AS lat, ST_X(geom) AS lon "
                          "FROM stops ORDER BY route_id, seq"):
        routes[s["route_id"]].stops.append(
            Stop(stop_id=s["stop_id"], name=s["name"], lat=s["lat"], lon=s["lon"], dist_m=s["dist_m"]))
    bus_route = {b["bus_id"]: b["route_id"] for b in conn.execute("SELECT bus_id, route_id FROM buses")}
    return routes, bus_route


# ------------------------------------------------------------------ write path
_UPSERT_BUS = """
INSERT INTO bus_state (bus_id, trip_id, last_seq, last_ts, d, speed_est, next_stop_idx, finished,
                       notified, next_stop, next_stop_eta_ts, gps_lost, geom, updated_at)
VALUES (%(bus_id)s, %(trip_id)s, %(last_seq)s, %(last_ts)s, %(d)s, %(speed_est)s, %(next_stop_idx)s,
        %(finished)s, %(notified)s, %(next_stop)s, %(next_stop_eta_ts)s, %(gps_lost)s,
        CASE WHEN %(lat)s::float8 IS NULL THEN NULL
             ELSE ST_SetSRID(ST_MakePoint(%(lon)s::float8, %(lat)s::float8), 4326) END,
        now())
ON CONFLICT (bus_id) DO UPDATE SET
    trip_id = EXCLUDED.trip_id, last_seq = EXCLUDED.last_seq, last_ts = EXCLUDED.last_ts,
    d = EXCLUDED.d, speed_est = EXCLUDED.speed_est, next_stop_idx = EXCLUDED.next_stop_idx,
    finished = EXCLUDED.finished, notified = EXCLUDED.notified, next_stop = EXCLUDED.next_stop,
    next_stop_eta_ts = EXCLUDED.next_stop_eta_ts, gps_lost = EXCLUDED.gps_lost,
    geom = EXCLUDED.geom, updated_at = now()
"""

_INSERT_NOTE = """
INSERT INTO notifications (bus_id, trip_id, stop_id, sent_ts, predicted_arrival_ts)
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (trip_id, stop_id) DO NOTHING
"""

_UPSERT_INCIDENT = """
INSERT INTO incidents (incident_id, type, bus_id, opened_ts, closed_ts, detail)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (incident_id) DO UPDATE SET closed_ts = COALESCE(EXCLUDED.closed_ts, incidents.closed_ts)
"""


def save_batch(conn: psycopg.Connection, buses: list[dict], notes: list[Notification],
               incidents: list[Incident], dead: list[dict]) -> dict:
    """One transaction for everything a batch produced.

    Returns how many alert inserts the UNIQUE constraint rejected. With state
    restore working this should always be 0 — a non-zero value means the
    tracker tried to alert twice and only the database stopped it.
    """
    rejected = 0
    with conn.transaction():
        with conn.cursor() as cur:
            if buses:
                cur.executemany(_UPSERT_BUS, buses)
            for n in notes:
                cur.execute(_INSERT_NOTE, (n.bus_id, n.trip_id, n.stop_id, n.sent_ts, n.predicted_arrival_ts))
                rejected += cur.rowcount == 0
            if incidents:
                cur.executemany(_UPSERT_INCIDENT, [
                    (i.incident_id, i.type.value, i.bus_id, i.opened_ts, i.closed_ts, i.detail)
                    for i in incidents])
            if dead:
                cur.executemany(
                    'INSERT INTO dead_letters (topic, partition, "offset", reason, payload) '
                    "VALUES (%(topic)s, %(partition)s, %(offset)s, %(reason)s, %(payload)s) "
                    'ON CONFLICT (topic, partition, "offset") DO NOTHING', dead)
    return {"alerts_rejected_by_db": rejected}


def load_state(conn: psycopg.Connection) -> tuple[list[dict], list[Incident]]:
    buses = list(conn.execute(
        "SELECT bus_id, trip_id, last_seq, last_ts, d, speed_est, next_stop_idx, finished, notified, "
        "ST_Y(geom) AS lat, ST_X(geom) AS lon FROM bus_state"))
    incs = [Incident(incident_id=r["incident_id"], type=IncidentType(r["type"]), bus_id=r["bus_id"],
                     opened_ts=r["opened_ts"], closed_ts=r["closed_ts"], detail=r["detail"])
            for r in conn.execute("SELECT * FROM incidents WHERE closed_ts IS NULL")]
    return buses, incs


def reset(conn: psycopg.Connection) -> None:
    """Clear runtime data (keeps routes). For tests and benchmarks."""
    with conn.transaction():
        conn.execute("TRUNCATE bus_state, notifications, incidents, dead_letters")


# ------------------------------------------------------------------- read path
def q_buses(conn) -> list[dict]:
    return list(conn.execute(
        "SELECT b.bus_id, b.route_id, s.trip_id, ST_Y(s.geom) AS lat, ST_X(s.geom) AS lon, s.last_ts, "
        "round(s.d::numeric, 1)::float8 AS progress_m, round(s.speed_est::numeric, 2)::float8 AS speed_est_mps, "
        "COALESCE(s.finished, false) AS finished, s.next_stop, s.next_stop_eta_ts, "
        "COALESCE(s.gps_lost, false) AS gps_lost "
        "FROM buses b LEFT JOIN bus_state s USING (bus_id) ORDER BY b.bus_id"))


def q_notifications(conn, limit: int) -> list[dict]:
    return list(conn.execute(
        "SELECT bus_id, trip_id, stop_id, sent_ts, predicted_arrival_ts FROM notifications "
        "ORDER BY id DESC LIMIT %s", (limit,)))


def q_incidents(conn, limit: int = 100) -> list[dict]:
    return list(conn.execute(
        "SELECT incident_id, type, bus_id, opened_ts, closed_ts, detail FROM incidents "
        "ORDER BY opened_ts DESC LIMIT %s", (limit,)))


def q_counts(conn) -> dict:
    return conn.execute(
        "SELECT (SELECT count(*) FROM notifications) AS notifications, "
        "(SELECT count(*) FROM incidents) AS incidents, "
        "(SELECT count(*) FROM dead_letters) AS dead_letters").fetchone()


def q_stops_near(conn, lat: float, lon: float, radius_m: float) -> list[dict]:
    """PostGIS: stops within radius_m metres, nearest first."""
    return list(conn.execute(
        "SELECT stop_id, route_id, name, ST_Y(geom) AS lat, ST_X(geom) AS lon, "
        "ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography) AS distance_m "
        "FROM stops WHERE ST_DWithin(geom::geography, "
        "ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography, %(r)s) "
        "ORDER BY distance_m LIMIT 50", {"lat": lat, "lon": lon, "r": radius_m}))
