"""Kafka -> tracker -> Postgres consumer.

    python -m fleetops.consumer

Delivery guarantee: at-least-once from Kafka, with effects applied once.
For every batch:
  1. decode + validate each message; bad ones go to the dead-letter topic/table
  2. apply good pings to the tracker
  3. write changed bus state + new alerts + incidents + dead letters to
     Postgres in ONE transaction
  4. only then commit the Kafka offsets

A crash between 3 and 4 means Kafka redelivers the batch. On restart the
tracker is rebuilt from bus_state, whose last_seq already covers those pings,
so they are dropped and no alert is sent twice. The UNIQUE(trip_id, stop_id, kind)
constraint on alerts is a second guard; `alerts_rejected_by_db` counts how
often it had to act (target: 0).

Env: FLEETOPS_KAFKA (localhost:9092), FLEETOPS_DB, FLEETOPS_GROUP (tracker),
     FLEETOPS_METRICS_PORT (9108), FLEETOPS_SEED / FLEETOPS_BUSES (route seeding)
"""

from __future__ import annotations

import json
import os
import signal
import time
from dataclasses import dataclass, field

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import ValidationError

from . import db
from .broker import DLQ_TOPIC, Consumer, Message, Producer
from .geo import LocalFrame
from .models import SCHEMA_VERSION, GpsPing
from .runtime import ensure_seeded, wait_for  # noqa: F401  (re-exported)
from .tracker import Tracker

MSGS = Counter("fleetops_consumer_messages_total", "Messages consumed by outcome", ["outcome"])
DLQ = Counter("fleetops_consumer_dead_letters_total", "Messages sent to the dead-letter topic", ["reason"])
ALERTS = Counter("fleetops_consumer_alerts_total", "Parent alerts written")
DB_REJECTS = Counter("fleetops_consumer_alerts_rejected_by_db_total",
                     "Alert inserts blocked by the UNIQUE constraint (should stay 0)")
BATCH = Histogram("fleetops_consumer_batch_seconds", "Process + persist + commit time per batch",
                  buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5))
E2E = Histogram("fleetops_e2e_latency_seconds", "Producer send -> committed in Postgres",
                buckets=(.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30))
LAG = Gauge("fleetops_consumer_lag_messages", "Messages in Kafka not yet committed by this group")
OPEN_GPS_LOST = Gauge("fleetops_consumer_gps_lost_open", "Buses currently in GPS_LOST")


class SimulatedCrash(RuntimeError):
    """Raised by tests to kill the consumer between the DB write and the offset commit."""


@dataclass
class BatchResult:
    consumed: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)
    dead: int = 0
    alerts: int = 0
    alerts_rejected_by_db: int = 0
    e2e_s: list[float] = field(default_factory=list)


def classify(m: Message) -> tuple[GpsPing | None, str | None]:
    """Decode one message. Returns (ping, None) or (None, reason)."""
    try:
        raw = json.loads(m.value)
    except (ValueError, UnicodeDecodeError):
        return None, "invalid_json"
    if not isinstance(raw, dict):
        return None, "invalid_json"
    if raw.get("schema_version") != SCHEMA_VERSION:
        return None, "unsupported_schema"
    try:
        return GpsPing.model_validate(raw), None
    except ValidationError:
        return None, "validation_error"


class Pipeline:
    def __init__(self, consumer: Consumer, dlq: Producer, conn, tracker: Tracker):
        self.consumer, self.dlq, self.conn, self.tracker = consumer, dlq, conn, tracker
        # Event-time watermarks. Each bus's pings live in one partition, and
        # partitions advance at different speeds, so a bus is judged against
        # its own partition's progress — not the fastest partition's.
        self.part_wm: dict[tuple[str, int], float] = {}
        self.part_idle: dict[tuple[str, int], int] = {}     # consecutive polls with no data
        self.bus_part: dict[str, tuple[str, int]] = {}
        self.watermark = max((b.last_ts or 0.0) for b in tracker.buses.values()) if tracker.buses else 0.0

    IDLE_POLLS = 3

    def event_now(self, bus_id: str) -> float:
        part = self.bus_part.get(bus_id)
        if part is None or self.part_idle.get(part, 0) >= self.IDLE_POLLS:
            # Unknown, or the whole partition has gone quiet while others still
            # flow: fall back to the global watermark so silence is detected.
            return self.watermark
        return self.part_wm[part]

    @classmethod
    def from_db(cls, consumer: Consumer, dlq: Producer, conn, *, restore: bool = True, **tracker_kw) -> "Pipeline":
        routes, bus_route = db.load_routes(conn)
        first = next(iter(routes.values())).stops[0]
        tracker = Tracker(routes, LocalFrame(first.lat, first.lon), bus_route, **tracker_kw)
        if restore:
            buses, incs = db.load_state(conn)
            tracker.restore(buses, incs)
        return cls(consumer, dlq, conn, tracker)

    def process(self, msgs: list[Message], *, crash_before_commit: bool = False) -> BatchResult:
        res = BatchResult(consumed=len(msgs))
        if not msgs:
            return res
        t0 = time.perf_counter()
        dead: list[dict] = []
        offsets: dict[tuple[str, int], int] = {}
        for m in msgs:
            offsets[(m.topic, m.partition)] = max(offsets.get((m.topic, m.partition), 0), m.offset + 1)
            ping, reason = classify(m)
            if ping is None:
                dead.append({"topic": m.topic, "partition": m.partition, "offset": m.offset,
                             "reason": reason, "payload": m.value.decode("utf-8", "replace")[:2000]})
                self.dlq.send(DLQ_TOPIC, m.key, m.value)
                DLQ.labels(reason).inc()
                continue
            outcome = self.tracker.ingest(ping)
            res.outcomes[outcome] = res.outcomes.get(outcome, 0) + 1
            MSGS.labels(outcome).inc()
            part = (m.topic, m.partition)
            self.bus_part[ping.bus_id] = part
            self.part_wm[part] = max(self.part_wm.get(part, 0.0), ping.ts)
            self.watermark = max(self.watermark, ping.ts)
        if dead:
            self.dlq.flush()
            MSGS.labels("dead_letter").inc(len(dead))
        seen = {(m.topic, m.partition) for m in msgs}
        for part in self.part_wm:
            self.part_idle[part] = 0 if part in seen else self.part_idle.get(part, 0) + 1
        self.tracker.check_stale(self.event_now)
        buses, notes, incs = self.tracker.drain_changes()
        saved = db.save_batch(self.conn, buses, notes, incs, dead)

        if crash_before_commit:
            raise SimulatedCrash("killed after DB commit, before Kafka offset commit")
        self.consumer.commit(offsets)

        now_ms = time.time() * 1000
        res.e2e_s = [(now_ms - m.timestamp_ms) / 1000 for m in msgs]
        for s in res.e2e_s:
            E2E.observe(max(0.0, s))
        res.dead, res.alerts = len(dead), len(notes) - saved["alerts_rejected_by_db"]
        res.alerts_rejected_by_db = saved["alerts_rejected_by_db"]
        ALERTS.inc(res.alerts)
        DB_REJECTS.inc(res.alerts_rejected_by_db)
        OPEN_GPS_LOST.set(sum(1 for b in self.tracker.buses.values() if b.bus_id in self.tracker._open_incident))
        BATCH.observe(time.perf_counter() - t0)
        return res

    def run_once(self, max_records: int = 500, timeout_ms: int = 500, **kw) -> BatchResult:
        res = self.process(self.consumer.poll(max_records, timeout_ms), **kw)
        lag = self.consumer.lag()
        if lag is not None:
            LAG.set(lag)
        return res


# ------------------------------------------------------------------------ main
def main() -> None:
    from .broker import PINGS_TOPIC, KafkaBroker

    bootstrap = os.getenv("FLEETOPS_KAFKA", "localhost:9092")
    conn = wait_for(lambda: db.connect(), "postgres")
    ensure_seeded(conn)
    broker = KafkaBroker(bootstrap)
    consumer = wait_for(lambda: broker.consumer(os.getenv("FLEETOPS_GROUP", "tracker"), PINGS_TOPIC), "kafka")
    pipe = Pipeline.from_db(consumer, broker.producer(), conn)
    start_http_server(int(os.getenv("FLEETOPS_METRICS_PORT", "9108")))
    print(f"consumer ready: {len(pipe.tracker.buses)} buses, restored watermark {pipe.watermark}", flush=True)

    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    while not stop:
        pipe.run_once()
    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
