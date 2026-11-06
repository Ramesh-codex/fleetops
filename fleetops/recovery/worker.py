"""Recovery worker: plans every open breakdown, supersedes stale plans, and
releases spares after their trip.

    python -m fleetops.recovery.worker

Env: FLEETOPS_DB, FLEETOPS_PLANNER (auto | rules | llm; auto = llm when
     ANTHROPIC_API_KEY is set, else rules), FLEETOPS_LLM_MODEL (claude-haiku-5-5),
     FLEETOPS_RECOVERY_INTERVAL (2 seconds)

Runs apart from the Kafka consumer on purpose: an LLM call takes seconds and
must never hold up GPS ingestion.
"""

from __future__ import annotations

import os
import signal
import time

from prometheus_client import Counter, start_http_server

from .. import db
from ..runtime import ensure_seeded, wait_for
from . import service
from .planner import LLMPlanner, RulePlanner

PLANS = Counter("fleetops_recovery_plans_total", "Recovery plans created", ["status", "planner"])
LLM_FALLBACKS = Counter("fleetops_recovery_llm_fallbacks_total", "LLM failures that fell back to the rule planner")


def make_planner():
    mode = os.getenv("FLEETOPS_PLANNER", "auto")
    has_key = bool(os.getenv("ANTHROPIC_API_KEY"))
    if mode == "rules" or (mode == "auto" and not has_key):
        return RulePlanner()
    import anthropic
    client = anthropic.Anthropic(timeout=30.0, max_retries=1)
    return LLMPlanner(client, model=os.getenv("FLEETOPS_LLM_MODEL", "claude-haiku-5-5"))


def run_once(conn, planner) -> list[dict]:
    created = []
    for iid in service.open_breakdowns_without_plan(conn):
        plan = service.create_plan(conn, iid, planner)
        if plan:
            PLANS.labels(plan["status"], plan["planner"]).inc()
            if plan["llm_error"]:
                LLM_FALLBACKS.inc()
            created.append(plan)
    service.supersede_stale(conn)
    service.release_finished(conn)
    return created


def main() -> None:
    conn = wait_for(lambda: db.connect(), "postgres")
    ensure_seeded(conn)
    planner = make_planner()
    start_http_server(int(os.getenv("FLEETOPS_METRICS_PORT", "9109")))
    interval = float(os.getenv("FLEETOPS_RECOVERY_INTERVAL", "2"))
    print(f"recovery worker ready, planner={planner.name}", flush=True)
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    while not stop:
        for p in run_once(conn, planner):
            print(f"{p['plan_id']}: {p['status']} via {p['planner']} - {p['summary']}", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
