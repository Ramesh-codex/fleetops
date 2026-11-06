"""Startup helpers shared by the API, consumer and producer.

Kept apart from consumer.py on purpose: importing consumer.py registers the
consumer's Prometheus metrics, and the API used to import these helpers from
there, so it published a second, always-zero copy of every consumer metric.
"""

from __future__ import annotations

import time

from . import db


def wait_for(make, what: str, attempts: int = 60, delay: float = 2.0):
    """Retry a connection until the dependency is up (containers start in any order)."""
    last = None
    for _ in range(attempts):
        try:
            return make()
        except Exception as e:  # noqa: BLE001 - any connection failure means 'not ready yet'
            last = e
            print(f"waiting for {what}: {type(e).__name__}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"{what} not reachable") from last


def ensure_seeded(conn) -> None:
    from .producer import sim_config_from_env
    from .simulator import simulate
    db.migrate(conn)
    cfg = sim_config_from_env()
    if not db.load_routes(conn)[0]:
        sim = simulate(cfg)
        db.seed_routes(conn, sim.routes, sim.bus_route)
    from .recovery.service import seed_fleet
    seed_fleet(conn, cfg.seed)
