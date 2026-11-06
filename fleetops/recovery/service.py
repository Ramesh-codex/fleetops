"""Recovery plans in Postgres: create, approve, reject, replan, release, audit.

Every state change writes an audit_log row in the same transaction.
"""

from __future__ import annotations

import random
from dataclasses import asdict

from psycopg.types.json import Jsonb

from .planner import Proposal
from .validator import validate
from .view import FleetView

ROLES_THAT_DECIDE = {"dispatcher", "admin"}


class Forbidden(PermissionError):
    pass


class Conflict(RuntimeError):
    def __init__(self, message: str, detail: dict | None = None):
        super().__init__(message)
        self.detail = detail or {}


# ------------------------------------------------------------------ seeding
def seed_fleet(conn, seed: int) -> None:
    """Synthetic depot, spare buses, drivers and riders per route. Idempotent."""
    if conn.execute("SELECT 1 FROM depots LIMIT 1").fetchone():
        return
    rng = random.Random(seed + 1000)
    with conn.transaction():
        conn.execute("INSERT INTO depots VALUES ('DEP-1', 'North Yard', ST_SetSRID(ST_MakePoint(-121.8863, 37.3520), 4326)),"
                     " ('DEP-2', 'South Yard', ST_SetSRID(ST_MakePoint(-121.8700, 37.3130), 4326))")
        conn.execute("INSERT INTO spare_buses VALUES "
                     "('SP-201','DEP-1',72,'available'), ('SP-202','DEP-1',24,'available'),"
                     "('SP-203','DEP-2',72,'available'), ('SP-204','DEP-2',72,'out_of_service'),"
                     "('SP-205','DEP-2',48,'available')")
        conn.execute("INSERT INTO drivers VALUES "
                     "('DRV-11','Driver A','DEP-1','available'), ('DRV-12','Driver B','DEP-1','off_shift'),"
                     "('DRV-21','Driver C','DEP-2','available'), ('DRV-22','Driver D','DEP-2','available')")
        for r in conn.execute("SELECT route_id FROM routes ORDER BY route_id").fetchall():
            conn.execute("UPDATE routes SET riders = %s WHERE route_id = %s", (rng.randint(20, 66), r["route_id"]))


# --------------------------------------------------------------------- view
def load_view(conn, incident_id: str, *, excluded: set[str] | None = None,
              reservations: bool = True) -> FleetView:
    inc = conn.execute("SELECT incident_id, type, bus_id, opened_ts, closed_ts, detail FROM incidents "
                       "WHERE incident_id = %s", (incident_id,)).fetchone()
    if inc is None:
        raise KeyError(incident_id)
    bus = conn.execute(
        "SELECT b.bus_id, b.route_id, s.trip_id, ST_Y(s.geom) AS lat, ST_X(s.geom) AS lon, s.last_ts, "
        "COALESCE(s.finished, false) AS finished, COALESCE(s.next_stop_idx, 0) AS next_stop_idx, "
        "COALESCE(s.d, 0) AS progress_m, COALESCE(s.gps_lost, false) AS gps_lost "
        "FROM buses b LEFT JOIN bus_state s USING (bus_id) WHERE b.bus_id = %s", (inc["bus_id"],)).fetchone()
    route = conn.execute("SELECT route_id, name, riders FROM routes WHERE route_id = %s", (bus["route_id"],)).fetchone()
    route = dict(route)
    route["stops"] = list(conn.execute(
        "SELECT stop_id, name, ST_Y(geom) AS lat, ST_X(geom) AS lon, dist_m FROM stops "
        "WHERE route_id = %s ORDER BY seq", (bus["route_id"],)))
    depots = {d["depot_id"]: {"name": d["name"], "lat": d["lat"], "lon": d["lon"]} for d in conn.execute(
        "SELECT depot_id, name, ST_Y(geom) AS lat, ST_X(geom) AS lon FROM depots")}
    spares = [dict(r) for r in conn.execute("SELECT bus_id, depot_id, capacity, status FROM spare_buses ORDER BY bus_id")]
    drivers = [dict(r) for r in conn.execute("SELECT driver_id, name, depot_id, status FROM drivers ORDER BY driver_id")]
    now = conn.execute("SELECT max(last_ts) AS t FROM bus_state").fetchone()["t"] or bus["last_ts"] or 0.0
    # Soft reservations: without them every concurrent breakdown was offered the
    # same nearest spare and driver, and all but the first approval would fail.
    # Planning only. At approval the row locks and real assignment status decide
    # (first approval wins); applying reservations there deadlocked two pending
    # plans that named the same spare: each blocked the other.
    held = [] if not reservations else list(conn.execute(
        "SELECT spare_bus_id, driver_id FROM recovery_plans "
        "WHERE status = 'pending_approval' AND incident_id <> %s", (incident_id,)))
    return FleetView(incident=dict(inc), bus=dict(bus), route=route, depots=depots, spares=spares,
                     drivers=drivers, now_ts=now, excluded_spares=excluded or set(),
                     reserved_spares={h["spare_bus_id"] for h in held if h["spare_bus_id"]},
                     reserved_drivers={h["driver_id"] for h in held if h["driver_id"]})


def _audit(conn, actor: str, role: str, action: str, target: str, detail: dict) -> None:
    conn.execute("INSERT INTO audit_log (actor, role, action, target, detail) VALUES (%s, %s, %s, %s, %s)",
                 (actor, role, action, target, Jsonb(detail)))


# --------------------------------------------------------------- planning
def open_breakdowns_without_plan(conn) -> list[str]:
    return [r["incident_id"] for r in conn.execute(
        "SELECT i.incident_id FROM incidents i WHERE i.type = 'BREAKDOWN_SUSPECTED' AND i.closed_ts IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM recovery_plans p WHERE p.incident_id = i.incident_id) "
        "ORDER BY i.opened_ts")]


def _summary(view: FleetView, p: Proposal) -> str:
    stop = view.remaining_stops()[0]["stop_id"] if view.remaining_stops() else "end of route"
    head = f"{view.bus['bus_id']} broke down on {view.route['name']} before {stop}"
    if not p.feasible:
        return f"{head}. No feasible replacement: {p.rationale}"
    v = p.validation
    return (f"{head}. Send {p.spare_bus_id} ({v.capacity} seats for {v.riders} riders) with driver "
            f"{p.driver_id}; arrives in about {v.eta_s / 60:.0f} min.")


def create_plan(conn, incident_id: str, planner, *, attempt: int = 1, excluded: set[str] | None = None,
                actor: str = "recovery-agent") -> dict | None:
    """Plan one incident. Idempotent per (incident, attempt): a duplicate call
    (e.g. a duplicate incident event) returns None and changes nothing."""
    if conn.execute("SELECT 1 FROM recovery_plans WHERE incident_id = %s AND attempt = %s",
                    (incident_id, attempt)).fetchone():
        return None
    view = load_view(conn, incident_id, excluded=excluded)
    p: Proposal = planner.propose(view)
    v = p.validation
    status = "pending_approval" if p.feasible else "no_feasible_plan"
    plan_id = f"PLAN-{incident_id.removeprefix('INC-')}-{attempt}"
    row = {
        "plan_id": plan_id, "incident_id": incident_id, "attempt": attempt, "status": status,
        "planner": p.planner, "bus_id": view.bus["bus_id"], "trip_id": view.bus.get("trip_id") or "",
        "route_id": view.route["route_id"], "spare_bus_id": p.spare_bus_id, "driver_id": p.driver_id,
        "eta_s": v.eta_s if p.feasible else None, "riders": v.riders, "capacity": v.capacity,
        "summary": _summary(view, p), "rationale": p.rationale,
        "evidence": Jsonb(p.evidence), "checks": Jsonb([asdict(c) for c in v.checks]),
        # System-detected risks first; the model's own risks are kept but labelled.
        "risks": Jsonb(v.risks + [f"(model) {r}" for r in p.model_risks]), "llm_error": p.llm_error,
    }
    with conn.transaction():
        cur = conn.execute(
            "INSERT INTO recovery_plans (plan_id, incident_id, attempt, status, planner, bus_id, trip_id, route_id, "
            "spare_bus_id, driver_id, eta_s, riders, capacity, summary, rationale, evidence, checks, risks, llm_error) "
            "VALUES (%(plan_id)s, %(incident_id)s, %(attempt)s, %(status)s, %(planner)s, %(bus_id)s, %(trip_id)s, "
            "%(route_id)s, %(spare_bus_id)s, %(driver_id)s, %(eta_s)s, %(riders)s, %(capacity)s, %(summary)s, "
            "%(rationale)s, %(evidence)s, %(checks)s, %(risks)s, %(llm_error)s) "
            "ON CONFLICT (incident_id, attempt) DO NOTHING", row)
        if cur.rowcount == 0:
            return None          # another worker got there first
        _audit(conn, actor, "system", "plan_created", plan_id,
               {"status": status, "planner": p.planner, "spare": p.spare_bus_id, "llm_error": p.llm_error})
    return get_plan(conn, plan_id)


def get_plan(conn, plan_id: str) -> dict | None:
    return conn.execute("SELECT * FROM recovery_plans WHERE plan_id = %s", (plan_id,)).fetchone()


def list_plans(conn, limit: int = 50) -> list[dict]:
    return list(conn.execute("SELECT * FROM recovery_plans ORDER BY created_at DESC LIMIT %s", (limit,)))


# --------------------------------------------------------------- decisions
def _require(role: str, actor: str, action: str, target: str, conn) -> None:
    if role not in ROLES_THAT_DECIDE:
        with conn.transaction():
            _audit(conn, actor, role, f"{action}_denied", target, {"reason": "role may not decide"})
        raise Forbidden(f"role {role!r} cannot {action} recovery plans")


def approve(conn, plan_id: str, actor: str, role: str) -> dict:
    """Approve a pending plan. Re-validates against the CURRENT fleet inside a
    transaction that locks the spare bus and driver rows, so two dispatchers
    can never assign the same spare, and a plan that went stale can't execute."""
    _require(role, actor, "approve", plan_id, conn)
    with conn.transaction():
        plan = conn.execute("SELECT * FROM recovery_plans WHERE plan_id = %s FOR UPDATE", (plan_id,)).fetchone()
        if plan is None:
            raise KeyError(plan_id)
        if plan["status"] != "pending_approval":
            raise Conflict(f"plan is {plan['status']}, not pending approval")
        conn.execute("SELECT 1 FROM spare_buses WHERE bus_id = %s FOR UPDATE", (plan["spare_bus_id"],))
        conn.execute("SELECT 1 FROM drivers WHERE driver_id = %s FOR UPDATE", (plan["driver_id"],))
        view = load_view(conn, plan["incident_id"], reservations=False)
        v = validate(view, plan["spare_bus_id"], plan["driver_id"])
        blocked = not v.feasible
        if blocked:
            # Record the block and COMMIT it; raising inside this block would
            # roll back the audit row along with everything else.
            _audit(conn, actor, role, "approve_blocked", plan_id, {"failures": v.failures()})
            conn.execute("UPDATE recovery_plans SET status = 'superseded', decided_at = now(), decided_by = %s, "
                         "decision_note = %s WHERE plan_id = %s",
                         (actor, "blocked at approval: " + "; ".join(v.failures()), plan_id))
        else:
            _assign(conn, plan, view, v, actor, role, plan_id)
    if blocked:
        raise Conflict("plan no longer passes validation", {"failures": v.failures()})
    return get_plan(conn, plan_id)


def _assign(conn, plan, view, v, actor, role, plan_id) -> None:
    """Apply an approved plan. Called inside approve()'s transaction."""
    conn.execute("UPDATE spare_buses SET status = 'assigned' WHERE bus_id = %s", (plan["spare_bus_id"],))
    conn.execute("UPDATE drivers SET status = 'assigned' WHERE driver_id = %s", (plan["driver_id"],))
    conn.execute("UPDATE recovery_plans SET status = 'approved', decided_at = now(), decided_by = %s, "
                 "eta_s = %s WHERE plan_id = %s", (actor, v.eta_s, plan_id))
    # Only now, after a human decision, do parents hear about the replacement.
    sent_ts = view.now_ts
    for s in view.remaining_stops():
        conn.execute(
            "INSERT INTO notifications (kind, bus_id, trip_id, stop_id, sent_ts, predicted_arrival_ts) "
            "VALUES ('REPLACEMENT_DISPATCHED', %s, %s, %s, %s, NULL) "
            "ON CONFLICT (trip_id, stop_id, kind) DO NOTHING",
            (plan["bus_id"], plan["trip_id"], s["stop_id"], sent_ts))
    _audit(conn, actor, role, "plan_approved", plan_id,
           {"spare": plan["spare_bus_id"], "driver": plan["driver_id"], "eta_s": v.eta_s,
            "parents_notified": len(view.remaining_stops())})


def reject(conn, plan_id: str, actor: str, role: str, note: str = "") -> dict:
    _require(role, actor, "reject", plan_id, conn)
    with conn.transaction():
        plan = conn.execute("SELECT * FROM recovery_plans WHERE plan_id = %s FOR UPDATE", (plan_id,)).fetchone()
        if plan is None:
            raise KeyError(plan_id)
        if plan["status"] != "pending_approval":
            raise Conflict(f"plan is {plan['status']}, not pending approval")
        conn.execute("UPDATE recovery_plans SET status = 'rejected', decided_at = now(), decided_by = %s, "
                     "decision_note = %s WHERE plan_id = %s", (actor, note[:500], plan_id))
        _audit(conn, actor, role, "plan_rejected", plan_id, {"note": note[:500]})
    return get_plan(conn, plan_id)


def replan(conn, incident_id: str, planner, actor: str, role: str) -> dict:
    """New attempt after a rejection; spares the dispatcher rejected are excluded."""
    _require(role, actor, "replan", incident_id, conn)
    rows = list(conn.execute("SELECT attempt, status, spare_bus_id FROM recovery_plans WHERE incident_id = %s",
                             (incident_id,)))
    if any(r["status"] == "pending_approval" for r in rows):
        raise Conflict("a plan for this incident is already waiting for a decision")
    if any(r["status"] == "approved" for r in rows):
        raise Conflict("this incident already has an approved plan")
    excluded = {r["spare_bus_id"] for r in rows if r["status"] == "rejected" and r["spare_bus_id"]}
    attempt = max((r["attempt"] for r in rows), default=0) + 1
    plan = create_plan(conn, incident_id, planner, attempt=attempt, excluded=excluded, actor=actor)
    if plan is None:
        raise Conflict("plan was created concurrently")
    return plan


# ------------------------------------------------------------ housekeeping
def supersede_stale(conn) -> int:
    """Pending plans whose breakdown already ended (bus moving again) can't be approved."""
    with conn.transaction():
        rows = list(conn.execute(
            "UPDATE recovery_plans p SET status = 'superseded', decided_at = now(), decided_by = 'system', "
            "decision_note = 'breakdown ended before a decision' FROM incidents i "
            "WHERE p.incident_id = i.incident_id AND p.status = 'pending_approval' AND i.closed_ts IS NOT NULL "
            "RETURNING p.plan_id"))
        for r in rows:
            _audit(conn, "recovery-worker", "system", "plan_superseded", r["plan_id"], {})
    return len(rows)


def release_finished(conn) -> int:
    """Return spares and drivers once the trip they rescued is over."""
    with conn.transaction():
        rows = list(conn.execute(
            "SELECT p.plan_id, p.spare_bus_id, p.driver_id FROM recovery_plans p "
            "JOIN bus_state s ON s.bus_id = p.bus_id "
            "JOIN spare_buses sb ON sb.bus_id = p.spare_bus_id "
            "WHERE p.status = 'approved' AND sb.status = 'assigned' "
            "AND (s.trip_id IS DISTINCT FROM p.trip_id OR s.finished) FOR UPDATE OF sb"))
        for r in rows:
            conn.execute("UPDATE spare_buses SET status = 'available' WHERE bus_id = %s", (r["spare_bus_id"],))
            conn.execute("UPDATE drivers SET status = 'available' WHERE driver_id = %s", (r["driver_id"],))
            _audit(conn, "recovery-worker", "system", "spare_released", r["plan_id"],
                   {"spare": r["spare_bus_id"], "driver": r["driver_id"]})
    return len(rows)


def audit_log(conn, limit: int = 100) -> list[dict]:
    return list(conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT %s", (limit,)))
