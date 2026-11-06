"""Recovery agent evaluation cases, against real Postgres.

The LLM is replaced by a scripted fake client with the same response shape as
the Anthropic SDK, so every case is deterministic and free to run.
"""

import os
from types import SimpleNamespace

import psycopg
import pytest

from fleetops import db
from fleetops.broker import PINGS_TOPIC, MemoryBroker
from fleetops.consumer import Pipeline
from fleetops.producer import day_pings, publish
from fleetops.recovery import service
from fleetops.recovery.planner import LLMPlanner, RulePlanner
from fleetops.recovery.tools import ToolError, run_tool
from fleetops.recovery.worker import run_once
from fleetops.simulator import SimConfig, simulate

TEST_DB = os.getenv("FLEETOPS_TEST_DB")
pytestmark = pytest.mark.skipif(not TEST_DB, reason="FLEETOPS_TEST_DB not set")


# ------------------------------------------------------------------ fixtures
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
def world(conn):
    """A fleet where every bus breaks down; consume until the first breakdown is open."""
    sim = simulate(SimConfig(seed=51, n_buses=6, breakdown_prob=1.0))
    db.seed_routes(conn, sim.routes, sim.bus_route)
    service.seed_fleet(conn, 51)
    conn.execute("UPDATE routes SET riders = 40")          # fixed, so cases can tweak it
    b = MemoryBroker()
    publish(day_pings(sim, 0), b.producer())
    pipe = Pipeline.from_db(b.consumer("g", PINGS_TOPIC), b.producer(), conn)
    while not service.open_breakdowns_without_plan(conn):
        assert pipe.run_once(max_records=20).consumed, "stream ended without a breakdown"
    return SimpleNamespace(sim=sim, pipe=pipe, incident=service.open_breakdowns_without_plan(conn)[0])


def audit_actions(conn):
    return [r["action"] for r in conn.execute("SELECT action FROM audit_log ORDER BY id")]


# -------------------------------------------------------- fake Anthropic client
def tool_use(name, args, i):
    return SimpleNamespace(content=[SimpleNamespace(type="tool_use", id=f"tu_{i}", name=name, input=args)],
                           stop_reason="tool_use")


class ScriptedClient:
    """Plays back a list of (tool_name, args) calls, one per request."""

    def __init__(self, script):
        self.script, self.calls = list(script), []
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kw):
        # Snapshot: the agent keeps appending to the same messages list.
        self.calls.append({**kw, "messages": list(kw["messages"])})
        if not self.script:
            return SimpleNamespace(content=[SimpleNamespace(type="text", text="done")], stop_reason="end_turn")
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return tool_use(step[0], step[1], len(self.calls))


# ----------------------------------------------------------------- planning

def test_rule_planner_picks_nearest_feasible_spare(conn, world):
    plan = service.create_plan(conn, world.incident, RulePlanner())
    assert plan["status"] == "pending_approval" and plan["planner"] == "rules"
    view = service.load_view(conn, world.incident)
    feasible = [s["bus_id"] for s in view.spares if s["status"] == "available" and s["capacity"] >= 40]
    assert plan["spare_bus_id"] == min(feasible, key=view.eta_s)
    assert plan["eta_s"] == pytest.approx(view.eta_s(plan["spare_bus_id"]))
    assert all(c["ok"] for c in plan["checks"])
    assert "plan_created" in audit_actions(conn)


def test_case_no_replacement_bus_available(conn, world):
    conn.execute("UPDATE spare_buses SET status = 'out_of_service'")
    plan = service.create_plan(conn, world.incident, RulePlanner())
    assert plan["status"] == "no_feasible_plan" and plan["spare_bus_id"] is None
    assert "no spare bus is available" in plan["summary"]


def test_case_insufficient_capacity(conn, world):
    conn.execute("UPDATE routes SET riders = 90")
    plan = service.create_plan(conn, world.incident, RulePlanner())
    assert plan["status"] == "no_feasible_plan"
    assert "seats 90 riders" in plan["summary"]


def test_case_driver_unavailable(conn, world):
    conn.execute("UPDATE drivers SET status = 'off_shift'")
    plan = service.create_plan(conn, world.incident, RulePlanner())
    assert plan["status"] == "no_feasible_plan"
    assert "no available driver" in plan["summary"]


def test_case_stale_gps_is_flagged_as_risk(conn, world):
    inc = conn.execute("SELECT bus_id FROM incidents WHERE incident_id = %s", (world.incident,)).fetchone()
    conn.execute("UPDATE bus_state SET gps_lost = true WHERE bus_id = %s", (inc["bus_id"],))
    plan = service.create_plan(conn, world.incident, RulePlanner())
    assert any(r.startswith("stale GPS") for r in plan["risks"])


def test_partitions_out_of_step_do_not_flag_stale_gps(conn, world):
    """Regression: during a backlog some partitions are minutes ahead; a bus
    that is still reporting must not be called stale because of that."""
    conn.execute("UPDATE bus_state SET last_ts = last_ts + 600 WHERE bus_id = "
                 "(SELECT bus_id FROM bus_state WHERE bus_id <> "
                 "(SELECT bus_id FROM incidents WHERE incident_id = %s) LIMIT 1)", (world.incident,))
    plan = service.create_plan(conn, world.incident, RulePlanner())
    assert not any(r.startswith("stale GPS") for r in plan["risks"])


def test_case_duplicate_incident_event_creates_one_plan(conn, world):
    first = service.create_plan(conn, world.incident, RulePlanner())
    assert service.create_plan(conn, world.incident, RulePlanner()) is None
    run_once(conn, RulePlanner())                      # the worker also sees it
    n = conn.execute("SELECT count(*) AS n FROM recovery_plans WHERE incident_id = %s",
                     (world.incident,)).fetchone()["n"]
    assert first and n == 1


# ---------------------------------------------------------------- LLM agent

def test_llm_plan_uses_tools_and_system_computed_eta(conn, world):
    view = service.load_view(conn, world.incident)
    spare = RulePlanner().propose(view)
    client = ScriptedClient([
        ("get_incident_details", {}),
        ("find_available_buses", {"min_capacity": 40}),
        ("find_available_drivers", {}),
        ("validate_recovery_plan", {"spare_bus_id": spare.spare_bus_id, "driver_id": spare.driver_id}),
        ("submit_plan_for_approval", {"spare_bus_id": spare.spare_bus_id, "driver_id": spare.driver_id,
                                      "rationale": "Nearest bus. It will be there in 2 minutes.",
                                      "risks": ["traffic"]}),
    ])
    plan = service.create_plan(conn, world.incident, LLMPlanner(client))
    assert plan["planner"] == "llm" and plan["status"] == "pending_approval" and plan["llm_error"] is None
    assert [e["tool"] for e in plan["evidence"]][:3] == ["get_incident_details", "find_available_buses",
                                                         "find_available_drivers"]
    # The model claimed 2 minutes; the stored ETA is the system's own number.
    assert plan["eta_s"] == pytest.approx(view.eta_s(spare.spare_bus_id))
    assert "(model) traffic" in plan["risks"]
    assert client.calls[0]["tools"] and client.calls[0]["model"]


def test_case_llm_invents_a_bus_falls_back_to_rules(conn, world):
    client = ScriptedClient([
        ("submit_plan_for_approval", {"spare_bus_id": "SP-999", "driver_id": "DRV-11", "rationale": "x", "risks": []}),
        ("submit_plan_for_approval", {"spare_bus_id": "SP-998", "driver_id": "DRV-11", "rationale": "x", "risks": []}),
    ])
    plan = service.create_plan(conn, world.incident, LLMPlanner(client))
    assert plan["planner"] == "rules" and plan["status"] == "pending_approval"
    assert plan["spare_bus_id"] not in ("SP-999", "SP-998")
    assert "did not submit a valid plan" in plan["llm_error"]
    validator_msgs = [e["output"] for e in plan["evidence"] if e["tool"] == "validator"]
    assert any("no spare bus called 'SP-999'" in m for m in validator_msgs)


def test_case_llm_corrects_itself_after_validator_feedback(conn, world):
    view = service.load_view(conn, world.incident)
    good = RulePlanner().propose(view)
    client = ScriptedClient([
        ("submit_plan_for_approval", {"spare_bus_id": "SP-204", "driver_id": good.driver_id,
                                      "rationale": "x", "risks": []}),                 # out of service
        ("submit_plan_for_approval", {"spare_bus_id": good.spare_bus_id, "driver_id": good.driver_id,
                                      "rationale": "fixed", "risks": []}),
    ])
    plan = service.create_plan(conn, world.incident, LLMPlanner(client))
    assert plan["planner"] == "llm" and plan["spare_bus_id"] == good.spare_bus_id
    # The rejection went back to the model as an error tool_result.
    second_request = client.calls[1]["messages"][-1]["content"][0]
    assert second_request["is_error"] and "out_of_service" in second_request["content"]


def test_case_llm_timeout_falls_back_to_rules(conn, world):
    client = ScriptedClient([TimeoutError("read timed out")])
    plan = service.create_plan(conn, world.incident, LLMPlanner(client))
    assert plan["planner"] == "rules" and plan["status"] == "pending_approval"
    assert "TimeoutError" in plan["llm_error"]


def test_case_llm_wrongly_says_nothing_is_feasible(conn, world):
    """Contradictory result: the model says no spare works, the tools say one does."""
    none = {"spare_bus_id": None, "driver_id": None, "rationale": "nothing fits", "risks": []}
    client = ScriptedClient([("submit_plan_for_approval", none), ("submit_plan_for_approval", none)])
    plan = service.create_plan(conn, world.incident, LLMPlanner(client))
    assert plan["status"] == "pending_approval" and plan["planner"] == "rules"
    assert "A feasible plan exists" in client.calls[1]["messages"][-1]["content"][0]["content"]


def test_tools_refuse_unknown_ids(conn, world):
    view = service.load_view(conn, world.incident)
    with pytest.raises(ToolError):
        run_tool(view, "get_vehicle_status", {"bus_id": "BUS-999"})
    with pytest.raises(ToolError):
        run_tool(view, "estimate_recovery_eta", {"spare_bus_id": "SP-999"})
    with pytest.raises(ToolError):
        run_tool(view, "delete_everything", {})


# ---------------------------------------------------------------- decisions

def test_approval_assigns_spare_and_notifies_parents(conn, world):
    plan = service.create_plan(conn, world.incident, RulePlanner())
    out = service.approve(conn, plan["plan_id"], "dana", "dispatcher")
    assert out["status"] == "approved" and out["decided_by"] == "dana"
    assert conn.execute("SELECT status FROM spare_buses WHERE bus_id = %s",
                        (plan["spare_bus_id"],)).fetchone()["status"] == "assigned"
    assert conn.execute("SELECT status FROM drivers WHERE driver_id = %s",
                        (plan["driver_id"],)).fetchone()["status"] == "assigned"
    n = conn.execute("SELECT count(*) AS n FROM notifications WHERE kind = 'REPLACEMENT_DISPATCHED' "
                     "AND trip_id = %s", (plan["trip_id"],)).fetchone()["n"]
    assert n == len(service.load_view(conn, world.incident).remaining_stops()) > 0
    assert "plan_approved" in audit_actions(conn)


def test_no_parent_notification_before_approval(conn, world):
    service.create_plan(conn, world.incident, RulePlanner())
    n = conn.execute("SELECT count(*) AS n FROM notifications WHERE kind = 'REPLACEMENT_DISPATCHED'").fetchone()["n"]
    assert n == 0


def test_case_unauthorized_action_attempt(conn, world):
    plan = service.create_plan(conn, world.incident, RulePlanner())
    with pytest.raises(service.Forbidden):
        service.approve(conn, plan["plan_id"], "vic", "viewer")
    assert service.get_plan(conn, plan["plan_id"])["status"] == "pending_approval"
    assert "approve_denied" in audit_actions(conn)
    assert conn.execute("SELECT count(*) AS n FROM spare_buses WHERE status = 'assigned'").fetchone()["n"] == 0


def test_case_approval_rejected_then_replan_excludes_that_bus(conn, world):
    first = service.create_plan(conn, world.incident, RulePlanner())
    service.reject(conn, first["plan_id"], "dana", "dispatcher", "that bus is needed later")
    with pytest.raises(service.Conflict):
        service.approve(conn, first["plan_id"], "dana", "dispatcher")
    second = service.replan(conn, world.incident, RulePlanner(), "dana", "dispatcher")
    assert second["attempt"] == 2 and second["spare_bus_id"] != first["spare_bus_id"]
    assert {"plan_rejected", "plan_created"} <= set(audit_actions(conn))


def _open_breakdowns(conn):
    return [r["incident_id"] for r in conn.execute(
        "SELECT incident_id FROM incidents WHERE type = 'BREAKDOWN_SUSPECTED' AND closed_ts IS NULL "
        "ORDER BY opened_ts")]


def _until_open_breakdowns(conn, world, n):
    """Consume until n breakdowns are open AT THE SAME TIME."""
    while len(_open_breakdowns(conn)) < n:
        assert world.pipe.run_once(max_records=10).consumed, f"never {n} simultaneous breakdowns"
    return _open_breakdowns(conn)


def test_concurrent_breakdowns_get_different_spares_and_drivers(conn, world):
    """Regression: every pending plan proposed the same nearest spare + driver,
    so all approvals after the first would have been blocked."""
    open_now = _until_open_breakdowns(conn, world, 3)
    plans = [service.create_plan(conn, iid, RulePlanner()) for iid in open_now]
    pend = [p for p in plans if p["status"] == "pending_approval"]
    assert len(pend) >= 2
    assert len({p["spare_bus_id"] for p in pend}) == len(pend)
    assert len({p["driver_id"] for p in pend}) == len(pend)
    for p in pend:
        assert service.approve(conn, p["plan_id"], "dana", "dispatcher")["status"] == "approved"


def test_two_plans_cannot_assign_the_same_spare(conn, world):
    """Approval re-validates under row locks; the second approval is blocked
    and the block is recorded (not rolled back with the error)."""
    a_inc, b_inc = _until_open_breakdowns(conn, world, 2)[:2]
    conn.execute("UPDATE spare_buses SET status = 'out_of_service' WHERE bus_id <> 'SP-201'")
    a = service.create_plan(conn, a_inc, RulePlanner())
    b = service.create_plan(conn, b_inc, RulePlanner())
    assert a["spare_bus_id"] == "SP-201"
    assert b["status"] == "no_feasible_plan"           # soft reservation: SP-201 is held by plan a
    # Build the clash the reservation normally prevents (e.g. two workers
    # planning at the same instant): force plan b onto SP-201 as well.
    conn.execute("UPDATE recovery_plans SET status = 'pending_approval', spare_bus_id = 'SP-201', "
                 "driver_id = %s WHERE plan_id = %s", (a["driver_id"], b["plan_id"]))
    service.approve(conn, a["plan_id"], "dana", "dispatcher")
    with pytest.raises(service.Conflict) as e:
        service.approve(conn, b["plan_id"], "sam", "dispatcher")
    assert any("spare_available" in f for f in e.value.detail["failures"])
    assert service.get_plan(conn, b["plan_id"])["status"] == "superseded"
    assert "approve_blocked" in audit_actions(conn)


def test_approval_blocked_after_breakdown_ends(conn, world):
    plan = service.create_plan(conn, world.incident, RulePlanner())
    while conn.execute("SELECT closed_ts FROM incidents WHERE incident_id = %s",
                       (world.incident,)).fetchone()["closed_ts"] is None:
        assert world.pipe.run_once(max_records=50).consumed
    with pytest.raises(service.Conflict) as e:
        service.approve(conn, plan["plan_id"], "dana", "dispatcher")
    assert any("incident_open" in f for f in e.value.detail["failures"])
    assert conn.execute("SELECT count(*) AS n FROM spare_buses WHERE status = 'assigned'").fetchone()["n"] == 0


def test_pending_plan_is_superseded_when_bus_moves_again(conn, world):
    plan = service.create_plan(conn, world.incident, RulePlanner())
    while conn.execute("SELECT closed_ts FROM incidents WHERE incident_id = %s",
                       (world.incident,)).fetchone()["closed_ts"] is None:
        assert world.pipe.run_once(max_records=50).consumed
    assert service.supersede_stale(conn) == 1
    assert service.get_plan(conn, plan["plan_id"])["status"] == "superseded"
    with pytest.raises(service.Conflict):
        service.approve(conn, plan["plan_id"], "dana", "dispatcher")


def test_spare_released_after_trip_ends(conn, world):
    plan = service.create_plan(conn, world.incident, RulePlanner())
    service.approve(conn, plan["plan_id"], "dana", "dispatcher")
    while world.pipe.run_once(max_records=500).consumed:
        pass
    assert service.release_finished(conn) == 1
    assert conn.execute("SELECT status FROM spare_buses WHERE bus_id = %s",
                        (plan["spare_bus_id"],)).fetchone()["status"] == "available"


# --------------------------------------------------------------------- API

def test_api_roles_and_decisions(conn, world, monkeypatch):
    from fastapi.testclient import TestClient

    from fleetops import api

    plan = service.create_plan(conn, world.incident, RulePlanner())
    monkeypatch.setattr(api, "MODE", "db")
    monkeypatch.setenv("FLEETOPS_DB", TEST_DB)
    H = lambda tok: {"Authorization": f"Bearer {tok}"}  # noqa: E731
    with TestClient(api.app) as c:
        assert c.get("/plans").status_code == 401                                  # no token
        assert c.get("/plans", headers=H("nope")).status_code == 401
        assert c.get("/plans", headers=H("dev-viewer")).json()[0]["plan_id"] == plan["plan_id"]
        assert c.post(f"/plans/{plan['plan_id']}/approve", headers=H("dev-viewer")).status_code == 403
        assert c.get("/audit", headers=H("dev-viewer")).status_code == 403
        r = c.post(f"/plans/{plan['plan_id']}/approve", headers=H("dev-dispatcher"))
        assert r.status_code == 200 and r.json()["status"] == "approved"
        again = c.post(f"/plans/{plan['plan_id']}/approve", headers=H("dev-dispatcher"))
        assert again.status_code == 409
        actions = [a["action"] for a in c.get("/audit", headers=H("dev-admin")).json()]
        assert "approve_denied" in actions and "plan_approved" in actions
        assert set(c.get("/auth/dev-tokens").json()) == {"admin", "dispatcher", "viewer"}
    monkeypatch.setenv("FLEETOPS_DEV_AUTH", "0")
    with TestClient(api.app) as c:
        assert c.get("/auth/dev-tokens").status_code == 404
        assert c.get("/plans", headers=H("dev-dispatcher")).status_code == 401      # dev tokens gone


def test_api_recovery_endpoints_need_db_mode():
    from fastapi.testclient import TestClient

    from fleetops import api
    with TestClient(api.app) as c:
        assert c.get("/plans", headers={"Authorization": "Bearer dev-viewer"}).status_code == 501


def test_case_llm_leaks_markup_into_rationale(conn, world):
    """Seen live with a real model: the risks list arrived inside the rationale
    string as raw markup. The stored plan must have clean text and the risks."""
    view = service.load_view(conn, world.incident)
    good = RulePlanner().propose(view)
    leaked = ('SP-203 is the only spare that fits.</rationale>\n<parameter name="risks">'
              '["Breakdown is only suspected", "Road distance may be longer"]')
    client = ScriptedClient([("submit_plan_for_approval", {
        "spare_bus_id": good.spare_bus_id, "driver_id": good.driver_id, "rationale": leaked, "risks": []})])
    plan = service.create_plan(conn, world.incident, LLMPlanner(client))
    assert plan["planner"] == "llm"
    assert plan["rationale"] == "SP-203 is the only spare that fits."
    assert "(model) Breakdown is only suspected" in plan["risks"]
    assert not any("<" in r for r in plan["risks"])


def test_submit_tool_is_strict():
    from fleetops.recovery.tools import TOOL_SPECS
    submit = next(t for t in TOOL_SPECS if t["name"] == "submit_plan_for_approval")
    assert submit["strict"] is True and submit["input_schema"]["additionalProperties"] is False


def test_no_feasible_reason_names_spares_held_by_other_plans(conn, world):
    """Regression from the demo recording: 'largest: 24' alone read as if the
    fleet had no big buses, when the big ones were held by pending plans."""
    open_now = _until_open_breakdowns(conn, world, 3)
    conn.execute("UPDATE spare_buses SET status = 'out_of_service' WHERE bus_id IN ('SP-203', 'SP-205')")
    first = service.create_plan(conn, open_now[0], RulePlanner())
    assert first["spare_bus_id"] == "SP-201"
    second = service.create_plan(conn, open_now[1], RulePlanner())
    assert second["status"] == "no_feasible_plan"
    assert "SP-201 would fit but is held by other pending plans" in second["summary"]
