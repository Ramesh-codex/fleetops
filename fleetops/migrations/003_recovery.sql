-- 003: breakdown recovery. Spare fleet, drivers, AI/rule-based recovery plans,
-- dispatcher decisions and an append-only audit log. Synthetic data only.

-- How many (synthetic) students ride each route; a replacement must seat them.
ALTER TABLE routes ADD COLUMN riders int NOT NULL DEFAULT 40;

CREATE TABLE depots (
    depot_id  text PRIMARY KEY,
    name      text NOT NULL,
    geom      geometry(Point, 4326) NOT NULL
);

CREATE TABLE spare_buses (
    bus_id    text PRIMARY KEY,
    depot_id  text NOT NULL REFERENCES depots(depot_id),
    capacity  int  NOT NULL CHECK (capacity > 0),
    status    text NOT NULL CHECK (status IN ('available', 'assigned', 'out_of_service'))
);

CREATE TABLE drivers (
    driver_id  text PRIMARY KEY,
    name       text NOT NULL,
    depot_id   text NOT NULL REFERENCES depots(depot_id),
    status     text NOT NULL CHECK (status IN ('available', 'assigned', 'off_shift'))
);

-- A proposed recovery for one breakdown incident. `attempt` lets a dispatcher
-- ask for a new plan after rejecting one; the UNIQUE pair makes the worker's
-- auto-planning idempotent (a duplicate incident event can't create a second plan).
CREATE TABLE recovery_plans (
    plan_id       text PRIMARY KEY,
    incident_id   text NOT NULL REFERENCES incidents(incident_id),
    attempt       int  NOT NULL DEFAULT 1,
    status        text NOT NULL CHECK (status IN
                    ('pending_approval', 'approved', 'rejected', 'no_feasible_plan', 'superseded')),
    planner       text NOT NULL,                 -- 'llm' or 'rules'
    bus_id        text NOT NULL,                 -- the broken-down bus
    trip_id       text NOT NULL,
    route_id      text NOT NULL,
    spare_bus_id  text,
    driver_id     text,
    eta_s         double precision,              -- computed by the system, never taken from the model
    riders        int,
    capacity      int,
    summary       text NOT NULL DEFAULT '',
    rationale     text NOT NULL DEFAULT '',
    evidence      jsonb NOT NULL DEFAULT '[]',   -- tool calls and their results
    checks        jsonb NOT NULL DEFAULT '[]',   -- validator results
    risks         jsonb NOT NULL DEFAULT '[]',
    llm_error     text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    decided_at    timestamptz,
    decided_by    text,
    decision_note text,
    UNIQUE (incident_id, attempt)
);
CREATE INDEX recovery_plans_pending_idx ON recovery_plans (status) WHERE status = 'pending_approval';

CREATE TABLE audit_log (
    id      bigserial PRIMARY KEY,
    at      timestamptz NOT NULL DEFAULT now(),
    actor   text NOT NULL,
    role    text NOT NULL,
    action  text NOT NULL,
    target  text NOT NULL,
    detail  jsonb NOT NULL DEFAULT '{}'
);
