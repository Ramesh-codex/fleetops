"""Breakdown recovery: tools, validator, planners (rules + LLM agent), service, worker.

Safety model, in one place:
  * The agent only sees data through read-only tools over a snapshot (FleetView).
  * The agent may only CHOOSE a spare bus and driver by id. ETA, capacity and
    riders in the stored plan are computed by the system, never copied from
    the model's text.
  * Every proposal passes the deterministic validator; an invalid one is
    returned to the model once with the reasons, then the rule planner takes over.
  * Nothing changes in the fleet until a dispatcher approves, and approval
    re-runs the validator inside a transaction that locks the spare and driver.
"""
