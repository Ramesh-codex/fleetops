"""Deterministic plan validation. The AI's proposal never bypasses this."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from .view import FleetView

MAX_ETA_S = 1800.0   # a replacement more than 30 min away is not a recovery


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


@dataclass
class Validation:
    feasible: bool
    checks: list[Check]
    risks: list[str]
    eta_s: float | None
    capacity: int | None
    riders: int

    def as_json(self) -> dict:
        return {"feasible": self.feasible, "checks": [asdict(c) for c in self.checks],
                "risks": self.risks, "eta_s": self.eta_s, "capacity": self.capacity, "riders": self.riders}

    def failures(self) -> list[str]:
        return [f"{c.name}: {c.detail}" for c in self.checks if not c.ok]


def validate(view: FleetView, spare_bus_id: str | None, driver_id: str | None) -> Validation:
    riders = int(view.route["riders"])
    checks: list[Check] = []

    inc = view.incident
    checks.append(Check("incident_open", inc["closed_ts"] is None and inc["type"] == "BREAKDOWN_SUSPECTED",
                        "breakdown incident is open" if inc["closed_ts"] is None else
                        "incident already closed (bus is moving again)"))
    checks.append(Check("trip_active", not view.bus.get("finished"),
                        "trip still in progress" if not view.bus.get("finished") else "trip already finished"))

    spare = view.spare(spare_bus_id) if spare_bus_id else None
    checks.append(Check("spare_exists", spare is not None,
                        f"{spare_bus_id} is in the spare fleet" if spare else f"no spare bus called {spare_bus_id!r}"))
    if spare:
        avail = view.spare_free(spare)
        why = ("available" if avail else
               "rejected earlier by the dispatcher" if spare_bus_id in view.excluded_spares else
               "proposed in another pending plan" if spare_bus_id in view.reserved_spares else spare["status"])
        checks.append(Check("spare_available", avail, why))
        checks.append(Check("capacity", spare["capacity"] >= riders,
                            f"{spare['capacity']} seats for {riders} riders"))

    driver = view.driver(driver_id) if driver_id else None
    checks.append(Check("driver_exists", driver is not None,
                        f"{driver_id} is on the roster" if driver else f"no driver called {driver_id!r}"))
    if driver:
        checks.append(Check("driver_available", view.driver_free(driver),
                            "proposed in another pending plan" if driver_id in view.reserved_drivers
                            else driver["status"]))
        if spare:
            checks.append(Check("driver_at_spare_depot", driver["depot_id"] == spare["depot_id"],
                                f"driver at {driver['depot_id']}, bus at {spare['depot_id']}"))

    eta = view.eta_s(spare_bus_id) if spare else None
    if spare:
        checks.append(Check("eta_within_limit", eta is not None and eta <= MAX_ETA_S,
                            "unknown (no bus position)" if eta is None else f"{eta / 60:.1f} min (limit {MAX_ETA_S / 60:.0f})"))

    risks = []
    # GPS staleness is the CONSUMER's judgement (bus_state.gps_lost), made with
    # per-partition watermarks. Recomputing it here against the newest timestamp
    # across all buses flagged every plan as stale while partitions were out of
    # step during a backlog (the same mistake as pipeline finding 5).
    if view.bus.get("gps_lost") or view.bus.get("last_ts") is None:
        age = view.gps_age_s
        risks.append(f"stale GPS: the bus has stopped reporting"
                     f"{'' if age is None else f' (last fix ~{age:.0f}s before the newest fleet ping)'}; "
                     "it may not be where the plan assumes")
    if spare and spare["capacity"] >= riders and spare["capacity"] < riders * 1.1:
        risks.append(f"tight capacity: {spare['capacity']} seats for {riders} riders")

    return Validation(feasible=all(c.ok for c in checks), checks=checks, risks=risks,
                      eta_s=eta, capacity=spare["capacity"] if spare else None, riders=riders)
