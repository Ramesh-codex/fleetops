"""Read-only tools the agent may call. Each returns JSON-able data from the
FleetView snapshot and nothing else — no tool can change the fleet."""

from __future__ import annotations

from .validator import validate
from .view import FleetView

TOOL_SPECS = [
    {"name": "get_incident_details", "description": "The breakdown incident being handled: id, bus, when it opened, detail.",
     "input_schema": {"type": "object", "properties": {}, "additionalProperties": False}},
    {"name": "get_vehicle_status", "description": "Current state of a bus on a route: position, route, trip, progress, GPS age in seconds.",
     "input_schema": {"type": "object", "properties": {"bus_id": {"type": "string"}},
                      "required": ["bus_id"], "additionalProperties": False}},
    {"name": "get_route_details", "description": "Route riders (students who need seats) and the stops still to serve.",
     "input_schema": {"type": "object", "properties": {"route_id": {"type": "string"}},
                      "required": ["route_id"], "additionalProperties": False}},
    {"name": "find_available_buses", "description": "Spare buses that are available now, with capacity, depot and computed ETA to the broken-down bus.",
     "input_schema": {"type": "object", "properties": {"min_capacity": {"type": "integer", "minimum": 0}},
                      "additionalProperties": False}},
    {"name": "find_available_drivers", "description": "Drivers available now, optionally only at one depot.",
     "input_schema": {"type": "object", "properties": {"depot_id": {"type": "string"}},
                      "additionalProperties": False}},
    {"name": "calculate_replacement_route", "description": "The route a spare would drive: depot -> broken-down bus -> remaining stops, with distances.",
     "input_schema": {"type": "object", "properties": {"spare_bus_id": {"type": "string"}},
                      "required": ["spare_bus_id"], "additionalProperties": False}},
    {"name": "estimate_recovery_eta", "description": "System-computed ETA (seconds) for a spare to reach the broken-down bus.",
     "input_schema": {"type": "object", "properties": {"spare_bus_id": {"type": "string"}},
                      "required": ["spare_bus_id"], "additionalProperties": False}},
    {"name": "validate_recovery_plan", "description": "Run the deterministic safety checks on a spare bus + driver pair before submitting.",
     "input_schema": {"type": "object", "properties": {"spare_bus_id": {"type": "string"}, "driver_id": {"type": "string"}},
                      "required": ["spare_bus_id", "driver_id"], "additionalProperties": False}},
    {"name": "submit_plan_for_approval", "strict": True,
     "description": ("Submit your recommendation for a dispatcher to approve. Use ids exactly as returned by the "
                     "tools. If no feasible replacement exists, set spare_bus_id and driver_id to null and explain."),
     "input_schema": {"type": "object", "properties": {
         "spare_bus_id": {"type": ["string", "null"]}, "driver_id": {"type": ["string", "null"]},
         "rationale": {"type": "string", "description": "Why this choice, in 1-3 sentences."},
         "risks": {"type": "array", "items": {"type": "string"}}},
         "required": ["spare_bus_id", "driver_id", "rationale", "risks"], "additionalProperties": False}},
]

TERMINAL_TOOL = "submit_plan_for_approval"


class ToolError(ValueError):
    pass


def run_tool(view: FleetView, name: str, args: dict) -> dict:
    """Execute one read-only tool. Unknown ids produce an error, never a guess."""
    if name == "get_incident_details":
        return dict(view.incident)
    if name == "get_vehicle_status":
        if args.get("bus_id") != view.bus["bus_id"]:
            raise ToolError(f"unknown bus {args.get('bus_id')!r}; the incident bus is {view.bus['bus_id']}")
        return {**view.bus, "gps_age_s": view.gps_age_s}
    if name == "get_route_details":
        if args.get("route_id") != view.route["route_id"]:
            raise ToolError(f"unknown route {args.get('route_id')!r}; the incident route is {view.route['route_id']}")
        rem = view.remaining_stops()
        return {"route_id": view.route["route_id"], "name": view.route["name"], "riders": view.route["riders"],
                "remaining_stops": [{"stop_id": s["stop_id"], "name": s["name"]} for s in rem]}
    if name == "find_available_buses":
        min_cap = int(args.get("min_capacity") or 0)
        out = []
        for s in view.spares:
            if not view.spare_free(s) or s["capacity"] < min_cap:
                continue
            eta = view.eta_s(s["bus_id"])
            out.append({**s, "depot_name": view.depots[s["depot_id"]]["name"],
                        "eta_s": None if eta is None else round(eta)})
        return {"available": sorted(out, key=lambda r: (r["eta_s"] is None, r["eta_s"]))}
    if name == "find_available_drivers":
        dep = args.get("depot_id")
        return {"available": [d for d in view.drivers if view.driver_free(d)
                              and (dep is None or d["depot_id"] == dep)]}
    if name in ("calculate_replacement_route", "estimate_recovery_eta"):
        s = view.spare(args.get("spare_bus_id", ""))
        if s is None:
            raise ToolError(f"no spare bus called {args.get('spare_bus_id')!r}")
        eta = view.eta_s(s["bus_id"])
        if name == "estimate_recovery_eta":
            return {"spare_bus_id": s["bus_id"], "eta_s": None if eta is None else round(eta)}
        dep = view.depots[s["depot_id"]]
        leg = view.distance_m(dep["lat"], dep["lon"], view.bus["lat"], view.bus["lon"]) if view.bus.get("lat") else None
        return {"spare_bus_id": s["bus_id"],
                "legs": [{"from": dep["name"], "to": f"{view.bus['bus_id']} breakdown point",
                          "straight_line_m": None if leg is None else round(leg)}]
                + [{"to": st["stop_id"]} for st in view.remaining_stops()],
                "eta_to_bus_s": None if eta is None else round(eta)}
    if name == "validate_recovery_plan":
        return validate(view, args.get("spare_bus_id"), args.get("driver_id")).as_json()
    raise ToolError(f"unknown tool {name!r}")
