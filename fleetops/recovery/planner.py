"""Planners. Both return a Proposal; neither changes anything.

RulePlanner  — deterministic: nearest available spare with enough seats and an
               available driver at the same depot. Always available.
LLMPlanner   — Claude with the read-only tools. Falls back to RulePlanner on
               timeout, API error, or a proposal that fails validation twice.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field

from .tools import TERMINAL_TOOL, TOOL_SPECS, ToolError, run_tool
from .validator import Validation, validate
from .view import FleetView


@dataclass
class Proposal:
    planner: str                       # 'rules' | 'llm'
    spare_bus_id: str | None
    driver_id: str | None
    rationale: str
    validation: Validation
    model_risks: list[str] = field(default_factory=list)
    evidence: list[dict] = field(default_factory=list)
    llm_error: str | None = None

    @property
    def feasible(self) -> bool:
        return self.spare_bus_id is not None and self.validation.feasible


# ------------------------------------------------------------------- rules
class RulePlanner:
    name = "rules"

    def propose(self, view: FleetView) -> Proposal:
        riders = int(view.route["riders"])
        evidence = [{"tool": "find_available_buses", "input": {},
                     "output": run_tool(view, "find_available_buses", {})}]
        best, reasons = None, []
        avail = [s for s in view.spares if view.spare_free(s)]
        if not avail:
            reasons.append("no spare bus is available")
        big = [s for s in avail if s["capacity"] >= riders]
        if avail and not big:
            reasons.append(f"no available spare seats {riders} riders "
                           f"(largest: {max(s['capacity'] for s in avail)})")
        for s in big:
            drv = next((d for d in view.drivers if view.driver_free(d) and d["depot_id"] == s["depot_id"]), None)
            if drv is None:
                reasons.append(f"no available driver at {s['depot_id']} for {s['bus_id']}")
                continue
            v = validate(view, s["bus_id"], drv["driver_id"])
            if not v.feasible:
                reasons.extend(v.failures())
                continue
            # Nearest first; among equals, the smallest bus that fits keeps big ones free.
            key = (round(v.eta_s), s["capacity"])
            if best is None or key < best[0]:
                best = (key, s, drv, v)
        if best:
            _, s, drv, v = best
            return Proposal("rules", s["bus_id"], drv["driver_id"],
                            f"Nearest available spare with enough seats ({s['capacity']} for {riders}) "
                            f"and a driver at the same depot.", v, evidence=evidence)
        why = "; ".join(dict.fromkeys(reasons)) or "no feasible replacement"
        return Proposal("rules", None, None, why, validate(view, None, None), evidence=evidence)


# --------------------------------------------------------------------- LLM
_LEAK = re.compile(r"</?\s*(rationale|parameter|invoke|antml)[^>]*>", re.I)


def clean_submission(args: dict) -> tuple[str, list[str]]:
    """Normalise the model's free-text fields.

    Seen live with a real model: the risks list arrived INSIDE the rationale
    string as raw markup ('...</rationale> <parameter name="risks">["..."]'),
    leaving `risks` empty and showing the junk to the dispatcher. Ids and
    numbers are validated elsewhere; this keeps the text honest too: split the
    leaked part off, recover the list if it parses, strip any leftover tags.
    """
    rationale = str(args.get("rationale") or "")
    risks = [str(r) for r in (args.get("risks") or []) if str(r).strip()]
    m = re.search(r"</\s*rationale\s*>|<\s*parameter\b", rationale, re.I)
    if m:
        leaked = rationale[m.start():]
        rationale = rationale[:m.start()]
        arr = re.search(r"\[.*\]", leaked, re.S)
        if arr:
            try:
                risks += [str(r) for r in json.loads(arr.group(0)) if str(r).strip()]
            except ValueError:
                pass
    rationale = _LEAK.sub("", rationale).strip()
    risks = [_LEAK.sub("", r).strip()[:400] for r in risks][:8]
    return rationale[:1200], risks
SYSTEM_PROMPT = """You are the recovery assistant for a school bus dispatcher (synthetic training data).
A bus has broken down. Investigate with the tools, then call submit_plan_for_approval exactly once.

Rules:
- Use only bus ids, driver ids and numbers returned by the tools. Never invent any.
- The replacement must seat all riders on the route and have a driver at the same depot.
- Prefer the nearest feasible spare; call validate_recovery_plan before submitting.
- If nothing is feasible, submit with spare_bus_id and driver_id set to null and say why.
- Your plan is a recommendation; a dispatcher decides. Mention real risks (e.g. stale GPS)."""


class LLMPlanner:
    name = "llm"

    def __init__(self, client, model: str = "claude-haiku-5-5", *, max_turns: int = 10,
                 deadline_s: float = 45.0, fallback: RulePlanner | None = None):
        self.client, self.model = client, model
        self.max_turns, self.deadline_s = max_turns, deadline_s
        self.fallback = fallback or RulePlanner()

    def propose(self, view: FleetView) -> Proposal:
        evidence: list[dict] = []
        try:
            p = self._run(view, evidence)
            if p is not None:
                return p
            err = "model did not submit a valid plan"
        except Exception as e:  # noqa: BLE001 - any model/API failure -> deterministic fallback
            err = f"{type(e).__name__}: {e}"[:300]
        fb = self.fallback.propose(view)
        fb.evidence = evidence + fb.evidence
        fb.llm_error = err
        return fb

    def _run(self, view: FleetView, evidence: list[dict]) -> Proposal | None:
        t_end = time.monotonic() + self.deadline_s
        messages = [{"role": "user", "content":
                     f"Breakdown incident {view.incident['incident_id']} on bus {view.bus['bus_id']}. Recover it."}]
        rejections = 0
        for _ in range(self.max_turns):
            if time.monotonic() > t_end:
                raise TimeoutError(f"no plan within {self.deadline_s:.0f}s")
            resp = self.client.messages.create(
                model=self.model, max_tokens=1024, system=SYSTEM_PROMPT, tools=TOOL_SPECS,
                tool_choice={"type": "auto", "disable_parallel_tool_use": True}, messages=messages)
            uses = [b for b in resp.content if getattr(b, "type", None) == "tool_use"]
            if not uses:
                messages += [{"role": "assistant", "content": resp.content},
                             {"role": "user", "content": "Call submit_plan_for_approval to finish."}]
                continue
            use = uses[0]
            args = dict(use.input or {})
            messages.append({"role": "assistant", "content": resp.content})
            if use.name == TERMINAL_TOOL:
                spare, driver = args.get("spare_bus_id"), args.get("driver_id")
                evidence.append({"tool": use.name, "input": args})
                if spare is None:
                    # The model says nothing is feasible. Trust but verify.
                    check = self.fallback.propose(view)
                    if check.feasible:
                        problem = (f"A feasible plan exists ({check.spare_bus_id} + {check.driver_id}). "
                                   "Re-check the tools and submit it, or explain precisely why not.")
                    else:
                        rationale, risks = clean_submission(args)
                        return Proposal("llm", None, None, rationale, validate(view, None, None), risks, evidence)
                else:
                    v = validate(view, spare, driver)
                    if v.feasible:
                        rationale, risks = clean_submission(args)
                        return Proposal("llm", spare, driver, rationale, v, risks, evidence)
                    problem = "Plan rejected by the safety validator: " + "; ".join(v.failures())
                rejections += 1
                evidence.append({"tool": "validator", "output": problem})
                if rejections >= 2:
                    return None
                messages.append({"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": use.id, "content": problem, "is_error": True}]})
                continue
            try:
                out, is_err = run_tool(view, use.name, args), False
            except ToolError as e:
                out, is_err = {"error": str(e)}, True
            evidence.append({"tool": use.name, "input": args, "output": out})
            messages.append({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": use.id, "content": json.dumps(out, default=str),
                 "is_error": is_err}]})
        return None
