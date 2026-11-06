import pytest

from fleetops.models import GpsPing
from fleetops.simulator import SimConfig, simulate
from fleetops.tracker import Tracker


def run(cfg: SimConfig, **tracker_kw):
    r = simulate(cfg)
    t = Tracker(r.routes, r.frame, r.bus_route, **tracker_kw)
    for p in r.pings:
        t.ingest(p)
        t.check_stale(p.ts)
    return r, t


def notif_keys(t):
    return [(n.bus_id, n.stop_id) for n in t.notifications]


# ---------------------------------------------------------------- correctness

def test_clean_run_every_bus_finishes_and_every_stop_notified_once():
    r, t = run(SimConfig(seed=11, n_buses=5))
    assert all(s["finished"] for s in t.snapshot())
    keys = notif_keys(t)
    assert len(keys) == len(set(keys))                       # once each
    expected = {(b, s.stop_id) for b, rid in r.bus_route.items() for s in r.routes[rid].stops[1:]}
    assert set(keys) == expected                             # every non-origin stop
    assert t.counters["duplicate"] == 0 and t.counters["out_of_order"] == 0
    assert not t.incidents


def test_progress_never_goes_backwards():
    r = simulate(SimConfig(seed=12, n_buses=3, reorder_prob=0.2, gps_noise_m=15))
    t = Tracker(r.routes, r.frame, r.bus_route)
    last = {b: 0.0 for b in r.bus_route}
    for p in r.pings:
        t.ingest(p)
        d = t.buses[p.bus_id].d
        assert d >= last[p.bus_id]
        last[p.bus_id] = d


# --------------------------------------------------------------- idempotency

def test_duplicates_do_not_change_state_or_notifications():
    clean_r, clean_t = run(SimConfig(seed=13, n_buses=4))
    dup_r, dup_t = run(SimConfig(seed=13, n_buses=4, duplicate_prob=0.3))
    # duplicate injection consumes randomness, so compare structure not floats
    assert dup_t.counters["duplicate"] > 0
    keys = notif_keys(dup_t)
    assert len(keys) == len(set(keys))
    assert set(keys) == set(notif_keys(clean_t))


def test_replaying_entire_stream_is_a_noop():
    r, t = run(SimConfig(seed=14, n_buses=3))
    before_state = t.snapshot()
    before_notifs = len(t.notifications)
    for p in r.pings:
        assert t.ingest(p) == "duplicate"
    assert t.snapshot() == before_state
    assert len(t.notifications) == before_notifs


def test_out_of_order_ping_is_dropped():
    r = simulate(SimConfig(seed=15, n_buses=1))
    t = Tracker(r.routes, r.frame, r.bus_route)
    pings = [p for p in r.pings]
    t.ingest(pings[5])
    d = t.buses[pings[5].bus_id].d
    assert t.ingest(pings[3]) == "out_of_order"
    assert t.buses[pings[5].bus_id].d == d


def test_unknown_bus_and_bad_schema_rejected():
    r = simulate(SimConfig(seed=16, n_buses=1))
    t = Tracker(r.routes, r.frame, r.bus_route)
    p = r.pings[0]
    assert t.ingest(p.model_copy(update={"bus_id": "BUS-999", "event_id": "x1"})) == "unknown_bus"
    assert t.ingest(p.model_copy(update={"schema_version": 2, "event_id": "x2"})) == "bad_schema"
    assert t.counters["accepted"] == 0


def test_dedup_window_is_bounded():
    r = simulate(SimConfig(seed=17, n_buses=1))
    t = Tracker(r.routes, r.frame, r.bus_route)
    for p in r.pings:
        t.ingest(p)
    st = next(iter(t.buses.values()))
    assert len(st.seen_set) == len(st.seen_ids) <= st.seen_ids.maxlen


# ------------------------------------------------------------------ incidents

def test_gps_loss_opens_and_closes_one_incident_per_dropout():
    r, t = run(SimConfig(seed=18, n_buses=6, dropout_prob=1.0))
    assert set(i.bus_id for i in t.incidents.values()) == set(r.dropouts)
    assert len(t.incidents) == len(r.dropouts)
    assert all(i.closed_ts is not None for i in t.incidents.values())


def test_finished_bus_never_flagged_as_gps_lost():
    r, t = run(SimConfig(seed=19, n_buses=3))
    t.check_stale(r.end_ts + 3600)
    assert not t.incidents


def test_stopped_but_pinging_bus_is_not_gps_lost():
    r, t = run(SimConfig(seed=20, n_buses=3, breakdown_prob=1.0))
    assert r.breakdowns
    assert not [i for i in t.incidents.values() if i.type == "GPS_LOST"]


# ---------------------------------------------------------------- breakdowns

def test_every_breakdown_is_detected_once_and_closed():
    r, t = run(SimConfig(seed=30, n_buses=8, breakdown_prob=1.0))
    bd = [i for i in t.incidents.values() if i.type == "BREAKDOWN_SUSPECTED"]
    assert sorted(i.bus_id for i in bd) == sorted(r.breakdowns)
    for i in bd:
        start, end = r.breakdowns[i.bus_id]
        # The rule is "<30 m progress in the last 150 s". A bus that was standing
        # at a stop just before it broke down satisfies it sooner, so the lower
        # bound is below 150 s; the upper bound is the window plus ~2 pings.
        assert start + 100 <= i.opened_ts <= start + 160
        assert end <= i.closed_ts <= end + 60                 # until RESUME_M of real progress


def test_detection_delay_is_window_length_across_many_breakdowns():
    """Regression: a fixed anchor detected some breakdowns 91 s late."""
    delays = []
    for seed in range(40, 46):
        r, t = run(SimConfig(seed=seed, n_buses=10, breakdown_prob=1.0))
        for i in t.incidents.values():
            if i.type == "BREAKDOWN_SUSPECTED":
                delays.append(i.opened_ts - r.breakdowns[i.bus_id][0])
        assert len([i for i in t.incidents.values() if i.type == "BREAKDOWN_SUSPECTED"]) == len(r.breakdowns)
    assert min(delays) >= 100 and max(delays) <= 160
    assert sorted(delays)[len(delays) // 2] >= 145          # typical case: about the window length


def test_no_false_breakdowns_in_heavy_traffic_or_long_dwells():
    cfg = SimConfig(seed=31, n_buses=30, traffic_prob=0.6, dwell_s=(30.0, 45.0))
    r, t = run(cfg)
    assert not [i for i in t.incidents.values() if i.type == "BREAKDOWN_SUSPECTED"]


def test_breakdown_sends_delay_update_then_fresh_alert():
    r, t = run(SimConfig(seed=32, n_buses=8, breakdown_prob=1.0))
    by_stop = {}
    for n in t.notifications:
        by_stop.setdefault((n.trip_id, n.stop_id), []).append(n.kind)
    delayed = [k for k, kinds in by_stop.items() if "DELAY_UPDATE" in kinds]
    assert delayed, "some stops were alerted before the breakdown"
    for k in delayed:
        assert by_stop[k] == ["BUS_APPROACHING", "DELAY_UPDATE", "APPROACHING_AFTER_DELAY"]
    # No stop ever gets more than one message of each kind.
    for kinds in by_stop.values():
        assert len(kinds) == len(set(kinds))


def test_breakdown_state_survives_restore():
    r = simulate(SimConfig(seed=33, n_buses=4, breakdown_prob=1.0))
    a = Tracker(r.routes, r.frame, r.bus_route)
    bus, (start, _end) = next(iter(r.breakdowns.items()))
    cut = next(i for i, p in enumerate(r.pings) if p.ts > start + 200)
    for p in r.pings[:cut]:
        a.ingest(p)
    assert bus in a._open_breakdown
    b = Tracker(r.routes, r.frame, r.bus_route)
    b.restore([a.export_bus(x) for x in a.buses],
              [i for i in a.incidents.values() if i.closed_ts is None])
    for p in r.pings[cut:]:
        a.ingest(p)
        b.ingest(p)
    ka = [(n.kind, n.stop_id) for n in a.notifications[-40:] if n.sent_ts > r.pings[cut].ts]
    kb = [(n.kind, n.stop_id) for n in b.notifications]
    assert ka == kb


# ----------------------------------------------------------------- ETA modes

def test_invalid_eta_mode():
    r = simulate(SimConfig(seed=21, n_buses=1))
    with pytest.raises(ValueError):
        Tracker(r.routes, r.frame, r.bus_route, eta_mode="magic")


def test_notifications_precede_arrival():
    r, t = run(SimConfig(seed=22, n_buses=6))
    for n in t.notifications:
        assert n.sent_ts <= r.truth_arrival[(n.bus_id, n.stop_id)]


def test_warmup_blocks_alerts_on_first_ping():
    r = simulate(SimConfig(seed=23, n_buses=1))
    first = r.pings[0]
    cold = Tracker(r.routes, r.frame, r.bus_route, require_warmup=False)
    cold.ingest(first)
    warm = Tracker(r.routes, r.frame, r.bus_route)
    warm.ingest(first)
    assert cold.notifications              # default-speed guess fired alerts
    assert not warm.notifications          # no measured speed yet -> silent


def test_moving_mode_beats_naive_on_eta_error():
    r = simulate(SimConfig(seed=24, n_buses=10))
    errs = {}
    for mode in ("naive", "moving"):
        t = Tracker(r.routes, r.frame, r.bus_route, eta_mode=mode)
        e = []
        for p in r.pings:
            t.ingest(p)
            for sid, eta in t.etas(p.bus_id).items():
                actual = r.truth_arrival[(p.bus_id, sid)]
                if 240 <= actual - p.ts <= 360:
                    e.append(abs(eta - actual))
        errs[mode] = sum(e) / len(e)
    assert errs["moving"] < errs["naive"] / 2


def test_gps_gap_detected_even_if_check_stale_never_runs():
    # A consumer chewing through a backlog in big batches may never call
    # check_stale() while a bus is silent. The gap must still be recorded.
    r = simulate(SimConfig(seed=25, n_buses=4, dropout_prob=1.0))
    t = Tracker(r.routes, r.frame, r.bus_route)
    for p in r.pings:
        t.ingest(p)                       # no check_stale at all
    assert {i.bus_id for i in t.incidents.values()} == set(r.dropouts)
    assert all(i.closed_ts is not None for i in t.incidents.values())


def test_gap_and_periodic_check_give_same_incident_ids():
    r = simulate(SimConfig(seed=26, n_buses=4, dropout_prob=1.0))
    a = Tracker(r.routes, r.frame, r.bus_route)
    b = Tracker(r.routes, r.frame, r.bus_route)
    for p in r.pings:
        a.ingest(p)
        a.check_stale(p.ts)
        b.ingest(p)
    assert set(a.incidents) == set(b.incidents)


def test_new_trip_resets_seq_and_speed():
    r = simulate(SimConfig(seed=27, n_buses=1))
    t = Tracker(r.routes, r.frame, r.bus_route)
    for p in r.pings:
        t.ingest(p)
    span = r.end_ts - r.pings[0].ts + 600
    day2 = [p.model_copy(update={"trip_id": "T2", "event_id": "d2-" + p.event_id, "ts": p.ts + span})
            for p in r.pings]
    assert t.ingest(day2[0]) == "accepted"            # seq restarted but new trip -> accepted
    assert t.buses[day2[0].bus_id].speed_est is None  # no speed carried over a 10-min gap
    for p in day2[1:]:
        t.ingest(p)
    assert t.buses[day2[0].bus_id].finished
    assert not t.incidents                             # gap between trips is not GPS loss


def test_straggler_from_previous_trip_is_dropped():
    r = simulate(SimConfig(seed=28, n_buses=1))
    t = Tracker(r.routes, r.frame, r.bus_route)
    p0, p1 = r.pings[0], r.pings[1]
    t.ingest(p1.model_copy(update={"trip_id": "T2", "event_id": "n1", "ts": p1.ts + 5000}))
    assert t.ingest(p0) == "out_of_order"


def test_export_restore_roundtrip():
    r = simulate(SimConfig(seed=29, n_buses=3))
    a = Tracker(r.routes, r.frame, r.bus_route)
    half = len(r.pings) // 2
    for p in r.pings[:half]:
        a.ingest(p)
    b = Tracker(r.routes, r.frame, r.bus_route)
    b.restore([a.export_bus(x) for x in a.buses], [])
    for p in r.pings[half:]:
        a.ingest(p)
        b.ingest(p)
    assert a.snapshot() == b.snapshot()


def test_validation_rejects_bad_coordinates():
    with pytest.raises(Exception):
        GpsPing(event_id="e", bus_id="b", trip_id="t", seq=0, ts=0, lat=95, lon=0, speed_mps=0, heading=0)


def test_no_eta_shown_while_broken_down():
    r = simulate(SimConfig(seed=34, n_buses=3, breakdown_prob=1.0))
    t = Tracker(r.routes, r.frame, r.bus_route)
    seen_open = False
    for p in r.pings:
        t.ingest(p)
        for b in t.snapshot():
            if b["breakdown"]:
                seen_open = True
                assert b["next_stop_eta_ts"] is None and t.etas(b["bus_id"]) == {}
    assert seen_open
