from fleetops.simulator import SimConfig, simulate


def test_same_seed_same_output():
    a = simulate(SimConfig(seed=7, n_buses=3, duplicate_prob=0.1, reorder_prob=0.1))
    b = simulate(SimConfig(seed=7, n_buses=3, duplicate_prob=0.1, reorder_prob=0.1))
    assert [p.model_dump() for p in a.pings] == [p.model_dump() for p in b.pings]
    assert a.truth_arrival == b.truth_arrival


def test_different_seed_differs():
    a = simulate(SimConfig(seed=1, n_buses=2))
    b = simulate(SimConfig(seed=2, n_buses=2))
    assert a.pings[0].lat != b.pings[0].lat


def test_every_stop_has_truth_arrival_in_order():
    r = simulate(SimConfig(seed=3, n_buses=4))
    for bus, rid in r.bus_route.items():
        times = [r.truth_arrival[(bus, s.stop_id)] for s in r.routes[rid].stops]
        assert times == sorted(times)


def test_clean_run_has_no_faults():
    r = simulate(SimConfig(seed=4, n_buses=3))
    ids = [p.event_id for p in r.pings]
    assert len(ids) == len(set(ids))
    for bus in r.bus_route:
        seqs = [p.seq for p in r.pings if p.bus_id == bus]
        assert seqs == sorted(seqs)


def test_duplicates_are_injected():
    r = simulate(SimConfig(seed=5, n_buses=3, duplicate_prob=0.2))
    ids = [p.event_id for p in r.pings]
    assert len(ids) > len(set(ids))


def test_reorder_is_injected():
    r = simulate(SimConfig(seed=5, n_buses=3, reorder_prob=0.2))
    bus = next(iter(r.bus_route))
    seqs = [p.seq for p in r.pings if p.bus_id == bus]
    assert seqs != sorted(seqs)


def test_dropout_creates_gap():
    cfg = SimConfig(seed=6, n_buses=2, dropout_prob=1.0)
    r = simulate(cfg)
    for bus, (start, end) in r.dropouts.items():
        ts = [p.ts for p in r.pings if p.bus_id == bus]
        assert not any(start <= t < end for t in ts)
        gaps = [b - a for a, b in zip(ts, ts[1:])]
        assert max(gaps) >= cfg.dropout_s[0]


def test_breakdown_injected_and_slows_trip():
    base = simulate(SimConfig(seed=8, n_buses=1))
    broke = simulate(SimConfig(seed=8, n_buses=1, breakdown_prob=1.0))
    assert broke.breakdowns
    assert broke.end_ts > base.end_ts + 250
