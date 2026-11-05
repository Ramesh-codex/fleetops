import os

os.environ.setdefault("FLEETOPS_BUSES", "4")
os.environ.setdefault("FLEETOPS_SPEEDUP", "1000")

from fastapi.testclient import TestClient  # noqa: E402

from fleetops import api  # noqa: E402


def test_endpoints_end_to_end():
    with TestClient(api.app) as c:
        assert c.get("/health").json()["status"] == "ok"
        api.rt.step()
        buses = c.get("/buses").json()["buses"]
        assert len(buses) == 4
        assert len(c.get("/routes").json()) == 4
        assert c.get(f"/buses/{buses[0]['bus_id']}").status_code == 200
        assert c.get("/buses/NOPE").status_code == 404
        assert "fleetops_pings_total" in c.get("/metrics").text
        assert "FleetOps Live" in c.get("/").text


def test_post_ping_is_idempotent():
    with TestClient(api.app) as c:
        p = api.rt.sim.pings[api.rt.cursor].model_dump()
        api.rt.cursor += 1        # take it out of the replay so we own it
        first = c.post("/pings", json=p).json()["outcome"]
        second = c.post("/pings", json=p).json()["outcome"]
        assert first == "accepted"
        assert second == "duplicate"


def test_post_ping_validates_input():
    with TestClient(api.app) as c:
        r = c.post("/pings", json={"bus_id": "BUS-101"})
        assert r.status_code == 422


def test_stops_near_demo_mode():
    with TestClient(api.app) as c:
        s = next(iter(api.rt.sim.routes.values())).stops[1]
        near = c.get(f"/stops/near?lat={s.lat}&lon={s.lon}&radius_m=50").json()
        assert near[0]["stop_id"] == s.stop_id
        assert c.get("/stops/near?lat=200&lon=0").status_code == 422
