import math

import pytest

from fleetops.geo import LocalFrame, Polyline


def test_frame_roundtrip():
    f = LocalFrame(37.3382, -121.8863)
    for lat, lon in [(37.34, -121.88), (37.30, -121.95), (37.3382, -121.8863)]:
        x, y = f.to_xy(lat, lon)
        lat2, lon2 = f.to_latlon(x, y)
        assert lat2 == pytest.approx(lat, abs=1e-9)
        assert lon2 == pytest.approx(lon, abs=1e-9)


def test_frame_distance_is_metres():
    f = LocalFrame(37.0, -122.0)
    _, y = f.to_xy(37.0 + 1 / 111.195, -122.0)   # ~1 km north
    assert y == pytest.approx(1000, rel=1e-3)


def test_polyline_length_and_point_at():
    line = Polyline([(0, 0), (300, 0), (300, 400)])
    assert line.length == pytest.approx(700)
    assert line.point_at(150) == pytest.approx((150, 0))
    assert line.point_at(500) == pytest.approx((300, 200))
    assert line.point_at(-10) == pytest.approx((0, 0))
    assert line.point_at(9999) == pytest.approx((300, 400))


def test_project_snaps_to_nearest_point():
    line = Polyline([(0, 0), (300, 0), (300, 400)])
    d, off = line.project(100, 7)
    assert d == pytest.approx(100)
    assert off == pytest.approx(7)


def test_project_window_prevents_backward_snap():
    # Route that doubles back: out east, then back west 20 m north of itself.
    line = Polyline([(0, 0), (500, 0), (500, 20), (0, 20)])
    # Point at x=100, y=10 is equally near the outbound and return legs.
    d_free, _ = line.project(100, 9)          # unconstrained: picks outbound leg
    assert d_free < 500
    d_win, _ = line.project(100, 11, d_min=600)
    assert d_win > 520                        # constrained: stays on return leg


def test_heading():
    line = Polyline([(0, 0), (0, 100), (100, 100)])
    assert line.heading_at(50) == pytest.approx(0)      # north
    assert line.heading_at(150) == pytest.approx(90)    # east


def test_polyline_needs_two_points():
    with pytest.raises(ValueError):
        Polyline([(0, 0)])
