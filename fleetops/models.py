"""Event and domain schemas.

GpsPing is the wire format every producer sends. `schema_version` is on every
event so consumers can reject or migrate versions they don't understand.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field

SCHEMA_VERSION = 1


class GpsPing(BaseModel):
    schema_version: int = SCHEMA_VERSION
    event_id: str                      # unique per ping; used for dedup
    bus_id: str
    trip_id: str
    seq: int = Field(ge=0)             # per-bus counter set by the device
    ts: float                          # device timestamp, seconds
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    speed_mps: float = Field(ge=0)
    heading: float = Field(ge=0, lt=360)


class Stop(BaseModel):
    stop_id: str
    name: str
    lat: float
    lon: float
    dist_m: float                      # distance along the route


class Route(BaseModel):
    route_id: str
    name: str
    path: list[tuple[float, float]]    # (lat, lon) vertices
    stops: list[Stop]


class NotificationKind(str, Enum):
    BUS_APPROACHING = "BUS_APPROACHING"
    DELAY_UPDATE = "DELAY_UPDATE"                       # "your bus is delayed" after an approaching alert
    APPROACHING_AFTER_DELAY = "APPROACHING_AFTER_DELAY"  # fresh alert once the bus moves again
    REPLACEMENT_DISPATCHED = "REPLACEMENT_DISPATCHED"    # dispatcher approved a replacement bus


class Notification(BaseModel):
    kind: str = NotificationKind.BUS_APPROACHING.value
    bus_id: str
    trip_id: str
    stop_id: str
    sent_ts: float
    predicted_arrival_ts: float | None    # None for DELAY_UPDATE (no meaningful ETA)


class IncidentType(str, Enum):
    GPS_LOST = "GPS_LOST"
    BREAKDOWN_SUSPECTED = "BREAKDOWN_SUSPECTED"


class Incident(BaseModel):
    incident_id: str
    type: IncidentType
    bus_id: str
    opened_ts: float
    closed_ts: float | None = None
    detail: str = ""
