-- 001: core schema. Synthetic data only.
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE routes (
    route_id   text PRIMARY KEY,
    name       text NOT NULL,
    path       geometry(LineString, 4326) NOT NULL
);

CREATE TABLE stops (
    stop_id    text PRIMARY KEY,
    route_id   text NOT NULL REFERENCES routes(route_id),
    seq        int  NOT NULL,
    name       text NOT NULL,
    dist_m     double precision NOT NULL,
    geom       geometry(Point, 4326) NOT NULL,
    UNIQUE (route_id, seq)
);
CREATE INDEX stops_geom_gix ON stops USING gist (geom);

CREATE TABLE buses (
    bus_id     text PRIMARY KEY,
    route_id   text NOT NULL REFERENCES routes(route_id)
);

-- Tracker state, one row per bus. Written in the same transaction as the
-- alerts it produced, so after a crash the restored state and the alerts agree.
CREATE TABLE bus_state (
    bus_id            text PRIMARY KEY REFERENCES buses(bus_id),
    trip_id           text,
    last_seq          bigint NOT NULL,
    last_ts           double precision,
    d                 double precision NOT NULL,
    speed_est         double precision,
    next_stop_idx     int NOT NULL,
    finished          boolean NOT NULL,
    notified          text[] NOT NULL DEFAULT '{}',
    next_stop         text,
    next_stop_eta_ts  double precision,
    gps_lost          boolean NOT NULL DEFAULT false,
    geom              geometry(Point, 4326),
    updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX bus_state_geom_gix ON bus_state USING gist (geom);

-- One alert per (trip, stop), enforced by the database as a second line of
-- defence behind the tracker's own once-per-stop rule.
CREATE TABLE notifications (
    id                    bigserial PRIMARY KEY,
    bus_id                text NOT NULL,
    trip_id               text NOT NULL,
    stop_id               text NOT NULL,
    sent_ts               double precision NOT NULL,
    predicted_arrival_ts  double precision NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (trip_id, stop_id)
);

CREATE TABLE incidents (
    incident_id  text PRIMARY KEY,
    type         text NOT NULL,
    bus_id       text NOT NULL,
    opened_ts    double precision NOT NULL,
    closed_ts    double precision,
    detail       text NOT NULL DEFAULT ''
);
CREATE INDEX incidents_open_idx ON incidents (bus_id) WHERE closed_ts IS NULL;

-- Messages the consumer could not process, kept for inspection and replay.
CREATE TABLE dead_letters (
    id          bigserial PRIMARY KEY,
    topic       text NOT NULL,
    partition   int  NOT NULL,
    "offset"    bigint NOT NULL,
    reason      text NOT NULL,
    payload     text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (topic, partition, "offset")
);
