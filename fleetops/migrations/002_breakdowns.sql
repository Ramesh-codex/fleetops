-- 002: breakdown detection and parent delay updates.

-- Tracker state needed to resume breakdown detection after a restart.
ALTER TABLE bus_state
    ADD COLUMN delayed      text[] NOT NULL DEFAULT '{}',
    ADD COLUMN motion       jsonb  NOT NULL DEFAULT '[]',
    ADD COLUMN bd_anchor_d  double precision NOT NULL DEFAULT 0,
    ADD COLUMN breakdown    boolean NOT NULL DEFAULT false;

-- A stop can now get up to three messages per trip: the approaching alert, a
-- "delayed" update if the bus breaks down, and a fresh alert once it moves.
ALTER TABLE notifications
    ADD COLUMN kind text NOT NULL DEFAULT 'BUS_APPROACHING',
    ALTER COLUMN predicted_arrival_ts DROP NOT NULL;
ALTER TABLE notifications DROP CONSTRAINT notifications_trip_id_stop_id_key;
ALTER TABLE notifications ADD CONSTRAINT notifications_trip_stop_kind_key UNIQUE (trip_id, stop_id, kind);

CREATE INDEX incidents_type_open_idx ON incidents (type) WHERE closed_ts IS NULL;
