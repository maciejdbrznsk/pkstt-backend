CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS feed_state (
    feed            TEXT PRIMARY KEY,
    source_url      TEXT NOT NULL,
    etag            TEXT,
    last_modified   TEXT,
    content_sha256  TEXT,
    feed_version    TEXT,
    feed_start_date DATE,
    feed_end_date   DATE,
    last_checked_at TIMESTAMPTZ,
    last_updated_at TIMESTAMPTZ,
    last_error      TEXT,
    stats           JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS agencies (
    feed            TEXT NOT NULL,
    agency_id       TEXT NOT NULL,
    agency_name     TEXT NOT NULL,
    agency_url      TEXT,
    agency_timezone TEXT,
    agency_phone    TEXT,
    PRIMARY KEY (feed, agency_id)
);

CREATE TABLE IF NOT EXISTS stop_groups (
    group_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    search_text TEXT NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    operators   TEXT[] NOT NULL,
    stop_count  INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stop_groups_search_trgm
    ON stop_groups USING GIN (search_text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_stop_groups_coords
    ON stop_groups (lat, lon);

CREATE TABLE IF NOT EXISTS stops (
    feed         TEXT NOT NULL,
    stop_id      TEXT NOT NULL,
    stop_code    TEXT,
    stop_name    TEXT NOT NULL,
    stop_lat     DOUBLE PRECISION NOT NULL,
    stop_lon     DOUBLE PRECISION NOT NULL,
    zone_id      TEXT,
    street_name  TEXT,
    town_name    TEXT,
    group_id     TEXT,
    platform_code TEXT NOT NULL DEFAULT '',
    is_on_demand BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (feed, stop_id)
);

CREATE INDEX IF NOT EXISTS idx_stops_group_id
    ON stops (group_id);

CREATE INDEX IF NOT EXISTS idx_stops_coords
    ON stops (stop_lat, stop_lon);

CREATE TABLE IF NOT EXISTS routes (
    feed             TEXT NOT NULL,
    route_id         TEXT NOT NULL,
    agency_id        TEXT,
    route_short_name TEXT NOT NULL,
    route_long_name  TEXT,
    route_type       INT NOT NULL DEFAULT 3,
    route_color      TEXT,
    route_text_color TEXT,
    PRIMARY KEY (feed, route_id)
);

CREATE INDEX IF NOT EXISTS idx_routes_short_name
    ON routes (route_short_name);

CREATE TABLE IF NOT EXISTS calendar_dates (
    feed           TEXT NOT NULL,
    service_id     TEXT NOT NULL,
    service_date   DATE NOT NULL,
    exception_type INT NOT NULL,
    PRIMARY KEY (feed, service_id, service_date)
);

CREATE INDEX IF NOT EXISTS idx_calendar_dates_lookup
    ON calendar_dates (service_date, feed, service_id)
    WHERE exception_type = 1;

CREATE TABLE IF NOT EXISTS shapes (
    feed        TEXT NOT NULL,
    shape_id    TEXT NOT NULL,
    points      BYTEA NOT NULL,
    point_count INT NOT NULL,
    PRIMARY KEY (feed, shape_id)
);

CREATE TABLE IF NOT EXISTS trips (
    feed                  TEXT NOT NULL,
    trip_id               TEXT NOT NULL,
    route_id              TEXT NOT NULL,
    service_id            TEXT NOT NULL,
    trip_headsign         TEXT,
    direction_id          INT NOT NULL DEFAULT 0,
    block_id              TEXT,
    shape_id              TEXT,
    wheelchair_accessible INT NOT NULL DEFAULT 0,
    brigade               TEXT,
    PRIMARY KEY (feed, trip_id)
);

CREATE INDEX IF NOT EXISTS idx_trips_route_service
    ON trips (feed, route_id, service_id);

CREATE TABLE IF NOT EXISTS stop_times (
    feed                TEXT NOT NULL,
    trip_id             TEXT NOT NULL,
    stop_sequence       INT NOT NULL,
    stop_id             TEXT NOT NULL,
    arrival_secs        INT NOT NULL,
    departure_secs      INT NOT NULL,
    pickup_type         INT NOT NULL DEFAULT 0,
    drop_off_type       INT NOT NULL DEFAULT 0,
    shape_dist_traveled DOUBLE PRECISION,
    PRIMARY KEY (feed, trip_id, stop_sequence)
);

CREATE INDEX IF NOT EXISTS idx_stop_times_stop_dep
    ON stop_times (feed, stop_id, departure_secs)
    INCLUDE (trip_id, stop_sequence, arrival_secs, pickup_type, drop_off_type);

CREATE TABLE IF NOT EXISTS fare_products (
    feed              TEXT NOT NULL,
    fare_product_id   TEXT NOT NULL,
    fare_product_name TEXT NOT NULL,
    fare_media_id     TEXT,
    rider_category_id TEXT,
    amount            TEXT NOT NULL,
    currency          TEXT NOT NULL,
    PRIMARY KEY (feed, fare_product_id)
);
