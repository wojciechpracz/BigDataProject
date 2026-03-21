-- =============================================================
-- 02_raw_load.sql  –  Raw (staging) layer
-- All columns stored as TEXT – no assumptions about data quality
-- Idempotent: DROP + CREATE before loading
-- =============================================================

-- ------------------------------------------------------------
-- raw.airlines
-- ------------------------------------------------------------
DROP TABLE IF EXISTS raw.airlines CASCADE;

CREATE TABLE raw.airlines (
    iata_code TEXT,
    airline   TEXT
);

-- ------------------------------------------------------------
-- raw.airports
-- ------------------------------------------------------------
DROP TABLE IF EXISTS raw.airports CASCADE;

CREATE TABLE raw.airports (
    iata_code TEXT,
    airport   TEXT,
    city      TEXT,
    state     TEXT,
    country   TEXT,
    latitude  TEXT,
    longitude TEXT
);

-- ------------------------------------------------------------
-- raw.flights  (31 columns, ~5M rows)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS raw.flights CASCADE;

CREATE TABLE raw.flights (
    year                TEXT,
    month               TEXT,
    day                 TEXT,
    day_of_week         TEXT,
    airline             TEXT,
    flight_number       TEXT,
    tail_number         TEXT,
    origin_airport      TEXT,
    destination_airport TEXT,
    scheduled_departure TEXT,
    departure_time      TEXT,
    departure_delay     TEXT,
    taxi_out            TEXT,
    wheels_off          TEXT,
    scheduled_time      TEXT,
    elapsed_time        TEXT,
    air_time            TEXT,
    distance            TEXT,
    wheels_on           TEXT,
    taxi_in             TEXT,
    scheduled_arrival   TEXT,
    arrival_time        TEXT,
    arrival_delay       TEXT,
    diverted            TEXT,
    cancelled           TEXT,
    cancellation_reason TEXT,
    air_system_delay    TEXT,
    security_delay      TEXT,
    airline_delay       TEXT,
    late_aircraft_delay TEXT,
    weather_delay       TEXT
);

-- ------------------------------------------------------------
-- Load data from CSV files
-- Files are mounted at /data/ inside the container (see docker-compose.yml)
-- ------------------------------------------------------------
COPY raw.airlines FROM '/data/airlines.csv' CSV HEADER;
COPY raw.airports FROM '/data/airports.csv' CSV HEADER;
COPY raw.flights  FROM '/data/flights.csv'  CSV HEADER;

-- ------------------------------------------------------------
-- Verification rowcounts
-- ------------------------------------------------------------
SELECT 'raw.airlines' AS table_name, COUNT(*) AS row_count FROM raw.airlines
UNION ALL
SELECT 'raw.airports',                COUNT(*)              FROM raw.airports
UNION ALL
SELECT 'raw.flights',                 COUNT(*)              FROM raw.flights;
