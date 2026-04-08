-- =============================================================
-- 03_cleaned.sql  –  Cleaned layer
-- Applies type casting, NULL normalization, deduplication,
-- and basic validation filters on top of the raw staging data.
-- Idempotent: DROP + CREATE before each table
-- =============================================================

-- ------------------------------------------------------------
-- cleaned.airlines
--   • Trim whitespace, uppercase IATA code
--   • Keep only valid 2-character IATA codes
--   • Deduplicate
-- ------------------------------------------------------------
DROP TABLE IF EXISTS cleaned.airlines CASCADE;

CREATE TABLE cleaned.airlines AS
SELECT DISTINCT
    UPPER(TRIM(iata_code)) AS iata_code,
    TRIM(airline)          AS airline
FROM raw.airlines
WHERE iata_code IS NOT NULL
  AND TRIM(iata_code) <> ''
  AND LENGTH(TRIM(iata_code)) = 2;

ALTER TABLE cleaned.airlines ADD PRIMARY KEY (iata_code);

-- ------------------------------------------------------------
-- cleaned.airports
--   • Trim whitespace, uppercase IATA code
--   • Cast latitude/longitude to FLOAT (drops rows with invalid coords)
--   • Filter rows missing key fields
-- ------------------------------------------------------------
DROP TABLE IF EXISTS cleaned.airports CASCADE;

CREATE TABLE cleaned.airports AS
SELECT
    UPPER(TRIM(iata_code))        AS iata_code,
    TRIM(airport)                 AS airport,
    TRIM(city)                    AS city,
    UPPER(TRIM(state))            AS state,
    UPPER(TRIM(country))          AS country,
    TRIM(latitude)::FLOAT         AS latitude,
    TRIM(longitude)::FLOAT        AS longitude
FROM raw.airports
WHERE iata_code IS NOT NULL AND TRIM(iata_code) <> ''
  AND latitude   IS NOT NULL AND TRIM(latitude)  <> ''
  AND longitude  IS NOT NULL AND TRIM(longitude) <> '';

ALTER TABLE cleaned.airports ADD PRIMARY KEY (iata_code);

-- ------------------------------------------------------------
-- cleaned.flights
--   • Cast all numeric columns (empty string → NULL via NULLIF)
--   • Cast CANCELLED and DIVERTED to BOOLEAN (stored as 0/1 in source)
--   • Normalize CANCELLATION_REASON: empty string → NULL
--   • Add computed flight_date DATE from YEAR + MONTH + DAY
--   • Filter rows missing mandatory fields (airline, origin, destination)
--   • Filter out non-IATA airport codes (numeric codes like '10397')
--     Only rows with 3-character alphabetic codes are retained (see Data Quality Risk #2)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS cleaned.flights CASCADE;

CREATE TABLE cleaned.flights AS
SELECT
    year::INTEGER                                               AS year,
    month::INTEGER                                              AS month,
    day::INTEGER                                                AS day,
    day_of_week::INTEGER                                        AS day_of_week,
    TRIM(airline)                                               AS airline,
    NULLIF(TRIM(flight_number), '')::INTEGER                    AS flight_number,
    NULLIF(TRIM(tail_number), '')                               AS tail_number,
    UPPER(TRIM(origin_airport))                                 AS origin_airport,
    UPPER(TRIM(destination_airport))                            AS destination_airport,
    NULLIF(TRIM(scheduled_departure), '')::INTEGER              AS scheduled_departure,
    NULLIF(TRIM(departure_time), '')::INTEGER                   AS departure_time,
    NULLIF(TRIM(departure_delay), '')::INTEGER                  AS departure_delay,
    NULLIF(TRIM(taxi_out), '')::INTEGER                         AS taxi_out,
    NULLIF(TRIM(wheels_off), '')::INTEGER                       AS wheels_off,
    NULLIF(TRIM(scheduled_time), '')::INTEGER                   AS scheduled_time,
    NULLIF(TRIM(elapsed_time), '')::INTEGER                     AS elapsed_time,
    NULLIF(TRIM(air_time), '')::INTEGER                         AS air_time,
    NULLIF(TRIM(distance), '')::INTEGER                         AS distance,
    NULLIF(TRIM(wheels_on), '')::INTEGER                        AS wheels_on,
    NULLIF(TRIM(taxi_in), '')::INTEGER                          AS taxi_in,
    NULLIF(TRIM(scheduled_arrival), '')::INTEGER                AS scheduled_arrival,
    NULLIF(TRIM(arrival_time), '')::INTEGER                     AS arrival_time,
    NULLIF(TRIM(arrival_delay), '')::INTEGER                    AS arrival_delay,
    (NULLIF(TRIM(diverted),   '')::INTEGER = 1)                 AS diverted,
    (NULLIF(TRIM(cancelled),  '')::INTEGER = 1)                 AS cancelled,
    NULLIF(TRIM(cancellation_reason), '')                       AS cancellation_reason,
    NULLIF(TRIM(air_system_delay),    '')::INTEGER              AS air_system_delay,
    NULLIF(TRIM(security_delay),      '')::INTEGER              AS security_delay,
    NULLIF(TRIM(airline_delay),       '')::INTEGER              AS airline_delay,
    NULLIF(TRIM(late_aircraft_delay), '')::INTEGER              AS late_aircraft_delay,
    NULLIF(TRIM(weather_delay),       '')::INTEGER              AS weather_delay,
    MAKE_DATE(year::INTEGER, month::INTEGER, day::INTEGER)      AS flight_date
FROM raw.flights
WHERE airline             IS NOT NULL AND TRIM(airline)             <> ''
  AND origin_airport      IS NOT NULL AND TRIM(origin_airport)      <> ''
  AND destination_airport IS NOT NULL AND TRIM(destination_airport) <> ''
  AND year                IS NOT NULL AND TRIM(year)                <> ''
  AND month               IS NOT NULL AND TRIM(month)               <> ''
  AND day                 IS NOT NULL AND TRIM(day)                 <> ''
  AND LENGTH(UPPER(TRIM(origin_airport))) = 3 AND UPPER(TRIM(origin_airport)) ~ '^[A-Z]{3}$'
  AND LENGTH(UPPER(TRIM(destination_airport))) = 3 AND UPPER(TRIM(destination_airport)) ~ '^[A-Z]{3}$';

-- ------------------------------------------------------------
-- Verification
-- ------------------------------------------------------------
SELECT 'cleaned.airlines' AS table_name, COUNT(*) AS row_count FROM cleaned.airlines
UNION ALL
SELECT 'cleaned.airports',               COUNT(*)              FROM cleaned.airports
UNION ALL
SELECT 'cleaned.flights',                COUNT(*)              FROM cleaned.flights;

-- Sanity check: no rows with NULL flight_date
SELECT 'flights with NULL flight_date' AS check_name,
       COUNT(*) AS count
FROM cleaned.flights
WHERE flight_date IS NULL;

-- Data Quality Risk #2 check: flights where origin_airport is not a 3-letter IATA code
SELECT 'flights with non-IATA origin_airport' AS check_name,
       COUNT(*) AS count
FROM cleaned.flights
WHERE LENGTH(origin_airport) != 3;
