-- =============================================================
-- 04_gold.sql  –  Gold (analytical) layer
-- Purpose-built aggregation tables for KPI reporting.
-- All tables join cleaned.flights with dimension tables.
-- Idempotent: DROP + CREATE before each table
-- =============================================================

-- ------------------------------------------------------------
-- gold.airline_performance
--   KPI: per-airline summary of delays, cancellations, on-time rate
--   JOIN: cleaned.flights → cleaned.airlines (to resolve IATA → name)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS gold.airline_performance CASCADE;

CREATE TABLE gold.airline_performance AS
SELECT
    f.airline                                                                   AS iata_code,
    a.airline                                                                   AS airline_name,
    COUNT(*)                                                                    AS total_flights,
    COUNT(*) FILTER (WHERE f.cancelled)                                         AS cancelled_flights,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE f.cancelled) / COUNT(*), 2
    )                                                                           AS cancellation_rate_pct,
    COUNT(*) FILTER (WHERE NOT f.cancelled)                                     AS operated_flights,
    ROUND(AVG(f.arrival_delay)   FILTER (WHERE NOT f.cancelled)::NUMERIC, 2)   AS avg_arrival_delay_min,
    ROUND(AVG(f.departure_delay) FILTER (WHERE NOT f.cancelled)::NUMERIC, 2)   AS avg_departure_delay_min,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE NOT f.cancelled AND COALESCE(f.arrival_delay, 0) <= 0)
              / NULLIF(COUNT(*) FILTER (WHERE NOT f.cancelled), 0), 2
    )                                                                           AS on_time_rate_pct
FROM cleaned.flights f
LEFT JOIN cleaned.airlines a ON f.airline = a.iata_code
GROUP BY f.airline, a.airline
ORDER BY avg_arrival_delay_min DESC NULLS LAST;

-- ------------------------------------------------------------
-- gold.airport_delay_summary
--   KPI: per-origin-airport summary of departure/arrival delays
--   JOIN: cleaned.flights → cleaned.airports (to resolve IATA → name/city)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS gold.airport_delay_summary CASCADE;

CREATE TABLE gold.airport_delay_summary AS
SELECT
    f.origin_airport                                                            AS iata_code,
    ap.airport                                                                  AS airport_name,
    ap.city                                                                     AS city,
    ap.state                                                                    AS state,
    COUNT(*)                                                                    AS total_departures,
    ROUND(AVG(f.departure_delay) FILTER (WHERE NOT f.cancelled)::NUMERIC, 2)   AS avg_departure_delay_min,
    ROUND(AVG(f.arrival_delay)   FILTER (WHERE NOT f.cancelled)::NUMERIC, 2)   AS avg_arrival_delay_min,
    COUNT(*) FILTER (WHERE f.cancelled)                                         AS cancelled_flights,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE f.cancelled) / COUNT(*), 2
    )                                                                           AS cancellation_rate_pct
FROM cleaned.flights f
LEFT JOIN cleaned.airports ap ON f.origin_airport = ap.iata_code
GROUP BY f.origin_airport, ap.airport, ap.city, ap.state
ORDER BY avg_departure_delay_min DESC NULLS LAST;

-- ------------------------------------------------------------
-- gold.delay_by_month_weekday
--   KPI: heatmap-ready aggregation of delays by month and day of week
--   Grain: one row per (month, day_of_week) combination
-- ------------------------------------------------------------
DROP TABLE IF EXISTS gold.delay_by_month_weekday CASCADE;

CREATE TABLE gold.delay_by_month_weekday AS
SELECT
    month,
    day_of_week,
    CASE day_of_week
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END                                                                         AS day_name,
    COUNT(*)                                                                    AS total_flights,
    ROUND(AVG(arrival_delay)   FILTER (WHERE NOT cancelled)::NUMERIC, 2)       AS avg_arrival_delay_min,
    ROUND(AVG(departure_delay) FILTER (WHERE NOT cancelled)::NUMERIC, 2)       AS avg_departure_delay_min,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE cancelled) / COUNT(*), 2
    )                                                                           AS cancellation_rate_pct
FROM cleaned.flights
GROUP BY month, day_of_week
ORDER BY month, day_of_week;

-- ------------------------------------------------------------
-- Demo KPI query (deliverable requirement)
-- Top 10 airlines by average arrival delay
-- ------------------------------------------------------------
SELECT
    airline_name,
    iata_code,
    total_flights,
    avg_arrival_delay_min,
    on_time_rate_pct,
    cancellation_rate_pct
FROM gold.airline_performance
ORDER BY avg_arrival_delay_min DESC NULLS LAST
LIMIT 10;
