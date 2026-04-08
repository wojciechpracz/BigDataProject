# Data Quality Risks

Three data quality risks identified during raw data exploration of the US flights dataset.

---

## Risk 1 – NULL delays for cancelled flights inflate on-time rate

**Affected columns:** `ARRIVAL_DELAY`, `DEPARTURE_DELAY`

**Description:**  
When a flight is cancelled, both delay columns are empty (loaded as NULL in the raw table). If NULLs are naively treated as 0 (i.e. "no delay"), cancelled flights appear as on-time, inflating the on-time performance metric.

**Evidence:**  
```sql
SELECT COUNT(*)
FROM raw.flights
WHERE cancelled = '1'
  AND (arrival_delay IS NULL OR arrival_delay = '');
-- Returns: large number of rows
```

**Mitigation applied in cleaned/gold layer:**  
All aggregations use `FILTER (WHERE NOT cancelled)` to exclude cancelled flights from delay calculations. Cancelled flights are counted separately via `cancellation_rate_pct`.

---

## Risk 2 – ORIGIN_AIRPORT and DESTINATION_AIRPORT contain non-IATA numeric codes

**Affected columns:** `ORIGIN_AIRPORT`, `DESTINATION_AIRPORT`

**Description:**  
Standard IATA airport codes are exactly 3 alphabetic characters. Some rows contain 5-digit numeric codes (e.g. `10397`) for major hub airports instead of the standard IATA code.

**Evidence:**  
```sql
SELECT COUNT(*)
FROM raw.flights
WHERE LENGTH(origin_airport) != 3 OR origin_airport ~ '[0-9]';
-- Returns non-zero count
```

**Mitigation applied:**  
Rows with numeric or non-3-letter airport codes are now filtered out in `cleaned.flights`. The WHERE clause validates that both `origin_airport` and `destination_airport` must be exactly 3 alphabetic characters:
```sql
AND LENGTH(UPPER(TRIM(origin_airport))) = 3 AND UPPER(TRIM(origin_airport)) ~ '^[A-Z]{3}$'
AND LENGTH(UPPER(TRIM(destination_airport))) = 3 AND UPPER(TRIM(destination_airport)) ~ '^[A-Z]{3}$'
```
This ensures all airport codes in the cleaned layer are valid IATA codes and can be reliably joined with the airports reference table.

---

## Risk 3 – CANCELLATION_REASON is an empty string instead of NULL for non-cancelled flights

**Affected columns:** `CANCELLATION_REASON`

**Description:**  
For the vast majority of flights (those not cancelled), `CANCELLATION_REASON` contains an empty string `''` rather than a proper NULL. This causes incorrect results when filtering or grouping by cancellation reason — e.g. `GROUP BY cancellation_reason` produces a spurious `''` bucket that dwarfs the actual cancellation reasons (A = Airline/Carrier, B = Weather, C = National Air System, D = Security).

**Evidence:**  
```sql
SELECT cancellation_reason, COUNT(*)
FROM raw.flights
GROUP BY cancellation_reason
ORDER BY count DESC;
-- '' row will have millions of entries
```

**Mitigation applied in cleaned layer:**  
```sql
NULLIF(TRIM(cancellation_reason), '') AS cancellation_reason
```
This converts empty strings to NULL, so any downstream `GROUP BY` or `WHERE cancellation_reason IS NOT NULL` filter behaves correctly.
