# Problem Statement

## Analytical Goal

**Which airlines and airports generate the most delays and cancellations in US domestic air travel, and how do delays vary by time of year and day of the week?**

## Business Context

Flight delays cost the US economy billions of dollars annually through lost productivity, missed connections, and crew costs. Airlines and airports need to understand where delays originate so they can target operational improvements. Passengers and travel planners benefit from knowing which airlines and routes carry the highest delay risk.

## Dataset

| File | Description | Key Fields |
|------|-------------|------------|
| `flights.csv` | ~5M domestic US flights from 2015 | Airline, origin/destination airport, delay breakdown by cause, cancellation status |
| `airlines.csv` | IATA code → airline name lookup | IATA_CODE, AIRLINE |
| `airports.csv` | IATA code → airport metadata | IATA_CODE, AIRPORT, CITY, STATE, LATITUDE, LONGITUDE |

## Key Questions

1. Which airlines have the highest average arrival delay? Which have the best on-time performance?
2. Which origin airports are associated with the most departure delays?
3. Are delays seasonal — do certain months show systematically higher delays?
4. Which days of the week have the worst delay patterns?
5. What are the primary causes of delays (air system, weather, airline, late aircraft, security)?

## Pipeline Architecture

```
CSV Files (dataset/)
        │
        ▼  COPY (bulk load, all TEXT)
┌────────────────────┐
│   raw  schema      │  raw.airlines, raw.airports, raw.flights
│  (staging layer)   │  • No constraints, no type casting
└────────────────────┘
        │
        ▼  INSERT ... SELECT + CAST + filter
┌────────────────────┐
│  cleaned  schema   │  cleaned.airlines, cleaned.airports, cleaned.flights
│  (quality layer)   │  • Correct data types, NULL normalization
│                    │  • Computed flight_date, deduplication
└────────────────────┘
        │
        ▼  INSERT ... SELECT + JOIN + GROUP BY
┌────────────────────┐
│   gold  schema     │  gold.airline_performance
│ (analytical layer) │  gold.airport_delay_summary
│                    │  gold.delay_by_month_weekday
└────────────────────┘
```

## Success Criteria

- `gold.airline_performance` returns a ranked list of airlines by avg arrival delay with on-time rate and cancellation rate
- `gold.airport_delay_summary` identifies the top 20 worst airports for departure delays
- `gold.delay_by_month_weekday` provides data for a month × weekday heatmap of average delays
- All SQL scripts are reproducible: re-running them from scratch yields the same result
