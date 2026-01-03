# Silver Layer (Cleaning & Normalization)

## Goal
Transform raw ingested prices (`bronze_prices`) into a clean, standardized, analytics-ready daily price table (`silver_prices_daily`) suitable for downstream aggregation and feature generation.

## Input
- `bronze_prices` (Delta table)
  - Contains raw OHLC(V) and provenance fields (`source`, `ingested_at`, `input_file`).

## Output
- `silver_prices_daily` (Delta table, partitioned by `symbol`)
- `silver_prices_rejected` (Delta table, partitioned by `symbol`) — rows failing validation with a `reject_reason`

## Transformations (SQL-first)
### 1) Type casting
All fields are cast into consistent types:
- `date` → DATE
- `open/high/low/close` → DOUBLE
- `volume` → BIGINT (nullable)

### 2) Deduplication
Enforces unique `(symbol, date)` by keeping the latest ingested record:
- `ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY ingested_at DESC) = 1`

### 3) Missing value handling
Rows missing required fields are excluded from Silver:
- Required: `symbol`, `date`, `open`, `high`, `low`, `close`
- `volume` is allowed to be NULL (e.g., FX)

Invalid rows are written to `silver_prices_rejected` with a reason.

### 4) Timestamp normalization
`ingested_at` is retained as a TIMESTAMP provenance column and used to select the most recent record in deduplication logic.

## Data quality checks (SQL)
- No duplicate `(symbol, date)` in `silver_prices_daily`
- No non-positive prices in OHLC columns
- OHLC consistency constraints:
  - `high >= greatest(open, close, low)`
  - `low <= least(open, close, high)`

## Notes
Silver is considered the “trusted” layer. Bronze remains the system of record for raw inputs and provenance.
