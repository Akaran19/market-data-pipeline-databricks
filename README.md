# Market Data Pipeline (Databricks + SQL)

## Problem
Quant and trading analysts need **reliable, analytics-ready market datasets** for pre-trade analysis.
Raw market data is often inconsistent (schema drift, missing days, duplicates), so we need a pipeline
that transforms raw inputs into **clean, queryable tables** with quality checks.

## Outputs
This project delivers a Databricks/Lakehouse-style pipeline with Delta tables:

### Bronze (Raw)
- `bronze_prices` — raw ingested market data + metadata (source, ingested_at)

### Silver (Cleaned / Normalized)
- `silver_prices_daily` — normalized schema, deduplicated, typed columns

### Gold (Analytics-ready)
- `gold_market_features_daily` — returns/volatility/liquidity-friendly metrics for analysis

### Data Quality
- `data_quality_checks` — missing days, duplicates, invalid values, stale ingestion flags

All outputs are designed to be consumed primarily through **SQL**.

## Architecture (Bronze / Silver / Gold)
- Bronze: ingest raw CSV data into Delta tables with minimal assumptions
- Silver: clean and standardize types, enforce uniqueness, normalize symbols/dates
- Gold: produce analytics-ready aggregates/features useful for pre-trade workflows
- Quality: write validation checks to a dedicated table on each run

## Data Sources
- Primary: daily OHLCV market data (CSV-based ingestion)
- Initial symbols (cross-asset): SPY, GLD, Oil proxy, EURUSD.
- Source options: Stooq and/or Yahoo Finance (via CSV export)

## How to Run (Databricks Community Edition)
1. Create a Databricks Community Edition workspace
2. Import notebooks from `/notebooks`
3. Run in order:
   1) `01_ingest_bronze`
   2) `02_transform_silver`
   3) `03_aggregate_gold`
   4) `04_data_quality_checks`
4. Query outputs via SQL (see `/sql/example_queries.sql`)

## Repo Structure
- `/notebooks/bronze` — ingestion logic
- `/notebooks/silver` — cleaning/normalization logic
- `/notebooks/gold` — analytics-ready outputs
- `/sql` — example stakeholder queries + validation checks
- `/docs` — design notes, assumptions, limitations

## Non-goals
- No ML models
- No dashboard/UI
- Focus is on SQL-first data products, reliability, and quality checks
