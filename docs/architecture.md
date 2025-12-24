# Architecture (Bronze / Silver / Gold)

## Bronze: Raw ingestion
- Table: bronze_prices
- Goal: ingest raw OHLCV with minimal transformation
- Guarantees: provenance columns (source, ingested_at)

## Silver: Cleaned & standardized
- Table: silver_prices_daily
- Goal: enforce schema + deduplicate + normalize types
- Guarantees: unique (symbol, date), no invalid values

## Gold: Analytics-ready
- Table: gold_market_features_daily
- Goal: deliver pre-trade friendly metrics (returns, vol, liquidity proxies)

## Quality checks
- Table: data_quality_checks
- Goal: record missing days, duplicates, outliers, stale runs
