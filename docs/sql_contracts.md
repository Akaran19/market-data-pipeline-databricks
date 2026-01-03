# SQL Contracts (How to Query the Data Products)

This project delivers SQL-queryable Delta tables designed for downstream consumption by analysts and quants.
All tables can be queried directly from Databricks SQL.

---

## Tables Overview

### 1) `bronze_prices` (Raw ingestion)
**Purpose:** System-of-record for raw ingested OHLC(V) with provenance.

**Grain:** 1 row per `symbol, date` (idempotent MERGE)

**Key columns:**
- `symbol` STRING
- `date` DATE
- `open/high/low/close` DOUBLE
- `volume` BIGINT (nullable)
- `source` STRING
- `ingested_at` TIMESTAMP
- `input_file` STRING (file provenance)

**Usage:** Debugging ingestion, tracing source issues, backfills.

---

### 2) `silver_prices_daily` (Clean + normalized)
**Purpose:** Trusted daily OHLC(V) after cleaning/validation.

**Grain:** 1 row per `symbol, date`

**Guarantees:**
- No missing keys (`symbol`, `date`)
- No missing OHLC fields
- Positive price values
- OHLC consistency checks applied
- `volume` nullable for instruments where it is not provided (e.g., FX)

**Usage:** Stable base table for analytics and Gold feature computation.

---

### 3) `gold_market_features_daily` (Analytics-ready / pre-trade features)
**Purpose:** Stakeholder-facing data product for pre-trade analysis.

**Grain:** 1 row per `symbol, date`

**Columns:**
- `close` DOUBLE
- `volume` BIGINT (nullable)
- `return_1d` DOUBLE (daily return)
- `vol_20d` DOUBLE (rolling 20-day volatility of returns)
- `avg_volume_20d` DOUBLE (rolling 20-day average volume; nullable for FX)
- `source` STRING
- `computed_at` TIMESTAMP (run timestamp)

**Usage:** Pre-trade monitoring, risk/volatility scans, liquidity screening, feature inputs.

---

### 4) `data_quality_checks` (Operational monitoring)
**Purpose:** Auditable history of data quality and reliability checks per pipeline run.

**Grain:** 1 row per check event (plus a per-run summary row)

**Key columns:**
- `run_ts` TIMESTAMP
- `layer` STRING (silver/gold/pipeline)
- `check_name` STRING
- `symbol` STRING (nullable)
- `check_status` STRING (PASS/FAIL)
- `metric_value` DOUBLE
- `threshold` DOUBLE
- `details` STRING

**Usage:** Identify gaps, jumps, staleness, and row-count anomalies.
