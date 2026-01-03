# Databricks notebook source
# ============================================================
# Notebook: 03_aggregate_gold
# Layer: Gold
# Purpose: Compute analytics-ready market features for pre-trade analysis (returns, vol, liquidity proxy).
# Inputs: silver_prices_daily
# Outputs: gold_market_features_daily (Delta table)
# Notes:
#  - SQL-first transformations via spark.sql
#  - One row per (symbol, date)
#  - Rolling windows use ROWS BETWEEN 19 PRECEDING AND CURRENT ROW (20 trading days)
# ============================================================

# COMMAND ----------
GOLD_TABLE = "gold_market_features_daily"

# COMMAND ----------
# 1) Create Gold table (Delta)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
  symbol STRING,
  date DATE,

  close DOUBLE,
  volume BIGINT,

  return_1d DOUBLE,
  vol_20d DOUBLE,
  avg_volume_20d DOUBLE,

  source STRING,
  computed_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (symbol)
""")

# COMMAND ----------
# 2) Base view: lagged close -> daily returns
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_gold_base AS
SELECT
  symbol,
  date,
  close,
  volume,
  source,
  (close / LAG(close) OVER (PARTITION BY symbol ORDER BY date)) - 1 AS return_1d
FROM silver_prices_daily
""")

# COMMAND ----------
# 3) Rolling features: 20D vol + 20D avg volume
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_gold_features AS
SELECT
  symbol,
  date,
  close,
  volume,
  return_1d,

  STDDEV_SAMP(return_1d) OVER (
    PARTITION BY symbol
    ORDER BY date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS vol_20d,

  AVG(CAST(volume AS DOUBLE)) OVER (
    PARTITION BY symbol
    ORDER BY date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS avg_volume_20d,

  source,
  CURRENT_TIMESTAMP() AS computed_at
FROM v_gold_base
""")

# COMMAND ----------
# 4) Populate Gold table (deterministic overwrite each run)
spark.sql(f"""
INSERT OVERWRITE {GOLD_TABLE}
SELECT
  symbol,
  date,
  close,
  volume,
  return_1d,
  vol_20d,
  avg_volume_20d,
  source,
  computed_at
FROM v_gold_features
""")

# COMMAND ----------
# 5) Basic validations (stakeholder usability checks)

# One row per (symbol, date)
spark.sql(f"""
SELECT symbol, date, COUNT(*) AS n
FROM {GOLD_TABLE}
GROUP BY symbol, date
HAVING n > 1
""").show()

# Coverage by symbol
spark.sql(f"""
SELECT
  symbol,
  COUNT(*) AS n,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM {GOLD_TABLE}
GROUP BY symbol
ORDER BY symbol
""").show()

# Latest snapshot preview
spark.sql(f"""
SELECT *
FROM {GOLD_TABLE}
ORDER BY date DESC, symbol
LIMIT 50
""").show(truncate=False)
