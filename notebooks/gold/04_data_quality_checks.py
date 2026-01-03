# Databricks notebook source
# ============================================================
# Notebook: 04_data_quality_checks
# Layer: Monitoring / QA
# Purpose: Validate pipeline outputs (Bronze/Silver/Gold) and write results to data_quality_checks.
# Inputs: silver_prices_daily, gold_market_features_daily
# Outputs: data_quality_checks (Delta)
# Notes:
#  - SQL-first checks
#  - Appends one record per failing event + summary checks per symbol
# ============================================================

# COMMAND ----------
# ---- Parameters (tune these once, then leave them) ----
GAP_DAYS_THRESHOLD = 4          # Flag gaps larger than this (calendar days) between observed dates
ABS_RETURN_THRESHOLD = 0.10     # Flag daily moves > 10% (basic jump detector)
STALE_DAYS_THRESHOLD = 7        # Flag if latest date is older than this many days

DQ_TABLE = "data_quality_checks"

# COMMAND ----------
# 0) Ensure table exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_TABLE} (
  run_ts TIMESTAMP,
  layer STRING,
  check_name STRING,
  symbol STRING,
  check_status STRING,
  metric_value DOUBLE,
  threshold DOUBLE,
  details STRING
)
USING DELTA
PARTITIONED BY (layer)
""")

# COMMAND ----------
# 1) Run timestamp (one value for the whole run)
spark.sql("CREATE OR REPLACE TEMP VIEW v_run AS SELECT CURRENT_TIMESTAMP() AS run_ts")

# COMMAND ----------
# 2) MISSING TRADING DAYS (gap detection)
# We don't assume an exchange calendar; we detect unusually large gaps between consecutive dates per symbol.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_gap_checks AS
WITH ordered AS (
  SELECT
    symbol,
    date,
    LAG(date) OVER (PARTITION BY symbol ORDER BY date) AS prev_date
  FROM silver_prices_daily
),
gaps AS (
  SELECT
    symbol,
    date,
    prev_date,
    DATEDIFF(date, prev_date) AS gap_days
  FROM ordered
  WHERE prev_date IS NOT NULL
)
SELECT
  (SELECT run_ts FROM v_run) AS run_ts,
  'silver' AS layer,
  'missing_trading_days_gap' AS check_name,
  symbol,
  CASE WHEN gap_days > {GAP_DAYS_THRESHOLD} THEN 'FAIL' ELSE 'PASS' END AS check_status,
  CAST(gap_days AS DOUBLE) AS metric_value,
  CAST({GAP_DAYS_THRESHOLD} AS DOUBLE) AS threshold,
  CONCAT('Gap detected: ', CAST(prev_date AS STRING), ' -> ', CAST(date AS STRING), ' (', CAST(gap_days AS STRING), ' days)') AS details
FROM gaps
WHERE gap_days > {GAP_DAYS_THRESHOLD}
""")

# COMMAND ----------
# 3) SUDDEN PRICE JUMPS (basic threshold on daily returns)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_jump_checks AS
SELECT
  (SELECT run_ts FROM v_run) AS run_ts,
  'gold' AS layer,
  'sudden_price_jump' AS check_name,
  symbol,
  'FAIL' AS check_status,
  CAST(ABS(return_1d) AS DOUBLE) AS metric_value,
  CAST({ABS_RETURN_THRESHOLD} AS DOUBLE) AS threshold,
  CONCAT('abs(return_1d)=', CAST(ABS(return_1d) AS STRING), ' on ', CAST(date AS STRING)) AS details
FROM gold_market_features_daily
WHERE return_1d IS NOT NULL
  AND ABS(return_1d) > {ABS_RETURN_THRESHOLD}
""")

# COMMAND ----------
# 4) STALE DATA DETECTION (per symbol)
# If latest available date is too old vs current_date, flag it.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_stale_checks AS
WITH latest AS (
  SELECT
    symbol,
    MAX(date) AS latest_date
  FROM gold_market_features_daily
  GROUP BY symbol
),
stale AS (
  SELECT
    symbol,
    latest_date,
    DATEDIFF(CURRENT_DATE(), latest_date) AS days_stale
  FROM latest
)
SELECT
  (SELECT run_ts FROM v_run) AS run_ts,
  'gold' AS layer,
  'stale_data' AS check_name,
  symbol,
  CASE WHEN days_stale > {STALE_DAYS_THRESHOLD} THEN 'FAIL' ELSE 'PASS' END AS check_status,
  CAST(days_stale AS DOUBLE) AS metric_value,
  CAST({STALE_DAYS_THRESHOLD} AS DOUBLE) AS threshold,
  CONCAT('Latest date = ', CAST(latest_date AS STRING), ', days_stale = ', CAST(days_stale AS STRING)) AS details
FROM stale
WHERE days_stale > {STALE_DAYS_THRESHOLD}
""")

# COMMAND ----------
# 5) OPTIONAL: Summary row counts by layer (nice operational signal)
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_layer_counts AS
SELECT
  (SELECT run_ts FROM v_run) AS run_ts,
  'pipeline' AS layer,
  'row_counts' AS check_name,
  NULL AS symbol,
  'PASS' AS check_status,
  CAST(NULL AS DOUBLE) AS metric_value,
  CAST(NULL AS DOUBLE) AS threshold,
  CONCAT(
    'bronze_prices=', CAST((SELECT COUNT(*) FROM bronze_prices) AS STRING),
    '; silver_prices_daily=', CAST((SELECT COUNT(*) FROM silver_prices_daily) AS STRING),
    '; gold_market_features_daily=', CAST((SELECT COUNT(*) FROM gold_market_features_daily) AS STRING)
  ) AS details
""")

# COMMAND ----------
# 6) Union all checks and append to data_quality_checks
spark.sql(f"""
INSERT INTO {DQ_TABLE}
SELECT * FROM v_gap_checks
UNION ALL
SELECT * FROM v_jump_checks
UNION ALL
SELECT * FROM v_stale_checks
UNION ALL
SELECT * FROM v_layer_counts
""")

# COMMAND ----------
# 7) Reporting: what failed in this run?
spark.sql(f"""
SELECT layer, check_name, symbol, check_status, metric_value, threshold, details
FROM {DQ_TABLE}
WHERE run_ts = (SELECT run_ts FROM v_run)
  AND check_status = 'FAIL'
ORDER BY layer, check_name, symbol
""").show(truncate=False)

# COMMAND ----------
# 8) Quick health summary for this run
spark.sql(f"""
SELECT layer, check_name, check_status, COUNT(*) AS n
FROM {DQ_TABLE}
WHERE run_ts = (SELECT run_ts FROM v_run)
GROUP BY layer, check_name, check_status
ORDER BY layer, check_name, check_status
""").show(truncate=False)
