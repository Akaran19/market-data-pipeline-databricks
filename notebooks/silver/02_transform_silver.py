# Databricks notebook source
# ============================================================
# Notebook: 02_transform_silver
# Layer: Silver
# Purpose: Clean, validate, and standardize Bronze market data (SQL-first)
# Inputs: bronze_prices
# Outputs: silver_prices_daily, silver_prices_rejected
# Guarantees:
#   - Unique (symbol, date) (latest ingested row kept)
#   - Positive prices (open/high/low/close > 0)
#   - Valid OHLC consistency (high/low bounds)
#   - Volume nullable (FX), but non-negative if present
# ============================================================

# COMMAND ----------
from pyspark.sql import functions as F

SILVER_TABLE = "silver_prices_daily"
REJECT_TABLE = "silver_prices_rejected"

# COMMAND ----------
# 1) SQL-FIRST: typing + dedup (latest ingested_at wins) into a temp view
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_silver_base AS
SELECT
  symbol,
  CAST(date AS DATE)          AS date,
  CAST(open AS DOUBLE)        AS open,
  CAST(high AS DOUBLE)        AS high,
  CAST(low AS DOUBLE)         AS low,
  CAST(close AS DOUBLE)       AS close,
  CAST(volume AS BIGINT)      AS volume,
  source,
  ingested_at,
  input_file
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY ingested_at DESC) AS rn
  FROM bronze_prices
) t
WHERE rn = 1
""")

# COMMAND ----------
# Load the SQL view for validation + rejects
df_std = spark.table("v_silver_base")

# COMMAND ----------
# 2) Define validity conditions (PySpark for clear rule logic + rejected rows)
cond_key = F.col("symbol").isNotNull() & F.col("date").isNotNull()

cond_prices_present = (
    F.col("open").isNotNull() &
    F.col("high").isNotNull() &
    F.col("low").isNotNull() &
    F.col("close").isNotNull()
)

cond_prices_positive = (
    (F.col("open") > 0) &
    (F.col("high") > 0) &
    (F.col("low") > 0) &
    (F.col("close") > 0)
)

cond_ohlc_consistent = (
    (F.col("high") >= F.greatest(F.col("open"), F.col("close"), F.col("low"))) &
    (F.col("low")  <= F.least(F.col("open"), F.col("close"), F.col("high")))
)

cond_volume_ok = (
    F.col("volume").isNull() | (F.col("volume") >= 0)
)

is_valid = cond_key & cond_prices_present & cond_prices_positive & cond_ohlc_consistent & cond_volume_ok

# COMMAND ----------
# 3) Build reject reasons (debuggable pipelines)
reject_reason = (
    F.when(~cond_key, F.lit("missing_key"))
     .when(~cond_prices_present, F.lit("missing_prices"))
     .when(~cond_prices_positive, F.lit("non_positive_price"))
     .when(~cond_ohlc_consistent, F.lit("ohlc_inconsistent"))
     .when(~cond_volume_ok, F.lit("invalid_volume"))
     .otherwise(F.lit(None))
)

df_rejects = (
    df_std
    .filter(~is_valid)
    .withColumn("reject_reason", reject_reason)
)

df_valid = df_std.filter(is_valid)

# COMMAND ----------
# 4) Create Silver tables if not exist (Delta, partitioned by symbol)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
  symbol STRING,
  date DATE,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT,
  source STRING,
  ingested_at TIMESTAMP,
  input_file STRING
)
USING DELTA
PARTITIONED BY (symbol)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REJECT_TABLE} (
  symbol STRING,
  date DATE,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT,
  source STRING,
  ingested_at TIMESTAMP,
  input_file STRING,
  reject_reason STRING
)
USING DELTA
PARTITIONED BY (symbol)
""")

# COMMAND ----------
# 5) Write outputs (deterministic overwrite each run)
df_valid.write.mode("overwrite").format("delta").saveAsTable(SILVER_TABLE)
df_rejects.write.mode("overwrite").format("delta").saveAsTable(REJECT_TABLE)

# COMMAND ----------
# 6) Validation / reporting (SQL checks expected by DE/SQL-heavy teams)
print("[silver_prices_daily] rows:", spark.table(SILVER_TABLE).count())
print("[silver_prices_rejected] rows:", spark.table(REJECT_TABLE).count())

spark.sql(f"""
SELECT symbol, COUNT(*) AS n
FROM {SILVER_TABLE}
GROUP BY symbol
ORDER BY symbol
""").show()

spark.sql(f"""
SELECT reject_reason, COUNT(*) AS n
FROM {REJECT_TABLE}
GROUP BY reject_reason
ORDER BY n DESC
""").show()

# No duplicates (symbol, date)
spark.sql(f"""
SELECT symbol, date, COUNT(*) AS n
FROM {SILVER_TABLE}
GROUP BY symbol, date
HAVING n > 1
""").show()

# No negative/zero prices
spark.sql(f"""
SELECT *
FROM {SILVER_TABLE}
WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
LIMIT 50
""").show()

# Optional: OHLC sanity (should return 0)
spark.sql(f"""
SELECT *
FROM {SILVER_TABLE}
WHERE high < GREATEST(open, close, low)
   OR low  > LEAST(open, close, high)
LIMIT 50
""").show()

display(spark.table(SILVER_TABLE).orderBy(F.col("date").desc()).limit(20))
