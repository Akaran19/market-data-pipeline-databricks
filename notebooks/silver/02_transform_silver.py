# Databricks notebook source
# ============================================================
# Notebook: 02_transform_silver
# Layer: Silver
# Purpose: Clean, validate, and standardize Bronze market data
# Inputs: bronze_prices
# Outputs: silver_prices_daily, silver_prices_rejected
# Guarantees:
#   - Unique (symbol, date)
#   - Valid OHLC values
#   - Positive prices
#   - Volume nullable (FX), but non-negative if present
# ============================================================

# COMMAND ----------
from pyspark.sql import functions as F

SILVER_TABLE = "silver_prices_daily"
REJECT_TABLE = "silver_prices_rejected"

# COMMAND ----------
# Load Bronze
df = spark.table("bronze_prices")

# COMMAND ----------
# Base standardization + latest-record selection if duplicates exist
# (Even though Bronze is deduped, this is defensive and looks professional.)
w = (
    F.window("ingested_at", "100 years")  # placeholder; not used
)

# Create a deterministic "latest" row per (symbol, date) using ingested_at
from pyspark.sql.window import Window
win = Window.partitionBy("symbol", "date").orderBy(F.col("ingested_at").desc())

df_std = (
    df
    .withColumn("rn", F.row_number().over(win))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------
# Define validity conditions
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
    (F.col("low") <= F.least(F.col("open"), F.col("close"), F.col("high")))
)

cond_volume_ok = (
    F.col("volume").isNull() | (F.col("volume") >= 0)
)

is_valid = cond_key & cond_prices_present & cond_prices_positive & cond_ohlc_consistent & cond_volume_ok

# COMMAND ----------
# Build reject reasons (for debugging + credibility)
reject_reason = F.when(~cond_key, F.lit("missing_key")) \
    .when(~cond_prices_present, F.lit("missing_prices")) \
    .when(~cond_prices_positive, F.lit("non_positive_price")) \
    .when(~cond_ohlc_consistent, F.lit("ohlc_inconsistent")) \
    .when(~cond_volume_ok, F.lit("invalid_volume")) \
    .otherwise(F.lit(None))

df_rejects = (
    df_std
    .filter(~is_valid)
    .withColumn("reject_reason", reject_reason)
)

df_valid = df_std.filter(is_valid)

# COMMAND ----------
# Create Silver tables if not exist
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
# Write Silver outputs (overwrite each run is fine for Silver in this project)
# In production you might MERGE, but overwrite keeps it simple and deterministic.
df_valid.write.mode("overwrite").format("delta").saveAsTable(SILVER_TABLE)
df_rejects.write.mode("overwrite").format("delta").saveAsTable(REJECT_TABLE)

# COMMAND ----------
# Validation / reporting
print("[silver_prices_daily] rows:", spark.table(SILVER_TABLE).count())
print("[silver_prices_rejected] rows:", spark.table(REJECT_TABLE).count())

spark.sql(f"SELECT symbol, COUNT(*) n FROM {SILVER_TABLE} GROUP BY symbol ORDER BY symbol").show()
spark.sql(f"SELECT reject_reason, COUNT(*) n FROM {REJECT_TABLE} GROUP BY reject_reason ORDER BY n DESC").show()

display(spark.table(SILVER_TABLE).orderBy(F.col('date').desc()).limit(20))
