# Databricks notebook source
# ============================================================
# Notebook: 01_ingest_bronze
# Layer: Bronze
# Purpose: Ingest raw Stooq CSVs into a Bronze Delta table with provenance columns.
# Inputs: CSV files in a Databricks Volume (/Volumes/.../market_data/raw/prices/)
# Outputs: Delta table bronze_prices
# Notes: Idempotent ingestion using MERGE on (symbol, date)
# ============================================================

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
# ---- Config ----
RAW_DIR = "/Volumes/workspace/default/portfolio/market_data/raw/prices"
SOURCE = "stooq"
BRONZE_TABLE = "bronze_prices"

# COMMAND ----------
# Read all CSVs (infer schema + header). We'll enforce types explicitly afterwards.
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_DIR}/*.csv")
    .withColumn("input_file", F.input_file_name())
)

# COMMAND ----------
# Extract symbol from filename (expects e.g. /.../SPY.csv)
df = df_raw.withColumn(
    "symbol",
    F.regexp_extract(F.col("input_file"), r"/([^/]+)\.csv$", 1)
)

# COMMAND ----------
# Normalize + enforce types
df = (
    df
    .withColumn("date", F.to_date(F.col("Date")))
    .withColumn("open", F.col("Open").cast("double"))
    .withColumn("high", F.col("High").cast("double"))
    .withColumn("low", F.col("Low").cast("double"))
    .withColumn("close", F.col("Close").cast("double"))
)

# Volume is optional (FX often has none)
if "Volume" in df.columns:
    df = df.withColumn("volume", F.col("Volume").cast("long"))
else:
    df = df.withColumn("volume", F.lit(None).cast("long"))

# Keep only the Bronze columns + provenance
df = (
    df.select("symbol", "date", "open", "high", "low", "close", "volume", "input_file")
      .withColumn("source", F.lit(SOURCE))
      .withColumn("ingested_at", F.current_timestamp())
)

# COMMAND ----------
# Basic cleanup: drop rows missing keys; de-dup within batch
df_clean = (
    df
    .filter(F.col("symbol").isNotNull() & F.col("date").isNotNull())
    .dropDuplicates(["symbol", "date"])
)

# COMMAND ----------
# Create table if it doesn't exist (partition by symbol)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
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

# COMMAND ----------
# Idempotent upsert into Delta using MERGE on (symbol, date)
df_clean.createOrReplaceTempView("bronze_incoming")

spark.sql(f"""
MERGE INTO {BRONZE_TABLE} AS target
USING bronze_incoming AS source
ON target.symbol = source.symbol AND target.date = source.date
WHEN MATCHED THEN UPDATE SET
  target.open = source.open,
  target.high = source.high,
  target.low = source.low,
  target.close = source.close,
  target.volume = source.volume,
  target.source = source.source,
  target.ingested_at = source.ingested_at,
  target.input_file = source.input_file
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# ---- Validation checks ----
print("[bronze_prices] total rows:", spark.table(BRONZE_TABLE).count())
print("[bronze_prices] distinct symbols:", spark.table(BRONZE_TABLE).select("symbol").distinct().count())

spark.table(BRONZE_TABLE).select(
    F.sum(F.col("symbol").isNull().cast("int")).alias("null_symbol"),
    F.sum(F.col("date").isNull().cast("int")).alias("null_date"),
    F.sum(F.col("close").isNull().cast("int")).alias("null_close"),
).show()

display(spark.table(BRONZE_TABLE).orderBy(F.col("date").desc()).limit(20))
