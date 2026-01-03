# Stakeholder Use Cases (SQL)

## Use Case 1: Pre-trade volatility snapshot
**Question:** “Which assets are most volatile right now?”

```sql
SELECT
  symbol,
  date,
  vol_20d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY vol_20d DESC;
```

## Use Case 2: Liquidity screening

**Question:** “Which assets have sufficient liquidity (volume proxy)?”

```sql
SELECT
  symbol,
  date,
  avg_volume_20d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY avg_volume_20d DESC;
```

## Use Case 3: Large move monitoring (risk / alerts)

**Question:** “What moved the most today?”

```sql
SELECT
  symbol,
  date,
  return_1d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY ABS(return_1d) DESC;
```

## Use Case 4: Data quality triage

**Question:** “Did anything fail in the latest pipeline run?”

```sql
WITH latest AS (
  SELECT MAX(run_ts) AS run_ts
  FROM data_quality_checks
)
SELECT *
FROM data_quality_checks
WHERE run_ts = (SELECT run_ts FROM latest)
  AND check_status = 'FAIL'
ORDER BY layer, check_name, symbol;
```