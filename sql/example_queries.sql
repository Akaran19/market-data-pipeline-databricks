-- ============================================================
-- Example queries for quants / analysts (pre-trade oriented)
-- Data product: gold_market_features_daily
-- ============================================================

-- 1) Latest snapshot (returns + vol)
SELECT
  symbol,
  date,
  close,
  return_1d,
  vol_20d
FROM gold_market_features_daily
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
ORDER BY symbol;

-- 2) Top moves today (cross-asset monitoring)
SELECT
  symbol,
  date,
  close,
  return_1d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY ABS(return_1d) DESC;

-- 3) Volatility regime scan (which assets are unusually volatile?)
SELECT
  symbol,
  date,
  vol_20d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY vol_20d DESC;

-- 4) Liquidity proxy (average volume over last 20 days)
SELECT
  symbol,
  date,
  avg_volume_20d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY avg_volume_20d DESC;

-- 5) Time window query (e.g., last 60 trading days for one symbol)
SELECT
  date,
  close,
  return_1d,
  vol_20d
FROM gold_market_features_daily
WHERE symbol = 'SPY'
ORDER BY date DESC
LIMIT 60;

-- ============================================================
-- Example SQL queries for pre-trade analysis
-- Data product: gold_market_features_daily
-- ============================================================

-- 1) Latest snapshot per asset (returns + volatility)
-- Typical morning query before trading starts
SELECT
  symbol,
  date,
  close,
  return_1d,
  vol_20d,
  avg_volume_20d
FROM gold_market_features_daily
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
ORDER BY symbol;


-- 2) Assets with largest absolute moves today
-- Used to detect unusual market activity
SELECT
  symbol,
  date,
  close,
  return_1d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY ABS(return_1d) DESC;


-- 3) Volatility regime scan
-- Which assets are currently the most volatile?
SELECT
  symbol,
  date,
  vol_20d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY vol_20d DESC;


-- 4) Liquidity screening (20-day average volume)
-- Used to filter tradable instruments
SELECT
  symbol,
  date,
  avg_volume_20d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
ORDER BY avg_volume_20d DESC;


-- 5) Time series for a single asset (recent window)
-- Typical input for exploratory analysis or model features
SELECT
  date,
  close,
  return_1d,
  vol_20d,
  avg_volume_20d
FROM gold_market_features_daily
WHERE symbol = 'SPY'
ORDER BY date DESC
LIMIT 60;


-- 6) Large-move alert candidates
-- Simple threshold-based screening for risk or monitoring
SELECT
  symbol,
  date,
  return_1d
FROM gold_market_features_daily
WHERE date = (SELECT MAX(date) FROM gold_market_features_daily)
  AND ABS(return_1d) > 0.02
ORDER BY ABS(return_1d) DESC;


-- 7) Volatility expansion detection
-- Assets where volatility is elevated relative to recent history
SELECT
  g.symbol,
  g.date,
  g.vol_20d,
  AVG(g2.vol_20d) AS avg_vol_last_60d
FROM gold_market_features_daily g
JOIN gold_market_features_daily g2
  ON g.symbol = g2.symbol
 AND g2.date BETWEEN g.date - INTERVAL 60 DAYS AND g.date
WHERE g.date = (SELECT MAX(date) FROM gold_market_features_daily)
GROUP BY g.symbol, g.date, g.vol_20d
HAVING g.vol_20d > 1.5 * AVG(g2.vol_20d)
ORDER BY g.vol_20d DESC;


-- 8) Cross-asset comparison on a specific date
-- Useful for dashboards or reports
SELECT
  symbol,
  close,
  return_1d,
  vol_20d
FROM gold_market_features_daily
WHERE date = DATE '2025-12-23'
ORDER BY symbol;


-- 9) Data completeness check (analyst-facing sanity query)
-- Confirms latest data is present for all assets
SELECT
  symbol,
  MAX(date) AS latest_date
FROM gold_market_features_daily
GROUP BY symbol
ORDER BY symbol;
