# Data Quality & Reliability Checks

## Goal
Ensure downstream stakeholders (analysts/quants) can trust the data products by running repeatable checks and recording results on every pipeline run.

## Output Table
- `data_quality_checks` (Delta)
  - `run_ts`: timestamp of the pipeline run
  - `layer`: bronze / silver / gold / pipeline
  - `check_name`: name of the check
  - `symbol`: instrument identifier (nullable for global checks)
  - `check_status`: PASS / FAIL
  - `metric_value`: measured value (e.g., gap days, abs return, days stale)
  - `threshold`: threshold used for FAIL logic
  - `details`: human-readable context

## Checks Implemented

### 1) Missing trading days (gap detection) — Silver
We detect unusually large gaps between consecutive observed dates per symbol:
- Uses `DATEDIFF(date, LAG(date))`
- Flags gaps greater than `GAP_DAYS_THRESHOLD`
- Does not assume a perfect exchange calendar; detects anomalies pragmatically.

### 2) Sudden price jumps — Gold
Flags unusually large daily returns:
- Checks `ABS(return_1d) > ABS_RETURN_THRESHOLD`
- Captures symbol + date in the details for investigation.

### 3) Stale data — Gold
Flags symbols whose latest available date is too old relative to the current date:
- `DATEDIFF(CURRENT_DATE(), MAX(date)) > STALE_DAYS_THRESHOLD`

### 4) Pipeline row counts (operational signal)
Records row counts across Bronze/Silver/Gold each run to make failures visible (e.g., sudden drops to zero).

## How to Use
- Run notebook `04_data_quality_checks` after Bronze/Silver/Gold.
- Query failures for the latest run:
  - filter `data_quality_checks` by the latest `run_ts` and `check_status='FAIL'`.
