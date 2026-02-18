USE CATALOG dbw_policy_lakehouse_dev_v1;
USE SCHEMA default;

-- Written premium by day/state/product
CREATE TABLE IF NOT EXISTS gold_written_premium_daily (
  day DATE,
  state STRING,
  product STRING,
  written_premium DOUBLE,
  policy_count BIGINT,
  _runDate STRING,
  _loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stpolicylakevmdev.dfs.core.windows.net/written_premium_daily/';

-- Payments by day
CREATE TABLE IF NOT EXISTS gold_payments_daily (
  day DATE,
  payments_total DOUBLE,
  payment_count BIGINT,
  _runDate STRING,
  _loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stpolicylakevmdev.dfs.core.windows.net/payments_daily/';

-- Active policies snapshot by runDate (simple KPI)
CREATE TABLE IF NOT EXISTS gold_active_policies_daily (
  day DATE,
  active_policies BIGINT,
  cancelled_policies BIGINT,
  _runDate STRING,
  _loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stpolicylakevmdev.dfs.core.windows.net/active_policies_daily/';-- Create gold aggregate tables
