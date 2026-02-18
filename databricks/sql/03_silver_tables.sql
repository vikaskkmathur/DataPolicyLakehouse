-- Create silver tables (including SCD2)
USE CATALOG dbw_policy_lakehouse_dev_v1;
USE SCHEMA default;

-- =========================
-- SILVER TABLES
-- =========================

CREATE TABLE IF NOT EXISTS silver_policies (
  policy_id STRING,
  product STRING,
  state STRING,
  effective_date DATE,
  written_premium DOUBLE,
  status STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING,
  _loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stpolicylakevmdev.dfs.core.windows.net/policies/';

CREATE TABLE IF NOT EXISTS silver_payments (
  payment_id STRING,
  policy_id STRING,
  amount DOUBLE,
  payment_date DATE,
  method STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING,
  _loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stpolicylakevmdev.dfs.core.windows.net/payments/';

CREATE TABLE IF NOT EXISTS silver_policyholders_current (
  holder_id STRING,
  policy_id STRING,
  full_name STRING,
  address STRING,
  city STRING,
  state STRING,
  zip STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING,
  _loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stpolicylakevmdev.dfs.core.windows.net/policyholders_current/';


-- =========================
-- SILVER REJECTS TABLES
-- (keep raw strings + reject metadata, same reasoning as Bronze rejects)
-- =========================

CREATE TABLE IF NOT EXISTS rejects_silver_policies (
  policy_id STRING,
  product STRING,
  state STRING,
  effective_date STRING,
  written_premium STRING,
  status STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING,
  reject_reason STRING,
  rejected_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/silver/policies/';

CREATE TABLE IF NOT EXISTS rejects_silver_payments (
  payment_id STRING,
  policy_id STRING,
  amount STRING,
  payment_date STRING,
  method STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING,
  reject_reason STRING,
  rejected_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/silver/payments/';

CREATE TABLE IF NOT EXISTS rejects_silver_policyholders (
  holder_id STRING,
  policy_id STRING,
  full_name STRING,
  address STRING,
  city STRING,
  state STRING,
  zip STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING,
  reject_reason STRING,
  rejected_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/silver/policyholders/';

USE CATALOG dbw_policy_lakehouse_dev_v1;
USE SCHEMA default;

CREATE TABLE IF NOT EXISTS silver_policyholders_scd2 (
  holder_id STRING,
  policy_id STRING,
  full_name STRING,
  address STRING,
  city STRING,
  state STRING,
  zip STRING,

  attr_hash STRING,

  effective_start_date STRING,  -- keep string to match your runDate approach
  effective_end_date STRING,    -- '9999-12-31' for open-ended
  is_current BOOLEAN,

  runDate STRING,
  _sourceFile STRING,
  _runDate STRING,
  _loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stpolicylakevmdev.dfs.core.windows.net/policyholders_scd2/';