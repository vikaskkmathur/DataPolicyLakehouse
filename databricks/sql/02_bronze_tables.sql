-- Create bronze Delta tables
USE CATALOG dbw_policy_lakehouse_dev_v1;
USE SCHEMA default;

-- BRONZE
CREATE TABLE bronze_policies (
  policy_id STRING,
  product STRING,
  state STRING,
  effective_date DATE,
  written_premium DOUBLE,
  status STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING
)
USING DELTA
LOCATION 'abfss://bronze@stpolicylakevmdev.dfs.core.windows.net/policies/'
PARTITIONED BY (_runDate);

CREATE TABLE bronze_policyholders (
  holder_id STRING,
  policy_id STRING,
  full_name STRING,
  address STRING,
  city STRING,
  state STRING,
  zip STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING
)
USING DELTA
LOCATION 'abfss://bronze@stpolicylakevmdev.dfs.core.windows.net/policyholders/'
PARTITIONED BY (_runDate);

CREATE TABLE bronze_payments (
  payment_id STRING,
  policy_id STRING,
  amount DOUBLE,
  payment_date DATE,
  method STRING,
  runDate STRING,
  _sourceFile STRING,
  _runDate STRING
)
USING DELTA
LOCATION 'abfss://bronze@stpolicylakevmdev.dfs.core.windows.net/payments/'
PARTITIONED BY (_runDate);

-- REJECTS (same columns + reject metadata)
CREATE TABLE rejects_policies (
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
LOCATION 'abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/policies/'
PARTITIONED BY (_runDate);

CREATE TABLE rejects_policyholders (
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
LOCATION 'abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/policyholders/'
PARTITIONED BY (_runDate);

CREATE TABLE rejects_payments (
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
LOCATION 'abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/payments/'
PARTITIONED BY (_runDate);

-- OPS LOG
CREATE TABLE ops_pipeline_run_log (
  pipeline_name STRING,
  layer STRING,
  entity STRING,
  runDate STRING,
  status STRING,
  rows_read BIGINT,
  rows_valid BIGINT,
  rows_rejected BIGINT,
  duration_sec INT,
  message STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://logs@stpolicylakevmdev.dfs.core.windows.net/pipeline_run_log/';