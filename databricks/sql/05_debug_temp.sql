USE CATALOG dbw_policy_lakehouse_dev_v1;
USE SCHEMA default;

SHOW TABLES;

SELECT COUNT(*) AS c
FROM dbw_policy_lakehouse_dev_v1.default.silver_policies
WHERE _runDate = '2026-02-08';

SELECT COUNT(*) AS c
FROM dbw_policy_lakehouse_dev_v1.default.rejects_silver_policies
WHERE _runDate = '2026-02-08';

SELECT *
FROM dbw_policy_lakehouse_dev_v1.default.ops_pipeline_run_log
ORDER BY ended_at DESC
LIMIT 20;

SELECT holder_id, policy_id, effective_start_date, effective_end_date, is_current
FROM dbw_policy_lakehouse_dev_v1.default.silver_policyholders_scd2
ORDER BY holder_id, policy_id, effective_start_date;

SELECT *
FROM dbw_policy_lakehouse_dev_v1.default.gold_written_premium_daily
WHERE _runDate = '2026-02-08'
ORDER BY day, state, product;

SELECT *
FROM dbw_policy_lakehouse_dev_v1.default.gold_payments_daily
WHERE _runDate = '2026-02-08'
ORDER BY day;

SELECT *
FROM dbw_policy_lakehouse_dev_v1.default.gold_active_policies_daily
WHERE _runDate = '2026-02-08'
ORDER BY day;
