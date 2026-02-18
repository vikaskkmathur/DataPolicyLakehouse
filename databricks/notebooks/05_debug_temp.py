#spark.table("dbw_policy_lakehouse_dev_v1.default.bronze_policies").printSchema()
#pol.printSchema()
spark.table("dbw_policy_lakehouse_dev_v1.default.ops_pipeline_run_log").printSchema()
spark.sql("DESCRIBE TABLE dbw_policy_lakehouse_dev_v1.default.bronze_policies").show(truncate=False)
spark.sql("SHOW CREATE TABLE dbw_policy_lakehouse_dev_v1.default.bronze_policies").show(truncate=False)
dbutils.fs.rm("abfss://bronze@stpolicylakevmdev.dfs.core.windows.net/policies", True)
dbutils.fs.rm("abfss://bronze@stpolicylakevmdev.dfs.core.windows.net/policyholders", True)
dbutils.fs.rm("abfss://bronze@stpolicylakevmdev.dfs.core.windows.net/payments", True)

dbutils.fs.rm("abfss://silver@stpolicylakevmdev.dfs.core.windows.net/policies", True)
dbutils.fs.rm("abfss://silver@stpolicylakevmdev.dfs.core.windows.net/policyholders", True)
dbutils.fs.rm("abfss://silver@stpolicylakevmdev.dfs.core.windows.net/payments", True)

dbutils.fs.rm("abfss://gold@stpolicylakevmdev.dfs.core.windows.net/policies", True)
dbutils.fs.rm("abfss://gold@stpolicylakevmdev.dfs.core.windows.net/policyholders", True)
dbutils.fs.rm("abfss://gold@stpolicylakevmdev.dfs.core.windows.net/payments", True)

dbutils.fs.rm("abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/policies", True)
dbutils.fs.rm("abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/policyholders", True)
dbutils.fs.rm("abfss://rejects@stpolicylakevmdev.dfs.core.windows.net/payments", True)

dbutils.fs.rm("abfss://logs@stpolicylakevmdev.dfs.core.windows.net/pipeline_run_log", True)
spark.sql("SELECT COUNT(*) AS c FROM dbw_policy_lakehouse_dev_v1.default.gold_active_policies_daily WHERE _runDate='2026-02-09'").show()
##spark.sql("SELECT COUNT(*) AS c FROM dbw_policy_lakehouse_dev_v1.default.rejects_policies WHERE _runDate='2026-02-09'").show()

#spark.sql("SELECT COUNT(*) AS c FROM dbw_policy_lakehouse_dev_v1.default.bronze_policyholders WHERE _runDate='2026-02-08'").show()
#spark.sql("SELECT COUNT(*) AS c FROM dbw_policy_lakehouse_dev_v1.default.rejects_policyholders WHERE _runDate='2026-02-08'").show()

#spark.sql("SELECT COUNT(*) AS c FROM dbw_policy_lakehouse_dev_v1.default.bronze_payments WHERE _runDate='2026-02-08'").show()
#spark.sql("SELECT COUNT(*) AS c FROM dbw_policy_lakehouse_dev_v1.default.rejects_payments WHERE _runDate='2026-02-08'").show()

spark.sql("SELECT * FROM dbw_policy_lakehouse_dev_v1.default.ops_pipeline_run_log ORDER BY ended_at DESC LIMIT 5").show(truncate=False)
spark.sql(f"DELETE FROM dbw_policy_lakehouse_dev_v1.default.bronze_payments WHERE _runDate='2026-02-08'")
spark.sql(f"DELETE FROM dbw_policy_lakehouse_dev_v1.default.bronze_policies WHERE _runDate='2026-02-08'")
spark.sql(f"DELETE FROM dbw_policy_lakehouse_dev_v1.default.bronze_policyholders WHERE _runDate='2026-02-08'")

spark.sql(f"DELETE FROM dbw_policy_lakehouse_dev_v1.default.ops_pipeline_run_log")
dbutils.fs.ls(f"/Volumes/dbw_policy_lakehouse_dev_v1/default/vol_gold_csv_exports/")
