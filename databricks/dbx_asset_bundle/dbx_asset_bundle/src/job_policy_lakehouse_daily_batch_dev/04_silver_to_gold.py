# Databricks notebook source
import pyspark.sql.functions as F

# --- PARAMETERS ---
dbutils.widgets.text("runDate", "", "Run Date (YYYY-MM-DD)") 
dbutils.widgets.text("exportCsv", "false", "Export CSV (true/false)")

runDate = dbutils.widgets.get("runDate").strip()
if not runDate:
    raise ValueError("runDate parameter is required (YYYY-MM-DD). Pass it from ADF baseParameters.")

exportCsv_str = dbutils.widgets.get("exportCsv").strip().lower()
EXPORT_CSV = exportCsv_str in ("true", "1", "yes", "y")

CATALOG = "dbw_policy_lakehouse_dev_v1"
SCHEMA  = "default"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# -------------------------
# Source tables
# -------------------------
pol = spark.table(f"{CATALOG}.{SCHEMA}.silver_policies")
pay = spark.table(f"{CATALOG}.{SCHEMA}.silver_payments")

# Only process records loaded in this run
pol_run = pol.where(F.col("_runDate") == runDate)
pay_run = pay.where(F.col("_runDate") == runDate)

loaded_at = F.current_timestamp()

# -------------------------
# GOLD 1: Written premium daily by state/product
# -------------------------
gold_wp = (pol_run
    .groupBy(F.col("effective_date").alias("day"), "state", "product")
    .agg(
        F.sum("written_premium").alias("written_premium"),
        F.countDistinct("policy_id").alias("policy_count")
    )
    .withColumn("_runDate", F.lit(runDate))
    .withColumn("_loaded_at", loaded_at)
)

# -------------------------
# GOLD 2: Payments daily totals
# -------------------------
gold_pay = (pay_run
    .groupBy(F.col("payment_date").alias("day"))
    .agg(
        F.sum("amount").alias("payments_total"),
        F.count("*").alias("payment_count")
    )
    .withColumn("_runDate", F.lit(runDate))
    .withColumn("_loaded_at", loaded_at)
)

# -------------------------
# GOLD 3: Simple active vs cancelled policies counts
# -------------------------
gold_active = (pol_run
    .groupBy(F.col("effective_date").alias("day"))
    .agg(
        F.sum(F.when(F.col("status") == "Active", 1).otherwise(0)).cast("bigint").alias("active_policies"),
        F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).cast("bigint").alias("cancelled_policies")
    )
    .withColumn("_runDate", F.lit(runDate))
    .withColumn("_loaded_at", loaded_at)
)

# -------------------------
# Idempotency: remove existing results for this runDate (rerun-safe)
# -------------------------
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.gold_written_premium_daily WHERE _runDate = '{runDate}'")
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.gold_payments_daily WHERE _runDate = '{runDate}'")
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.gold_active_policies_daily WHERE _runDate = '{runDate}'")

# -------------------------
# Write gold tables (insert with correct column ordering)
# -------------------------
gold_wp.select("day","state","product","written_premium","policy_count","_runDate","_loaded_at") \
       .write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.gold_written_premium_daily")

gold_pay.select("day","payments_total","payment_count","_runDate","_loaded_at") \
        .write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.gold_payments_daily")

gold_active.select("day","active_policies","cancelled_policies","_runDate","_loaded_at") \
          .write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.gold_active_policies_daily")

print(f"✅ Silver → Gold completed for runDate={runDate}")

# -------------------------
# OPTIONAL: CSV exports (tiny cost impact; easy to view/download)
# EXPORT_CSV = True
# -------------------------

if EXPORT_CSV:
    
    csv_base = f"/Volumes/{CATALOG}/{SCHEMA}/vol_gold_csv_exports/runDate={runDate}"

    (gold_wp.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(f"{csv_base}/written_premium_daily"))

    (gold_pay.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(f"{csv_base}/payments_daily"))

    (gold_active.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(f"{csv_base}/active_policies_daily"))

    print("✅ CSV exports written to:", csv_base)