import time
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# -------------------------
# Parameters
# -------------------------
dbutils.widgets.text("runDate", "", "Run Date (YYYY-MM-DD)") 
runDate = dbutils.widgets.get("runDate").strip()
if not runDate:
    raise ValueError("runDate parameter is required (YYYY-MM-DD). Pass it from ADF baseParameters.")

CATALOG = "dbw_policy_lakehouse_dev_v1"
SCHEMA  = "default"

PIPELINE_NAME = "policy_lakehouse"
LAYER = "silver"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

t0 = time.time()

# -------------------------
# Ops log helper (uses correct column ordering)
# -------------------------
def write_ops_log(entity, status, rows_read, rows_valid, rows_rejected, message=None):
    duration = int(time.time() - t0)
    msg = "" if message is None else str(message)

    payload = {
        "pipeline_name": PIPELINE_NAME,
        "layer": LAYER,
        "entity": entity,
        "runDate": runDate,
        "status": status,
        "rows_read": int(rows_read),
        "rows_valid": int(rows_valid),
        "rows_rejected": int(rows_rejected),
        "duration_sec": int(duration),
        "message": msg
    }

    df = (spark.createDataFrame([payload])
            .withColumn("started_at", F.current_timestamp())
            .withColumn("ended_at", F.current_timestamp()))

    ordered = df.select(
        "pipeline_name","layer","entity","runDate","status",
        "rows_read","rows_valid","rows_rejected","duration_sec",
        "message","started_at","ended_at"
    )

    ordered.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.ops_pipeline_run_log")


# -------------------------
# Generic MERGE helper
# -------------------------
def merge_into(target_table, source_df, merge_condition, update_cols, insert_cols):
    tgt = DeltaTable.forName(spark, target_table)
    (tgt.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(set=update_cols)
        .whenNotMatchedInsert(values=insert_cols)
        .execute()
    )


# =========================================================
# POLICIES: Bronze -> Silver (stronger rules + merge)
# Key: policy_id + effective_date
# =========================================================
try:
    pol_bronze = (spark.table(f"{CATALOG}.{SCHEMA}.bronze_policies")
                    .where(F.col("_runDate") == runDate))

    rows_read = pol_bronze.count()

    # Stronger Silver rules (example)
    # - policy_id required
    # - effective_date required
    # - written_premium must be > 0 (not just >=0)
    # - state must be 2 letters
    reject_cond = (
        F.col("policy_id").isNull() |
        F.col("effective_date").isNull() |
        F.col("written_premium").isNull() |
        (F.col("written_premium") <= 0) |
        (F.length(F.col("state")) != 2)
    )

    rejects = (pol_bronze.where(reject_cond)
               .withColumn("reject_reason", F.lit("POLICY_SILVER_RULES_FAILED"))
               .withColumn("rejected_at", F.current_timestamp())
               # keep strings for rejects payload
               .select(
                   "policy_id","product","state",
                   F.col("effective_date").cast("string").alias("effective_date"),
                   F.col("written_premium").cast("string").alias("written_premium"),
                   "status","runDate","_sourceFile","_runDate",
                   "reject_reason","rejected_at"
               ))

    valid = (pol_bronze.where(~reject_cond)
             .withColumn("_loaded_at", F.current_timestamp())
             .dropDuplicates(["policy_id","effective_date","_runDate"]))

    rows_valid = valid.count()
    rows_rej = rejects.count()

    if rows_rej > 0:
        rejects.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.rejects_silver_policies")

    target = f"{CATALOG}.{SCHEMA}.silver_policies"

    # MERGE condition (keys)
    cond = "t.policy_id = s.policy_id AND t.effective_date = s.effective_date"

    update_cols = {
        "product": "s.product",
        "state": "s.state",
        "written_premium": "s.written_premium",
        "status": "s.status",
        "runDate": "s.runDate",
        "_sourceFile": "s._sourceFile",
        "_runDate": "s._runDate",
        "_loaded_at": "s._loaded_at"
    }

    insert_cols = {
        "policy_id": "s.policy_id",
        "product": "s.product",
        "state": "s.state",
        "effective_date": "s.effective_date",
        "written_premium": "s.written_premium",
        "status": "s.status",
        "runDate": "s.runDate",
        "_sourceFile": "s._sourceFile",
        "_runDate": "s._runDate",
        "_loaded_at": "s._loaded_at"
    }

    merge_into(target, valid, cond, update_cols, insert_cols)
    write_ops_log("policies", "SUCCESS", rows_read, rows_valid, rows_rej)

except Exception as e:
    write_ops_log("policies", "FAILED", 0, 0, 0, message=str(e))
    raise


# =========================================================
# PAYMENTS: Key = payment_id
# =========================================================
try:
    pay_bronze = (spark.table(f"{CATALOG}.{SCHEMA}.bronze_payments")
                    .where(F.col("_runDate") == runDate))

    rows_read = pay_bronze.count()

    reject_cond = (
        F.col("payment_id").isNull() |
        F.col("policy_id").isNull() |
        F.col("amount").isNull() |
        (F.col("amount") <= 0) |
        F.col("payment_date").isNull()
    )

    rejects = (pay_bronze.where(reject_cond)
               .withColumn("reject_reason", F.lit("PAYMENT_SILVER_RULES_FAILED"))
               .withColumn("rejected_at", F.current_timestamp())
               .select(
                   "payment_id","policy_id",
                   F.col("amount").cast("string").alias("amount"),
                   F.col("payment_date").cast("string").alias("payment_date"),
                   "method","runDate","_sourceFile","_runDate",
                   "reject_reason","rejected_at"
               ))

    valid = (pay_bronze.where(~reject_cond)
             .withColumn("_loaded_at", F.current_timestamp())
             .dropDuplicates(["payment_id","_runDate"]))

    rows_valid = valid.count()
    rows_rej = rejects.count()

    if rows_rej > 0:
        rejects.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.rejects_silver_payments")

    target = f"{CATALOG}.{SCHEMA}.silver_payments"
    cond = "t.payment_id = s.payment_id"

    update_cols = {
        "policy_id": "s.policy_id",
        "amount": "s.amount",
        "payment_date": "s.payment_date",
        "method": "s.method",
        "runDate": "s.runDate",
        "_sourceFile": "s._sourceFile",
        "_runDate": "s._runDate",
        "_loaded_at": "s._loaded_at"
    }

    insert_cols = {
        "payment_id": "s.payment_id",
        "policy_id": "s.policy_id",
        "amount": "s.amount",
        "payment_date": "s.payment_date",
        "method": "s.method",
        "runDate": "s.runDate",
        "_sourceFile": "s._sourceFile",
        "_runDate": "s._runDate",
        "_loaded_at": "s._loaded_at"
    }

    merge_into(target, valid, cond, update_cols, insert_cols)
    write_ops_log("payments", "SUCCESS", rows_read, rows_valid, rows_rej)

except Exception as e:
    write_ops_log("payments", "FAILED", 0, 0, 0, message=str(e))
    raise


# =========================================================
# POLICYHOLDERS CURRENT: Key = holder_id + policy_id
# (SCD2 comes next)
# =========================================================
try:
    ph_bronze = (spark.table(f"{CATALOG}.{SCHEMA}.bronze_policyholders")
                    .where(F.col("_runDate") == runDate))

    rows_read = ph_bronze.count()

    reject_cond = (
        F.col("holder_id").isNull() |
        F.col("policy_id").isNull() |
        F.col("full_name").isNull()
    )

    rejects = (ph_bronze.where(reject_cond)
               .withColumn("reject_reason", F.lit("POLICYHOLDER_SILVER_RULES_FAILED"))
               .withColumn("rejected_at", F.current_timestamp())
               .select(
                   "holder_id","policy_id","full_name","address","city","state","zip",
                   "runDate","_sourceFile","_runDate",
                   "reject_reason","rejected_at"
               ))

    valid = (ph_bronze.where(~reject_cond)
             .withColumn("_loaded_at", F.current_timestamp())
             .dropDuplicates(["holder_id","policy_id","_runDate"]))

    rows_valid = valid.count()
    rows_rej = rejects.count()

    if rows_rej > 0:
        rejects.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.rejects_silver_policyholders")

    target = f"{CATALOG}.{SCHEMA}.silver_policyholders_current"
    cond = "t.holder_id = s.holder_id AND t.policy_id = s.policy_id"

    update_cols = {
        "full_name": "s.full_name",
        "address": "s.address",
        "city": "s.city",
        "state": "s.state",
        "zip": "s.zip",
        "runDate": "s.runDate",
        "_sourceFile": "s._sourceFile",
        "_runDate": "s._runDate",
        "_loaded_at": "s._loaded_at"
    }

    insert_cols = {
        "holder_id": "s.holder_id",
        "policy_id": "s.policy_id",
        "full_name": "s.full_name",
        "address": "s.address",
        "city": "s.city",
        "state": "s.state",
        "zip": "s.zip",
        "runDate": "s.runDate",
        "_sourceFile": "s._sourceFile",
        "_runDate": "s._runDate",
        "_loaded_at": "s._loaded_at"
    }

    merge_into(target, valid, cond, update_cols, insert_cols)
    write_ops_log("policyholders", "SUCCESS", rows_read, rows_valid, rows_rej)

except Exception as e:
    write_ops_log("policyholders", "FAILED", 0, 0, 0, message=str(e))
    raise


print(f"✅ Bronze → Silver upsert completed for runDate={runDate}")
