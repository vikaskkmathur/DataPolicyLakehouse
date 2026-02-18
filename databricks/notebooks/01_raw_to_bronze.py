import time
import pyspark.sql.functions as F

# -------------------------
# Parameters
# -------------------------
# For Test Run in databricks on specific date: dbutils.widgets.text("runDate", "2026-02-08")
dbutils.widgets.text("runDate", "", "Run Date (YYYY-MM-DD)") 
runDate = dbutils.widgets.get("runDate").strip()
if not runDate:
    raise ValueError("runDate parameter is required (YYYY-MM-DD). Pass it from ADF baseParameters.")

CATALOG = "dbw_policy_lakehouse_dev_v1"
SCHEMA  = "default"
VOL_RAW = "vol_raw"

PIPELINE_NAME = "policy_lakehouse"
LAYER = "bronze"

t0 = time.time()

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

raw_base = f"/Volumes/{CATALOG}/{SCHEMA}/{VOL_RAW}"
paths = {
    "policies":      f"{raw_base}/policies/runDate={runDate}/",
    "policyholders": f"{raw_base}/policyholders/runDate={runDate}/",
    "payments":      f"{raw_base}/payments/runDate={runDate}/"
}

# -------------------------
# Helpers
# -------------------------
def read_csv_with_source(path: str):
    """
    Read CSV folder. In Unity Catalog, do NOT use input_file_name().
    Use _metadata.file_path when available. [1](https://www.slideshare.net/slideshow/delta-lake-and-the-delta-architecture/249656950)[2](https://www.dremio.com/blog/3-ways-to-convert-a-delta-lake-table-into-an-apache-iceberg-table/)
    """
    df = spark.read.option("header", True).csv(path)

    # _metadata isn't always present for all readers; fallback to path.
    if "_metadata" in df.columns:
        df = df.withColumn("_sourceFile", F.col("_metadata.file_path"))
    else:
        df = df.withColumn("_sourceFile", F.lit(path))

    return df.withColumn("_runDate", F.lit(runDate))


def quarantine_split(df, reject_condition, reason: str):
    """
    Split valid vs invalid (quarantine) pattern. [3](https://stackoverflow.com/questions/60021261/what-is-my-file-system-name-and-storage-account-name-and-how-do-i-find-it)[4](https://learn.microsoft.com/en-us/fabric/data-factory/connector-azure-data-lake-storage-gen2-copy-activity)
    """
    rejects = (df.where(reject_condition)
                 .withColumn("reject_reason", F.lit(reason))
                 .withColumn("rejected_at", F.current_timestamp()))
    valid = df.where(~reject_condition)
    return valid, rejects


import time
import pyspark.sql.functions as F

def write_ops_log(entity, status, rows_read, rows_valid, rows_rejected, t0, message=None):
    
    duration = int(time.time() - t0)
    msg = "" if message is None else str(message)
    #runDateKey = int(runDate.replace("-", ""))
    
    payload = {
        "pipeline_name": PIPELINE_NAME,      # string
        "layer": LAYER,                      # string
        "entity": entity,                    # string
        "runDate": runDate,                  # string
        "status": status,                    # string
        "rows_read": int(rows_read),         # int/long
        "rows_valid": int(rows_valid),       # int/long
        "rows_rejected": int(rows_rejected), # int/long
        "duration_sec": duration,            # int
        "message": msg                       # string (never None)
    }

    log_df = (spark.createDataFrame([payload])
                .withColumn("started_at", F.current_timestamp())
                .withColumn("ended_at", F.current_timestamp()))
    
    ordered_log_df = log_df.select(
        "pipeline_name",
        "layer",
        "entity",
        "runDate",
        "status",
        "rows_read",
        "rows_valid",
        "rows_rejected",
        "duration_sec",
        "message",
        "started_at",
        "ended_at"
    )

    ordered_log_df.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.ops_pipeline_run_log")


# =========================================================
# POLICIES
# =========================================================
try:
    pol_raw = read_csv_with_source(paths["policies"])
    rows_read = pol_raw.count()

    # typed casts for Bronze
    pol_typed = (pol_raw
        .withColumn("effective_date_cast", F.to_date(F.col("effective_date")))
        .withColumn("written_premium_cast", F.col("written_premium").cast("double"))
    )

    reject_cond = (
        F.col("policy_id").isNull() |
        (F.col("effective_date").isNotNull() & F.col("effective_date_cast").isNull()) |
        F.col("written_premium").isNull() |
        F.col("written_premium_cast").isNull() |
        (F.col("written_premium_cast") < 0)
    )

    valid, rej = quarantine_split(pol_typed, reject_cond, "POLICY_BASIC_VALIDATION_FAILED")

    # Bronze output schema (typed)
    pol_valid_out = (valid
        .withColumn("effective_date", F.col("effective_date_cast"))
        .withColumn("written_premium", F.col("written_premium_cast"))
        .select("policy_id","product","state","effective_date","written_premium","status","runDate","_sourceFile","_runDate")
    )

    # Rejects keep raw strings + metadata
    pol_rej_out = (rej
        .select("policy_id","product","state","effective_date","written_premium","status","runDate","_sourceFile","_runDate","reject_reason","rejected_at")
    )

    rows_valid = pol_valid_out.count()
    rows_rej   = pol_rej_out.count()

    pol_valid_out.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.bronze_policies")
    pol_rej_out.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.rejects_policies")

    write_ops_log("policies", "SUCCESS", rows_read, rows_valid, rows_rej, t0=t0)

except Exception as e:
    write_ops_log("policies", "FAILED", 0, 0, 0, t0=t0, message=str(e))
    raise


# =========================================================
# POLICYHOLDERS
# =========================================================
try:
    ph_raw = read_csv_with_source(paths["policyholders"])
    rows_read = ph_raw.count()

    reject_cond = F.col("holder_id").isNull() | F.col("policy_id").isNull()

    valid, rej = quarantine_split(ph_raw, reject_cond, "POLICYHOLDER_BASIC_VALIDATION_FAILED")

    ph_valid_out = valid.select(
        "holder_id","policy_id","full_name","address","city","state","zip","runDate","_sourceFile","_runDate"
    )

    ph_rej_out = rej.select(
        "holder_id","policy_id","full_name","address","city","state","zip","runDate","_sourceFile","_runDate","reject_reason","rejected_at"
    )

    rows_valid = ph_valid_out.count()
    rows_rej   = ph_rej_out.count()

    ph_valid_out.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.bronze_policyholders")
    ph_rej_out.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.rejects_policyholders")

    write_ops_log("policyholders", "SUCCESS", rows_read, rows_valid, rows_rej, t0=t0)

except Exception as e:
    write_ops_log("policyholders", "FAILED", 0, 0, 0, t0=t0, message=str(e))
    raise


# =========================================================
# PAYMENTS
# =========================================================
try:
    pay_raw = read_csv_with_source(paths["payments"])
    rows_read = pay_raw.count()

    pay_typed = (pay_raw
        .withColumn("payment_date_cast", F.to_date(F.col("payment_date")))
        .withColumn("amount_cast", F.col("amount").cast("double"))
    )

    reject_cond = (
        F.col("payment_id").isNull() |
        F.col("amount").isNull() |
        F.col("amount_cast").isNull() |
        (F.col("amount_cast") <= 0) |
        (F.col("payment_date").isNotNull() & F.col("payment_date_cast").isNull())
    )

    valid, rej = quarantine_split(pay_typed, reject_cond, "PAYMENT_BASIC_VALIDATION_FAILED")

    pay_valid_out = (valid
        .withColumn("payment_date", F.col("payment_date_cast"))
        .withColumn("amount", F.col("amount_cast"))
        .select("payment_id","policy_id","amount","payment_date","method","runDate","_sourceFile","_runDate")
    )

    pay_rej_out = rej.select(
        "payment_id","policy_id","amount","payment_date","method","runDate","_sourceFile","_runDate","reject_reason","rejected_at"
    )

    rows_valid = pay_valid_out.count()
    rows_rej   = pay_rej_out.count()

    pay_valid_out.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.bronze_payments")
    pay_rej_out.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.rejects_payments")

    write_ops_log("payments", "SUCCESS", rows_read, rows_valid, rows_rej, t0=t0)

except Exception as e:
    write_ops_log("payments", "FAILED", 0, 0, 0, t0=t0, message=str(e))
    raise


print(f"✅ Raw → Bronze completed for runDate={runDate}")
