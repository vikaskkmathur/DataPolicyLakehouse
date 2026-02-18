import pyspark.sql.functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("runDate", "", "Run Date (YYYY-MM-DD)") 
runDate = dbutils.widgets.get("runDate").strip()
if not runDate:
    raise ValueError("runDate parameter is required (YYYY-MM-DD). Pass it from ADF baseParameters.")

CATALOG = "dbw_policy_lakehouse_dev_v1"
SCHEMA  = "default"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

SRC = f"{CATALOG}.{SCHEMA}.silver_policyholders_current"
TGT = f"{CATALOG}.{SCHEMA}.silver_policyholders_scd2"

# 1) Source snapshot for this run
src = (spark.table(SRC)
       .where(F.col("_runDate") == runDate))

# 2) Hash the attributes you want to track
src = (src.withColumn(
            "attr_hash",
            F.sha2(F.concat_ws("||",
                               F.coalesce(F.col("full_name"), F.lit("")),
                               F.coalesce(F.col("address"), F.lit("")),
                               F.coalesce(F.col("city"), F.lit("")),
                               F.coalesce(F.col("state"), F.lit("")),
                               F.coalesce(F.col("zip"), F.lit(""))
                              ), 256))
       .withColumn("effective_start_date", F.lit(runDate))
       .withColumn("effective_end_date", F.lit("9999-12-31"))
       .withColumn("is_current", F.lit(True))
       .withColumn("_loaded_at", F.current_timestamp())
)

# 3) If target doesn't exist yet, initialize it
if not spark.catalog.tableExists(TGT):
    (src.write.format("delta").mode("overwrite").saveAsTable(TGT))
    print("✅ Initialized SCD2 table")
else:
    tgt = DeltaTable.forName(spark, TGT)

    current = (spark.table(TGT).where(F.col("is_current") == True)
               .select("holder_id","policy_id","attr_hash"))

    # changed or new keys = those not matching current hash
    changes = (src.alias("s")
               .join(current.alias("c"),
                     on=["holder_id","policy_id"],
                     how="left")
               .where(F.col("c.attr_hash").isNull() | (F.col("s.attr_hash") != F.col("c.attr_hash")))
               .select("s.*"))

    # 4) Expire previous current rows for keys that changed
    (tgt.alias("t")
        .merge(changes.alias("s"),
               "t.holder_id = s.holder_id AND t.policy_id = s.policy_id AND t.is_current = true")
        .whenMatchedUpdate(set={
            "effective_end_date": "s.effective_start_date",
            "is_current": "false"
        })
        .execute())

    # 5) Insert new current versions
    (changes.write.format("delta").mode("append").saveAsTable(TGT))

    print(f"✅ SCD2 processed for runDate={runDate}")
