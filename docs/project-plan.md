Before you start: a “minimum cost” setup (once)
Recommended resources (lean)
	1	Storage Account (ADLS Gen2)
	◦	Create a container like: raw, bronze, silver, gold, logs
	2	Azure Data Factory (Consumption-based)
	3	Azure Databricks workspace
	◦	Use single-node clusters + auto-terminate (10–15 min)
	4	Key Vault (optional but useful)
	◦	Store storage keys/SAS tokens/connection strings
Cost guardrails (do these every time)
	•	Databricks
	◦	Use Single Node cluster
	◦	Smallest VM available (varies by region)
	◦	Auto-terminate: 10 min
	◦	Don’t leave clusters running
	•	ADF
	◦	Prefer Copy Activity + basic orchestration
	◦	Avoid Mapping Data Flows early (they spin Spark behind the scenes → can cost more)
	•	Storage
	◦	Use lifecycle policies (optional) to delete raw files after X days
	•	Budget
	◦	Set an Azure Budget alert at something like $5–$20

8 tiny, useful practice scenarios (ADF + Databricks)
Each scenario is intentionally small, but teaches a pattern you’ll use on real projects.

1) “Hello Pipeline”: HTTP → ADLS Gen2 (Raw landing)
Why it’s useful: Builds the muscle of ingesting external data and landing it in a lake.
What you build
	•	ADF pipeline with:
	◦	Copy Activity
	◦	Source: HTTP (public CSV/JSON)
	◦	Sink: ADLS Gen2 raw/yourdataset/date=YYYY-MM-DD/…
Skills you practice
	•	Linked Services, Datasets
	•	Parameterized paths (date partitions)
	•	Trigger manually or scheduled
Keep it cheap
	•	Small dataset (a few MB)
	•	Run once/day or on-demand
Dataset ideas (small)
	•	Public “sample CSV” files from GitHub (tiny)
	•	Any small open data API that returns JSON

2) Bronze → Silver with Databricks Notebook (Delta Lake basics)
Why: Teaches the modern lakehouse pattern.
What you build
	•	Databricks notebook reads from raw/…
	•	Writes Delta to bronze/…
	•	Cleans/types/dedupes, writes Delta to silver/…
Skills
	•	Spark read/write
	•	Delta format basics
	•	Partitioning by date
	•	Simple data quality checks (null checks, uniqueness)
Cost tips
	•	Single-node cluster + auto-terminate
	•	Dataset small (≤ 50MB)

3) ADF orchestrates Databricks: “Copy → Notebook → Publish”
Why: This is very close to real enterprise orchestration.
What you build ADF pipeline steps:
	1	Copy raw data from HTTP → ADLS raw/
	2	Execute Databricks Notebook (transform raw→silver)
	3	Copy curated output to gold/ as CSV/Parquet
Skills
	•	ADF Databricks Notebook activity
	•	Passing parameters (input path, output path, runDate)
	•	Dependency chaining + failure handling
Keep it cheap
	•	One pipeline run per day (or manual)
	•	Notebook runs 2–5 minutes

4) Incremental loads (Watermark pattern) with ADF
Why: Incremental processing is everywhere.
What you build
	•	ADF pipeline that pulls only “new” data since last run:
	◦	Store a watermark value (e.g., last_loaded_date)
	◦	Use ADF variables/parameters to filter source
	◦	Update watermark after successful run
Where to store watermark (cheap options)
	•	A small JSON file in ADLS (simplest)
	•	ADF pipeline variable + write back to ADLS
	•	(Optional) Azure SQL Database serverless can cost more—skip for now
Skills
	•	Incremental pattern
	•	Idempotency (“rerun safe”)
	•	Simple state management

5) Error handling + alerting “the right way” (super valuable)
Why: Most real work is reliable operations, not just transforms.
What you build
	•	ADF pipeline with:
	◦	Try/Catch style using “On Failure” paths
	◦	Write error details to ADLS logs/
	◦	Optional: send email via Logic Apps (can be minimal)
Skills
	•	Activity dependencies
	•	Capturing error messages
	•	Logging strategy
	•	Re-run behavior
Keep it cheap
	•	No always-on services
	•	Logs stored as small JSON

6) Mini “dimension + fact” model in the lake (Gold aggregates)
Why: Lets you practice analytics outputs without paying for a warehouse.
What you build
	•	Databricks creates:
	◦	dim_* (small lookup tables)
	◦	fact_* (events/transactions)
	•	Then produces a “Gold” aggregate:
	◦	e.g., daily counts, totals by category
Skills
	•	Basic modeling
	•	Aggregations and performance basics
	•	File layout (partition by date)
Cost tip
	•	Avoid standing up Synapse/SQL Warehouse for now
	•	Just write gold outputs as Parquet/Delta + validate using notebook queries

7) Data quality checks + quarantine (“rejects” folder)
Why: This is a strong differentiator in interviews.
What you build
	•	In Databricks:
	◦	Validate schema + required columns
	◦	Split data into:
	▪	silver/valid/
	▪	silver/rejects/ (include reason column)
	•	ADF orchestrates and stores a run summary in logs/
Skills
	•	Practical data validation
	•	Quarantine pattern
	•	Observability (records in/out/rejected)
Cost
	•	Very low, mostly storage pennies

8) “Slowly Changing Dimension Type 2” (SCD2) on a tiny dataset
Why: Common DE interview topic and real-world requirement.
What you build
	•	A small “customer” file with updates (name/address changes)
	•	Databricks merges into a Delta table using:
	◦	effective_start_date / effective_end_date / is_current
	•	ADF triggers notebook daily
Skills
	•	Delta MERGE
	•	SCD2 logic
	•	Incremental updates
Cost
	•	Small compute burst, small data

My top 3 “best ROI for minimal cost”
If you want to keep it super focused, do these first:
	1	ADF Copy (HTTP → ADLS) + partitioned raw landing
	2	ADF triggers Databricks notebook (raw→silver→gold)
	3	Incremental load with watermark + logging + rerun safety
That trio is exactly the core of many production data platforms.
Below is a complete, end-to-end plan + step-by-step setup, then build steps for 2 pipelines + 4 notebooks, including watermark/incremental, MERGE upserts, SCD2, data quality + rejects, and run logging.

What you’ll build (mini “Policy Admin Daily Export → Lakehouse”)
Input (daily batch drops, tiny CSVs)
	•	policies (policy_id, product, state, effective_date, written_premium, status, runDate)
	•	policyholders (holder_id, policy_id, full_name, address, city, state, zip, runDate)
	•	payments (payment_id, policy_id, amount, payment_date, method, runDate)
Lake layers
	•	Landing: simulated vendor/system drop (CSV)
	•	Raw: exact copy from landing (CSV)
	•	Bronze: Delta per runDate (schema applied)
	•	Silver: Delta “curated tables” with MERGE (upsert) + SCD2
	•	Gold: Delta (primary) + optional CSV extracts for easy viewing
Orchestration
	•	ADF pipeline 1: Landing → Raw
	•	ADF pipeline 2: Daily batch:
	◦	read watermark → pick runDates → ForEach
	◦	copy raw + run Databricks notebooks
	◦	log metrics + update watermark

Cost controls (so you stay under $50/month)
Databricks (most important)
	•	Single Node cluster ✅
	•	smallest VM available
	•	Auto-terminate: 10 minutes ✅
	•	Run manually while learning; later schedule 1/day
ADF
	•	Use Copy activity + orchestration
	•	Avoid Mapping Data Flows for now (can spin Spark)
Storage
	•	LRS, cool tier optional
	•	Data size tiny (MBs) → pennies

2-week plan (30–45 minutes/day)
Week 1 — Setup + ingestion + basic transforms
Day 1: Create RG + ADLS Gen2 + containers + folder conventionsDay 2: Create Databricks workspace + single-node cluster + secrets + ADLS accessDay 3: Generate tiny synthetic landing exports (30 days)Day 4: ADF pipeline pl_landing_to_raw (parameterized runDate)Day 5: Databricks notebook 01_raw_to_bronze (Delta)Day 6: Databricks notebook 02_bronze_to_silver_upsert (MERGE policies & payments)Day 7: ADF pipeline pl_daily_policy_batch (orchestrate copy + notebooks)
Week 2 — Incremental + SCD2 + gold + logging
Day 8: Watermark + ForEach incremental (file-based state)Day 9: Add run logging (row counts in/out, rejects, duration)Day 10: SCD2 notebook for policyholders (03_policyholders_scd2)Day 11: Gold aggregates (04_silver_to_gold) + optional CSV exportsDay 12: Data quality rules + rejects/quarantineDay 13: Failure handling paths + alert placeholder (email later if you want)Day 14: Portfolio polish: README + screenshots + simple architecture diagram

Step-by-step instructions (starting from setup)
0) Naming + folder conventions (use these throughout)
Azure resources
	•	Resource group: rg-policy-lakehouse-dev
	•	Storage: stpolicylake<unique>
	•	ADF: adf-policy-lakehouse-dev
	•	Databricks: dbw-policy-lakehouse-dev
Containers
Create containers:
	•	landing, raw, bronze, silver, gold, logs, rejects
Folder layout
Use partition-like folders (simple & common):
	•	landing/policies/runDate=YYYY-MM-DD/policies.csv
	•	raw/policies/runDate=YYYY-MM-DD/policies.csv
	•	Similar for policyholders, payments

1) Azure setup (portal)
1.1 Create Resource Group
Azure Portal → Resource Groups → Create
	•	Name: rg-policy-lakehouse-dev
	•	Region: your preferred region
1.2 Create Storage Account (ADLS Gen2)
Azure Portal → Storage accounts → Create:
	•	Performance: Standard
	•	Redundancy: LRS
	•	Enable Hierarchical namespace (HNS) = ON ✅
Then create containers listed above.
1.3 Create Azure Data Factory
Azure Portal → Data factories → Create
	•	Name: adf-policy-lakehouse-dev
1.4 Create Azure Databricks workspace
Azure Portal → Azure Databricks → Create
	•	Tier: Standard (fine for learning)

2) Databricks setup (cheap + secure enough for learning)
2.1 Create a minimal-cost cluster
Databricks → Compute → Create:
	•	Single Node ✅
	•	Smallest node type
	•	Runtime: LTS
	•	Auto-termination 10 minutes ✅
2.2 Store storage key in Databricks Secrets
	1	Storage account → Access keys → copy Key1
	2	Databricks → Secrets:
	◦	Create scope: kv-scope-dev
	◦	Add secret:
	▪	key: adls_key
	▪	value: your storage key
2.3 Test ADLS access from a notebook
Create notebook 00_env_test and run:
Python

storage_account = "stpolicylake<unique>"

spark.conf.set(
f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
dbutils.secrets.get(scope="kv-scope-dev", key="adls_key")
)

test_path = f"abfss://landing@{storage_account}.dfs.core.windows.net/"
display(dbutils.fs.ls(test_path))

If you see the folder listing → you’re good.
3) Generate synthetic insurance batch exports (landing)
Create notebook 00_generate_landing_exports and run (tiny + realistic):

Python

from datetime import date, timedelta
import pandas as pd
import random

storage_account = "stpolicylake<unique>"
spark.conf.set(
f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
dbutils.secrets.get(scope="kv-scope-dev", key="adls_key")
)

landing = f"abfss://landing@{storage_account}.dfs.core.windows.net"
start = date.today() - timedelta(days=30)

states = ["TX","CA","FL","NY","IL"]
products = ["AUTO","HOME","RENTERS"]
pay_methods = ["ACH","CC","CHECK"]

for i in range(30):
d = start + timedelta(days=i)
runDate = d.isoformat()

# Policies: new/renewal/endorsement mix
policies = []
for idx in range(60):
policy_id = f"P{10000 + idx}"
eff = d.isoformat()
premium = round(random.uniform(200, 2500), 2)
status = random.choice(["Active","Active","Active","Cancelled"]) # mostly active
policies.append([policy_id, random.choice(products), random.choice(states), eff, premium, status, runDate])

policies_df = spark.createDataFrame(
pd.DataFrame(policies, columns=["policy_id","product","state","effective_date","written_premium","status","runDate"])
)
(policies_df.coalesce(1)
.write.mode("overwrite").option("header","true")
.csv(f"{landing}/policies/runDate={runDate}/policies.csv"))

# Policyholders: simulate occasional address change
holders = []
for h in range(25):
holder_id = f"H{5000 + h}"
policy_id = f"P{10000 + random.randint(0,59)}"
name = f"Holder_{h}"
addr = f"{random.randint(1,999)} Main St"
city = random.choice(["Houston","Austin","Dallas","Miami","Orlando","LA","NYC"])
st = random.choice(states)
zipc = str(random.randint(70000, 99999))
holders.append([holder_id, policy_id, name, addr, city, st, zipc, runDate])

holders_df = spark.createDataFrame(
pd.DataFrame(holders, columns=["holder_id","policy_id","full_name","address","city","state","zip","runDate"])
)
(holders_df.coalesce(1)
.write.mode("overwrite").option("header","true")
.csv(f"{landing}/policyholders/runDate={runDate}/policyholders.csv"))

# Payments
payments = []
for p in range(80):
payment_id = f"PMT{runDate.replace('-','')}{p:03d}"
policy_id = f"P{10000 + random.randint(0,59)}"
amt = round(random.uniform(25, 350), 2)
pay_dt = runDate
method = random.choice(pay_methods)
payments.append([payment_id, policy_id, amt, pay_dt, method, runDate])

payments_df = spark.createDataFrame(
pd.DataFrame(payments, columns=["payment_id","policy_id","amount","payment_date","method","runDate"])
)
(payments_df.coalesce(1)
.write.mode("overwrite").option("header","true")
.csv(f"{landing}/payments/runDate={runDate}/payments.csv"))

print("Landing exports generated.")

4) ADF pipeline #1 — pl_landing_to_raw (parameterized copy)
4.1 Create Linked Service to ADLS Gen2
ADF Studio → Manage → Linked services → New:
	•	Azure Data Lake Storage Gen2
	•	Auth: Account key (simple for personal learning)
	•	Test connection
4.2 Create datasets (DelimitedText) with parameters
Create these datasets (source + sink):
	•	ds_landing_policies_csv, ds_raw_policies_csv
	•	ds_landing_policyholders_csv, ds_raw_policyholders_csv
	•	ds_landing_payments_csv, ds_raw_payments_csv
For each dataset:
	•	Add parameter: runDate (String)
	•	Folder path example for policies landing:
	◦	policies/runDate=@{dataset().runDate}/
	•	File name: policies.csv(Repeat similarly for others.)
4.3 Build pipeline
Create pipeline pl_landing_to_raw
	•	Parameter: runDate (String)
Add 3 Copy activities:
	•	CopyPolicies: landing → raw
	•	CopyPolicyholders: landing → raw
	•	CopyPayments: landing → raw
Publish and Trigger Now:
	•	runDate: pick one from your generated range (e.g., yesterday)
Validate in Storage: raw/.../runDate=.../ exists.

5) Databricks notebooks — Bronze/Silver/Gold
Notebook 01_raw_to_bronze (per runDate)
Purpose: read raw CSV → write Delta partition






Python


from pyspark.sql.functions import input_file_name, lit

dbutils.widgets.text("runDate", "")
runDate = dbutils.widgets.get("runDate")

storage_account = "stpolicylake<unique>"
spark.conf.set(
f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
dbutils.secrets.get(scope="kv-scope-dev", key="adls_key")
)

raw = f"abfss://raw@{storage_account}.dfs.core.windows.net"
bronze = f"abfss://bronze@{storage_account}.dfs.core.windows.net"

def to_bronze(entity, file_name):
df = (spark.read.option("header", True)
.csv(f"{raw}/{entity}/runDate={runDate}/")
.withColumn("_runDate", lit(runDate))
.withColumn("_sourceFile", input_file_name()))
(df.write.format("delta")
.mode("overwrite")
.save(f"{bronze}/{entity}/runDate={runDate}/"))

for entity, fname in [("policies","policies.csv"), ("policyholders","policyholders.csv"), ("payments","payments.csv")]:
to_bronze(entity, fname)

print("Bronze written for runDate:", runDate)

Notebook 02_bronze_to_silver_upsert (MERGE policies & payments)
Purpose: typed + deduped + MERGE into curated Delta tables.

Python

from pyspark.sql.functions import col, to_date
from delta.tables import DeltaTable

dbutils.widgets.text("runDate", "")
runDate = dbutils.widgets.get("runDate")

storage_account = "stpolicylake<unique>"
spark.conf.set(
f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
dbutils.secrets.get(scope="kv-scope-dev", key="adls_key")
)

bronze = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
silver = f"abfss://silver@{storage_account}.dfs.core.windows.net"

def upsert_policies():
src = (spark.read.format("delta")
.load(f"{bronze}/policies/runDate={runDate}/")
.select(
col("policy_id"),
col("product"),
col("state"),
to_date(col("effective_date")).alias("effective_date"),
col("written_premium").cast("double").alias("written_premium"),
col("status"),
col("_runDate").alias("ingest_date")
)
.dropDuplicates(["policy_id","effective_date"])
)

tgt_path = f"{silver}/policies"
if not DeltaTable.isDeltaTable(spark, tgt_path):
(src.write.format("delta").mode("overwrite").save(tgt_path))
return

tgt = DeltaTable.forPath(spark, tgt_path)
(tgt.alias("t")
.merge(src.alias("s"),
"t.policy_id = s.policy_id AND t.effective_date = s.effective_date")
.whenMatchedUpdate(set={
"product":"s.product",
"state":"s.state",
"written_premium":"s.written_premium",
"status":"s.status",
"ingest_date":"s.ingest_date"
})
.whenNotMatchedInsert(values={
"policy_id":"s.policy_id",
"product":"s.product",
"state":"s.state",
"effective_date":"s.effective_date",
"written_premium":"s.written_premium",
"status":"s.status",
"ingest_date":"s.ingest_date"
})
.execute()
)

def upsert_payments():
src = (spark.read.format("delta")
.load(f"{bronze}/payments/runDate={runDate}/")
.select(
col("payment_id"),
col("policy_id"),
col("amount").cast("double").alias("amount"),
to_date(col("payment_date")).alias("payment_date"),
col("method"),
col("_runDate").alias("ingest_date")
)
.dropDuplicates(["payment_id"])
)

tgt_path = f"{silver}/payments"
if not DeltaTable.isDeltaTable(spark, tgt_path):
(src.write.format("delta").mode("overwrite").save(tgt_path))
return

tgt = DeltaTable.forPath(spark, tgt_path)
(tgt.alias("t")
.merge(src.alias("s"), "t.payment_id = s.payment_id")
.whenMatchedUpdateAll()
.whenNotMatchedInsertAll()
.execute()
)

upsert_policies()
upsert_payments()
print("Silver upserts done for runDate:", runDate)

Notebook 03_policyholders_scd2 (SCD Type 2)
Purpose: keep history of policyholder changes (address/name changes).

Python

from pyspark.sql.functions import col, lit, current_date, sha2, concat_ws
from delta.tables import DeltaTable

dbutils.widgets.text("runDate", "")
runDate = dbutils.widgets.get("runDate")

storage_account = "stpolicylake<unique>"
spark.conf.set(
f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
dbutils.secrets.get(scope="kv-scope-dev", key="adls_key")
)

bronze = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
silver = f"abfss://silver@{storage_account}.dfs.core.windows.net"

src = (spark.read.format("delta")
.load(f"{bronze}/policyholders/runDate={runDate}/")
.select(
col("holder_id"),
col("policy_id"),
col("full_name"),
col("address"),
col("city"),
col("state"),
col("zip"),
lit(runDate).alias("as_of_date")
))

# hash to detect change
src = src.withColumn(
"attr_hash",
sha2(concat_ws("||", "full_name","address","city","state","zip"), 256)
)

tgt_path = f"{silver}/policyholders_scd2"

if not DeltaTable.isDeltaTable(spark, tgt_path):
init = (src
.withColumn("effective_start_date", col("as_of_date"))
.withColumn("effective_end_date", lit("9999-12-31"))
.withColumn("is_current", lit(True)))
(init.write.format("delta").mode("overwrite").save(tgt_path))
print("Initialized SCD2 table.")
else:
tgt = DeltaTable.forPath(spark, tgt_path)

# Find current records that changed
current = spark.read.format("delta").load(tgt_path).filter(col("is_current") == True)

changes = (src.alias("s")
.join(current.alias("c"), on=["holder_id","policy_id"], how="left")
.filter((col("c.attr_hash").isNull()) | (col("s.attr_hash") != col("c.attr_hash")))
.select("s.*")
)

# Expire current records for changed keys
(tgt.alias("t")
.merge(changes.alias("s"),
"t.holder_id = s.holder_id AND t.policy_id = s.policy_id AND t.is_current = true")
.whenMatchedUpdate(set={
"effective_end_date": "s.as_of_date",
"is_current": "false"
})
.execute())

# Insert new current versions
inserts = (changes
.withColumn("effective_start_date", col("as_of_date"))
.withColumn("effective_end_date", lit("9999-12-31"))
.withColumn("is_current", lit(True)))

(inserts.write.format("delta").mode("append").save(tgt_path))
print("SCD2 processed for runDate:", runDate)

Notebook 04_silver_to_gold (aggregates + optional CSV extracts)
Purpose: create gold reporting tables, then optionally export to CSV.

Python

from pyspark.sql.functions import col, to_date, sum as _sum, count as _count

storage_account = "stpolicylake<unique>"
spark.conf.set(
f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
dbutils.secrets.get(scope="kv-scope-dev", key="adls_key")
)

silver = f"abfss://silver@{storage_account}.dfs.core.windows.net"
gold = f"abfss://gold@{storage_account}.dfs.core.windows.net"

pol = spark.read.format("delta").load(f"{silver}/policies")
pay = spark.read.format("delta").load(f"{silver}/payments")

written_premium_daily = (pol
.groupBy("state","product", col("effective_date").alias("day"))
.agg(_sum("written_premium").alias("written_premium"))
)

payments_daily = (pay
.groupBy(col("payment_date").alias("day"))
.agg(_sum("amount").alias("payments_total"), _count("*").alias("payment_count"))
)

(written_premium_daily.write.format("delta").mode("overwrite").save(f"{gold}/written_premium_daily"))
(payments_daily.write.format("delta").mode("overwrite").save(f"{gold}/payments_daily"))

# Optional CSV extracts (tiny cost impact)
(written_premium_daily.coalesce(1)
.write.mode("overwrite").option("header","true")
.csv(f"{gold}/csv_exports/written_premium_daily"))

(payments_daily.coalesce(1)
.write.mode("overwrite").option("header","true")
.csv(f"{gold}/csv_exports/payments_daily"))

print("Gold tables written (Delta + CSV exports).")

✅ This gives you Delta for durable analytics and CSV for easy download/viewing with negligible added cost.

6) ADF pipeline #2 — pl_daily_policy_batch (incremental via watermark + ForEach)
Why watermark file (cheapest & simplest)
No Azure SQL needed. You store state as JSON in ADLS:
	•	logs/state/watermark.json
Example contents:

JSON


{"lastProcessedRunDate":"2026-01-15"}

6.1 Create initial watermark file
In storage: create blob file at logs/state/watermark.json with the JSON above. Pick a date inside your generated range.
6.2 ADF activities (high-level)
Pipeline parameters:
	•	defaultStartDate (optional) e.g., 2026-01-01
Pipeline variables:
	•	lastDate (String)
	•	runDates (Array)
Activities:
	1	Get Metadata (or Lookup) to read watermark.json
	2	Set Variable lastDate from file content
	3	Until loop (or derived array) to build runDates from lastDate+1 to today
	4	ForEach over runDates:
	◦	Execute pl_landing_to_raw for that date
	◦	Databricks Notebook: 01_raw_to_bronze (runDate)
	◦	Databricks Notebook: 02_bronze_to_silver_upsert (runDate)
	◦	Databricks Notebook: 03_policyholders_scd2 (runDate)
	5	Databricks Notebook: 04_silver_to_gold (optional daily or only after loop)
	6	Update watermark.json to the latest processed date
6.3 Practical implementation note (ADF JSON parsing)
ADF doesn’t love parsing arbitrary JSON from storage without a dataset/format. The most reliable low-friction approach is:
	•	Use a Lookup activity pointing to a DelimitedText dataset with:
	◦	First row only
	◦	File contains just the date string (simpler than JSON)
So your watermark file becomes:
	•	logs/state/watermark.txt containing:
2026-01-15
This is the simplest and avoids JSON parsing pain.
6.4 Build runDates (simple approach)
Instead of generating arrays in ADF (painful), you can:
	•	Choose “process only yesterday” for now (most common pattern)
	•	Or pass a list manually while learning
Low-friction incremental for learning (recommended):
	•	Set pipeline parameter runDate = formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-dd')
	•	Later enhance to full catch-up logic
Since you’re practicing, I suggest this sequence:
	1	First implement “yesterday” only
	2	Then evolve to catch-up using Until/ForEach
If you want, I’ll provide the exact ADF expressions + activity-by-activity click path for catch-up. (It’s doable; just verbose.)

What to do next (actionable)
To keep momentum, here’s the best “start now” order:
	1	Finish Setup (Azure + containers + Databricks cluster + secret + test)
	2	Run 00_generate_landing_exports
	3	Build ADF pl_landing_to_raw and test for a single runDate
	4	Run notebooks 01 + 02 manually for same runDate
	5	Create ADF pl_daily_policy_batch that calls:
	◦	pl_landing_to_raw
	◦	01, 02, 03
	◦	(optional) 04
Your Practice Scenario (Insurance Policy Domain, Batch Only)
“Policy Admin Daily Export → Lakehouse”:
Daily batch drops (tiny CSVs):
	•	policies: policy_id, product, state, effective_date, written_premium, status, runDate
	•	policyholders: holder_id, policy_id, full_name, address, city, state, zip, runDate
	•	payments: payment_id, policy_id, amount, payment_date, method, runDate
Target layers:
	•	raw (CSV, exact copy)
	•	bronze (Delta per runDate)
	•	silver (Delta curated with MERGE + SCD2)
	•	gold (Delta + optional CSV exports)
Orchestration:
	•	ADF pipeline 1: landing → raw (Copy)
	•	ADF pipeline 2: raw → bronze/silver/gold (Databricks activities)
	•	Manual triggers until stable
Why this is realistic:
	•	ADF MI authenticates to Azure services securely (no secrets). [learn.microsoft.com]
	•	ADF can authenticate to Databricks using MI (no PAT token). [techcommun...rosoft.com]

2-Week Plan (Manual Runs, ~30–45 min/day)
Week 1 — Secure setup + first E2E run
Day 1: Create RG + ADLS Gen2 + containers/foldersDay 2: Create ADF + enable System-assigned MIDay 3: Create Databricks + small single-node cluster (auto-terminate)Day 4: Grant ADF MI permissions (Storage + Databricks)Day 5: Generate tiny landing CSV drops (Databricks notebook)Day 6: ADF pipeline pl_landing_to_raw (parameterized runDate)Day 7: ADF pipeline pl_daily_policy_batch (raw→bronze→silver; manual trigger)
Week 2 — Incremental patterns + modeling + outputs
Day 8: MERGE upserts (policies, payments)Day 9: SCD2 for policyholdersDay 10: Gold aggregates + optional CSV extractDay 11: Data quality + rejects/quarantineDay 12: Logging (row counts, run summary)Day 13: Failure handling (on-failure branches)Day 14: Portfolio polish (README + screenshots + architecture)

Step-by-Step Instructions — Starting from Setup (MI + Manual Runs)
Phase 0 — Cost Guardrails (Do this first)
	1	Databricks cluster
	◦	Single Node
	◦	smallest VM
	◦	Auto-terminate 10 minutes
	2	ADF
	◦	use Copy Activity + orchestration (avoid Mapping Data Flows early)
	3	Storage
	◦	LRS, tiny files
(Your $50/month budget will be safe if clusters aren’t left running.)

Phase 1 — Create Resources (Azure Portal)
1.1 Resource Group
	•	Create: rg-policy-lakehouse-dev
1.2 Storage Account (ADLS Gen2)
Create storage account with:
	•	Hierarchical namespace (HNS) = Enabled (ADLS Gen2)
	•	Redundancy: LRS
Create containers:
	•	landing, raw, bronze, silver, gold, logs, rejects
1.3 Azure Data Factory
Create: adf-policy-lakehouse-dev
1.4 Azure Databricks
Create workspace (choose the tier you prefer; we’ll handle MI storage access based on capabilities later).

Phase 2 — Enable ADF Managed Identity (System-assigned)
ADF supports managed identity for accessing services without credentials. [learn.microsoft.com]
	1	Azure Portal → your Data Factory
	2	Identity blade
	3	Turn System assigned = On
	4	Save
Copy these values (you’ll use them):
	•	Object ID
	•	Application (client) ID

Phase 3 — Grant ADF MI Access to ADLS Gen2 (RBAC + optional ACL)
3.1 Assign RBAC role (coarse permissions)
ADLS Gen2 data access uses Azure RBAC roles like:
	•	Storage Blob Data Contributor (read/write/delete)
	•	Storage Blob Data Reader (read/list) These roles are specifically for data access (not just management-plane “Contributor”). [learn.microsoft.com]
Steps
	1	Storage account → Access Control (IAM) → Add role assignment
	2	Role: Storage Blob Data Contributor
	3	Assign access to: Managed identity
	4	Select: your ADF managed identity
	5	Scope: recommended container level (ex: raw, landing) or storage account level while learning
3.2 (Sometimes required) Add POSIX ACLs (fine-grained permissions)
With HNS enabled, ACLs can be evaluated along with RBAC, and ACLs are used for directory/file-level permissions. [learn.microsoft.com]
If you get “permission denied” while accessing folders:
	1	Storage account → Containers → choose container (e.g., raw)
	2	Select a folder (or create one) → Manage ACL
	3	Add your ADF MI with:
	◦	rwx on folders it needs to traverse/write (at least x to traverse)
	4	Apply to subfolders if needed
Practical learning shortcut: assign RBAC at storage account scope first; if you hit HNS permission issues, add ACLs to just your main root folders (raw/, bronze/, etc.).

Phase 4 — Create ADF Linked Service to ADLS Using Managed Identity
Managed identity is supported in ADF for accessing storage resources. [learn.microsoft.com]
	1	Open ADF Studio
	2	Manage → Linked services → New
	3	Choose Azure Data Lake Storage Gen2
	4	Authentication method: Managed Identity
	5	Select your storage account
	6	Test connection → Create

Phase 5 — Configure ADF → Databricks Using ADF Managed Identity (No PAT)
ADF Databricks activities support Managed Identity authentication to Databricks REST APIs, so you can avoid PAT tokens. [techcommun...rosoft.com]
5.1 Grant ADF MI access inside Databricks workspace
High-level requirement: your ADF MI must have rights to use the workspace (and for some operations, contributor-level permissions). [techcommun...rosoft.com], [kimanimbugua.com]
Do BOTH of these (most reliable in practice):
	1	Azure Portal (workspace-level)Databricks workspace resource → Access control (IAM)Add role assignment: ContributorAssign to: ADF managed identity [techcommun...rosoft.com], [kimanimbugua.com]
	2	Databricks workspace (workspace ACLs)Databricks → Admin settings → (Service principals / Workspace access)Add the ADF MI service principal and allow workspace access (UI varies by workspace settings).(This aligns with the guidance that MI auth relies on AAD principal access.) [techcommun...rosoft.com], [kimanimbugua.com]
5.2 Create ADF Linked Service to Databricks using MI
	1	ADF Studio → Manage → Linked services → New
	2	Choose Azure Databricks
	3	Workspace: select your Databricks workspace
	4	Authentication type: Managed Service Identity
	5	Choose cluster type:
	◦	For learning: “Existing interactive cluster”
	6	Test connection → Create [techcommun...rosoft.com], [kimanimbugua.com]
Tip: Databricks Job activity is now a best-practice way to orchestrate Databricks Workflows from ADF and supports MI auth as well. (You can start with Notebook activity, then upgrade later.) [databricks.com]

Phase 6 — Databricks → ADLS Gen2 with Managed Identity (Two Options)
Option A (Best, secretless): Unity Catalog + Databricks Access Connector (Managed Identity)
This is the modern “no secrets” approach:
	•	Create a Databricks Access Connector
	•	Grant it Storage roles
	•	Create Storage Credential + External Location in Unity Catalog using that managed identity [docs.azure.cn], [community....bricks.com]
Steps
	1	Azure Portal → Create resource → Azure Databricks Access Connector
	2	Storage account → IAM → Add role assignment
	◦	Role: Storage Blob Data Contributor
	◦	Assign to: the Access Connector managed identity [learn.microsoft.com], [docs.azure.cn]
	3	In Databricks (Unity Catalog):
	◦	Catalog → External data → Credentials → Create credential
	◦	Type: Managed identity
	◦	Provide the Access Connector Resource ID [docs.azure.cn], [community....bricks.com]
	4	Create an External Location for your ADLS path (e.g., abfss://raw@<storage>.dfs.core.windows.net/) and grant your user/workloads permission.
✅ Result: Databricks notebooks can read/write abfss://... without keys/secrets.
Potential caveat: Unity Catalog availability can depend on workspace/account setup. If you don’t see Catalog/External Data options, use Option B for now.

Option B (Fallback, still cheap): Service Principal OAuth (only if Option A not available)
This is not MI, but it’s the lowest-friction fallback for practice if Unity Catalog isn’t enabled. Databricks supports multiple Azure credential methods to access ADLS/Blob (service principal, SAS, keys). [learn.microsoft.com]
If you want, I can provide a “minimal secure” SP setup with secrets in Key Vault (still low cost), but since you asked for MI, I’m keeping the mainline as Option A.

Phase 7 — Build the Two ADF Pipelines (Manual Runs)
Pipeline 1: pl_landing_to_raw
	•	Parameter: runDate (string)
	•	Activities (3 Copy activities):
	◦	landing/policies/runDate=… → raw/policies/runDate=…
	◦	landing/policyholders/runDate=… → raw/policyholders/runDate=…
	◦	landing/payments/runDate=… → raw/payments/runDate=…
Auth: ADLS linked service via ADF MI. [learn.microsoft.com]
Pipeline 2: pl_daily_policy_batch
	•	Parameter: runDate
	•	Steps:
	1	Execute pipeline pl_landing_to_raw
	2	Databricks Notebook/Job activity:
	▪	01_raw_to_bronze
	▪	02_bronze_to_silver_upsert
	▪	03_policyholders_scd2
	▪	04_silver_to_gold (writes Delta + optional CSV)
Auth: Databricks linked service via ADF MI. [techcommun...rosoft.com]
Manual run: Trigger now with a runDate you generated.

Where CSV Fits (without meaningful extra cost)
Keep Delta as the “source of truth” in silver/gold.Export small CSV outputs (gold aggregates) for easy viewing. This adds minimal compute time and negligible storage.



