Phase 1 — Create a Resource Group
	1	Go to Azure Portal → search Resource groups
	2	Click Create
	3	Set:
	◦	Subscription: your new subscription
	◦	Resource group name: rg-policy-lakehouse-dev
	◦	Region: pick one close to you (for latency) or where you plan to keep all services
	4	Click Review + create → Create
✅ Outcome: one container to manage & delete everything later with a single action.

Phase 2 — Create ADLS Gen2 (Storage Account) + Containers
We’ll use ADLS Gen2 because it supports lake-style folders and RBAC/ACL permissions. [learn.microsoft.com]
2.1 Create the Storage Account (ADLS Gen2)
	1	Azure Portal → search Storage accounts → Create
	2	Basics:
	◦	Resource group: rg-policy-lakehouse-dev
	◦	Storage account name: stpolicylake<unique> (must be globally unique)
	◦	Region: same as your RG (recommended)
	◦	Performance: Standard
	◦	Redundancy: LRS (cheapest)
	3	Advanced tab:
	◦	Enable Hierarchical namespace = ON ✅ (this is what makes it ADLS Gen2)
	◦	Access Tier - Cool
	4	Click Review + create → Create
2.2 Create containers (lake zones)
	1	Open the storage account → left menu Data storage → Containers
	2	Create these containers:
	◦	landing (simulated source drops)
	◦	raw
	◦	bronze
	◦	silver
	◦	gold
	◦	logs
	◦	rejects
✅ Outcome: you now have a low-cost lake layout.
Phase 3 — Create Azure Data Factory (ADF) + enable Managed Identity
ADF can use Managed Identity to authenticate to services like Storage without storing secrets. [learn.microsoft.com]
3.1 Create ADF
	1	Azure Portal → search Data factories → Create
	2	Basics:
	◦	Resource group: rg-policy-lakehouse-dev
	◦	Name: adf-policy-lakehouse-dev
	◦	Region: same region as storage
	3	Create
3.2 Enable System-assigned Managed Identity on ADF
	1	Open adf-policy-lakehouse-dev
	2	Left menu → Identity
	3	Set System assigned → On
	4	Click Save
Managed Identity in ADF eliminates credential management and uses Entra tokens instead. [learn.microsoft.com]
✅ Outcome: ADF now has an identity you can grant permissions to.
Phase 4 — Grant ADF MI access to ADLS (RBAC + optional ACL)
ADLS authorization commonly involves:
	•	Azure RBAC for container/account scope permissions (coarse-grain) [learn.microsoft.com]
	•	ACLs for folder/file level permissions (fine-grain) [learn.microsoft.com]
4.1 Assign RBAC role to ADF MI
	1	Go to your Storage account → Access Control (IAM) → Add role assignment
	2	Role: Storage Blob Data Contributor (read/write/delete data) [learn.microsoft.com]
	3	Assign access to: Managed identity
	4	Select: your ADF managed identity (adf-policy-lakehouse-dev)
	5	Scope:
	◦	easiest while learning: Storage account
	◦	more controlled: assign per container (landing, raw, etc.)
4.2 (Only if needed) Add ACLs
If later you see “Permission denied” when ADF writes into directories:
	1	Storage account → Containers → pick container (e.g., raw)
	2	Create a folder like policies/ and open Manage ACL
	3	Add the ADF managed identity and give:
	◦	folders: rwx
	◦	ensure execute (x) exists to traverse directories [learn.microsoft.com]
✅ Outcome: ADF can read/write in the data lake using MI (no keys).
Phase 5 — Create Databricks workspace (and ADF → Databricks via Managed Identity)
ADF Databricks activities can authenticate using Managed Identity (no PAT tokens required). [techcommun...rosoft.com]
5.1 Create Azure Databricks workspace
	1	Azure Portal → search Azure Databricks → Create
	2	Basics:
	◦	Resource group: rg-policy-lakehouse-dev
	◦	Workspace name: dbw-policy-lakehouse-dev
	◦	Region: same region
	◦	Pricing tier: choose what you prefer (Standard is okay for learning; features differ by account/tier)
	◦	workspace type - hybrid
	3	Create
5.2 Grant ADF MI rights to call Databricks APIs
This is required so ADF MI can authenticate to Databricks REST APIs. [techcommun...rosoft.com], [kimanimbugua.com]
	1	Open the Databricks workspace resource in Azure Portal
	2	Access Control (IAM) → Add role assignment
	3	Role: Contributor (in tab - Privileged administrator roles)
	4	Assign to: Managed identity → select adf-policy-lakehouse-dev
ADF MI needs permission on the Databricks workspace to use MI authentication for Databricks linked service / activities. [techcommun...rosoft.com], [kimanimbugua.com]
✅ Outcome: ADF can securely invoke Databricks without PAT.
Phase 6 — Create Databricks compute (cheap)
	1	Open Databricks workspace → Launch Workspace
	2	Go to Compute → Create compute
	3	Configure:
	◦	Single node ✅ (lowest cost)
	◦	Node type: smallest available
	◦	Databricks Runtime: LTS
	◦	Auto-termination: 10 minutes ✅
	4	Create
✅ Outcome: minimal-cost cluster ready.
Phase 7 — Databricks ↔ ADLS access using Managed Identity (two paths)
You asked for Managed Identity “end-to-end”. MI itself adds no cost, but the implementation path depends on whether your workspace has Unity Catalog features available.
Path A (Best / Secretless): Unity Catalog + Access Connector (Managed Identity)
This is Databricks’ modern approach: managed identity via Databricks Access Connector, then Storage Credential + External Location. [docs.azure.cn], [community....bricks.com]
A1) Create Databricks Access Connector
	1	Azure Portal → search Databricks Access Connector → Create
	2	Put it in rg-policy-lakehouse-dev
A2) Grant Access Connector permissions on Storage
	1	Storage account → IAM
	2	Add role assignment: Storage Blob Data Contributor
	3	Assign to: the Access Connector managed identity [learn.microsoft.com], [docs.azure.cn]
A3) In Databricks create storage credential + external location
	1	Databricks → Catalog → External data → Credentials
	2	Create Storage credential using Managed identity and the Access Connector resource ID [docs.azure.cn]
	3	Create External location pointing to:
	◦	abfss://raw@stpolicylake<unique>.dfs.core.windows.net/
	◦	if this gives event grid permission error the give more permissions 
	⁃	register event grid - az provider register --namespace Microsoft.EventGrid
	⁃	Storage Blob Data Contributor (data access) [learn.microsoft.com], [stackoverflow.com]
	⁃	Storage Queue Data Contributor (queue operations) [learn.microsoft.com], [stackoverflow.com]
	⁃	Storage Account Contributor or Contributor (needed because Event Grid setup often requires Microsoft.Storage/storageAccounts/write) [learn.microsoft.com], [stackoverflow.com]
	⁃	EventGrid EventSubscription Contributor (create/manage Event Grid subscription) [learn.microsoft.com], [community....bricks.com]
	◦	and similarly for bronze, silver, etc.
✅ Outcome: Databricks can read/write lake paths with MI and no secrets.

Path B (Fallback only if UC isn’t available): Service Principal OAuth
Databricks supports multiple Azure credential methods for accessing storage (service principal, SAS, keys). If Unity Catalog menus aren’t available, we can still proceed cheaply with SP OAuth (still secure, but not MI). [learn.microsoft.com]
For now, continue to Phase 8 (ADF) — you can build ADF pipelines independent of which Databricks storage access path you use.
Phase 8 — Create ADF linked services (MI)
8.1 ADF → ADLS Gen2 linked service using MI
	1	Open ADF Studio: from ADF resource → Open Azure Data Factory Studio
	2	Manage → Linked services → New
	3	give the name you like may be ls_adls2_pol_lake_dev_mi
	4	Select (Data Store tab) Azure Data Lake Storage Gen2
	5	Auth method: Managed Identity [learn.microsoft.com]
	6	Select your storage account → Test connection → Create
8.2 ADF → Databricks linked service using MI
	1	Manage → Linked services → New
	2	give the name you like may be ls_adb_pol_lake_dev_mi
	3	Select (Compute tab) Azure Databricks
	4	Choose your Databricks workspace
	5	Authentication type: Managed Service Identity [techcommun...rosoft.com]
	6	Cluster: select your existing interactive cluster
	7	Test connection → Create
✅ Outcome: ADF can control storage + Databricks without storing any secrets.
Generate Data Programmatically
1.1 Create External Location for landing (if you don’t already have it)

1.2 Create an External Volume for landing (recommended)
Databricks recommends governing file access using volumes; it keeps your notebooks from hardcoding abfss://… paths everywhere. [community....bricks.com], [dev.to]
Databricks → Catalog → Schema → Create Volumes → Create volume (External). [community....bricks.com], [youtube.com]
Pick:
	•	Catalog: use your workspace catalog “dbw_policy_lakehouse_dev_v1”  (or create a new catalog like policy_dev if you prefer)
	•	Schema: use “default” or create new “landing” (example below uses policy_dev.landing)
	•	Volume name: vol_landing
	•	External location: landing-stpolicylakevmdev
	•	Storage path (optional subdir): leave blank or ./ to use the container root

Repeat for raw 
Repeat for gold (for csv file creation) - (name - vol_gold_csv_exports), (external locations - gold-stpolicylakevmdev) , (path - abfss://gold@stpolicylakevmdev.dfs.core.windows.net/csv_exports).
Part 2 — Databricks notebook: Generate “landing” CSV drops (tiny, partitioned by runDate)
2.1 Create notebook
Databricks → Workspace → Create → NotebookName: 00_generate_landing_exportsLanguage: PythonAttach to your small single-node cluster (you already created). [learn.microsoft.com], [youtube.com]
2.2 Paste and run this notebook code
This writes tiny CSVs to the landing volume in folders like: policies/runDate=YYYY-MM-DD/policies.csv
Note: Writing CSV produces a folder with part-*.csv. That’s fine for ADF Copy (it can copy the folder). If you want a single fixed filename later, we can add a small “rename” step.






Python


from datetime import date, timedelta
import pandas as pd
import random

# CHANGE THESE if you used different catalog/schema/volume names
CATALOG = "policy_dev" # or your workspace catalog name
SCHEMA = "landing"
VOLUME = "vol_landing"

landing_base = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

states = ["TX","CA","FL","NY","IL"]
products = ["AUTO","HOME","RENTERS"]
pay_methods = ["ACH","CC","CHECK"]

start = date.today() - timedelta(days=7) # keep it small/cheap (7 days)

def write_csv(df_spark, out_path):
(df_spark
.coalesce(1)
.write.mode("overwrite")
.option("header","true")
.csv(out_path))

for i in range(7):
d = start + timedelta(days=i)
runDate = d.isoformat()

# policies
policies = []
for idx in range(50): # tiny
policy_id = f"P{10000 + idx}"
eff = runDate
premium = round(random.uniform(200, 2500), 2)
status = random.choice(["Active","Active","Active","Cancelled"])
policies.append([policy_id, random.choice(products), random.choice(states), eff, premium, status, runDate])

df_p = spark.createDataFrame(pd.DataFrame(
policies,
columns=["policy_id","product","state","effective_date","written_premium","status","runDate"]
))
write_csv(df_p, f"{landing_base}/policies/runDate={runDate}/")

# policyholders
holders = []
for h in range(20):
holder_id = f"H{5000 + h}"
policy_id = f"P{10000 + random.randint(0,49)}"
name = f"Holder_{h}"
addr = f"{random.randint(1,999)} Main St"
city = random.choice(["Houston","Austin","Dallas","Miami","Orlando","LA","NYC"])
st = random.choice(states)
zipc = str(random.randint(70000, 99999))
holders.append([holder_id, policy_id, name, addr, city, st, zipc, runDate])

df_h = spark.createDataFrame(pd.DataFrame(
holders,
columns=["holder_id","policy_id","full_name","address","city","state","zip","runDate"]
))
write_csv(df_h, f"{landing_base}/policyholders/runDate={runDate}/")

# payments
payments = []
for p in range(60):
payment_id = f"PMT{runDate.replace('-','')}{p:03d}"
policy_id = f"P{10000 + random.randint(0,49)}"
amt = round(random.uniform(25, 350), 2)
pay_dt = runDate
method = random.choice(pay_methods)
payments.append([payment_id, policy_id, amt, pay_dt, method, runDate])

df_pay = spark.createDataFrame(pd.DataFrame(
payments,
columns=["payment_id","policy_id","amount","payment_date","method","runDate"]
))
write_csv(df_pay, f"{landing_base}/payments/runDate={runDate}/")

print("✅ Landing exports generated under:", landing_base)

2.3 Verify files exist
Run: Python
from datetime import date, timedelta
import pandas as pd
import random

CATALOG = "dbw_policy_lakehouse_dev_v1"     
SCHEMA  = "default"
VOLUME  = "vol_landing"

landing_base = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

states = ["TX","CA","FL","NY","IL"]
products = ["AUTO","HOME","RENTERS"]
pay_methods = ["ACH","CC","CHECK"]

start = date.today() - timedelta(days=7)   # keep it small/cheap (7 days)

def write_csv(df_spark, out_path):
    (df_spark
        .coalesce(1)
        .write.mode("overwrite")
        .option("header","true")
        .csv(out_path))

for i in range(7):
    d = start + timedelta(days=i)
    runDate = d.isoformat()

    # policies
    policies = []
    for idx in range(50):  # tiny
        policy_id = f"P{10000 + idx}"
        eff = runDate
        premium = round(random.uniform(200, 2500), 2)
        status = random.choice(["Active","Active","Active","Cancelled"])
        policies.append([policy_id, random.choice(products), random.choice(states), eff, premium, status, runDate])

    df_p = spark.createDataFrame(pd.DataFrame(
        policies,
        columns=["policy_id","product","state","effective_date","written_premium","status","runDate"]
    ))
    write_csv(df_p, f"{landing_base}/policies/runDate={runDate}/")

    # policyholders
    holders = []
    for h in range(20):
        holder_id = f"H{5000 + h}"
        policy_id = f"P{10000 + random.randint(0,49)}"
        name = f"Holder_{h}"
        addr = f"{random.randint(1,999)} Main St"
        city = random.choice(["Houston","Austin","Dallas","Miami","Orlando","LA","NYC"])
        st = random.choice(states)
        zipc = str(random.randint(70000, 99999))
        holders.append([holder_id, policy_id, name, addr, city, st, zipc, runDate])

    df_h = spark.createDataFrame(pd.DataFrame(
        holders,
        columns=["holder_id","policy_id","full_name","address","city","state","zip","runDate"]
    ))
    write_csv(df_h, f"{landing_base}/policyholders/runDate={runDate}/")

    # payments
    payments = []
    for p in range(60):
        payment_id = f"PMT{runDate.replace('-','')}{p:03d}"
        policy_id = f"P{10000 + random.randint(0,49)}"
        amt = round(random.uniform(25, 350), 2)
        pay_dt = runDate
        method = random.choice(pay_methods)
        payments.append([payment_id, policy_id, amt, pay_dt, method, runDate])

    df_pay = spark.createDataFrame(pd.DataFrame(
        payments,
        columns=["payment_id","policy_id","amount","payment_date","method","runDate"]
    ))
    write_csv(df_pay, f"{landing_base}/payments/runDate={runDate}/")

print("✅ Landing exports generated under:", landing_base)


dbutils.fs.ls(f"{landing_base}/policies/")

You should see runDate=YYYY-MM-DD/ folders.

Part 3 — ADF: Build pipeline pl_landing_to_raw (parameterized)
You already created:
	•	Linked service to ADLS using Managed Identity (good) [learn.microsoft.com]
	•	Linked service to Databricks using MI (good for later notebook activities) [docs.azure.cn], [learn.microsoft.com]
Now we build the first pipeline:
3.1 Create datasets (DelimitedText) for landing + raw
In ADF Studio → Author → Datasets → New datasetChoose: Azure Data Lake Storage Gen2 → DelimitedText
Create 6 datasets total:
Landing source datasets
	•	ds_landing_policies_csv
	•	ds_landing_policyholders_csv
	•	ds_landing_payments_csv
Raw sink datasets
	•	ds_raw_policies_csv
	•	ds_raw_policyholders_csv
	•	ds_raw_payments_csv

For each dataset:
	1	Select your ADLS linked service (MI-based)
	2	Point to the right container (landing or raw) - file path -> file system 
	3	Parameterize the folder path (only for raw, leave the directory and file name empty for landing

Add dataset parameter:
	•	runDate (String)

Folder path pattern:
	•	For policies landing: policies/runDate=@{dataset().runDate}/
	•	For policies raw: policies/runDate=@{dataset().runDate}/
Same idea for policyholders and payments.
✅ This keeps everything driven by one pipeline parameter.
3.2 Create pipeline pl_landing_to_raw
ADF Studio → Author → Pipelines → New pipelineName: pl_landing_to_raw
Pipeline parameter
	•	runDate (String)
Add 3 Copy activities
Add Copy data activity x 3:
	1	CopyPolicies
	•	Source, set the location using wildcards
	•	For CopyPolicies → Source:
	•	File path type: Wildcard file path ✅
	•	File system: landing ✅
	•	Wildcard folder path:policies/runDate=@{pipeline().parameters.runDate}/ ✅
	•	Wildcard file name: *.csv (or part-*.csv) ✅
	•	
	•	Sink: ds_raw_policies_csv with same runDate param
	•	Similar for 
	2	CopyPolicyholders
	3	CopyPayments
Connect them sequentially (or run in parallel if you want; sequential is simpler for first test).
Part 4 — Run your first end-to-end test (manual)
4.1 Pick a runDate that exists
From Databricks notebook output, pick one date you generated (example: yesterday).
4.2 Debug (optional) vs Trigger (published)
	•	If you run Debug, it uses draft changes.
	•	If you run Trigger now, it uses published changes.
Since you’ve already been publishing, you can do Trigger now.
Trigger now
ADF pipeline → Add trigger → Trigger nowSet parameter:
	•	runDate = YYYY-MM-DD
4.3 Validate in ADLS
Go to Storage Account → container raw and confirm you see:
	•	raw/policies/runDate=YYYY-MM-DD/
	•	raw/policyholders/runDate=YYYY-MM-DD/
	•	raw/payments/runDate=YYYY-MM-DD/
✅ Once this works, your ingestion layer is done.

One-time SQL: Create Delta external tables (in your catalog+schema)
Bronze, rejects and logs

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

Create notebook: 01_raw_to_bronze
Databricks → Workspace → Create → NotebookName: 01_raw_to_bronzeLanguage: Python

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

1) One-time SQL: Create Silver + Silver Rejects tables
Run this once in Databricks SQL (or a notebook SQL cell):
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

2) Create notebook: 02_bronze_to_silver_upsert
Databricks → Workspace → Create → NotebookName: 02_bronze_to_silver_upsertLanguage: Python
What this notebook will do
For each entity:
	1	Read Bronze table filtered by _runDate = runDate
	2	Apply stronger DQ rules (beyond Bronze)
	3	Split valid vs invalid → write invalid to rejects_silver_* [stackoverflow.com], [oneuptime.com]
	4	MERGE valid rows into Silver table (rerun safe)
	5	Write ops log (you already fixed ordering — we’ll reuse the same technique)

import time
import pyspark.sql.functions as F
from delta.tables import DeltaTable

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

Next Phase (Phase 3): Policyholders SCD Type 2 (history tracking)
Now we’ll create a history table for policyholders so you can track changes (address/name updates) over time — a very common real-world requirement.
Why SCD2 here?
Silver is where you “merge/conform” entities into enterprise views. The Databricks medallion definition explicitly describes Silver as the layer where entities are matched/merged/cleansed to provide an enterprise view. SCD2 is a standard technique to maintain that enterprise view with change history. [learn.microsoft.com]

Step 3.1 — Create the SCD2 table (one-time SQL)
Run this in Databricks SQL:

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

effective_start_date STRING, -- keep string to match your runDate approach
effective_end_date STRING, -- '9999-12-31' for open-ended
is_current BOOLEAN,

runDate STRING,
_sourceFile STRING,
_runDate STRING,
_loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stpolicylakevmdev.dfs.core.windows.net/policyholders_scd2/';

We’re keeping effective_* as string dates to stay consistent with your current approach (you can switch to DATE later if you want).

Step 3.2 — Create notebook 03_policyholders_scd2
Create a Databricks notebook: 03_policyholders_scd2, Python.
What it does
	•	Reads silver_policyholders_current for the runDate
	•	Computes an attribute hash from relevant columns
	•	Inserts new records (new holders or changed attributes)
	•	“Expires” the previous current record by setting effective_end_date = runDate, is_current = false
This is the heart of SCD2.

Notebook code (copy/paste)
Python

import pyspark.sql.functions as F
from delta.tables import DeltaTable

# For Test Run in databricks on specific date: dbutils.widgets.text("runDate", "2026-02-08")
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

Step 3.3 — Verify SCD2 results
Run: SQL

SELECT holder_id, policy_id, effective_start_date, effective_end_date, is_current
FROM dbw_policy_lakehouse_dev_v1.default.silver_policyholders_scd2
ORDER BY holder_id, policy_id, effective_start_date;
``

Right now, because your data generator likely didn’t create true “changes” yet, you may just see initial rows with is_current=true.

Phase 4: Silver → Gold (Business aggregates + optional CSV exports)
In a medallion/lakehouse pattern, Gold is the “business-ready” layer: curated aggregates and KPI tables optimized for reporting/dashboarding. [learn.microsoft.com], [docs.databricks.com]
We’ll build a single notebook:
	•	04_silver_to_gold
It will:
	•	Read from Silver tables
	•	Produce Gold Delta tables (primary)
	•	Optionally export CSV snapshots (for easy viewing)

4.1 One-time SQL: Create Gold tables (external Delta)
Run this once in Databricks SQL:

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
LOCATION 'abfss://gold@stpolicylakevmdev.dfs.core.windows.net/active_policies_daily/';

Gold tables are Delta tables to keep them queryable and reliable for reporting use cases. [docs.databricks.com], [github.com]

4.2 Create notebook: 04_silver_to_gold
Databricks → Workspace → Create → NotebookName: 04_silver_to_goldLanguage: Python
Paste this code (ready to run)
import pyspark.sql.functions as F

# For Test Run in databricks on specific date: dbutils.widgets.text("runDate", "2026-02-08")
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

A) Create a folder under /Shared
	1	Open your Databricks workspace UI
	•	Azure Portal → Databricks workspace → Launch Workspace
	2	Go to the Workspace browser
	•	Left sidebar → Workspace
	3	Open the Shared area
	•	In the left tree, click Shared(You should see something like /Shared)
	4	Create your project folder
	•	Inside Shared, click Create (or the ⋮ / context menu)
	•	Choose Folder
	•	Name it something like:policy_lakehouse_dev
✅ Result: you now have:/Shared/policy_lakehouse_dev
Option 1 (Recommended): Move notebooks (cleanest)
Do this for each notebook:
	1	In Workspace, expand Users → your user folder (e.g., /Users/myusername/)
	2	Find the notebook (e.g., 01_raw_to_bronze)
	3	Click the notebook’s ⋮ (three dots) menu
	4	Choose Move
	5	Select destination: /Shared/policy_lakehouse_dev/
	6	Click Move
Repeat for:
	•	01_raw_to_bronze
	•	02_bronze_to_silver_upsert
	•	03_policyholders_scd2
	•	04_silver_to_gold
✅ After moving, update ADF notebook paths to:
	•	/Shared/policy_lakehouse_dev/01_raw_to_bronze…and so on.

2) Create the master ADF pipeline
2.1 Create pipeline
ADF Studio → Author → + → PipelineName: pl_daily_policy_batch
2.2 Add pipeline parameters
Select the pipeline canvas (click empty area) → Parameters tab → + New
Add:
	•	runDate (String)
	•	exportCsv (String) (optional; used only by final notebook)
Keep exportCsv as string so it maps cleanly to Databricks widgets (strings). [community....bricks.com], [docs.databricks.com]

3) Add activity #1: Execute existing ADF pipeline pl_landing_to_raw
3.1 Drag “Execute Pipeline” activity
Activities → General → Execute PipelineRename activity: ep_landing_to_raw
3.2 Settings
	•	Invoked pipeline: pl_landing_to_raw
	•	Wait on completion: ✅ checked
3.3 Parameters mapping
Map:
	•	runDate = @pipeline().parameters.runDate

4) Add Databricks notebook activities (01 → 04)
ADF Studio → Activities → Databricks → Notebook(Use the Notebook activity, since you already created the Databricks linked service with MI.) [stackoverflow.com], [stackoverflow.com]
Common config for each notebook activity
For each of the notebook activities below:
Azure Databricks tab
	•	Linked service: your MI Databricks linked service (ls_adb_pol_lake_dev_mi)
	•	Cluster: select your existing interactive cluster (single node, auto-terminate)
Settings tab
	•	Notebook path: browse and select the notebook
	•	Base parameters: add params as shown below
ADF supports passing notebook inputs through Base parameters. [stackoverflow.com], [stackoverflow.com]

4.1 Activity #2: nb_01_raw_to_bronze
	•	Notebook: 01_raw_to_bronze
	•	Base parameters:
	◦	runDate = @pipeline().parameters.runDate
4.2 Activity #3: nb_02_bronze_to_silver
	•	Notebook: 02_bronze_to_silver_upsert
	•	Base parameters:
	◦	runDate = @pipeline().parameters.runDate
4.3 Activity #4: nb_03_policyholders_scd2
	•	Notebook: 03_policyholders_scd2
	•	Base parameters:
	◦	runDate = @pipeline().parameters.runDate
4.4 Activity #5: nb_04_silver_to_gold
	•	Notebook: 04_silver_to_gold
	•	Base parameters:
	◦	runDate = @pipeline().parameters.runDate
	◦	exportCsv = @pipeline().parameters.exportCsv
This matches the fact that Databricks widgets retrieve parameter values by name using dbutils.widgets.get(). [youtube.com], [community....bricks.com]

5) Wire dependencies (Success chaining)
On the pipeline canvas, connect activities in sequence (green “On Success”):
ep_landing_to_raw → nb_01_raw_to_bronze → nb_02_bronze_to_silver → nb_03_policyholders_scd2 → nb_04_silver_to_gold
This creates a simple, readable linear workflow while you’re in manual mode.
Even though you already granted Azure RBAC “Contributor” on the Databricks workspace for ADF MI, Unity Catalog permissions are separate. Unity Catalog requires explicit privileges on securable objects (catalog/schema/table) and follows an inheritance model.

GRANT USE CATALOG ON CATALOG dbw_policy_lakehouse_dev_v1 TO <ADF_SP>;
GRANT USE SCHEMA  ON SCHEMA  dbw_policy_lakehouse_dev_v1.default TO <ADF_SP>;

-- Grants inherited to tables under the schema
GRANT SELECT, MODIFY ON SCHEMA dbw_policy_lakehouse_dev_v1.default TO <ADF_SP>;

GRANT USE CATALOG ON CATALOG dbw_policy_lakehouse_dev_v1 TO `eaae74a0-482e-414b-992b-34a3279f62d0`;
GRANT USE SCHEMA  ON SCHEMA  dbw_policy_lakehouse_dev_v1.default TO `eaae74a0-482e-414b-992b-34a3279f62d0`;

-- Grants inherited to tables under the schema
GRANT SELECT, MODIFY ON SCHEMA dbw_policy_lakehouse_dev_v1.default TO `eaae74a0-482e-414b-992b-34a3279f62d0`;

GRANT READ VOLUME ON SCHEMA dbw_policy_lakehouse_dev_v1.default TO `eaae74a0-482e-414b-992b-34a3279f62d0`;

GRANT WRITE VOLUME ON VOLUME dbw_policy_lakehouse_dev_v1.default.vol_gold_csv_exports TO `eaae74a0-482e-414b-992b-34a3279f62d0`;



6) Publish and run manually (your chosen approach)
6.1 Publish
Click Publish all (top bar).Publishing is required for “Trigger now” to use the latest pipeline state.
6.2 Trigger now (manual)
Pipeline → Add trigger → Trigger now
Provide:
	•	runDate = 2026-02-08
	•	exportCsv = true (or false)
ADF passes these into the notebook using baseParameters. [stackoverflow.com], [stackoverflow.com]
Part A — Create a single Databricks Workflow (Job) with 4 tasks (serverless)
We’ll do this in your Dev Databricks workspace first. Later you’ll replicate to -model and *-prod using IaC/Asset Bundles.

4 [lakefs.io]

Step A1) Ensure notebooks are in /Shared/...
Your tasks will point to notebook paths. Keep them in /Shared/policy_lakehouse_dev/... (or similar) so service identities can access them consistently.
Step A2) Make notebooks parameter-ready (widgets)
You already did this, but confirm:
In notebooks 01–04 (top cell)






Python


dbutils.widgets.text("runDate", "", "Run Date (YYYY-MM-DD)")
runDate = dbutils.widgets.get("runDate").strip()
if not runDate:
raise ValueError("runDate is required")

In notebook 04 only

Python

dbutils.widgets.text("exportCsv", "false", "Export CSV (true/false)")
exportCsv_str = dbutils.widgets.get("exportCsv").strip().lower()
EXPORT_CSV = exportCsv_str in ("true","1","yes","y")

Databricks notebooks commonly read task/job parameters using widgets (dbutils.widgets.get). [community....rosoft.com], [bandittracker.com]

Step A3) Create the Databricks Workflow (multi-task Job)
	1	In Databricks workspace, go to Workflows (Jobs / Workflows UI).
	2	Click Create job (or Create workflow/job).
	3	Name it something like:job_policy_lakehouse_daily_batch_dev
Add Task 1: raw_to_bronze
	•	Task type: Notebook
	•	Notebook path: /Shared/.../01_raw_to_bronze
	•	Parameters:
	◦	runDate = {{job.parameters.runDate}} (or whatever the UI expects for job parameters)
	•	Compute: Serverless (choose serverless in the task/compute section)
Add Task 2: bronze_to_silver
	•	Depends on: Task 1
	•	Notebook: /Shared/.../02_bronze_to_silver_upsert
	•	Parameters:
	◦	runDate = {{job.parameters.runDate}}
	•	Compute: Serverless
Add Task 3: policyholders_scd2
	•	Depends on: Task 2
	•	Notebook: /Shared/.../03_policyholders_scd2
	•	Parameters:
	◦	runDate = {{job.parameters.runDate}}
	•	Compute: Serverless
Add Task 4: silver_to_gold
	•	Depends on: Task 3
	•	Notebook: /Shared/.../04_silver_to_gold
	•	Parameters:
	◦	runDate = {{job.parameters.runDate}}
	◦	exportCsv = {{job.parameters.exportCsv}}
	•	Compute: Serverless
Databricks’ recommended ADF integration is to trigger Workflows (Jobs) rather than notebook activities. [docs.databricks.com], [stackoverflow.com]

Step A4) Create Job parameters (global)
In the Job’s Parameters (job-level parameters), add:
	•	runDate (default blank or a test value)
	•	exportCsv (default false)
This allows ADF to pass them later. ADF Job activity supports passing “jobParameters”. [community....bricks.com], [docs.databricks.com]

Step A5) Save and Test the job in Databricks
Run the job manually:
	•	runDate = 2026-02-08
	•	exportCsv = true
Confirm:
	•	Gold Delta tables updated
	•	CSV exports written via UC volume
	•	ops log updated

Step A6) Capture the Job ID
In Databricks Job UI:
	•	Copy the Job ID from the job details URL or “Job details” panel.
You’ll need this for ADF Job activity. ADF Databricks Job activity uses jobId to trigger the job. [community....bricks.com]

Part B — Update ADF: Replace 4 Notebook activities with ONE Databricks Job activity
Microsoft’s ADF documentation explains the Databricks Job activity and that it supports passing parameters to the job. It also notes Job activity runs on serverless clusters and you don’t need to specify a cluster the same way as notebook activities. [community....bricks.com], [docs.databricks.com]
Step B1) Add pipeline parameters (if not already)
In your ADF pipeline pl_daily_policy_batch, define parameters:
	•	runDate (String)
	•	exportCsv (String) (only needed for the last step, but harmless globally)
Step B2) Keep pl_landing_to_raw as-is
Your pipeline structure becomes:
	1	Execute pipeline: pl_landing_to_raw
	2	Databricks Job activity: triggers workflow job (01→04 inside Databricks)
Step B3) Remove the 4 Databricks Notebook activities
Delete:
	•	nb_01_raw_to_bronze
	•	nb_02_bronze_to_silver
	•	nb_03_policyholders_scd2
	•	nb_04_silver_to_gold
Step B4) Add the Databricks Job activity
	1	In ADF Activities pane → Databricks → Job activity.
	2	Name it: dbx_job_policy_lakehouse_daily
Azure Databricks tab
	•	Choose your Databricks linked service (MI): ls_adb_pol_lake_dev_mi
Settings tab
	•	Job ID: paste the Databricks Job ID from Step A6
	•	Job parameters:
	◦	runDate = @pipeline().parameters.runDate
	◦	exportCsv = @pipeline().parameters.exportCsv
Microsoft Learn states the Databricks Job activity supports specifying the job and passing “jobParameters”, and it runs Databricks jobs (including serverless jobs). Databricks also recommends using ADF Databricks Job activity for orchestrating Jobs/Workflows. [community....bricks.com], [stackoverflow.com] [docs.databricks.com], [community....bricks.com]
Step B5) Connect dependencies
Chain:
	•	ep_landing_to_raw → dbx_job_policy_lakehouse_daily (On Success)
Step B6) Publish and test (manual)
	1	Publish ADF changes
	2	Trigger now:
	◦	runDate=2026-02-08
	◦	exportCsv=true
	3	Monitor:
	◦	ADF activity should show the Databricks run link / run id
	◦	Databricks workflow should show 4 tasks executed sequentially

Part C — Notes on permissions (so this doesn’t break again)
Since ADF triggers Databricks job runs under a service principal identity, ensure that SP has:
	•	UC: USE CATALOG, USE SCHEMA
	•	UC volumes: READ VOLUME on vol_raw, READ/WRITE VOLUME on vol_gold_csv_exports (for CSV export)
	•	UC tables: SELECT/MODIFY on needed tables (bronze/silver/gold/rejects/ops log)
(You already did most of this during the notebook-activity phase; Job activity uses the same identity model, so the same UC privileges apply.)

Part D — Why this is better (what you’ll notice immediately)
✅ Only one Databricks activity in ADF (simpler pipeline maintenance)✅ Databricks handles the multi-task orchestration where it belongs✅ Serverless compute (no slow “cluster warm-up” per notebook activity)✅ Easier to move across environments
Databricks explicitly calls this pattern an “immediate best practice” and encourages migrating from notebook activities to Job activity. [docs.databricks.com], [stackoverflow.com]

Quick check questions (so I tailor the exact UI steps if needed)
	1	In your ADF activity list, do you see a Databricks “Job” activity already, or only “Notebook”?
	2	In your Databricks Workflows UI, do you have a “Serverless” compute option available for Jobs?
If you answer those two, I can give you “exact button labels” matching your UI (ADF has slight UI differences across regions/versions).
1) How to add the Databricks Job/Workflow into your repo (as code)
✅ Recommended approach: Databricks Asset Bundles (DAB)
Databricks Asset Bundles let you define Databricks resources (jobs/workflows, notebooks, etc.) as source files/YAML and deploy them to multiple targets (dev/model/prod) programmatically. [docs.databricks.com], [docs.databricks.com]
Step 1.1 — Install/verify Databricks CLI
Bundle commands require Databricks CLI versions 0.205+ (and docs call out newer versions). Check your version: [docs.databricks.com], [docs.databricks.com]

Shell

databricks -v

Step 1.2 — Create a bundle in your repo
From your repo root:

databricks bundle init

The bundle lifecycle is: create skeleton → develop → validate → deploy → run. [learn.microsoft.com], [docs.databricks.com]
Step 1.3 — Generate bundle config from your existing Job (best option)
Because your workflow job already exists and works, use the CLI to autogenerate bundle configuration from the existing job ID:

Shell

databricks bundle generate job --existing-job-id <JOB_ID>

This is exactly what Databricks recommends for migrating existing jobs into bundles (it generates resource YAML and downloads referenced artifacts). [docs.azure.cn], [docs.databricks.com]
Alternative (UI): you can also retrieve the YAML representation of a job from the Databricks UI, but the CLI generate approach is faster and more repeatable. [docs.azure.cn], [docs.databricks.com]
Step 1.4 — Bind the bundle to the existing job (so deploy updates it)
After generating and deploying, use “bind” so your bundle manages the same existing job in the workspace (instead of creating a new one). Databricks calls this “bundle deployment bind”. [docs.azure.cn], [docs.databricks.com]
(Exact command usage is in the same migration doc; the key idea is: generate → deploy → bind.) [docs.azure.cn], [docs.databricks.com]
Step 1.5 — Put notebooks under version control
Export your notebooks as .py and commit them (you already started this). Bundles typically reference source files and deploy them alongside job definitions as part of a project. [docs.databricks.com], [learn.microsoft.com]
Step 1.6 — Add job parameters in the bundle
Your job uses runDate and exportCsv. Databricks job parameters are key/value pairs that can be defined in UI/JSON/YAML and overridden per run; values are strings or dynamic references. [learn.microsoft.com]
So ensure your job definition includes job parameters like:
	•	runDate (default blank)
	•	exportCsv (default "false")
(Your ADF will override them each run). [learn.microsoft.com], [learn.microsoft.com]
Step 1.7 — Commit the bundle artifacts
You should now have files like:
	•	databricks.yml (bundle config with targets)
	•	resources/*.yml (job/workflow definitions)
	•	src/ or databricks/notebooks/*.py (your notebooks)
Commit them:

Shell

git add databricks.yml resources/ databricks/notebooks/ docs/
git commit -m "Add Databricks workflow as code (Asset Bundle)"
git push


2) CI/CD: What’s the “next step” now that the job is in the repo?
You have two CI/CD streams:
	1	Databricks CI/CD (deploy bundle to model/prod workspaces)
	2	ADF CI/CD (deploy ADF pipelines and update the Job activity’s jobId per environment)
Let’s do them in the right order.

2A) Databricks CI/CD (Deploy bundles to dev/model/prod workspaces)
Why this is the best path
Bundles are designed for CI/CD and let you deploy the same workflow to multiple targets (dev/model/prod). [docs.databricks.com], [learn.microsoft.com]
Step 2A.1 — Define targets in databricks.yml
Set up:
	•	dev
	•	model
	•	prod
Bundles are deployed with:

Shell


databricks bundle deploy -t <target>

Target selection is built into bundle commands. [docs.databricks.com], [learn.microsoft.com]
Step 2A.2 — Create a CI workflow (GitHub Actions or Azure DevOps)
At minimum, CI should:
	•	validate bundle
	•	deploy to model (manual approval)
	•	deploy to prod (manual approval)
Common commands:
	•	databricks bundle validate
	•	databricks bundle deploy -t model
	•	databricks bundle deploy -t prod
Databricks documents bundle deploy and explains how bundle identity is determined by bundle name + target, and that deploy updates existing resources managed by the bundle. [docs.databricks.com], [docs.databricks.com]
Tip: Use OAuth machine-to-machine (service principal) for fully automated CI/CD instead of user auth. Databricks explicitly calls out M2M as the recommendation for automation. [docs.databricks.com], [learn.microsoft.com]

2B) ADF CI/CD (Deploy pipeline + environment-specific jobId)
The key issue you must plan for
Databricks job IDs will be different in each workspace (dev vs model vs prod), so your ADF “Databricks Job activity” must use the correct jobId for that environment. ADF Job activity triggers jobs by ID and supports passing jobParameters. [learn.microsoft.com], [community....bricks.com]
There are two clean ways to handle jobId differences:
Option 1 (Recommended): Parameterize jobId in ADF ARM template
There is a known approach to parameterize the jobId property in the ADF pipeline ARM template so you can override it per environment during deployment. The job activity payload uses typeProperties.jobId, and you can parameterize it using ADF custom parameterization techniques. [cloudsilva.com], [learn.microsoft.com]
Option 2: Store jobId in an environment config + set ADF pipeline variable
Less common for “pure” ADF CI/CD; Option 1 is the cleaner infra-as-code method. [cloudsilva.com], [learn.microsoft.com]

2C) ADF CI/CD recommended flow (Microsoft guidance)
ADF CI/CD is commonly implemented with ARM templates representing pipelines/datasets/linked services and promoted across environments. [learn.microsoft.com], [learn.microsoft.com]
Step 2C.1 — Dev ADF connected to Git
Only DEV should be Git-connected; other environments get deployments from CI/CD. [github.com], [learn.microsoft.com]
Step 2C.2 — Generate ARM templates
You have two choices:
	•	Manual publish (ADF UI “Publish” generates templates to publish branch) – classic approach [learn.microsoft.com], [learn.microsoft.com]
	•	Automated publish using @microsoft/azure-data-factory-utilities (recommended for truer automation) [learn.microsoft.com], [github.com]
Microsoft explicitly says automated publish makes “Validate all” and “Export ARM template” available as an npm package and recommends Node 20.x compatibility. [learn.microsoft.com]
Step 2C.3 — Deploy ARM templates to model/prod RGs
In release/deploy stage, deploy:
	•	ARMTemplateForFactory.json
	•	ARMTemplateParametersForFactory.json and override environment differences (linked services, workspace URLs, jobId, etc.). [learn.microsoft.com], [learn.microsoft.com]

3) Practical order of execution (what you should do next)
Since you said “job activity tested and working”, do this next:
Step 1 — Put Databricks workflow into repo (today)
	•	Create bundle
	•	Generate job config from existing job
	•	Commit YAML + notebooks [docs.azure.cn], [docs.databricks.com]
Step 2 — Deploy the bundle to Model workspace manually once (to get Model jobId)
	•	databricks bundle deploy -t model [docs.databricks.com], [learn.microsoft.com]
	•	Note the model jobId
Step 3 — Deploy the bundle to Prod workspace manually once (to get Prod jobId)
	•	databricks bundle deploy -t prod [docs.databricks.com], [learn.microsoft.com]
	•	Note the prod jobId
Step 4 — Parameterize ADF jobId and set overrides per environment
Because ADF Job activity references jobId directly, you must override it per env. [learn.microsoft.com], [cloudsilva.com]
Step 5 — Build CI/CD pipelines
	•	Databricks: bundle validate + deploy per target [docs.databricks.com], [docs.databricks.com]
	•	ADF: ARM publish + deploy per env [learn.microsoft.com], [learn.microsoft.com]

Part 1 — Build the Logic App (HTTP trigger → write log to ADLS → send email)
1. Create the Logic App
	1	Azure Portal → Create resource → Logic App (Consumption) (simplest/fastest).
	2	Name: la-policy-lakehouse-alerts-dev (example)
	3	Create it.
2. Add trigger: “When an HTTP request is received”
	•	In Logic App designer choose Request → When an HTTP request is received.
	•	Click Use sample payload to generate schema, paste the JSON below, click Done.
Sample payload (schema generator)

JSON


{
"status": "Success",
"environment": "dev",
"pipelineName": "pl_daily_policy_batch",
"runId": "xxxxxxxx",
"triggerTimeUtc": "2026-02-18T00:00:00Z",
"runDate": "2026-02-08",
"exportCsv": "true",
"activityName": "dbx_job_policy_lakehouse_daily",
"errorCode": "",
"errorMessage": ""
}

ADF Web activity can send JSON body and headers (Content‑Type: application/json). [stackoverflow.com], [codesharepoint.com]

3. Add action: “Create blob (V2)” to write a log file to ADLS logs/
Use Azure Blob Storage connector (it can access ADLS Gen2 storage using multi‑protocol support). [learn.microsoft.com], [learn.microsoft.com]
Steps
	1	New step → search Azure Blob Storage → choose Create blob (V2) (or “Create blob”).
	2	Create a connection to your ADLS/Blob account (stpolicylakevmdev).
	3	Configure:
	◦	Container: logs
	◦	Blob name (example): adf-alerts/@{triggerBody()?['pipelineName']}/@{triggerBody()?['runDate']}/@{triggerBody()?['runId']}.json
	◦	
	◦	Blob content: use a Compose action or just put: @{string(triggerBody())}
	◦	
The Azure Blob Storage connector provides blob operations (create/update/get/delete) and is documented for Logic Apps. [learn.microsoft.com], [learn.microsoft.com]
✅ Result: every run creates a single JSON file under: logs/adf-alerts/<pipeline>/<runDate>/<runId>.json

4. Add action: Send email (Office 365 Outlook)
Use Office 365 Outlook connector in Logic Apps (work/school account). [learn.microsoft.com]
Steps
	1	New step → search Office 365 Outlook → choose Send an email (V2).
	2	Sign in to your org mailbox (or a shared service mailbox).
	3	Configure:
	◦	To: your email(s)
	◦	Subject: Policy Lakehouse @{triggerBody()?['status']} | @{triggerBody()?['environment']} | @{triggerBody()?['runDate']}
	◦	
	◦	Body (HTML recommended): <p><b>Status:</b> @{triggerBody()?['status']}</p><p><b>Environment:</b> @{triggerBody()?['environment']}</p><p><b>Pipeline:</b> @{triggerBody()?['pipelineName']}</p><p><b>RunId:</b> @{triggerBody()?['runId']}</p><p><b>runDate:</b> @{triggerBody()?['runDate']}</p><p><b>exportCsv:</b> @{triggerBody()?['exportCsv']}</p><p><b>Activity:</b> @{triggerBody()?['activityName']}</p><p><b>ErrorCode:</b> @{triggerBody()?['errorCode']}</p><p><b>ErrorMessage:</b> @{triggerBody()?['errorMessage']}</p>

Microsoft documents how to add Office 365 Outlook actions to Logic Apps workflows. [learn.microsoft.com]

5. Copy the Logic App trigger URL
In the Request trigger, copy the HTTP POST URL (this includes a SAS token by default).
https://prod-77.eastus.logic.azure.com:443/workflows/a72d70ae85c04d4abcf0a505fd18659f/triggers/When_an_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_an_HTTP_request_is_received%2Frun&sv=1.0&sig=6Mp54jxoPEeJ2VmlaqGfD-z94kf0aznYy22o2r5Z8_o
Security option (recommended later)
You can secure the Logic App trigger to be callable by ADF Managed Identity (no SAS URL) — Microsoft provides guidance specifically for invoking Logic Apps from ADF using Managed Identity authentication. For practice, SAS URL is okay; for production, MI auth is better. [techcommun...rosoft.com]

Part 2 — Update ADF pipeline (Try/Catch style + logging + email)
You already have a working pl_daily_policy_batch that runs:
	1	pl_landing_to_raw
	2	dbx_job_policy_lakehouse_daily (Databricks Job activity)
Now add “Success” and “Failure” branches on the Databricks Job activity.
Azure Data Factory doesn’t have a single “container” object that automatically catches failures of any activity the way an SSIS Sequence Container with event handlers can. Instead, ADF’s “try/catch” is built using dependency conditions (On Success / On Failure / On Completion / On Skip) and/or wrapper pipelines. [bacancytec...nology.com], [downhill-data.com]
The recommended org-standard pattern to get a true “outer box” failure handler is:
✅ Wrapper (parent) pipeline that executes a child pipeline containing all your main steps.Parent handles success/failure in ONE place based on the Execute Pipeline activity outcome.
This is the cleanest way to ensure:
	•	if any step fails inside the child pipeline → parent catches it → calls Logic App once.

Create a parent pipeline: pl_policy_lakehouse_orchestrator
This becomes your “outer box” pipeline.
Inside parent:
Pipeline variables (for failure capture)
	•	vErrorMessage (String)
	•	vErrorCode (String)
You can use Set Variable activ
Execute Pipeline activity: ep_run_child
	◦	Invoked pipeline: pl_daily_policy_batch_02
	◦	Pass params: runDate, exportCsv

2. Create the “Success notify” branch (Upon Success)
ADF supports four conditional paths (Upon Success/Failure/Completion/Skip). [learn.microsoft.com]
Add a Web activity on success
	1	Click the Databricks Job activity → + → choose On Success
	2	Add Web activity named wa_notify_success
	3	Configure:
	◦	URL: Logic App trigger URL
	◦	Method: POST
	◦	Headers:
	▪	Content-Type: application/json
	◦	Body (dynamic content):


JSON


{
"status": "Success",
"environment": "dev",
"pipelineName": "@{pipeline().Pipeline}",
"runId": "@{pipeline().RunId}",
"triggerTimeUtc": "@{pipeline().TriggerTime}",
"runDate": "@{pipeline().parameters.runDate}",
"exportCsv": "@{pipeline().parameters.exportCsv}",
"activityName": "dbx_job_policy_lakehouse_daily",
"errorCode": "",
"errorMessage": ""
}

pipeline().Pipeline, pipeline().RunId, pipeline().TriggerTime are standard ADF system variables. [learn.microsoft.com]

3. Create the “Failure notify” branch (Upon Failure)
Step 3.1 Capture error details (Set Variable)
	1	From the Databricks Job activity → + → choose On Failure
	2	Add a Set Variable activity named sv_error_message:
	◦	variable: vErrorMessage
	◦	value: Plain Text@activity('dbx_job_policy_lakehouse_daily').error.message
	◦	
	3	Accessing .error.message is the standard way to capture failure detail from an activity. [learn.microsoft.com], [stackoverflow.com]
	4	(Optional) Add another Set Variable for error code if you want:
	◦	variable: vErrorCode
	◦	value: Plain Text@string(activity('dbx_job_policy_lakehouse_daily').error.code)
	◦	
Step 3.2 Call Logic App (email + ADLS log)
Add a Web activity after the Set Variable(s) called wa_notify_failure with:
Body
JSON


{
"status": "Failure",
"environment": "dev",
"pipelineName": "@{pipeline().Pipeline}",
"runId": "@{pipeline().RunId}",
"triggerTimeUtc": "@{pipeline().TriggerTime}",
"runDate": "@{pipeline().parameters.runDate}",
"exportCsv": "@{pipeline().parameters.exportCsv}",
"activityName": "dbx_job_policy_lakehouse_daily",
"errorCode": "@{variables('vErrorCode')}",
"errorMessage": "@{variables('vErrorMessage')}"
}

The “Try/Catch” patterns and how pipeline status behaves are documented by Microsoft (Try‑Catch vs Do‑If‑Else). [learn.microsoft.com]

4. Decide pipeline “final status” behavior (important)
You said: “email for success as well for failure.”Do you want the pipeline run to FAIL when the job fails (even after sending email), or do you want the pipeline to show SUCCESS because the failure branch handled it?
Microsoft’s error handling doc describes how these patterns affect overall pipeline status. [learn.microsoft.com]
Option A (recommended for production): pipeline should FAIL
Use Do‑If‑Else style: define both success + failure branches. In this mode, when the main activity fails, the overall pipeline run is considered failed even if the failure branch succeeds. [learn.microsoft.com]
Option B (sometimes used): pipeline always succeeds if failure branch succeeded
That is Try‑Catch behavior (only “On Failure” path), which makes pipelines appear green even if the main activity failed. [learn.microsoft.com]
✅ For your practice + real ops: I strongly suggest Option A.
Optional: Add explicit “Fail” activity to control error text
If you want a custom final error message (e.g., include runDate), add a Fail activity at the end of the failure branch. The Fail activity lets you set error message and code. [sqlservercentral.com]

Part 3 — Test plan (quick)
Test Success
	•	Run with a known good runDate
	•	Confirm:
	◦	email arrives (“Success”)
	◦	JSON log file appears under logs/adf-alerts/... [learn.microsoft.com], [learn.microsoft.com]
Test Failure
Easiest ways:
	•	intentionally pass a runDate that has no data (and make notebook raise), or
	•	temporarily force the job to fail (e.g., raise Exception in notebook 01)
	•	Confirm:
	◦	pipeline shows failed (if you chose Option A) [learn.microsoft.com]
	◦	email arrives with error details (vErrorMessage)
	◦	JSON log file created in ADLS logs container

Part 4 — “Nice to have” improvements (still minimal)
	1	Secure Logic App endpoint using ADF Managed Identity
	◦	Microsoft provides a pattern to invoke Logic App from ADF Web Activity using Managed Identity (no SAS URL). [techcommun...rosoft.com]
	2	Add Teams alert later (same Logic App).
	3	Add a correlation link in email (ADF Monitor URL + Databricks run URL if you include it).

Tenant - e825103c-93ab-4e80-96f6-8faabb0758da

For - 	1	Secure Logic App endpoint using ADF Managed Identity

Step-by-step (Logic App side)
Step 1.1 — Use an HTTP Request trigger (existing)
In your Logic App workflow, your trigger should be When an HTTP request is received. [techcommun...rosoft.com], [learn.microsoft.com]
Step 1.2 — Add an Authorization Policy (OAuth / Entra ID)
	1	Open your Logic App (Consumption) in Azure Portal.
	2	Go to Authorization (or under security settings depending on portal UX).
	3	Add Policy with Policy Type: AAD (OAuth). [techcommun...rosoft.com], [learn.microsoft.com]
	4	Configure claims (typical values when calling from ADF):
	◦	Issuer: https://sts.windows.net/<TENANT_ID>/ [techcommun...rosoft.com], [sqlservercentral.com]
	◦	Audience: https://management.azure.com/ [techcommun...rosoft.com], [sqlservercentral.com]
Note: Audience needs to match what you’ll put in ADF Web activity Authentication “Resource”. [techcommun...rosoft.com], [sqlservercentral.com]
Step 1.3 — Ensure trigger only allows Bearer calls
Microsoft’s reference pattern adds a trigger condition like:

Plain Text

@startsWith(triggerOutputs()?['headers']?['Authorization'], 'Bearer')

This ensures the call is using a Bearer token. [techcommun...rosoft.com], [learn.microsoft.com]
Step 1.4 — (Optional but helpful) Include Authorization headers in trigger outputs
If you want to inspect/debug the Authorization header, set operationOptions to include it:
	•	Open Code view for the trigger and add:
	◦	operationOptions: "IncludeAuthorizationHeadersInOutputs" [techcommun...rosoft.com], [learn.microsoft.com]

Step-by-step (ADF side)
Step 1.5 — Use Web activity with Managed Identity auth
In your ADF pipeline failure/success handler (Web activity):
	1	URL: paste the Logic App trigger URL but remove SAS parameters.The pattern is: keep the base URL and api-version, but remove sig, sp, sv parameters (or as instructed by the pattern you follow). The TechCommunity guide explicitly says remove the SAS token when entering the Logic App URL so the call won’t use SAS. [techcommun...rosoft.com], [sqlservercentral.com]
	2	Method: POST
	3	Body
	◦	{
	◦	  "status": "Failure",
	◦	  "errorCode": "@{string(activity('ep_run_policy_batch_02').error.errorCode)}",
	◦	  "errorMessage": "@{activity('ep_run_policy_batch_02').error.message}"
	◦	}
	◦	
	4	Headers: Content-Type: application/json (as you already do) [linkedin.com], [stackoverflow.com]
	5	Authentication: select Managed Identity in Web activity.
	6	Resource: set to the same Audience you configured in the Logic App policy:
	◦	https://management.azure.com/ [techcommun...rosoft.com], [sqlservercentral.com]
With this configuration, ADF will obtain a token using its managed identity and send it as a bearer token automatically. [techcommun...rosoft.com], [learn.microsoft.com]
✅ Result: Logic App is no longer triggered by “secret URL”; it’s triggered only by valid Entra tokens (Managed Identity).

The expression 'string(activity('ep_run_policy_batch_02').error.code)' cannot be evaluated because property 'code' doesn't exist, available properties are 'errorCode, message, failureType, target, details'.
