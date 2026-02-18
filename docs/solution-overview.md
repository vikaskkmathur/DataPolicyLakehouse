# Policy Lakehouse (Dev) – Build Inventory & Purpose

**Generated:** 2026-02-18

## 1. Environment naming
- **Dev resource group:** `rg-policy-lakehouse-dev`
- **Dev ADF:** `adf-policy-lakehouse-dev`
- **Dev Databricks workspace:** `dbw-policy-lakehouse-dev-v1`
- **Unity Catalog:** `dbw_policy_lakehouse_dev_v1.default`
- **Storage account:** `stpolicylakevmdev`
- **Environment naming:**
  - Model/UAT: `*-model`
  - Prod: `*-prod`

## 2. Azure resources created (Dev)
### 2.1 Resource Group
- **rg-policy-lakehouse-dev** – logical container for all Dev resources (RBAC, cost, lifecycle).

### 2.2 Storage (ADLS Gen2)
- **Storage account:** `stpolicylakevmdev`
- **Containers:**
  - `landing` – inbound CSV drops
  - `raw` – copied “as-received” files
  - `bronze` – Delta table storage (external tables)
  - `silver` – Delta table storage (external tables)
  - `gold` – Delta table storage (external tables)
  - `rejects` – quarantine tables (Delta)
  - `logs` – pipeline run log table (Delta)

**Purpose:** separate raw file ingestion from curated Delta tables (Bronze/Silver/Gold) and operational tables (logs/rejects).

### 2.3 Azure Databricks (Unity Catalog enabled)
- **Workspace:** `dbw-policy-lakehouse-dev-v1`
- **Catalog:** `dbw_policy_lakehouse_dev_v1`
- **Schema:** `default`

#### 2.3.1 Unity Catalog volumes
- `vol_raw` – governed access to raw files (`/Volumes/dbw_policy_lakehouse_dev_v1/default/vol_raw/...`)
- `vol_gold_csv_exports` – governed location for Gold CSV exports (`/Volumes/dbw_policy_lakehouse_dev_v1/default/vol_gold_csv_exports/...`)

**Purpose:** volumes govern non-tabular files with UC privileges (READ/WRITE VOLUME).

#### 2.3.2 Unity Catalog tables
**Bronze (typed Delta):**
- `bronze_policies`, `bronze_payments`, `bronze_policyholders`

**Silver (typed Delta, conformed):**
- `silver_policies`, `silver_payments`, `silver_policyholders_current`
- `silver_policyholders_scd2` (history)

**Gold (Delta aggregates):**
- `gold_written_premium_daily`, `gold_payments_daily`, `gold_active_policies_daily`

**Rejects / quarantine:**
- Bronze rejects: `rejects_policies`, `rejects_payments`, `rejects_policyholders`
- Silver rejects: `rejects_silver_policies`, `rejects_silver_payments`, `rejects_silver_policyholders`

**Ops log:**
- `ops_pipeline_run_log`

**Purpose:** Delta tables support reliable batch writes, MERGE upserts, and repeatable runs.

#### 2.3.3 Databricks notebooks (in /Shared)
- `01_raw_to_bronze` – reads raw CSV via volume, writes typed Bronze Delta + rejects + ops log
- `02_bronze_to_silver_upsert` – rules, dedupe, MERGE into Silver + rejects + ops log
- `03_policyholders_scd2` – SCD2 history maintenance
- `04_silver_to_gold` – aggregates to Gold tables; optional CSV exports via `vol_gold_csv_exports`

**Parameters:**
- `runDate` required for all notebooks (passed from ADF)
- `exportCsv` only used by `04_silver_to_gold`

### 2.4 Azure Data Factory
- **Factory:** `adf-policy-lakehouse-dev`

#### 2.4.1 Linked services
- **ADLS Gen2:** `ls_adls2_pol_lake_dev_mi` (Managed Identity)
- **Databricks:** `ls_adb_pol_lake_dev_mi` (Managed Identity)

#### 2.4.2 Datasets
- Landing + raw datasets per entity (policies, policyholders, payments)
- Uses folder paths and wildcard file name patterns for Spark-generated `part-*.csv`

#### 2.4.3 Pipelines
- `pl_landing_to_raw` – copies landing → raw (wildcards)
- `pl_daily_policy_batch` – orchestrates:
  1) `pl_landing_to_raw`
  2) Databricks notebooks 01 → 04 sequentially (manual trigger until stable)

## 3. Permissions (summary)
### 3.1 Unity Catalog permissions for ADF service principal
The ADF MI runs notebooks as a service principal and requires:
- `USE CATALOG` on `dbw_policy_lakehouse_dev_v1`
- `USE SCHEMA` on `dbw_policy_lakehouse_dev_v1.default`
- Table privileges (`SELECT`, `MODIFY`) on required tables
- Volume privileges (`READ VOLUME`, `WRITE VOLUME`) for `vol_raw` and CSV export volume

## 4. How to run (Dev)
1. Trigger ADF pipeline `pl_daily_policy_batch`
2. Provide `runDate` (YYYY-MM-DD)
3. Optionally set `exportCsv=true` for the Gold notebook

## 5. Repo contents
- `docs/solution-overview.md` – this document
- `docs/architecture-diagram.png` – high-level component diagram
- `databricks/notebooks/` – export notebooks as source `.py`
- `databricks/sql/` – store DDL + grants scripts
- `adf/` – store ADF collaboration branch JSON artifacts
- `iac/` – infrastructure-as-code (Bicep/Terraform) placeholders
- `cicd/` – CI/CD pipeline placeholders
