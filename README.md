# Policy Lakehouse

This repository contains the Azure Data Factory + Azure Databricks (Unity Catalog) lakehouse implementation.

## Environments
- Dev: `rg-policy-lakehouse-dev` / `adf-policy-lakehouse-dev` / `dbw-policy-lakehouse-dev-v1`
- Model: suffix `*-model`
- Prod: suffix `*-prod`

## Quick start
1. Run ADF pipeline `pl_daily_policy_batch` with parameter `runDate` (YYYY-MM-DD).
2. Optional: set `exportCsv=true` to export Gold CSV snapshots (written via UC volume).

See [docs/solution-overview.md](docs/solution-overview.md) for full inventory.
