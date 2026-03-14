# ecommerce-analytics-pipeline

Cloud-native analytics pipeline built on the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Ingests raw CSVs into Databricks Delta tables, transforms with dbt, and serves KPI marts for reporting.

**Stack**: AWS S3 → Databricks (Bronze) → dbt (Silver/Gold) → Databricks SQL Warehouse  
**Governance**: Unity Catalog | **CI/CD**: GitHub Actions | **IaC**: Terraform

---

## Start Here

Use [docs/runbook.md](docs/runbook.md) as the single source of truth for setup and execution.

---

## Documentation

| Document | Purpose |
|---|---|
| [docs/runbook.md](docs/runbook.md) | Full step-by-step operational guide |
| [docs/architecture.md](docs/architecture.md) | Stack, data flow, layer model definitions |
| [docs/deployment-strategy.md](docs/deployment-strategy.md) | Branch strategy, CI gates, prod promotion |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Common failures and fixes |
| [docs/adr/](docs/adr/) | Architecture decision records |
| [docs/development-history.md](docs/development-history.md) | Phase-by-phase development archive |
| [docs/archive/](docs/archive/) | Legacy per-phase runbooks |

---

## Repository Structure

```text
ecommerce-analytics-pipeline/
├── databricks/
│   ├── notebooks/          ← ingestion + quality checks + publish gold
│   ├── sql/                ← Unity Catalog setup, external locations, reset
│   └── workflows/          ← Databricks Workflow JSON definitions
├── dbt/
│   ├── models/
│   │   ├── staging/        ← stg_* source models
│   │   ├── silver/         ← dim_* and fct_* conformed models
│   │   └── gold/           ← mart_* analytics models
│   ├── snapshots/          ← SCD2 snapshot definitions
│   └── macros/             ← SCD2 MERGE hooks
├── docs/
│   ├── architecture.md
│   ├── runbook.md
│   ├── deployment-strategy.md
│   ├── troubleshooting.md
│   ├── adr/                ← Architecture Decision Records
│   ├── archive/            ← Legacy phase runbooks
│   └── development-history.md
├── scripts/
│   ├── upload_to_s3.py
│   └── reset_s3_data.py
└── terraform/              ← S3 buckets + IAM role provisioning
```

