# Analytics Platform

Medallion data platform: **Azure SQL → Bronze → Silver → Gold → Power BI**, on Databricks.

Target architecture: `docs/10_roadmap.md` (staged plan) and the reference
`production_databricks_etl_architecture.docx`.

## Layout

```
config/           ingestion_config.yaml  - WHAT to process (source of truth, synced to control.tables)
src/
  common/         config, connections, logging, exceptions  - shared utilities
  bronze/         pipeline, watermark, writer               - raw ingestion
  silver/         (next)                                    - cleaned + conformed
  gold/           (later)                                   - star schema
  quality/        (later)                                   - data-quality checks
notebooks/        thin drivers that import from src/
sql/              control-schema DDL
tests/            unit + integration
resources/        Databricks Job / DAB definitions
docs/             architecture, decisions, cheatsheet, roadmap
```

This project contains **only the pipeline**. The fake source database is built and
maintained by a separate project, `../source-simulator` — a test tool that is never
imported or deployed here.

## Separation of concerns

| Concern | Owner |
| --- | --- |
| WHAT to process | `config/ingestion_config.yaml` → `control.tables` |
| HOW to process | Python modules in `src/` |
| WHEN / in what order | Databricks Jobs |
| WHAT resources exist | DAB (`databricks.yml`, `resources/`) |
| HOW code is promoted | Git + CI/CD |
| Operational state | `control.*` tables |

## Schemas

```
workspace.control      tables, watermarks, pipeline_runs, data_quality_results
workspace.nb_bronze    Bronze (Round 1: notebooks + Jobs)
workspace.nb_silver    Silver
workspace.nb_gold      Gold
```

Rounds 2 and 3 rebuild the same medallion as Lakeflow Declarative Pipelines (`ldp_*`)
and dbt (`dbt_*`). Source stays `dbo.*` in Azure SQL.

## Running

**Setup (once)**
```
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

**Databricks** — clone this repo as a Git folder, then run `notebooks/1_Bronze.ipynb`.
Notebooks add `src/` to `sys.path` and import from it; they contain no ETL logic.

Run `sql/02_ddl_control.sql` once first to create the `control` schema.

**Tests**
```
.venv\Scripts\python.exe -m pytest tests/unit
.venv\Scripts\python.exe -m ruff check src tests
```

**Test data** — see `../source-simulator`. Use its `simulate` mode to produce source
deltas between pipeline runs when verifying incremental loading.

## Secrets

The Azure SQL password lives in a Databricks secret scope, never in code:

```python
dbutils.secrets.get(scope="ap_source", key="azsql_password")
```

Local runs of the simulator read `.env` (gitignored). See `.env.example`.
