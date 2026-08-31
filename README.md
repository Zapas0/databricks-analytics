# Analytics Platform

Medallion data platform: **Azure SQL → Bronze → Silver → Gold → Power BI**, on Databricks.

Target architecture: `docs/10_roadmap.md` (staged plan) and the reference
`production_databricks_etl_architecture.docx`.

## Layout

```
config/           ingestion_config.yaml  - WHAT to process (synced to control.tables)
src/
  common.py       connection + config              - shared by every layer
  bronze.py       the Bronze pipeline              - raw ingestion
  silver.py       (next)                           - cleaned + conformed
notebooks/        thin drivers that import from src/
sql/              control-schema DDL
tests/            unit tests, no cluster required
resources/        Databricks Job / DAB definitions
docs/             architecture, decisions, cheatsheet, roadmap
```

One file per layer, plus `common.py` for what every layer needs. The functions
in `bronze.py` are the notebook's functions, with the same names and the same
order — `find_watermark → get_latest_wm → pull_delta → push_delta → update_wm`.

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
workspace.control      tables, watermarks, pipeline_runs
workspace.nb_bronze    Bronze (Round 1: notebooks + Jobs)
workspace.nb_silver    Silver
workspace.nb_gold      Gold
```

`data_quality_results` comes back when the checks that write to it are built,
after Silver.

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
