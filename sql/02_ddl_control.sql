-- Control schema. Run once, before the first Bronze run.
--
-- Why these live in a table rather than in the YAML file: in production there
-- is no repo checked out next to the job. control.tables becomes the source of
-- truth, and being a real table it is queryable in SQL.

CREATE SCHEMA IF NOT EXISTS workspace.control;


-- WHAT to ingest. Written by sync_config_to_control(), read by the pipeline.
CREATE TABLE IF NOT EXISTS workspace.control.tables (
    source_system    STRING,     -- 'azuresql'
    source_schema    STRING,     -- 'dbo'
    source_table     STRING,     -- 'dbo.Invoice'  (matches watermarks.table_name)
    target_catalog   STRING,     -- 'workspace'
    target_schema    STRING,     -- 'nb_bronze'
    target_table     STRING,     -- 'workspace.nb_bronze.invoice'
    load_strategy    STRING,     -- 'incremental' | 'full_reload'
    primary_key      STRING,     -- 'Id'  or composite: 'InvoiceLineId, CodingSeq'
    watermark_col    STRING,     -- NULL for full_reload
    delay_minutes    INT,        -- overlap window, absorbs clock skew
    enabled          BOOLEAN,
    updated_at       TIMESTAMP
);


-- HOW FAR each table has been ingested.
-- Advanced only after a successful write, and only to the ceiling captured
-- before the read - see the BUGFIX 1 notes in src/bronze.py.
CREATE TABLE IF NOT EXISTS workspace.control.watermarks (
    source_system           STRING,
    table_name              STRING,      -- 'dbo.Invoice'
    watermark_column        STRING,
    last_watermark          TIMESTAMP,
    last_successful_run_id  STRING,
    updated_at              TIMESTAMP
);


-- WHAT HAPPENED. One row per table per run, success and failure alike.
-- The printed summary scrolls away; this table does not.
CREATE TABLE IF NOT EXISTS workspace.control.pipeline_runs (
    run_id           STRING,
    pipeline_name    STRING,      -- 'bronze_ingest'
    layer            STRING,      -- 'bronze' | 'silver' | 'gold'
    table_name       STRING,
    status           STRING,      -- 'success' | 'failed'
    started_at       TIMESTAMP,
    finished_at      TIMESTAMP,
    error_message    STRING
);


-- NOTE: workspace.control.data_quality_results is deliberately not created
-- here. Data quality comes after Silver; the table will be added back with the
-- checks that write to it, rather than sitting empty in the meantime.
--
-- It already exists in the workspace from an earlier version. Drop it when you
-- want to, with:  DROP TABLE IF EXISTS workspace.control.data_quality_results;
