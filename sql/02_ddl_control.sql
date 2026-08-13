/*==============================================================================
  Control schema — operational state for the platform
  File: 02_ddl_control.sql            Run in Databricks (SQL Editor or %sql cell)
------------------------------------------------------------------------------
  Per the target architecture, operational state lives in its own schema, not
  scattered across the data layers:

    control.tables                 WHAT to process (synced from config yaml)
    control.watermarks             how far each table has been ingested
    control.pipeline_runs          one row per table per run (success + failure)
    control.data_quality_results   one row per check per run

  Idempotent: safe to re-run. Never drops data.
==============================================================================*/

CREATE SCHEMA IF NOT EXISTS workspace.control;

/*------------------------------------------------------------------
  control.tables — the metadata that drives ingestion.
  config/ingestion_config.yaml (in Git) is the source of truth;
  sync_config_to_control() loads it here. Production code reads THIS.
------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS workspace.control.tables (
    source_system    STRING,     -- 'azuresql'
    source_schema    STRING,     -- 'dbo'
    source_table     STRING,     -- 'dbo.Invoice'  (fully qualified, matches watermarks)
    target_catalog   STRING,     -- 'workspace'
    target_schema    STRING,     -- 'nb_bronze'
    target_table     STRING,     -- 'workspace.nb_bronze.invoice'
    load_strategy    STRING,     -- 'incremental' | 'full_reload'
    primary_key      STRING,     -- 'Id'  or composite: 'InvoiceLineId,CodingSeq'
    watermark_col    STRING,     -- NULL for full_reload
    delay_minutes    INT,        -- overlap window, guards clock skew
    enabled          BOOLEAN,
    updated_at       TIMESTAMP
);

/*------------------------------------------------------------------
  control.watermarks — how far we have successfully ingested.
  last_successful_run_id ties the mark back to the run that set it,
  so you can trace which run produced the current state.
------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS workspace.control.watermarks (
    source_system           STRING,
    table_name              STRING,      -- 'dbo.Invoice'
    watermark_column        STRING,
    last_watermark          TIMESTAMP,
    last_successful_run_id  STRING,
    updated_at              TIMESTAMP
);

/*------------------------------------------------------------------
  control.pipeline_runs — the durable run log.
  Written on success AND failure. Print output disappears; this does not.
------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS workspace.control.pipeline_runs (
    run_id           STRING,
    pipeline_name    STRING,      -- 'bronze_ingest'
    layer            STRING,      -- 'bronze' | 'silver' | 'quality' | 'gold'
    table_name       STRING,
    status           STRING,      -- 'success' | 'failed'
    started_at       TIMESTAMP,
    finished_at      TIMESTAMP,
    rows_read        BIGINT,
    rows_written     BIGINT,
    watermark_start  TIMESTAMP,   -- where this run began
    watermark_end    TIMESTAMP,   -- where it advanced to
    error_message    STRING
);

/*------------------------------------------------------------------
  control.data_quality_results — one row per check per run.
------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS workspace.control.data_quality_results (
    run_id          STRING,
    table_name      STRING,
    check_name      STRING,
    status          STRING,       -- 'pass' | 'warn' | 'fail'
    expected_value  STRING,
    actual_value    STRING,
    failed_rows     BIGINT,
    executed_at     TIMESTAMP
);

/*==============================================================================
  MIGRATION — move existing watermarks out of nb_bronze into control.
  Only inserts marks that are not already present, so it is safe to re-run.
  Run once, verify, then drop the old table (statement at the bottom).
==============================================================================*/
INSERT INTO workspace.control.watermarks
      (source_system, table_name, watermark_column,
       last_watermark, last_successful_run_id, updated_at)
SELECT 'azuresql'          AS source_system,
       old.table_name,
       CAST(NULL AS STRING) AS watermark_column,   -- backfilled by the config sync
       old.watermark_value  AS last_watermark,
       CAST(NULL AS STRING) AS last_successful_run_id,
       old.updated_at
FROM   workspace.nb_bronze.ingestion_watermark AS old
WHERE  NOT EXISTS (
           SELECT 1
           FROM   workspace.control.watermarks AS new
           WHERE  new.table_name = old.table_name
       );

-- Verify before dropping:
--   SELECT * FROM workspace.control.watermarks;
-- Then retire the old table:
--   DROP TABLE IF EXISTS workspace.nb_bronze.ingestion_watermark;
