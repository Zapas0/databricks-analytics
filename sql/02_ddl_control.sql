
CREATE SCHEMA IF NOT EXISTS workspace.control;


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

CREATE TABLE IF NOT EXISTS workspace.control.watermarks (
    source_system           STRING,
    table_name              STRING,      -- 'dbo.Invoice'
    watermark_column        STRING,
    last_watermark          TIMESTAMP,
    last_successful_run_id  STRING,
    updated_at              TIMESTAMP
);


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

