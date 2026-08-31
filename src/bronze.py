"""Bronze ingestion.

This is the Bronze notebook, moved into a module. Same functions, same names,
same order, one table at a time:

    find_watermark -> get_latest_wm (ceiling) -> pull_delta -> push_delta -> update_wm

The loop at the bottom marks failures and carries on, exactly as the notebook
did. One bad table does not stop the other sixteen, and the job does not break.
"""
import datetime
import uuid

from common import read_jdbc

# pyspark and delta are imported inside the functions that use them, not here.
# They only exist in a Databricks runtime, and keeping them out of the top of
# the file means pytest can import this module on your laptop and test the pure
# functions (merge_condition, and pull_delta's predicate) with no cluster.

WATERMARKS_TABLE = "workspace.control.watermarks"
RUNS_TABLE = "workspace.control.pipeline_runs"

# Far enough back that a table that has never run pulls everything. This is why
# there is no "if first run" branch anywhere - bootstrap and steady state use
# the same code path.
EPOCH = datetime.datetime(1900, 1, 1)


# --------------------------------------------------------------------------
# find_watermark           <- notebook cell 6
# --------------------------------------------------------------------------

def find_watermark(spark, config):
    """Where did this table get to last time? EPOCH if it has never run.

    Changed from the notebook: reads workspace.control.watermarks instead of
    workspace.nb_bronze.ingestion_watermark, because that table was migrated
    into the control schema and dropped. Column name follows:
    watermark_value -> last_watermark.
    """
    from pyspark.sql import functions as F

    if config.get("strategy") == "full_reload":
        return EPOCH

    row = (
        spark.table(WATERMARKS_TABLE)
        .where(F.col("table_name") == config["source_table"])
        .select("last_watermark")
        .first()
    )

    # A row can exist with a NULL mark. Treat that as never-run rather than
    # returning None, which would blow up the subtraction in pull_delta.
    if row is None or row[0] is None:
        return EPOCH

    return row[0]


# --------------------------------------------------------------------------
# target_table_exists      <- notebook cell 7
# --------------------------------------------------------------------------

def target_table_exists(spark, config):
    """Unchanged from the notebook."""
    return spark.catalog.tableExists(config["target_table"])


# --------------------------------------------------------------------------
# get_latest_wm            <- notebook cell 10
# --------------------------------------------------------------------------
# Moved UP in the order. In the notebook this ran last, inside update_wm.
# See BUGFIX 1 on pull_delta below for why it now runs first.

def get_latest_wm(spark, conn, config):
    """The highest watermark value currently in the source table.

    Cheap: an indexed MAX() that returns one row. Returns None for a table with
    no watermark column, and for an empty source table.
    """
    if config.get("watermark_col") is None:
        return None

    max_query = (
        f"(SELECT MAX({config['watermark_col']}) AS watermark_value "
        f"FROM {config['source_table']}) AS q"
    )

    return read_jdbc(spark, conn, max_query).first()["watermark_value"]


# --------------------------------------------------------------------------
# pull_delta               <- notebook cell 8
# --------------------------------------------------------------------------

def pull_delta(spark, conn, config, wm, ceiling):
    """Read this batch from the source, filtered inside the database.

    BUGFIX 1 - the batch now has a ceiling.

    The notebook read  WHERE col > watermark  with no upper bound, and then
    update_wm called MAX() on the source *after* the write had finished. Any
    row that arrived while the write was running got counted by that MAX() but
    was never in the batch. The watermark jumped past it, and it was never
    ingested - a silent, permanent loss of exactly the rows that arrive when
    the pipeline is busiest.

    The fix is to capture the ceiling BEFORE reading and bound the batch:
    [watermark - delay, ceiling]. Anything arriving after the ceiling is simply
    the next run's work.
    """
    if config["watermark_col"] is None:
        return read_jdbc(spark, conn, config["source_table"])

    # The lower bound reaches back by `delay` minutes. Re-reading a few rows is
    # free because the write is a MERGE; missing a row is not.
    low = wm - datetime.timedelta(minutes=int(config["delay"]))

    where = f"{config['watermark_col']} > '{low}'"
    if ceiling is not None:
        where += f" AND {config['watermark_col']} <= '{ceiling}'"

    query = f"(SELECT * FROM {config['source_table']} WHERE {where}) AS q"

    return read_jdbc(spark, conn, query)


# --------------------------------------------------------------------------
# push_delta               <- notebook cell 9
# --------------------------------------------------------------------------

def merge_condition(config):
    """Build the MERGE join predicate from the configured primary key.

    BUGFIX 2 - the notebook hardcoded "t.Id = s.Id".

    That is correct for the fifteen tables keyed on Id, and silently wrong for
    the two that are not:
        InvoiceLineCoding -> InvoiceLineId + CodingSeq
        InvoiceLabel      -> InvoiceId + LabelId
    On those, every source row matches many target rows and the MERGE corrupts
    the table. Building the predicate from primary_key handles both cases with
    the same line of code. Backticks guard column names that clash with SQL
    keywords.
    """
    keys = [key.strip() for key in str(config["primary_key"]).split(",") if key.strip()]
    return " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)


def push_delta(spark, delta, config, run_id):
    """Write the batch. MERGE on the key if the table exists, else create it.

    MERGE-on-key rather than append is what makes a re-run safe: the same batch
    written twice updates rows in place instead of duplicating them.

    Changed from the notebook: run_id is passed in rather than generated here,
    so every table in one run shares a single run id. Previously each table got
    its own, which made a run impossible to reconstruct afterwards.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    delta = delta.withColumns(
        {
            "_ingest_run_id": F.lit(run_id).cast("string"),
            "_source": F.lit(config["source_table"]).cast("string"),
            "_ingested_at": F.current_timestamp(),
        }
    )

    if target_table_exists(spark, config):
        (
            DeltaTable.forName(spark, config["target_table"])
            .alias("t")
            .merge(delta.alias("s"), merge_condition(config))
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        delta.write.format("delta").mode("overwrite").saveAsTable(config["target_table"])


# --------------------------------------------------------------------------
# update_wm                <- notebook cell 11
# --------------------------------------------------------------------------

def update_wm(spark, config, ceiling, run_id):
    """Move the watermark to the ceiling this run actually used.

    BUGFIX 1, second half. The notebook called get_latest_wm() here, re-reading
    MAX() from the source after the write. Now the ceiling captured before the
    read is passed in, so the watermark can only ever advance to a value we
    genuinely ingested up to.

    Called only after push_delta succeeds. If the write fails the mark does not
    move, so the next run redoes the batch - harmlessly, because it is a MERGE.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if ceiling is None:
        return None

    row = spark.createDataFrame(
        [
            (
                config.get("source_system", "azuresql"),
                config["source_table"],
                config.get("watermark_col"),
                ceiling,
                run_id,
            )
        ],
        "source_system string, table_name string, watermark_column string, "
        "last_watermark timestamp, last_successful_run_id string",
    ).withColumn("updated_at", F.current_timestamp())

    (
        DeltaTable.forName(spark, WATERMARKS_TABLE)
        .alias("t")
        .merge(row.alias("s"), "t.table_name = s.table_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    return ceiling


# --------------------------------------------------------------------------
# the driver loop          <- notebook cell 12
# --------------------------------------------------------------------------

def ingest_table(spark, conn, config, run_id):
    """One table, start to finish. Returns the watermark it advanced to.

    The ORDER here is the whole design:
      1. where did we get to        find_watermark
      2. ceiling, BEFORE reading    get_latest_wm     <- BUGFIX 1
      3. read the bounded batch     pull_delta
      4. write it                   push_delta
      5. only now, move the mark    update_wm
    """
    wm = find_watermark(spark, config)
    ceiling = get_latest_wm(spark, conn, config)

    delta = pull_delta(spark, conn, config, wm, ceiling)
    push_delta(spark, delta, config, run_id)

    return update_wm(spark, config, ceiling, run_id)


def run_bronze(spark, conn, configs, run_id=None):
    """Ingest every table, marking failures and carrying on.

    This is the notebook's loop. A table that fails is recorded and skipped;
    the rest still run; the function returns a summary instead of raising, so
    the Databricks job does not go red because one source table misbehaved.
    """
    run_id = run_id or str(uuid.uuid4())
    successful, failed = [], []
    records = []

    print(f"bronze run {run_id} - {len(configs)} tables")

    for config in configs:
        table_name = config.get("source_table", "Unknown")
        started = datetime.datetime.now()

        try:
            print("\n" + "=" * 60)
            print(f"Processing table: {table_name}")
            print(f"Strategy: {config.get('strategy', 'Unknown')}")
            print("=" * 60)

            new_wm = ingest_table(spark, conn, config, run_id)

            print(f"OK  {table_name}  watermark -> {new_wm}")
            successful.append(table_name)
            records.append((run_id, table_name, "success", started, None))

        except Exception as error:
            # Mark it and keep going. Whatever went wrong with this table, the
            # other tables have nothing to do with it.
            print(f"FAILED  {table_name}")
            print(f"Error: {error}")
            failed.append({"table": table_name, "error": str(error)})
            records.append((run_id, table_name, "failed", started, str(error)[:2000]))

    _write_run_log(spark, records)

    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Total tables processed: {len(configs)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed tables:")
        for failure in failed:
            print(f"  - {failure['table']}")
            print(f"    Error: {failure['error']}")

    return {"run_id": run_id, "successful": successful, "failed": failed}


def _write_run_log(spark, records):
    """Append one row per table to control.pipeline_runs.

    The printed summary scrolls away when the notebook is closed; this table is
    still there next week. Same information, made durable.
    """
    if not records:
        return

    rows = [
        (
            run_id,
            "bronze_ingest",
            "bronze",
            table,
            status,
            started,
            datetime.datetime.now(),
            error,
        )
        for run_id, table, status, started, error in records
    ]

    schema = (
        "run_id string, pipeline_name string, layer string, table_name string, "
        "status string, started_at timestamp, finished_at timestamp, "
        "error_message string"
    )

    (
        spark.createDataFrame(rows, schema)
        .write.mode("append")
        .format("delta")
        .saveAsTable(RUNS_TABLE)
    )
