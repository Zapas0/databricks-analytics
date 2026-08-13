"""Watermark handling.

The rule that makes the pipeline crash-safe: the watermark is advanced only
AFTER the write succeeds. If a run dies mid-way the mark has not moved, so the
next run re-does the batch - harmlessly, because the write is a MERGE.

The second rule, less obvious: capture the ceiling BEFORE reading (`source_max`),
not after writing. Reading the source max afterwards can include rows that
arrived while the batch was being written - advancing the watermark past rows
that were never ingested, which loses them permanently.
"""
from datetime import datetime

from common.connections import read_jdbc, subquery

CONTROL_SCHEMA = "workspace.control"
WATERMARKS_TABLE = f"{CONTROL_SCHEMA}.watermarks"

# Far enough back that the first run pulls everything, so bootstrap and steady
# state use the same code path - no "if first run" branch anywhere.
EPOCH = datetime(1900, 1, 1)


def find_watermark(spark, cfg):
    """Where did this table get to last time? EPOCH if it has never run."""
    if not cfg.get("watermark_col"):
        return EPOCH

    row = (
        spark.table(WATERMARKS_TABLE)
        .where(f"table_name = '{cfg['source_table']}'")
        .select("last_watermark")
        .first()
    )

    # A row can exist with a NULL mark (e.g. migrated in). Treat it as never-run
    # rather than returning None, which would break the arithmetic downstream.
    if row is None or row[0] is None:
        return EPOCH

    return row[0]


def source_max(spark, settings, password, cfg):
    """The ceiling for this run (T0), read BEFORE pulling any data.

    Cheap: an indexed MAX() on the watermark column.
    Returns None when the source table is empty.
    """
    if not cfg.get("watermark_col"):
        return None

    sql = f"SELECT MAX({cfg['watermark_col']}) AS mx FROM {cfg['source_table']}"
    row = read_jdbc(spark, settings, password, subquery(sql)).first()

    return None if row is None else row["mx"]


def update_watermark(spark, cfg, new_value, run_id):
    """Upsert the mark. Idempotent, keyed on table_name."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if new_value is None:
        return None

    row = spark.createDataFrame(
        [
            (
                cfg.get("source_system", "azuresql"),
                cfg["source_table"],
                cfg.get("watermark_col"),
                new_value,
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

    return new_value
