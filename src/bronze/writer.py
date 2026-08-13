"""Bronze writes.

Bronze mirrors the source: values stay raw (messy text, nulls, CSV columns and
all). We only ADD lineage columns. Cleaning belongs in Silver.

Idempotency comes from MERGE-on-key rather than append, so re-running a batch
updates rows in place instead of duplicating them.
"""
from common.config import primary_keys
from common.connections import read_jdbc, subquery

METADATA_COLUMNS = ("_ingest_run_id", "_source", "_ingested_at")


def build_merge_condition(cfg):
    """Join predicate for the MERGE.

    Built from the configured primary key so composite keys work
    (e.g. InvoiceLineCoding is keyed on InvoiceLineId + CodingSeq).
    Backticks guard column names that collide with SQL keywords.
    """
    keys = primary_keys(cfg)
    return " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)


def add_metadata(df, cfg, run_id):
    from pyspark.sql import functions as F

    return df.withColumns(
        {
            "_ingest_run_id": F.lit(run_id).cast("string"),
            "_source": F.lit(cfg["source_table"]).cast("string"),
            "_ingested_at": F.current_timestamp(),
        }
    )


def ensure_table(spark, settings, password, cfg):
    """Create the target with the right schema if it does not exist yet.

    MERGE needs an existing target, so a brand-new table must be created first.
    `WHERE 1=0` fetches the column definitions without any rows, and
    mode("ignore") makes this a no-op on every subsequent run.
    """
    from pyspark.sql import functions as F

    if spark.catalog.tableExists(cfg["target_table"]):
        return False

    empty = read_jdbc(
        spark, settings, password,
        subquery(f"SELECT * FROM {cfg['source_table']} WHERE 1=0"),
    )

    (
        empty.withColumns(
            {
                "_ingest_run_id": F.lit(None).cast("string"),
                "_source": F.lit(None).cast("string"),
                "_ingested_at": F.lit(None).cast("timestamp"),
            }
        )
        .write.mode("ignore")
        .format("delta")
        .saveAsTable(cfg["target_table"])
    )

    return True


def write_batch(spark, cfg, batch):
    """full_reload replaces the table; incremental upserts on the primary key."""
    from delta.tables import DeltaTable

    if cfg["strategy"] == "full_reload":
        # V2 writer: swaps data and schema atomically, and creates the table if
        # missing. Unity Catalog managed tables on serverless reject
        # .option("overwriteSchema", "true").
        batch.writeTo(cfg["target_table"]).createOrReplace()
        return

    (
        DeltaTable.forName(spark, cfg["target_table"])
        .alias("t")
        .merge(batch.alias("s"), build_merge_condition(cfg))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def last_operation_metrics(spark, table_name):
    """What the most recent write actually did.

    Delta already records this, so we get row counts without a second query -
    which matters because .cache() is unavailable on serverless and every extra
    action would re-read the source.
    """
    from delta.tables import DeltaTable

    row = (
        DeltaTable.forName(spark, table_name)
        .history(1)
        .select("operationMetrics")
        .first()
    )

    if row is None or row[0] is None:
        return {}

    return row[0]


def metrics_to_counts(metrics, strategy):
    """(rows_read, rows_written) from Delta's operation metrics."""

    def as_int(key):
        value = metrics.get(key)
        return 0 if value is None else int(value)

    if strategy == "full_reload":
        rows = as_int("numOutputRows")
        return rows, rows

    return (
        as_int("numSourceRows"),
        as_int("numTargetRowsInserted") + as_int("numTargetRowsUpdated"),
    )
