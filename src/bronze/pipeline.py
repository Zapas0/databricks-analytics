"""Bronze orchestration.

One code path per table, identical on the first run and the thousandth:

    ensure table -> read watermark -> capture ceiling T0 -> pull [W-delay, T0]
    -> add lineage -> write (MERGE) -> advance watermark to T0

The ordering is the whole design. The watermark moves last, and only to a
ceiling captured before the read, so a crash or a mid-run insert can never make
the pipeline skip data.
"""
import time
from datetime import datetime, timedelta

from bronze.watermark import find_watermark, source_max, update_watermark
from bronze.writer import (
    add_metadata,
    ensure_table,
    last_operation_metrics,
    metrics_to_counts,
    write_batch,
)
from common.connections import read_jdbc, subquery
from common.exceptions import is_transient
from common.logging import RunTracker, get_logger

logger = get_logger("bronze")

DEFAULT_RETRIES = 3
RETRY_WAIT_SECONDS = 15
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def build_where_clause(cfg, watermark, ceiling):

    column = cfg["watermark_col"]
    low = (watermark - timedelta(minutes=int(cfg["delay"]))).strftime(TS_FORMAT)

    where = f"{column} >= '{low}'"
    if ceiling is not None:
        where += f" AND {column} <= '{ceiling.strftime(TS_FORMAT)}'"

    return where


def pull_delta(spark, settings, password, cfg, watermark, ceiling):
    """Read the batch, filtered inside the database.

    The predicate goes into the `dbtable` subquery so SQL Server does the
    filtering and only changed rows cross the network.
    """
    if cfg["strategy"] == "full_reload":
        return read_jdbc(spark, settings, password, cfg["source_table"])

    where = build_where_clause(cfg, watermark, ceiling)

    return read_jdbc(
        spark, settings, password,
        subquery(f"SELECT * FROM {cfg['source_table']} WHERE {where}"),
    )


def ingest_table(spark, settings, password, cfg, run_id):
    """Ingest one table. Returns (rows_read, rows_written, watermark_start, watermark_end)."""
    ensure_table(spark, settings, password, cfg)

    watermark = find_watermark(spark, cfg)

    ceiling = source_max(spark, settings, password, cfg)

    batch = add_metadata(
        pull_delta(spark, settings, password, cfg, watermark, ceiling), cfg, run_id
    )


    write_batch(spark, cfg, batch)

    rows_read, rows_written = metrics_to_counts(
        last_operation_metrics(spark, cfg["target_table"]), cfg["strategy"]
    )

    new_watermark = watermark
    if cfg.get("watermark_col") and ceiling is not None:
        update_watermark(spark, cfg, ceiling, run_id)
        new_watermark = ceiling

    return rows_read, rows_written, watermark, new_watermark


def run_bronze(spark, settings, password, configs, run_id=None, retries=DEFAULT_RETRIES):
    """Ingest every enabled table.

    """
    tracker = RunTracker(spark, "bronze_ingest", "bronze", run_id)
    logger.info("bronze run %s starting (%d tables)", tracker.run_id, len(configs))

    failed = []

    for cfg in configs:
        table = cfg["source_table"]

        if not cfg.get("is_active", True):
            logger.info("%-22s skipped (disabled)", table)
            continue

        started = datetime.now()

        for attempt in range(1, retries + 1):
            try:
                rows_read, rows_written, wm_start, wm_end = ingest_table(
                    spark, settings, password, cfg, tracker.run_id
                )
                tracker.record(
                    table, "success", started,
                    rows_read=rows_read, rows_written=rows_written,
                    watermark_start=wm_start, watermark_end=wm_end,
                )
                logger.info(
                    "%-22s ok    read=%-7d written=%-7d watermark=%s",
                    table, rows_read, rows_written, wm_end,
                )
                break

            except Exception as error:

                if attempt < retries and is_transient(error):
                    logger.warning(
                        "%-22s transient (attempt %d/%d): %s",
                        table, attempt, retries, str(error)[:150],
                    )
                    time.sleep(RETRY_WAIT_SECONDS)
                    continue

                failed.append(table)
                tracker.record(table, "failed", started, error_message=error)
                logger.error("%-22s FAILED %s", table, str(error)[:300])
                break

    tracker.flush()

    if failed:
        raise RuntimeError(f"bronze run {tracker.run_id}: {len(failed)} table(s) failed: {failed}")

    logger.info("bronze run %s complete", tracker.run_id)
    return tracker.run_id
