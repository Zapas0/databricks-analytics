"""Observability.

Two layers, and the durable one is not the logger:

  get_logger()  live commentary while a run is happening. Scrolls away.
  RunTracker    writes control.pipeline_runs - queryable forever, success AND failure.

Ask "what is happening right now?" of the logger; ask "what happened last Tuesday?"
of the table.
"""
import logging
import sys
import uuid
from datetime import datetime

CONTROL_SCHEMA = "workspace.control"
RUNS_TABLE = f"{CONTROL_SCHEMA}.pipeline_runs"

RUNS_SCHEMA = (
    "run_id string, pipeline_name string, layer string, table_name string, "
    "status string, started_at timestamp, finished_at timestamp, "
    "rows_read bigint, rows_written bigint, "
    "watermark_start timestamp, watermark_end timestamp, error_message string"
)


def get_logger(name="platform"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def new_run_id():
    return str(uuid.uuid4())


class RunTracker:
    """Collects one record per table for a run, then writes them in one go.

    Batching the write means a run costs a single append rather than one tiny
    Delta commit per table.
    """

    def __init__(self, spark, pipeline_name, layer, run_id=None):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.layer = layer
        self.run_id = run_id or new_run_id()
        self._records = []

    def record(
        self,
        table_name,
        status,
        started_at,
        rows_read=None,
        rows_written=None,
        watermark_start=None,
        watermark_end=None,
        error_message=None,
    ):
        self._records.append(
            (
                self.run_id,
                self.pipeline_name,
                self.layer,
                table_name,
                status,
                started_at,
                datetime.now(),
                None if rows_read is None else int(rows_read),
                None if rows_written is None else int(rows_written),
                watermark_start,
                watermark_end,
                None if error_message is None else str(error_message)[:2000],
            )
        )

    def flush(self):
        if not self._records:
            return 0

        df = self.spark.createDataFrame(self._records, RUNS_SCHEMA)
        df.write.mode("append").format("delta").saveAsTable(RUNS_TABLE)

        written = len(self._records)
        self._records = []
        return written
