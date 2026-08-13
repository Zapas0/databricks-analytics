"""Merge-condition building and Delta metric parsing. No Spark required."""
import pytest

from bronze.writer import build_merge_condition, metrics_to_counts


def test_single_key_condition():
    assert build_merge_condition({"primary_key": "Id"}) == "t.`Id` = s.`Id`"


def test_composite_key_condition():
    """InvoiceLineCoding has no surrogate key - the merge must use both columns.

    Matching on only the first column would collapse split-coded lines together.
    """
    condition = build_merge_condition({"primary_key": "InvoiceLineId, CodingSeq"})
    assert condition == (
        "t.`InvoiceLineId` = s.`InvoiceLineId` AND t.`CodingSeq` = s.`CodingSeq`"
    )


def test_keys_are_backticked():
    """Guards column names that collide with SQL keywords."""
    assert build_merge_condition({"primary_key": "Order"}) == "t.`Order` = s.`Order`"


def test_merge_metrics_split_read_from_written():
    metrics = {
        "numSourceRows": "179",
        "numTargetRowsInserted": "12",
        "numTargetRowsUpdated": "7",
    }
    assert metrics_to_counts(metrics, "incremental") == (179, 19)


def test_idle_run_reads_rows_but_writes_none():
    """The overlap window re-reads rows; identical values mean zero updates."""
    metrics = {"numSourceRows": "179", "numTargetRowsInserted": "0", "numTargetRowsUpdated": "0"}
    assert metrics_to_counts(metrics, "incremental") == (179, 0)


def test_full_reload_uses_output_rows():
    assert metrics_to_counts({"numOutputRows": "300"}, "full_reload") == (300, 300)


@pytest.mark.parametrize("metrics", [{}, {"numSourceRows": None}])
def test_missing_metrics_default_to_zero(metrics):
    assert metrics_to_counts(metrics, "incremental") == (0, 0)
