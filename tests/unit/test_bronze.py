"""Tests for src/bronze.py.

The two bugs that could lose or corrupt data are pinned down here, so they
cannot come back quietly.

pull_delta normally calls read_jdbc, which needs a cluster. The test swaps
read_jdbc for a fake that just records the SQL it was handed. That is enough to
check the predicate is right, and it runs in milliseconds with no Spark.
"""
import datetime

import bronze

# --- BUGFIX 2: composite primary keys -------------------------------------
# The notebook hardcoded "t.Id = s.Id".

def test_merge_condition_for_a_single_key():
    assert bronze.merge_condition({"primary_key": "Id"}) == "t.`Id` = s.`Id`"


def test_merge_condition_for_a_composite_key():
    """InvoiceLineCoding has no surrogate key - it needs both columns.

    With only "t.Id = s.Id" this table has no Id at all, and with any single
    column every source row matches many target rows and the MERGE corrupts it.
    """
    condition = bronze.merge_condition({"primary_key": "InvoiceLineId, CodingSeq"})

    assert condition == "t.`InvoiceLineId` = s.`InvoiceLineId` AND t.`CodingSeq` = s.`CodingSeq`"


def test_merge_condition_tolerates_spacing():
    assert bronze.merge_condition({"primary_key": "InvoiceId,LabelId"}) == (
        "t.`InvoiceId` = s.`InvoiceId` AND t.`LabelId` = s.`LabelId`"
    )


# --- BUGFIX 1: the batch has a ceiling ------------------------------------

CFG = {
    "source_table": "dbo.Invoice",
    "watermark_col": "ChangedTimestamp",
    "delay": 15,
}
WM = datetime.datetime(2026, 8, 1, 12, 0, 0)
CEILING = datetime.datetime(2026, 8, 3, 9, 30, 0)


def capture_sql(monkeypatch):
    """Replace read_jdbc with a fake that records the dbtable it is given."""
    seen = {}

    def fake_read_jdbc(spark, conn, dbtable):
        seen["dbtable"] = dbtable
        return "fake dataframe"

    monkeypatch.setattr(bronze, "read_jdbc", fake_read_jdbc)
    return seen


def test_lower_bound_reaches_back_by_the_delay(monkeypatch):
    """delay=15 means we re-read the last 15 minutes to absorb clock skew.

    Re-reading is free - the write is a MERGE. Missing a row is not.
    """
    seen = capture_sql(monkeypatch)

    bronze.pull_delta(None, None, CFG, WM, CEILING)

    assert "ChangedTimestamp > '2026-08-01 11:45:00'" in seen["dbtable"]


def test_upper_bound_is_the_ceiling(monkeypatch):
    """This is the fix for the silent data loss.

    Without the upper bound, a row arriving mid-write is counted by the
    source MAX() but never ingested - and the watermark moves past it forever.
    """
    seen = capture_sql(monkeypatch)

    bronze.pull_delta(None, None, CFG, WM, CEILING)

    assert "ChangedTimestamp <= '2026-08-03 09:30:00'" in seen["dbtable"]


def test_no_ceiling_means_no_upper_bound(monkeypatch):
    """An empty source table gives ceiling=None. The read still works."""
    seen = capture_sql(monkeypatch)

    bronze.pull_delta(None, None, CFG, WM, None)

    assert "<=" not in seen["dbtable"]
    assert "ChangedTimestamp > '2026-08-01 11:45:00'" in seen["dbtable"]


def test_full_reload_reads_the_whole_table(monkeypatch):
    """No watermark column means no predicate at all - just SELECT the table."""
    seen = capture_sql(monkeypatch)

    bronze.pull_delta(None, None, {"source_table": "dbo.Company", "watermark_col": None}, WM, None)

    assert seen["dbtable"] == "dbo.Company"
