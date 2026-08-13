"""The incremental predicate.

This encodes two fixes for real data-loss bugs, so it is worth pinning down:
  - the overlap window (lower bound) absorbs clock skew and late commits
  - the ceiling (upper bound) stops rows arriving mid-run from being skipped
"""
from datetime import datetime

from bronze.pipeline import build_where_clause

CFG = {"watermark_col": "ChangedTimestamp", "delay": 15}
W = datetime(2026, 8, 1, 12, 0, 0)
T0 = datetime(2026, 8, 3, 9, 30, 0)


def test_lower_bound_subtracts_the_overlap_window():
    where = build_where_clause(CFG, W, None)
    assert where == "ChangedTimestamp >= '2026-08-01 11:45:00'"


def test_zero_overlap_uses_the_watermark_exactly():
    where = build_where_clause({**CFG, "delay": 0}, W, None)
    assert where == "ChangedTimestamp >= '2026-08-01 12:00:00'"


def test_ceiling_bounds_the_batch():
    """Without an upper bound, rows committed during the run get skipped forever:
    the watermark would advance past timestamps that were never ingested."""
    where = build_where_clause(CFG, W, T0)
    assert where == (
        "ChangedTimestamp >= '2026-08-01 11:45:00' "
        "AND ChangedTimestamp <= '2026-08-03 09:30:00'"
    )


def test_empty_source_has_no_ceiling():
    """source_max() returns None for an empty table - the clause stays open-ended."""
    assert "<=" not in build_where_clause(CFG, W, None)


def test_watermark_column_is_taken_from_config():
    """ExchangeRate must filter on CreatedTimestamp (when the row landed),
    not RateDate (what the row is about) - rates arrive backdated."""
    cfg = {"watermark_col": "CreatedTimestamp", "delay": 15}
    assert build_where_clause(cfg, W, None).startswith("CreatedTimestamp >=")


def test_epoch_watermark_pulls_everything():
    """First run: the default 1900 watermark means one code path handles bootstrap."""
    where = build_where_clause(CFG, datetime(1900, 1, 1), None)
    assert where == "ChangedTimestamp >= '1899-12-31 23:45:00'"
