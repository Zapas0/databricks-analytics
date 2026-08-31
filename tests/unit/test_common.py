"""Tests for src/common.py.

These run on your laptop with no cluster, because process_config is pure
Python - it takes a dict and returns a dict. That is the whole reason it is
worth keeping config parsing separate from anything that touches Spark.
"""
import pytest

from common import process_config

RAW = {
    "defaults": {
        "strategy": "incremental",
        "primary_key": "Id",
        "delay": 1,
        "target_catalog": "workspace",
        "target_schema": "nb_bronze",
        "is_active": True,
    },
    "tables": [
        {"source_table": "dbo.Invoice", "watermark_col": "ChangedTimestamp"},
        {"source_table": "dbo.Company", "strategy": "full_reload", "watermark_col": None},
    ],
}


def test_defaults_are_merged_into_each_table():
    configs = process_config(RAW)

    assert configs[0]["primary_key"] == "Id"       # from defaults
    assert configs[0]["delay"] == 1                # from defaults
    assert configs[0]["strategy"] == "incremental"  # from defaults


def test_a_table_can_override_a_default():
    configs = process_config(RAW)

    assert configs[1]["strategy"] == "full_reload"  # the entry wins
    assert configs[1]["primary_key"] == "Id"        # still from defaults


def test_target_table_is_catalog_schema_lowername():
    configs = process_config(RAW)

    assert configs[0]["target_table"] == "workspace.nb_bronze.invoice"
    assert configs[1]["target_table"] == "workspace.nb_bronze.company"


# --- BUGFIX 3: dead config is rejected, not silently ignored --------------

def test_an_unimplemented_strategy_is_refused():
    """incremental_reconcile was declared but never implemented.

    It ran as plain `incremental` and nobody noticed. Now it fails loudly at
    config time, which is the only moment it is cheap to notice.
    """
    raw = {
        **RAW,
        "tables": [{"source_table": "dbo.WorkflowTask",
                    "strategy": "incremental_reconcile",
                    "watermark_col": "CreatedTimestamp"}],
    }

    with pytest.raises(ValueError, match="not implemented"):
        process_config(raw)


def test_incremental_without_a_watermark_column_is_refused():
    """It would quietly full-reload every run while calling itself incremental."""
    raw = {**RAW, "tables": [{"source_table": "dbo.Invoice", "watermark_col": None}]}

    with pytest.raises(ValueError, match="requires a watermark_col"):
        process_config(raw)


# --- BUGFIX 4 -------------------------------------------------------------
# The notebook did source_table.split("dbo")[1], which splits on the literal
# text "dbo" rather than on the separator.

def test_reserved_word_brackets_are_stripped():
    """dbo.[User] must become ...nb_bronze.user, not ...nb_bronze.[User].

    SQL Server needs the brackets because User is a reserved word. Delta must
    not inherit them - a table literally named "[User]" is not what we want.
    """
    raw = {**RAW, "tables": [{"source_table": "dbo.[User]", "watermark_col": "ChangedTimestamp"}]}

    assert process_config(raw)[0]["target_table"] == "workspace.nb_bronze.user"


def test_a_table_whose_name_contains_dbo_survives():
    """The old split("dbo") lost the name entirely on this input."""
    raw = {**RAW, "tables": [{"source_table": "dbo.dboSettings", "watermark_col": "ChangedTimestamp"}]}

    assert process_config(raw)[0]["target_table"] == "workspace.nb_bronze.dbosettings"
