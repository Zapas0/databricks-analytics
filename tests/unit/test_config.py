"""Config parsing. No Spark required - this is why the logic lives in modules."""
import pytest

from common.config import primary_keys, process_config
from common.exceptions import ConfigError

BASE = {
    "defaults": {
        "strategy": "incremental",
        "primary_key": "Id",
        "delay": 15,
        "target_catalog": "workspace",
        "target_schema": "nb_bronze",
        "is_active": True,
    },
    "tables": [{"source_table": "dbo.Invoice", "watermark_col": "ChangedTimestamp"}],
}


def test_defaults_are_merged_into_each_table():
    cfg = process_config(BASE)[0]
    assert cfg["strategy"] == "incremental"
    assert cfg["primary_key"] == "Id"
    assert cfg["delay"] == 15


def test_table_entry_overrides_defaults():
    raw = {
        "defaults": BASE["defaults"],
        "tables": [{"source_table": "dbo.Company", "strategy": "full_reload", "watermark_col": None}],
    }
    assert process_config(raw)[0]["strategy"] == "full_reload"


def test_falsy_override_is_respected():
    """is_active: false and delay: 0 are valid values, not 'missing'.

    A truthiness check would silently replace them with the defaults.
    """
    raw = {
        "defaults": BASE["defaults"],
        "tables": [
            {
                "source_table": "dbo.Invoice",
                "watermark_col": "ChangedTimestamp",
                "is_active": False,
                "delay": 0,
            }
        ],
    }
    cfg = process_config(raw)[0]
    assert cfg["is_active"] is False
    assert cfg["delay"] == 0


def test_target_table_is_derived():
    cfg = process_config(BASE)[0]
    assert cfg["target_table"] == "workspace.nb_bronze.invoice"
    assert cfg["source_schema"] == "dbo"


def test_target_name_splits_on_dot_not_on_schema_name():
    """Splitting on the literal 'dbo' breaks for names containing it."""
    raw = {
        "defaults": BASE["defaults"],
        "tables": [{"source_table": "dbo.dboSettings", "watermark_col": "ChangedTimestamp"}],
    }
    assert process_config(raw)[0]["target_table"] == "workspace.nb_bronze.dbosettings"


def test_reserved_word_brackets_are_stripped():
    """Source needs dbo.[User]; the Delta target must not carry the brackets."""
    raw = {
        "defaults": BASE["defaults"],
        "tables": [{"source_table": "dbo.[User]", "watermark_col": "ChangedTimestamp"}],
    }
    cfg = process_config(raw)[0]
    assert cfg["target_table"] == "workspace.nb_bronze.user"
    assert cfg["source_table"] == "dbo.[User]"   # unchanged for the JDBC query


def test_incremental_without_watermark_column_is_rejected():
    raw = {"defaults": BASE["defaults"], "tables": [{"source_table": "dbo.Invoice"}]}
    with pytest.raises(ConfigError, match="watermark_col"):
        process_config(raw)


def test_unknown_strategy_is_rejected():
    raw = {
        "defaults": BASE["defaults"],
        "tables": [{"source_table": "dbo.Invoice", "strategy": "magic", "watermark_col": "x"}],
    }
    with pytest.raises(ConfigError, match="unknown strategy"):
        process_config(raw)


def test_missing_required_key_is_rejected():
    raw = {"defaults": {}, "tables": [{"source_table": "dbo.Invoice"}]}
    with pytest.raises(ConfigError, match="missing keys"):
        process_config(raw)


def test_empty_tables_is_rejected():
    with pytest.raises(ConfigError, match="no 'tables'"):
        process_config({"defaults": BASE["defaults"], "tables": []})


@pytest.mark.parametrize(
    "value,expected",
    [
        ("Id", ["Id"]),
        ("InvoiceLineId,CodingSeq", ["InvoiceLineId", "CodingSeq"]),
        ("InvoiceLineId, CodingSeq", ["InvoiceLineId", "CodingSeq"]),
        (" InvoiceId , LabelId ", ["InvoiceId", "LabelId"]),
    ],
)
def test_primary_keys_handles_composite_and_whitespace(value, expected):
    assert primary_keys({"primary_key": value}) == expected
