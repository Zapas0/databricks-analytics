"""Configuration.

config/ingestion_config.yaml (in Git) is the source of truth. It is synced into
control.tables, and production code reads the control table - so operators can
see and query what the platform is configured to do, while changes still go
through version control.

These functions are deliberately free of Spark for the parsing half, so they can
be unit-tested without a cluster.
"""
import yaml

from common.exceptions import ConfigError

CONTROL_SCHEMA = "workspace.control"
TABLES_TABLE = f"{CONTROL_SCHEMA}.tables"

REQUIRED_KEYS = ("source_table", "strategy", "primary_key", "target_catalog", "target_schema")


def load_yaml(path):
    with open(path) as handle:
        return yaml.safe_load(handle)


def process_config(raw):
    """Merge defaults into each table entry and derive the target table name.

    `defaults | entry` merges dicts with the entry winning, so a table only has
    to declare what differs from the defaults.
    """
    defaults = raw.get("defaults", {})
    entries = raw.get("tables") or []

    if not entries:
        raise ConfigError("config has no 'tables' entries")

    configs = []
    for entry in entries:
        cfg = defaults | entry

        missing = [key for key in REQUIRED_KEYS if key not in cfg]
        if missing:
            raise ConfigError(f"{cfg.get('source_table', '<unknown>')}: missing keys {missing}")

        if cfg["strategy"] not in ("incremental", "full_reload"):
            raise ConfigError(f"{cfg['source_table']}: unknown strategy {cfg['strategy']!r}")

        if cfg["strategy"] == "incremental" and not cfg.get("watermark_col"):
            raise ConfigError(f"{cfg['source_table']}: incremental requires watermark_col")

        # Split on the separator, not on the schema name: "dbo.Invoice" -> "invoice".
        # Splitting on "dbo" breaks for any table whose name contains it.
        # Strip brackets too: reserved words are quoted at the source
        # ("dbo.[User]") but must not carry the brackets into the target name.
        short_name = cfg["source_table"].split(".")[-1].strip("[]").lower()
        cfg["source_schema"] = cfg["source_table"].split(".")[0].strip("[]")
        cfg["target_table"] = f"{cfg['target_catalog']}.{cfg['target_schema']}.{short_name}"
        cfg.setdefault("source_system", "azuresql")
        cfg.setdefault("delay", 15)
        cfg.setdefault("is_active", True)

        configs.append(cfg)

    return configs


def load_configs(path):
    """Read the YAML file and return fully resolved config dicts."""
    return process_config(load_yaml(path))


def primary_keys(cfg):
    """Primary key may be composite: 'InvoiceLineId, CodingSeq'."""
    return [key.strip() for key in cfg["primary_key"].split(",") if key.strip()]


# --- control.tables sync -----------------------------------------------------

def sync_config_to_control(spark, configs):
    """Upsert the YAML config into control.tables (the runtime source of record)."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    rows = [
        (
            cfg["source_system"],
            cfg["source_schema"],
            cfg["source_table"],
            cfg["target_catalog"],
            cfg["target_schema"],
            cfg["target_table"],
            cfg["strategy"],
            cfg["primary_key"],
            cfg.get("watermark_col"),
            int(cfg["delay"]),
            bool(cfg["is_active"]),
        )
        for cfg in configs
    ]

    schema = (
        "source_system string, source_schema string, source_table string, "
        "target_catalog string, target_schema string, target_table string, "
        "load_strategy string, primary_key string, watermark_col string, "
        "delay_minutes int, enabled boolean"
    )

    incoming = spark.createDataFrame(rows, schema).withColumn(
        "updated_at", F.current_timestamp()
    )

    (
        DeltaTable.forName(spark, TABLES_TABLE)
        .alias("t")
        .merge(incoming.alias("s"), "t.source_table = s.source_table")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    return len(rows)


def get_enabled_table_configs(spark):
    """Read the enabled tables back out of control.tables.

    Returns the same dict shape the pipeline uses, so callers cannot tell whether
    the config came from YAML or the control table.
    """
    rows = (
        spark.table(TABLES_TABLE)
        .where("enabled = true")
        .orderBy("source_table")
        .collect()
    )

    return [
        {
            "source_system": row["source_system"],
            "source_schema": row["source_schema"],
            "source_table": row["source_table"],
            "target_catalog": row["target_catalog"],
            "target_schema": row["target_schema"],
            "target_table": row["target_table"],
            "strategy": row["load_strategy"],
            "primary_key": row["primary_key"],
            "watermark_col": row["watermark_col"],
            "delay": row["delay_minutes"],
            "is_active": row["enabled"],
        }
        for row in rows
    ]
