"""Shared helpers: the source connection, and the config.

Bronze needs these today and Silver will need them tomorrow, so they live here
rather than inside bronze.py.

Everything in this file came from cells 1-5 of the Bronze notebook.
"""
import yaml

# NOTE: pyspark and delta are deliberately NOT imported at the top of this file.
# They only exist inside a Databricks runtime. Importing them inside the
# functions that need them keeps this module importable from a plain Python
# process on your laptop - which is what lets `pytest` run the pure functions
# (load_config, process_config, merge_condition) with no cluster and no JVM.

CONTROL_SCHEMA = "workspace.control"
TABLES_TABLE = f"{CONTROL_SCHEMA}.tables"

# BUGFIX 3 - strategies the pipeline actually implements.
#
# Your workspace config.yaml declares `incremental_reconcile` on WorkflowTask,
# but no code has ever branched on it: pull_delta only asks whether
# watermark_col is None, so that table has been running as plain `incremental`
# the whole time. Silent, and exactly the kind of thing you only discover when
# the numbers are already wrong.
#
# A timestamp watermark cannot see a row disappear, so hard-deleted rows
# (WorkflowTask, InvoiceLineCoding, InvoiceLabel, Attachment) stay in Bronze
# forever. Fixing that needs a reconcile pass we have not built yet.
#
# Until then: reject the strategy at config time rather than pretend to honour
# it. A loud failure at startup beats a quiet wrong answer for months.
KNOWN_STRATEGIES = ("incremental", "full_reload")


# --------------------------------------------------------------------------
# 1. Connection            <- notebook cell 1
# --------------------------------------------------------------------------
# In the notebook, `jdbc_url`, `user` and `password` were three loose globals.
# A .py module has no notebook globals, so we build the same three values and
# hand them back in one dict. That dict is the only thing the other functions
# need to reach SQL Server.

def connect(server, database, user, scope, key, dbutils, port=1433):
    """Build the JDBC connection info.

    The password is read from the Databricks secret scope at call time. It is
    never written into the file and never committed.
    """
    return {
        "url": (
            f"jdbc:sqlserver://{server}:{port};"
            f"database={database};"
            "encrypt=true;trustServerCertificate=false;loginTimeout=60"
        ),
        "user": user,
        "password": dbutils.secrets.get(scope=scope, key=key),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def read_jdbc(spark, conn, dbtable):
    """Read from SQL Server.

    `dbtable` is whatever goes after FROM: either a plain table name, or a
    parenthesised subquery with an alias. Passing a subquery is how the
    incremental filter gets pushed down into the database, so only the rows we
    want cross the network.

    The notebook repeated this spark.read block three times (twice in
    pull_delta, once in get_latest_wm). This is that same block, written once.
    """
    return (
        spark.read.format("jdbc")
        .option("url", conn["url"])
        .option("dbtable", dbtable)
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", conn["driver"])
        .load()
    )


# --------------------------------------------------------------------------
# 2. Config                <- notebook cells 2-5
# --------------------------------------------------------------------------

def load_config(path):
    """Read the YAML file. Unchanged from the notebook."""
    with open(path) as file:
        return yaml.safe_load(file)


def process_config(config):
    """Merge the defaults into each table entry and derive the target name.

    `defaults | table` merges two dicts with the right-hand side winning, so a
    table entry only has to declare what differs from the defaults.
    """
    defaults = config.get("defaults", {})
    merged = [defaults | table for table in config.get("tables")]

    for table in merged:
        # BUGFIX 3 - refuse a strategy nothing implements.
        if table.get("strategy") not in KNOWN_STRATEGIES:
            raise ValueError(
                f"{table.get('source_table', '<unknown>')}: strategy "
                f"{table.get('strategy')!r} is not implemented. "
                f"Use one of {KNOWN_STRATEGIES}."
            )

        # An incremental table with no watermark column would pull the whole
        # table every run while pretending to be incremental.
        if table["strategy"] == "incremental" and not table.get("watermark_col"):
            raise ValueError(
                f"{table['source_table']}: incremental requires a watermark_col"
            )

        # BUGFIX 4 - deriving the target table name.
        #
        # The notebook did:  table["source_table"].split("dbo")[1]
        # which splits on the literal text "dbo". That breaks two ways:
        #   "dbo.[User]"      -> ".[User]"   -> brackets end up in the table name
        #   "dbo.dboSettings" -> "."         -> name lost entirely
        #
        # Splitting on the separator instead of the schema name is safe for any
        # table, and .strip("[]") drops the quoting SQL Server needs for
        # reserved words like [User] and [Label].
        short_name = table["source_table"].split(".")[-1].strip("[]").lower()

        table["target_table"] = (
            f"{table['target_catalog']}.{table['target_schema']}.{short_name}"
        )

    return merged


# --------------------------------------------------------------------------
# 3. control.tables
# --------------------------------------------------------------------------
# Why this exists: the YAML file is a development convenience. In production
# there is no repo checkout sitting next to the job, so control.tables becomes
# the source of truth - and being a real table, it is queryable in SQL.
#
# The flow is:  YAML (in Git)  ->  sync  ->  control.tables  ->  the pipeline reads

def sync_config_to_control(spark, configs):
    """Upsert the YAML config into control.tables. Returns the row count."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    rows = [
        (
            cfg.get("source_system", "azuresql"),
            cfg["source_table"].split(".")[0].strip("[]"),
            cfg["source_table"],
            cfg["target_catalog"],
            cfg["target_schema"],
            cfg["target_table"],
            cfg["strategy"],
            cfg["primary_key"],
            cfg.get("watermark_col"),
            int(cfg["delay"]),
            bool(cfg.get("is_active", True)),
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

    Returns exactly the same dict shape process_config() produces, so the
    pipeline cannot tell whether the config came from YAML or from the table.
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
            "source_table": row["source_table"],
            "target_catalog": row["target_catalog"],
            "target_schema": row["target_schema"],
            "target_table": row["target_table"],
            "strategy": row["load_strategy"],
            "primary_key": row["primary_key"],
            "watermark_col": row["watermark_col"],
            # Guard against a NULL in the table: int(None) would explode later.
            "delay": 0 if row["delay_minutes"] is None else row["delay_minutes"],
            "is_active": row["enabled"],
        }
        for row in rows
    ]
