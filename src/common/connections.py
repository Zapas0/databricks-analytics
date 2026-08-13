"""Source connectivity.

No notebook globals: the connection is built from a Settings object, and the
password is read from a Databricks secret scope at call time - never stored,
never passed around as a literal.
"""
from dataclasses import dataclass

from common.exceptions import ConfigError


@dataclass(frozen=True)
class SourceSettings:
    """Everything needed to reach the source database except the password."""

    server: str
    database: str
    user: str
    secret_scope: str
    secret_key: str
    port: int = 1433
    driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    login_timeout: int = 60

    @property
    def jdbc_url(self):
        return (
            f"jdbc:sqlserver://{self.server}:{self.port};"
            f"database={self.database};"
            f"encrypt=true;trustServerCertificate=false;"
            f"loginTimeout={self.login_timeout}"
        )


def get_password(settings, dbutils):
    """Read the password from the Databricks secret scope.

    dbutils is passed in rather than imported: it only exists inside a Databricks
    runtime, and passing it keeps these modules importable (and unit-testable)
    from a plain Python process.
    """
    if dbutils is None:
        raise ConfigError(
            "dbutils is required to read the source password. "
            "Pass it in from the notebook: run_bronze(dbutils=dbutils)"
        )
    return dbutils.secrets.get(scope=settings.secret_scope, key=settings.secret_key)


def read_jdbc(spark, settings, password, dbtable):
    """Read from the source.

    `dbtable` is whatever goes after FROM, so it accepts either a table name or a
    parenthesised subquery with an alias. Passing a subquery is how we push the
    incremental filter down into the database instead of pulling the whole table.
    """
    return (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", dbtable)
        .option("user", settings.user)
        .option("password", password)
        .option("driver", settings.driver)
        .load()
    )


def subquery(sql):
    """Wrap a SELECT so it is valid as a JDBC `dbtable` value."""
    return f"({sql}) AS q"
