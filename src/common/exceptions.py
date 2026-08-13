"""Typed errors.

The point of the split is retry policy: a TransientError is worth retrying
(the source was briefly unreachable), a PermanentError is not (retrying a schema
mismatch just fails three times instead of once).
"""


class PipelineError(Exception):
    """Base for everything this platform raises."""


class TransientError(PipelineError):
    """Temporary and worth retrying: connection timeout, paused database, throttling."""


class PermanentError(PipelineError):
    """Retrying will not help: bad config, schema mismatch, missing column."""


class ConfigError(PermanentError):
    """The configuration itself is wrong or incomplete."""


# Substrings that identify a transient failure. Anything not matching is treated
# as permanent, so we fail fast instead of burning retries on a real bug.
_TRANSIENT_MARKERS = (
    "timeout",
    "timed out",
    "connection reset",
    "connection refused",
    "temporarily unavailable",
    "is not currently available",   # Azure SQL waking from auto-pause
    "login failed for user 'sa'",   # transient during failover, not a bad password
    "deadlock",
    "throttl",
)


def is_transient(error):
    """Best-effort classification of an arbitrary exception."""
    if isinstance(error, TransientError):
        return True
    if isinstance(error, PermanentError):
        return False

    text = str(error).lower()
    return any(marker in text for marker in _TRANSIENT_MARKERS)
