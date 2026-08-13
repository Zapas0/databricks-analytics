"""Retry classification.

Getting this wrong is expensive in both directions: retrying a permanent error
wastes three attempts, and not retrying a transient one fails a healthy pipeline.
"""
import pytest

from common.exceptions import PermanentError, TransientError, is_transient


@pytest.mark.parametrize(
    "message",
    [
        "Login timeout expired",
        "TCP Provider: Timeout error [258]",
        "Connection reset by peer",
        "Database 'x' is not currently available",   # serverless waking up
        "Transaction was deadlocked",
        "Request was throttled",
    ],
)
def test_transient_failures_are_retried(message):
    assert is_transient(Exception(message)) is True


@pytest.mark.parametrize(
    "message",
    [
        "Invalid column name 'Foo'",
        "Cannot perform Merge as multiple source rows matched the same target row",
        "Table or view not found",
        "PERSIST TABLE is not supported on serverless compute",
    ],
)
def test_permanent_failures_are_not_retried(message):
    assert is_transient(Exception(message)) is False


def test_explicit_types_win_over_message_matching():
    assert is_transient(TransientError("anything")) is True
    assert is_transient(PermanentError("timeout")) is False
