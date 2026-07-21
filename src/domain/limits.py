"""Provider-command numeric limits shared by Commerce domain boundaries."""

from __future__ import annotations


MAX_COMMAND_INTEGER = 9_999_999_999
MAX_AMOUNT_MINOR = 99_999_999


def bounded_positive_integer(value: object, field_name: str) -> int:
    if type(value) is not int or not 1 <= value <= MAX_COMMAND_INTEGER:
        raise ValueError(f"{field_name} must be a bounded positive integer")
    return value


def bounded_nonnegative_integer(
    value: object,
    field_name: str,
    *,
    maximum: int = MAX_COMMAND_INTEGER,
) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError(f"{field_name} must be a bounded non-negative integer")
    return value
