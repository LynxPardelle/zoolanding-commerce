"""Subscription eligibility rules."""

from .catalog import validate_sellable_type


def validate_recurring_sellable_type(value: object) -> str:
    sellable_type = validate_sellable_type(value)
    if sellable_type == "physical":
        raise ValueError("physical recurring offers are not supported")
    return sellable_type
