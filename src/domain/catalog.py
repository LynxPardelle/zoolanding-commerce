"""Closed sellable type registry."""

SELLABLE_TYPES = frozenset({"physical", "service", "subscription", "add_on"})


def validate_sellable_type(value: object) -> str:
    if not isinstance(value, str) or value not in SELLABLE_TYPES:
        raise ValueError("unsupported sellable type")
    return value
