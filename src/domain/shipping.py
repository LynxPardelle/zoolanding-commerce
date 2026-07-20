"""Closed shipping method registry."""

SHIPPING_METHODS = frozenset({"fixed", "free", "pickup"})


def validate_shipping_method(value: object) -> str:
    if not isinstance(value, str) or value not in SHIPPING_METHODS:
        raise ValueError("unsupported shipping method")
    return value
