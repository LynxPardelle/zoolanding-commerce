"""Inventory quantity primitives."""


def validate_quantity(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("quantity must be a non-negative integer")
    return value
