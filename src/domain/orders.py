"""Order calculations that never use floating-point money."""

from .inventory import validate_quantity
from .offers import Money


def line_total(unit_price: Money, quantity: object) -> Money:
    if not isinstance(unit_price, Money):
        raise ValueError("unit_price must be Money")
    return Money(
        unit_price.amount_minor * validate_quantity(quantity),
        unit_price.currency,
        unit_price.supported_currencies,
    )
