"""Immutable, provider-neutral internal order snapshots."""

from dataclasses import dataclass
import re

from .inventory import _positive_quantity, validate_quantity
from .offers import Money


MAX_CHECKOUT_LINES = 20
_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)


def _safe_id(value: object, field_name: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe canonical identifier")
    return value


def line_total(unit_price: Money, quantity: object) -> Money:
    if not isinstance(unit_price, Money):
        raise ValueError("unit_price must be Money")
    return Money(
        unit_price.amount_minor * validate_quantity(quantity),
        unit_price.currency,
        unit_price.supported_currencies,
    )


@dataclass(frozen=True, slots=True)
class CheckoutLine:
    """Server-resolved line snapshot; browser prices are never authoritative."""

    line_id: str
    offer_version_id: str
    quantity: int
    unit_price: Money
    stock_id: str | None = None

    def __post_init__(self) -> None:
        _safe_id(self.line_id, "line_id")
        _safe_id(self.offer_version_id, "offer_version_id")
        _positive_quantity(self.quantity)
        if type(self.unit_price) is not Money:
            raise ValueError("unit_price must be immutable Money")
        if self.stock_id is not None:
            _safe_id(self.stock_id, "stock_id")


@dataclass(frozen=True, slots=True)
class PendingOrder:
    order_id: str
    payment_attempt_id: str
    lines: tuple[CheckoutLine, ...]

    def __post_init__(self) -> None:
        _safe_id(self.order_id, "order_id")
        _safe_id(self.payment_attempt_id, "payment_attempt_id")
        if type(self.lines) is not tuple or not 1 <= len(self.lines) <= MAX_CHECKOUT_LINES:
            raise ValueError("lines must be a tuple containing 1 to 20 entries")
        if any(type(line) is not CheckoutLine for line in self.lines):
            raise ValueError("lines must contain immutable CheckoutLine values")
        line_ids = [line.line_id for line in self.lines]
        if len(set(line_ids)) != len(line_ids):
            raise ValueError("line_id values must be unique")
        offer_version_ids = [line.offer_version_id for line in self.lines]
        if len(set(offer_version_ids)) != len(offer_version_ids):
            raise ValueError("offer_version_id values must be unique within an order")
        currencies = {line.unit_price.currency for line in self.lines}
        if len(currencies) != 1:
            raise ValueError("an order must use one currency")

    @property
    def total(self) -> Money:
        first = self.lines[0].unit_price
        return Money(
            sum(line.unit_price.amount_minor * line.quantity for line in self.lines),
            first.currency,
            first.supported_currencies,
        )
