"""Provider-neutral inventory invariants and reconciliation decisions."""

from dataclasses import dataclass, replace
import re


CHECKOUT_EXPIRY_SECONDS = 35 * 60
RECONCILIATION_GRACE_SECONDS = 5 * 60
RECONCILER_INTERVAL_SECONDS = 5 * 60

_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)
_CHECKOUT_EVIDENCE = frozenset(
    {
        "confirmed_created",
        "confirmed_not_created",
        "confirmed_expiry_precondition_not_created",
        "timeout",
        "network_error",
        "provider_5xx",
        "provider_429",
        "ambiguous",
    }
)
_UNCERTAIN_CHECKOUT_EVIDENCE = frozenset(
    {"timeout", "network_error", "provider_5xx", "provider_429", "ambiguous"}
)
_RECONCILIATION_STATUSES = frozenset(
    {"paid", "terminal_unpaid", "not_created", "pending", "unknown", "lookup_failure"}
)


def validate_quantity(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("quantity must be a non-negative integer")
    return value


def _positive_quantity(value: object) -> int:
    quantity = validate_quantity(value)
    if quantity == 0:
        raise ValueError("quantity must be positive")
    return quantity


def _safe_id(value: object, field_name: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe canonical identifier")
    return value


def _epoch(value: object, field_name: str = "epoch") -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


@dataclass(frozen=True, slots=True)
class StockState:
    stock_id: str
    location_id: str
    tracked: bool
    on_hand: int
    reserved: int
    revision: int

    def __post_init__(self) -> None:
        _safe_id(self.stock_id, "stock_id")
        _safe_id(self.location_id, "location_id")
        if type(self.tracked) is not bool:
            raise ValueError("tracked must be boolean")
        validate_quantity(self.on_hand)
        validate_quantity(self.reserved)
        if self.reserved > self.on_hand:
            raise ValueError("reserved stock cannot exceed on-hand stock")
        if type(self.revision) is not int or self.revision <= 0:
            raise ValueError("revision must be a positive integer")

    @property
    def available(self) -> int:
        return self.on_hand - self.reserved

    def _require_tracked(self) -> None:
        if not self.tracked:
            raise ValueError("untracked stock cannot be mutated")

    def adjust(self, delta: object) -> "StockState":
        self._require_tracked()
        if type(delta) is not int or delta == 0:
            raise ValueError("stock adjustment must be a non-zero integer")
        updated_on_hand = self.on_hand + delta
        if updated_on_hand < self.reserved:
            raise ValueError("stock adjustment cannot consume reserved stock")
        return replace(self, on_hand=updated_on_hand, revision=self.revision + 1)

    def reserve(self, quantity: object) -> "StockState":
        self._require_tracked()
        quantity = _positive_quantity(quantity)
        if quantity > self.available:
            raise ValueError("insufficient available stock")
        return replace(self, reserved=self.reserved + quantity, revision=self.revision + 1)

    def commit(self, quantity: object) -> "StockState":
        self._require_tracked()
        quantity = _positive_quantity(quantity)
        if quantity > self.reserved:
            raise ValueError("cannot commit more than reserved stock")
        return replace(
            self,
            on_hand=self.on_hand - quantity,
            reserved=self.reserved - quantity,
            revision=self.revision + 1,
        )

    def release(self, quantity: object) -> "StockState":
        self._require_tracked()
        quantity = _positive_quantity(quantity)
        if quantity > self.reserved:
            raise ValueError("cannot release more than reserved stock")
        return replace(self, reserved=self.reserved - quantity, revision=self.revision + 1)


@dataclass(frozen=True, slots=True)
class ReservationTiming:
    reservation_created_at: int
    checkout_expires_at: int
    reconcile_after: int


def reservation_timing(created_at: object) -> ReservationTiming:
    created_at = _epoch(created_at, "created_at")
    checkout_expires_at = created_at + CHECKOUT_EXPIRY_SECONDS
    return ReservationTiming(
        reservation_created_at=created_at,
        checkout_expires_at=checkout_expires_at,
        reconcile_after=checkout_expires_at + RECONCILIATION_GRACE_SECONDS,
    )


@dataclass(frozen=True, slots=True)
class CheckoutOutcome:
    action: str
    retry_same_attempt: bool
    requires_reconciliation: bool
    completion_reason: str | None


def checkout_outcome(evidence: object) -> CheckoutOutcome:
    """Classify already-normalized provider evidence without parsing provider errors."""

    if type(evidence) is not str or evidence not in _CHECKOUT_EVIDENCE:
        raise ValueError("unsupported checkout evidence")
    if evidence in {"confirmed_not_created", "confirmed_expiry_precondition_not_created"}:
        return CheckoutOutcome("release", False, False, evidence)
    if evidence in _UNCERTAIN_CHECKOUT_EVIDENCE:
        return CheckoutOutcome("hold", True, True, None)
    return CheckoutOutcome("hold", False, True, None)


@dataclass(frozen=True, slots=True)
class ReconciliationOutcome:
    action: str
    next_reconcile_at: int | None
    completion_reason: str | None


def reconciliation_outcome(status: object, now_epoch: object) -> ReconciliationOutcome:
    """Decide from a canonical internal status; time alone never releases stock."""

    if type(status) is not str or status not in _RECONCILIATION_STATUSES:
        raise ValueError("unsupported reconciliation status")
    now_epoch = _epoch(now_epoch, "now_epoch")
    if status == "paid":
        return ReconciliationOutcome("commit", None, "canonical_paid")
    if status == "terminal_unpaid":
        return ReconciliationOutcome("release", None, "canonical_terminal_unpaid")
    if status == "not_created":
        return ReconciliationOutcome("release", None, "canonical_not_created")
    return ReconciliationOutcome("hold", now_epoch + RECONCILER_INTERVAL_SECONDS, None)
