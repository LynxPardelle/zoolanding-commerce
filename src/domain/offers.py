"""Immutable, provider-neutral offer and discount versions."""

from dataclasses import dataclass, field, replace
import hashlib
import json
import re
import unicodedata

from .catalog import validate_sellable_type
from .fiscal import TAX_BEHAVIORS
from .limits import (
    MAX_AMOUNT_MINOR,
    bounded_nonnegative_integer,
    bounded_positive_integer,
)
from .subscriptions import validate_recurring_sellable_type


LIFECYCLE_STATES = (
    "draft",
    "provisioning",
    "active",
    "existing_only",
    "retired",
)
_NEXT_LIFECYCLE_STATE = {
    current: target
    for current, target in zip(LIFECYCLE_STATES, LIFECYCLE_STATES[1:])
}
_RECURRENCE_INTERVALS = frozenset({"month", "year"})
_DISCOUNT_DURATIONS = frozenset({"once", "forever", "repeating"})
_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)
_SAFE_CUSTOMER_CODE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", re.ASCII)
_URL_LIKE = re.compile(
    r"(?:\b[A-Za-z][A-Za-z0-9+.-]{0,31}:(?=\S)|www\.|"
    r"\b(?:[a-z0-9-]+\.)+[a-z]{2,63}(?:[/?:#]\S*)?)",
    re.IGNORECASE | re.ASCII,
)


@dataclass(frozen=True, slots=True)
class Money:
    amount_minor: int
    currency: str
    supported_currencies: frozenset[str] = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        bounded_nonnegative_integer(
            self.amount_minor,
            "amount_minor",
            maximum=MAX_AMOUNT_MINOR,
        )
        if not isinstance(self.currency, str) or re.fullmatch(r"[A-Z]{3}", self.currency, re.ASCII) is None:
            raise ValueError("currency must be a canonical three-letter code")
        if type(self.supported_currencies) is not frozenset or not self.supported_currencies:
            raise ValueError("supported_currencies must be a non-empty frozenset")
        if any(
            not isinstance(code, str) or re.fullmatch(r"[A-Z]{3}", code, re.ASCII) is None
            for code in self.supported_currencies
        ):
            raise ValueError("supported_currencies must contain canonical three-letter codes")
        if self.currency not in self.supported_currencies:
            raise ValueError("currency is not enabled by the owning policy")


def _validate_safe_id(value: object, field_name: str, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe canonical identifier")
    return value


def _validate_positive_revision(value: object, field_name: str) -> int:
    return bounded_positive_integer(value, field_name)


def _validate_optional_positive_integer(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    return bounded_positive_integer(value, field_name)


def _validate_lifecycle(state: object, revision: object) -> None:
    if type(state) is not str or state not in LIFECYCLE_STATES:
        raise ValueError("unsupported lifecycle state")
    _validate_positive_revision(revision, "lifecycle_revision")


def _validate_display(value: object, field_name: str, maximum_length: int) -> str | None:
    if value is None:
        return None
    if type(value) is not str or not value or value.strip() != value:
        raise ValueError(f"{field_name} must be non-empty canonical text")
    if len(value) > maximum_length:
        raise ValueError(f"{field_name} exceeds its maximum length")
    if any(
        unicodedata.category(character).startswith("C")
        or unicodedata.category(character) in {"Zl", "Zp"}
        for character in value
    ):
        raise ValueError(f"{field_name} contains control characters")
    if "<" in value or ">" in value or _URL_LIKE.search(value) is not None:
        raise ValueError(f"{field_name} must be plain display text without URLs")
    return value


def _fingerprint(snapshot: dict[str, object]) -> str:
    canonical = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _validate_transition(
    current_state: str,
    current_revision: int,
    target_state: object,
    target_revision: object,
) -> tuple[str, int]:
    _validate_lifecycle(target_state, target_revision)
    if _NEXT_LIFECYCLE_STATE.get(current_state) != target_state:
        raise ValueError("lifecycle transitions must advance exactly one state")
    if target_revision <= current_revision:
        raise ValueError("lifecycle_revision must advance monotonically")
    return target_state, target_revision


@dataclass(frozen=True, slots=True)
class OfferRecurrence:
    interval: str
    interval_count: int = 1
    billing_scheme: str = "per_unit"
    usage_type: str = "licensed"

    def __post_init__(self) -> None:
        if type(self.interval) is not str or self.interval not in _RECURRENCE_INTERVALS:
            raise ValueError("unsupported recurrence interval")
        if type(self.interval_count) is not int or self.interval_count != 1:
            raise ValueError("recurring offers require interval_count=1")
        if self.billing_scheme != "per_unit" or type(self.billing_scheme) is not str:
            raise ValueError("recurring offers require per_unit billing")
        if self.usage_type != "licensed" or type(self.usage_type) is not str:
            raise ValueError("recurring offers require licensed usage")


@dataclass(frozen=True, slots=True)
class OfferVersion:
    version_id: str
    catalog_item_id: str
    variant_id: str | None
    revision: int
    sellable_type: str
    unit_price: Money
    tax_behavior: str
    recurrence: OfferRecurrence | None = None
    lifecycle_state: str = "draft"
    lifecycle_revision: int = 1
    presentation_revision: int = 1
    display_name: str | None = None
    display_description: str | None = None

    def __post_init__(self) -> None:
        _validate_safe_id(self.version_id, "version_id")
        _validate_safe_id(self.catalog_item_id, "catalog_item_id")
        _validate_safe_id(self.variant_id, "variant_id", optional=True)
        _validate_positive_revision(self.revision, "revision")
        validate_sellable_type(self.sellable_type)
        if type(self.unit_price) is not Money:
            raise ValueError("unit_price must be immutable Money")
        if type(self.tax_behavior) is not str or self.tax_behavior not in TAX_BEHAVIORS:
            raise ValueError("unsupported tax behavior")
        if self.recurrence is not None and type(self.recurrence) is not OfferRecurrence:
            raise ValueError("recurrence must be immutable OfferRecurrence")
        if self.sellable_type == "subscription" and self.recurrence is None:
            raise ValueError("subscription offers must be recurring")
        if self.recurrence is not None:
            validate_recurring_sellable_type(self.sellable_type)
        _validate_lifecycle(self.lifecycle_state, self.lifecycle_revision)
        _validate_positive_revision(self.presentation_revision, "presentation_revision")
        _validate_display(self.display_name, "display_name", 160)
        _validate_display(self.display_description, "display_description", 1_000)

    @property
    def sale_type(self) -> str:
        return "recurring" if self.recurrence is not None else "one_time"

    def provider_snapshot(self) -> dict[str, object]:
        recurrence = None
        if self.recurrence is not None:
            recurrence = {
                "interval": self.recurrence.interval,
                "intervalCount": self.recurrence.interval_count,
                "usageType": self.recurrence.usage_type,
            }
        return {
            "schemaVersion": 1,
            "amountMinor": self.unit_price.amount_minor,
            "billingScheme": "per_unit",
            "currency": self.unit_price.currency,
            "recurrence": recurrence,
            "saleType": self.sale_type,
            "taxBehavior": self.tax_behavior,
        }

    @property
    def provider_fingerprint(self) -> str:
        return _fingerprint(self.provider_snapshot())

    def with_lifecycle(self, state: str, revision: int) -> "OfferVersion":
        target_state, target_revision = _validate_transition(
            self.lifecycle_state,
            self.lifecycle_revision,
            state,
            revision,
        )
        return replace(
            self,
            lifecycle_state=target_state,
            lifecycle_revision=target_revision,
        )

    def with_presentation(
        self,
        revision: int,
        *,
        display_name: str | None,
        display_description: str | None,
    ) -> "OfferVersion":
        _validate_positive_revision(revision, "presentation_revision")
        if revision <= self.presentation_revision:
            raise ValueError("presentation_revision must advance monotonically")
        return replace(
            self,
            presentation_revision=revision,
            display_name=display_name,
            display_description=display_description,
        )


@dataclass(frozen=True, slots=True)
class DiscountVersion:
    version_id: str
    revision: int
    duration: str
    percentage_basis_points: int | None = None
    fixed_amount: Money | None = None
    duration_in_months: int | None = None
    eligible_offer_version_ids: frozenset[str] = frozenset()
    redemption_limit: int | None = None
    redeem_by_epoch: int | None = None
    customer_facing_code: str | None = None
    lifecycle_state: str = "draft"
    lifecycle_revision: int = 1
    presentation_revision: int = 1
    display_name: str | None = None
    display_description: str | None = None

    def __post_init__(self) -> None:
        _validate_safe_id(self.version_id, "version_id")
        _validate_positive_revision(self.revision, "revision")
        has_percentage = self.percentage_basis_points is not None
        has_fixed_amount = self.fixed_amount is not None
        if has_percentage == has_fixed_amount:
            raise ValueError("discounts require exactly one value type")
        if has_percentage and (
            type(self.percentage_basis_points) is not int
            or not 1 <= self.percentage_basis_points <= 10_000
        ):
            raise ValueError("percentage_basis_points must be between 1 and 10000")
        if has_fixed_amount and (
            type(self.fixed_amount) is not Money
            or self.fixed_amount.amount_minor <= 0
        ):
            raise ValueError("fixed_amount must be positive immutable Money")
        if type(self.duration) is not str or self.duration not in _DISCOUNT_DURATIONS:
            raise ValueError("unsupported discount duration")
        if self.duration == "repeating":
            _validate_optional_positive_integer(
                self.duration_in_months,
                "duration_in_months",
            )
            if self.duration_in_months is None:
                raise ValueError("repeating discounts require duration_in_months")
            if self.duration_in_months > 36:
                raise ValueError("duration_in_months exceeds the provider limit")
        elif self.duration_in_months is not None:
            raise ValueError("duration_in_months is only valid for repeating discounts")
        if type(self.eligible_offer_version_ids) is not frozenset:
            raise ValueError("eligible_offer_version_ids must be a frozenset")
        if len(self.eligible_offer_version_ids) > 200 or any(
            _SAFE_ID.fullmatch(value) is None
            for value in self.eligible_offer_version_ids
            if type(value) is str
        ):
            raise ValueError("eligible_offer_version_ids contains an unsafe identifier")
        if any(type(value) is not str for value in self.eligible_offer_version_ids):
            raise ValueError("eligible_offer_version_ids contains an unsafe identifier")
        _validate_optional_positive_integer(self.redemption_limit, "redemption_limit")
        if self.redemption_limit is not None and self.redemption_limit > 1_000_000:
            raise ValueError("redemption_limit exceeds the provider limit")
        _validate_optional_positive_integer(self.redeem_by_epoch, "redeem_by_epoch")
        if self.customer_facing_code is not None and (
            type(self.customer_facing_code) is not str
            or _SAFE_CUSTOMER_CODE.fullmatch(self.customer_facing_code) is None
        ):
            raise ValueError("customer_facing_code must be a safe ASCII identifier")
        _validate_lifecycle(self.lifecycle_state, self.lifecycle_revision)
        _validate_positive_revision(self.presentation_revision, "presentation_revision")
        _validate_display(self.display_name, "display_name", 160)
        _validate_display(self.display_description, "display_description", 1_000)

    @property
    def discount_type(self) -> str:
        return "percentage" if self.percentage_basis_points is not None else "fixed_amount"

    def provider_snapshot(self) -> dict[str, object]:
        if self.percentage_basis_points is not None:
            value: dict[str, object] = {
                "basisPoints": self.percentage_basis_points,
                "type": "percentage",
            }
        else:
            value = {
                "amountMinor": self.fixed_amount.amount_minor,
                "currency": self.fixed_amount.currency,
                "type": "fixed_amount",
            }
        return {
            "schemaVersion": 1,
            "customerFacingCode": self.customer_facing_code,
            "duration": self.duration,
            "durationInMonths": self.duration_in_months,
            "eligibleOfferVersionIds": sorted(self.eligible_offer_version_ids),
            "redeemByEpoch": self.redeem_by_epoch,
            "redemptionLimit": self.redemption_limit,
            "value": value,
        }

    @property
    def provider_fingerprint(self) -> str:
        return _fingerprint(self.provider_snapshot())

    def with_lifecycle(self, state: str, revision: int) -> "DiscountVersion":
        target_state, target_revision = _validate_transition(
            self.lifecycle_state,
            self.lifecycle_revision,
            state,
            revision,
        )
        return replace(
            self,
            lifecycle_state=target_state,
            lifecycle_revision=target_revision,
        )

    def with_presentation(
        self,
        revision: int,
        *,
        display_name: str | None,
        display_description: str | None,
    ) -> "DiscountVersion":
        _validate_positive_revision(revision, "presentation_revision")
        if revision <= self.presentation_revision:
            raise ValueError("presentation_revision must advance monotonically")
        return replace(
            self,
            presentation_revision=revision,
            display_name=display_name,
            display_description=display_description,
        )
