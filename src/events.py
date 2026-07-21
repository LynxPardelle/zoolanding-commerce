"""Closed provider-neutral integration events for Commerce."""

from __future__ import annotations

from dataclasses import dataclass
import re
from types import MappingProxyType
from typing import Any, Mapping

try:
    from storage import CommerceScope, CommerceStore
except ModuleNotFoundError:
    from src.storage import CommerceScope, CommerceStore


_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)
_MIGRATION_ITEM_ID = re.compile(r"migration-item-[a-f0-9]{40}", re.ASCII)
_CURRENCY = re.compile(r"[A-Z]{3}", re.ASCII)
_ROOT_KEYS = frozenset(
    {
        "schemaVersion",
        "eventId",
        "eventType",
        "occurredAt",
        "environment",
        "tenantId",
        "draftId",
        "domain",
        "data",
    }
)
_PAYMENT_KEYS = frozenset({"reservationId", "orderId", "paymentAttemptId"})
_REFUND_KEYS = frozenset({"orderId", "refundId", "amountMinor", "currency"})
_SUBSCRIPTION_KEYS = frozenset(
    {"subscriptionId", "offerVersionId", "status", "currentPeriodEnd", "sourceRevision"}
)
_EVENT_TYPES = frozenset(
    {
        "commerce.payment.succeeded.v1",
        "commerce.payment.terminal_unpaid.v1",
        "commerce.refund.confirmed.v1",
        "commerce.subscription.updated.v1",
        "migration.preview_ready.v1",
        "migration.progressed.v1",
        "migration.item_needs_review.v1",
        "migration.completed.v1",
    }
)
_SUBSCRIPTION_STATUSES = frozenset({"active", "past_due", "canceled"})
_MIGRATION_EVENT_TYPES = frozenset({
    "migration.preview_ready.v1",
    "migration.progressed.v1",
    "migration.item_needs_review.v1",
    "migration.completed.v1",
})
_MIGRATION_JOB_STATES = frozenset({
    "draft", "previewing", "awaiting_approval", "scheduled", "running", "paused",
    "cancel_requested", "canceling", "completed", "completed_with_errors", "canceled",
})
_MIGRATION_REASON_CODES = frozenset({
    "ambiguous-price",
    "near-term-schedule",
    "nonpositive-proration",
    "payment-failed",
    "pending-invoice-items",
    "pending-update",
    "phase-limit",
    "provider-unknown",
    "retry-exhausted",
    "scope-mismatch",
    "snapshot-too-large",
    "source-drift",
    "tax-approval",
    "unmapped-price",
    "unpaid-invoice",
    "unsupported-collection-mode",
    "unsupported-payment-method",
    "unsupported-schedule",
})
MAX_EVENT_FUTURE_SKEW_SECONDS = 300


class IntegrationEventValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class IntegrationEvent:
    event_id: str
    event_type: str
    occurred_at: int
    scope: CommerceScope
    data: Mapping[str, Any]


def parse_integration_event(value: object) -> IntegrationEvent:
    try:
        if not isinstance(value, Mapping) or set(value) != _ROOT_KEYS:
            raise ValueError
        if type(value["schemaVersion"]) is not int or value["schemaVersion"] != 1:
            raise ValueError
        event_id = _safe_id(value["eventId"])
        event_type = value["eventType"]
        if type(event_type) is not str or event_type not in _EVENT_TYPES:
            raise ValueError
        occurred_at = _epoch(value["occurredAt"])
        scope = CommerceScope(
            value["environment"],
            value["tenantId"],
            value["draftId"],
            value["domain"],
        )
        data = value["data"]
        if not isinstance(data, Mapping):
            raise ValueError
        normalized = _data(event_type, data)
        return IntegrationEvent(
            event_id,
            event_type,
            occurred_at,
            scope,
            MappingProxyType(normalized),
        )
    except (KeyError, TypeError, ValueError):
        raise IntegrationEventValidationError("integration event is invalid") from None


def _data(event_type: str, value: Mapping[str, Any]) -> dict[str, Any]:
    if event_type in _MIGRATION_EVENT_TYPES:
        return _migration_data(event_type, value)
    if event_type.startswith("commerce.payment."):
        if set(value) != _PAYMENT_KEYS:
            raise ValueError
        return {key: _safe_id(value[key]) for key in sorted(_PAYMENT_KEYS)}
    if event_type == "commerce.refund.confirmed.v1":
        if set(value) != _REFUND_KEYS:
            raise ValueError
        amount_minor = value["amountMinor"]
        currency = value["currency"]
        if type(amount_minor) is not int or amount_minor <= 0:
            raise ValueError
        if type(currency) is not str or _CURRENCY.fullmatch(currency) is None:
            raise ValueError
        return {
            "orderId": _safe_id(value["orderId"]),
            "refundId": _safe_id(value["refundId"]),
            "amountMinor": amount_minor,
            "currency": currency,
        }
    if set(value) != _SUBSCRIPTION_KEYS:
        raise ValueError
    status = value["status"]
    if type(status) is not str or status not in _SUBSCRIPTION_STATUSES:
        raise ValueError
    return {
        "subscriptionId": _safe_id(value["subscriptionId"]),
        "offerVersionId": _safe_id(value["offerVersionId"]),
        "status": status,
        "currentPeriodEnd": _epoch(value["currentPeriodEnd"]),
        "sourceRevision": _positive_int(value["sourceRevision"]),
    }


class IntegrationEventProcessor:
    def __init__(
        self,
        store: CommerceStore,
        subscription_projector: Any = None,
        migration_store: Any = None,
    ) -> None:
        if type(store) is not CommerceStore:
            raise ValueError("store must be a CommerceStore")
        self.store = store
        self.subscription_projector = subscription_projector
        self.migration_store = migration_store

    def process(self, event: IntegrationEvent, *, now_epoch: int) -> dict[str, Any]:
        if type(event) is not IntegrationEvent:
            raise IntegrationEventValidationError("integration event is invalid")
        now_epoch = _epoch(now_epoch)
        if event.occurred_at > now_epoch + MAX_EVENT_FUTURE_SKEW_SECONDS:
            raise IntegrationEventValidationError("integration event timestamp is invalid")
        data = event.data
        if event.event_type in _MIGRATION_EVENT_TYPES:
            if self.migration_store is None or not hasattr(
                self.migration_store, "apply_verified_event"
            ):
                raise RuntimeError("migration storage is unavailable")
            return self.migration_store.apply_verified_event(
                event.scope,
                event_id=event.event_id,
                event_type=event.event_type,
                occurred_at=event.occurred_at,
                data=dict(data),
                now_epoch=now_epoch,
            )
        if event.event_type in {
            "commerce.payment.succeeded.v1",
            "commerce.payment.terminal_unpaid.v1",
        }:
            return self.store.apply_payment_event(
                event.scope,
                event_id=event.event_id,
                event_type=event.event_type,
                reservation_id=data["reservationId"],
                order_id=data["orderId"],
                payment_attempt_id=data["paymentAttemptId"],
                occurred_at=event.occurred_at,
                now_epoch=now_epoch,
            )
        if event.event_type == "commerce.refund.confirmed.v1":
            return self.store.record_refund_event(
                event.scope,
                event_id=event.event_id,
                order_id=data["orderId"],
                refund_id=data["refundId"],
                amount_minor=data["amountMinor"],
                currency=data["currency"],
                occurred_at=event.occurred_at,
                now_epoch=now_epoch,
            )
        if self.subscription_projector is None or not hasattr(
            self.subscription_projector, "apply_verified_event"
        ):
            raise RuntimeError("subscription projection is unavailable")
        return self.subscription_projector.apply_verified_event(
            event.scope,
            {
                "eventId": event.event_id,
                **dict(data),
                "occurredAt": event.occurred_at,
            },
            now_epoch=now_epoch,
        )


def _safe_id(value: object) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError
    return value


def _epoch(value: object) -> int:
    if type(value) is not int or not 0 <= value <= 9_999_999_999:
        raise ValueError
    return value


def _positive_int(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 9_999_999_999:
        raise ValueError
    return value


def _migration_data(event_type: str, value: Mapping[str, Any]) -> dict[str, Any]:
    common = {"commercialRequestId", "jobId", "connectionId", "revision", "dedupeKey"}
    extras = {
        "migration.preview_ready.v1": {
            "dryRunRevision", "dryRunHash", "expiresAt", "counts"
        },
        "migration.progressed.v1": {"state", "counts"},
        "migration.item_needs_review.v1": {"itemId", "reasonCode"},
        "migration.completed.v1": {"state", "counts"},
    }[event_type]
    if set(value) != common | extras:
        raise ValueError
    selected = {
        "commercialRequestId": _safe_id(value["commercialRequestId"]),
        "jobId": _safe_id(value["jobId"]),
        "connectionId": _safe_id(value["connectionId"]),
        "revision": _positive_int(value["revision"]),
        "dedupeKey": _safe_id(value["dedupeKey"]),
    }
    if event_type == "migration.preview_ready.v1":
        dry_hash = value["dryRunHash"]
        if type(dry_hash) is not str or re.fullmatch(r"[a-f0-9]{64}", dry_hash, re.ASCII) is None:
            raise ValueError
        selected.update({
            "dryRunRevision": _positive_int(value["dryRunRevision"]),
            "dryRunHash": dry_hash,
            "expiresAt": _positive_int(value["expiresAt"]),
            "counts": _migration_counts(value["counts"]),
        })
    elif event_type in {"migration.progressed.v1", "migration.completed.v1"}:
        state = value["state"]
        if type(state) is not str or state not in _MIGRATION_JOB_STATES:
            raise ValueError
        if event_type == "migration.completed.v1" and state not in {
            "completed", "completed_with_errors", "canceled"
        }:
            raise ValueError
        if event_type == "migration.progressed.v1" and state in {
            "completed", "completed_with_errors", "canceled"
        }:
            raise ValueError
        selected.update({"state": state, "counts": _migration_counts(value["counts"])})
    else:
        item_id = value["itemId"]
        reason_code = value["reasonCode"]
        if (
            type(item_id) is not str
            or _MIGRATION_ITEM_ID.fullmatch(item_id) is None
            or reason_code not in _MIGRATION_REASON_CODES
        ):
            raise ValueError
        selected.update({
            "itemId": item_id,
            "reasonCode": reason_code,
        })
    return selected


def _migration_counts(value: object) -> dict[str, int]:
    keys = {"total", "pending", "applied", "needsReview", "failed"}
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError
    selected = {}
    for key in keys:
        item = value[key]
        if type(item) is not int or not 0 <= item <= 9_999_999_999:
            raise ValueError
        selected[key] = item
    if selected["total"] != sum(selected[key] for key in keys - {"total"}):
        raise ValueError
    return selected
