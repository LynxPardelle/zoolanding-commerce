"""Provider-neutral subscription projection from verified integration events."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
from typing import Any, Mapping

try:  # Lambda CodeUri is src/.
    from storage import (
        CommerceScope,
        ConditionalWriteFailed,
        StorageConflict,
        _DynamoBackend,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.storage import (
        CommerceScope,
        ConditionalWriteFailed,
        StorageConflict,
        _DynamoBackend,
    )


SUBSCRIPTION_STATUSES = frozenset({"active", "past_due", "paused", "canceled"})
EVENT_RECEIPT_TTL_SECONDS = 90 * 24 * 60 * 60
MAX_EVENT_FUTURE_SKEW_SECONDS = 300
EVENT_FIELDS = frozenset({
    "eventId",
    "subscriptionId",
    "offerVersionId",
    "status",
    "currentPeriodEnd",
    "sourceRevision",
    "occurredAt",
})
_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)


class SubscriptionProjectionStore:
    def __init__(self, backend: Any, operations_table_name: str) -> None:
        self._backend = backend
        self._table_name = _table_name(operations_table_name)

    @classmethod
    def from_environment(cls) -> "SubscriptionProjectionStore":
        table_name = os.getenv("COMMERCE_OPERATIONS_TABLE_NAME", "").strip()
        if not table_name:
            raise RuntimeError("subscription storage is unavailable")
        try:
            import boto3  # type: ignore

            backend = _DynamoBackend(boto3.client("dynamodb"))
        except Exception:
            raise RuntimeError("subscription storage is unavailable") from None
        return cls(backend, table_name)

    def apply_verified_event(
        self,
        scope: CommerceScope,
        event: object,
        *,
        now_epoch: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        parsed = _event(event)
        processed_at = _epoch(now_epoch, "now_epoch")
        if parsed["occurredAt"] > processed_at + MAX_EVENT_FUTURE_SKEW_SECONDS:
            raise ValueError("subscription event timestamp is invalid")
        sk = f"SUBSCRIPTION#{parsed['subscriptionId']}"
        event_hash = _event_hash(parsed)
        receipt_sk = f"EVENT_INBOX#{parsed['eventId']}"
        receipt = self._backend.get(self._table_name, selected_scope.partition_key, receipt_sk)
        if receipt is not None:
            return _receipt_result(receipt, selected_scope, parsed["eventId"], event_hash)
        current_item = self._backend.get(self._table_name, selected_scope.partition_key, sk)
        current = _stored_projection(current_item, selected_scope, parsed["subscriptionId"]) if current_item else None
        if current is not None:
            if parsed["sourceRevision"] < current["sourceRevision"]:
                result = {**_projection(current), "stale": True}
                receipt_item = _receipt_item(
                    selected_scope, parsed, event_hash, result, processed_at
                )
                operation = {
                    "kind": "put",
                    "table_name": self._table_name,
                    "item": receipt_item,
                    "condition": "absent",
                }
                try:
                    self._backend.transact([operation], _client_token(selected_scope, operation))
                except ConditionalWriteFailed:
                    latest = self._backend.get(self._table_name, selected_scope.partition_key, receipt_sk)
                    if latest is not None:
                        return _receipt_result(latest, selected_scope, parsed["eventId"], event_hash)
                    raise StorageConflict("subscription event receipt changed") from None
                return result
            if parsed["sourceRevision"] == current["sourceRevision"]:
                raise StorageConflict("subscription event order is ambiguous")
            revision = current["revision"] + 1
            condition: object = {
                "revision": current["revision"],
                "lastEventId": current["lastEventId"],
                "sourceRevision": current["sourceRevision"],
            }
        else:
            revision = 1
            condition = "absent"
        item = {
            "pk": selected_scope.partition_key,
            "sk": sk,
            "itemType": "SubscriptionProjection",
            "environment": selected_scope.environment,
            "tenantId": selected_scope.tenant_id,
            "draftId": selected_scope.draft_id,
            "domain": selected_scope.domain,
            "subscriptionId": parsed["subscriptionId"],
            "offerVersionId": parsed["offerVersionId"],
            "status": parsed["status"],
            "currentPeriodEnd": parsed["currentPeriodEnd"],
            "sourceRevision": parsed["sourceRevision"],
            "occurredAt": parsed["occurredAt"],
            "lastEventId": parsed["eventId"],
            "lastEventHash": event_hash,
            "revision": revision,
        }
        result = _projection(item)
        operation = {
            "kind": "put",
            "table_name": self._table_name,
            "item": item,
            "condition": condition,
        }
        receipt_operation = {
            "kind": "put",
            "table_name": self._table_name,
            "item": _receipt_item(selected_scope, parsed, event_hash, result, processed_at),
            "condition": "absent",
        }
        try:
            operations = [operation, receipt_operation]
            self._backend.transact(operations, _client_token(selected_scope, operations))
        except ConditionalWriteFailed:
            latest = self._backend.get(self._table_name, selected_scope.partition_key, receipt_sk)
            if latest is not None:
                return _receipt_result(latest, selected_scope, parsed["eventId"], event_hash)
            raise StorageConflict("subscription projection state changed") from None
        return result

    def get_projection(
        self,
        scope: CommerceScope,
        subscription_id: object,
    ) -> dict[str, Any] | None:
        selected_scope = _scope(scope)
        selected_id = _safe_id(subscription_id, "subscription_id")
        item = self._backend.get(
            self._table_name,
            selected_scope.partition_key,
            f"SUBSCRIPTION#{selected_id}",
        )
        return _projection(_stored_projection(item, selected_scope, selected_id)) if item else None


def _event(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != EVENT_FIELDS:
        raise ValueError("subscription event is invalid")
    parsed = {
        "eventId": _safe_id(value.get("eventId"), "event_id"),
        "subscriptionId": _safe_id(value.get("subscriptionId"), "subscription_id"),
        "offerVersionId": _safe_id(value.get("offerVersionId"), "offer_version_id"),
        "status": value.get("status"),
        "currentPeriodEnd": _epoch(value.get("currentPeriodEnd"), "current_period_end"),
        "sourceRevision": _positive_int(value.get("sourceRevision"), "source_revision"),
        "occurredAt": _epoch(value.get("occurredAt"), "occurred_at"),
    }
    if type(parsed["status"]) is not str or parsed["status"] not in SUBSCRIPTION_STATUSES:
        raise ValueError("subscription status is invalid")
    return parsed


def _stored_projection(
    item: object,
    scope: CommerceScope,
    subscription_id: str,
) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise StorageConflict("stored subscription projection is invalid")
    projection = copy.deepcopy(dict(item))
    if (
        projection.get("pk") != scope.partition_key
        or projection.get("sk") != f"SUBSCRIPTION#{subscription_id}"
        or projection.get("itemType") != "SubscriptionProjection"
        or projection.get("environment") != scope.environment
        or projection.get("tenantId") != scope.tenant_id
        or projection.get("draftId") != scope.draft_id
        or projection.get("domain") != scope.domain
        or projection.get("subscriptionId") != subscription_id
        or projection.get("status") not in SUBSCRIPTION_STATUSES
    ):
        raise StorageConflict("stored subscription projection is invalid")
    _safe_id(projection.get("offerVersionId"), "offer_version_id")
    _safe_id(projection.get("lastEventId"), "event_id")
    if type(projection.get("lastEventHash")) is not str or not re.fullmatch(r"[a-f0-9]{64}", projection["lastEventHash"]):
        raise StorageConflict("stored subscription projection is invalid")
    _epoch(projection.get("currentPeriodEnd"), "current_period_end")
    _epoch(projection.get("occurredAt"), "occurred_at")
    _positive_int(projection.get("sourceRevision"), "source_revision")
    _positive_int(projection.get("revision"), "revision")
    return projection


def _projection(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "subscriptionId": item["subscriptionId"],
        "offerVersionId": item["offerVersionId"],
        "status": item["status"],
        "currentPeriodEnd": item["currentPeriodEnd"],
        "revision": item["revision"],
        "stale": False,
    }


def _receipt_item(
    scope: CommerceScope,
    event: Mapping[str, Any],
    event_hash: str,
    result: Mapping[str, Any],
    processed_at: int,
) -> dict[str, Any]:
    return {
        "pk": scope.partition_key,
        "sk": f"EVENT_INBOX#{event['eventId']}",
        "itemType": "IntegrationEventInbox",
        "environment": scope.environment,
        "tenantId": scope.tenant_id,
        "draftId": scope.draft_id,
        "domain": scope.domain,
        "eventId": event["eventId"],
        "eventType": "commerce.subscription.updated.v1",
        "eventHash": event_hash,
        "result": copy.deepcopy(dict(result)),
        "processedAt": processed_at,
        "expiresAt": processed_at + EVENT_RECEIPT_TTL_SECONDS,
    }


def _receipt_result(
    item: object,
    scope: CommerceScope,
    event_id: str,
    event_hash: str,
) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise StorageConflict("subscription event receipt is invalid")
    if (
        item.get("pk") != scope.partition_key
        or item.get("sk") != f"EVENT_INBOX#{event_id}"
        or item.get("itemType") != "IntegrationEventInbox"
        or item.get("environment") != scope.environment
        or item.get("tenantId") != scope.tenant_id
        or item.get("draftId") != scope.draft_id
        or item.get("domain") != scope.domain
        or item.get("eventId") != event_id
        or item.get("eventType") != "commerce.subscription.updated.v1"
        or item.get("eventHash") != event_hash
        or not isinstance(item.get("result"), Mapping)
    ):
        raise StorageConflict("subscription event ID collision")
    result = dict(item["result"])
    if set(result) != {"subscriptionId", "offerVersionId", "status", "currentPeriodEnd", "revision", "stale"}:
        raise StorageConflict("subscription event receipt is invalid")
    return copy.deepcopy(result)


def _scope(value: object) -> CommerceScope:
    if type(value) is not CommerceScope:
        raise ValueError("scope must be an immutable CommerceScope")
    return value


def _safe_id(value: object, field_name: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field_name} is invalid")
    return value


def _epoch(value: object, field_name: str) -> int:
    if type(value) is not int or not 0 <= value <= 9_999_999_999:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _positive_int(value: object, field_name: str) -> int:
    if type(value) is not int or not 1 <= value <= 9_999_999_999:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _table_name(value: object) -> str:
    if type(value) is not str or not value.strip() or any(ord(character) < 33 for character in value):
        raise ValueError("table_name is invalid")
    return value


def _event_hash(event: Mapping[str, Any]) -> str:
    encoded = json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _client_token(scope: CommerceScope, operation: object) -> str:
    encoded = json.dumps(
        {"scope": scope.partition_key, "operation": operation},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:36]
