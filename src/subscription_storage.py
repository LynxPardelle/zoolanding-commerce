"""Provider-neutral subscription projections and server-owned command state."""

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
        IDEMPOTENCY_TTL_SECONDS,
        StorageConflict,
        StorageOutcomeUnknown,
        _DynamoBackend,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.storage import (
        CommerceScope,
        ConditionalWriteFailed,
        IDEMPOTENCY_TTL_SECONDS,
        StorageConflict,
        StorageOutcomeUnknown,
        _DynamoBackend,
    )


SUBSCRIPTION_STATUSES = frozenset({"active", "past_due", "canceled"})
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


class SubscriptionCommandStore:
    """Durable server-owned inputs and access decisions for subscription commands."""

    def __init__(self, backend: Any, operations_table_name: str) -> None:
        self._backend = backend
        self._table_name = _table_name(operations_table_name)

    @classmethod
    def from_environment(cls) -> "SubscriptionCommandStore":
        table_name = os.getenv("COMMERCE_OPERATIONS_TABLE_NAME", "").strip()
        if not table_name:
            raise RuntimeError("subscription command storage is unavailable")
        try:
            import boto3  # type: ignore

            backend = _DynamoBackend(boto3.client("dynamodb"))
        except Exception:
            raise RuntimeError("subscription command storage is unavailable") from None
        return cls(backend, table_name)

    def preview_timestamp(
        self,
        scope: CommerceScope,
        operation: object,
        command_input: object,
        *,
        idempotency_key: object,
        now_epoch: object,
    ) -> int:
        selected_scope = _scope(scope)
        parsed_input = _preview_input(operation, command_input)
        now = _positive_int(now_epoch, "now_epoch")
        digest = _command_digest(operation, idempotency_key)
        request_hash = _hash_json({
            "scope": _scope_fields(selected_scope),
            "operation": operation,
            "input": parsed_input,
        })
        sk = f"SUBSCRIPTION_PREVIEW#{digest}"
        existing = self._backend.get(self._table_name, selected_scope.partition_key, sk)
        if existing is not None:
            return _preview_replay(existing, selected_scope, sk, operation, request_hash)
        receipt = {
            "pk": selected_scope.partition_key,
            "sk": sk,
            "itemType": "SubscriptionPreviewTimestamp",
            **_scope_fields(selected_scope),
            "operation": operation,
            "requestHash": request_hash,
            "previewTimestamp": now,
            "createdAt": now,
            "expiresAt": now + IDEMPOTENCY_TTL_SECONDS,
        }
        replayed = self._put_with_replay(
            selected_scope,
            receipt,
            lambda item: _preview_replay(item, selected_scope, sk, operation, request_hash),
            "subscription preview timestamp",
        )
        return now if replayed is None else replayed

    def apply_access_transition(
        self,
        scope: CommerceScope,
        command_input: object,
        *,
        command_id: object,
        idempotency_key: object,
        now_epoch: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        parsed_input = _access_command(command_input)
        selected_command_id = _safe_id(command_id, "command_id")
        now = _positive_int(now_epoch, "now_epoch")
        action = parsed_input["action"]
        digest = _command_digest(action, idempotency_key)
        request_hash = _hash_json({
            "scope": _scope_fields(selected_scope),
            "operation": action,
            "input": parsed_input,
        })
        subscription_id = parsed_input["subscriptionId"]
        receipt_sk = f"SUBSCRIPTION_ACCESS_IDEMPOTENCY#{digest}"
        existing_receipt = self._backend.get(
            self._table_name,
            selected_scope.partition_key,
            receipt_sk,
        )
        if existing_receipt is not None:
            return _access_replay(
                existing_receipt,
                selected_scope,
                receipt_sk,
                selected_command_id,
                action,
                request_hash,
                subscription_id,
            )

        access_sk = f"SUBSCRIPTION_ACCESS#{subscription_id}"
        raw_access = self._backend.get(
            self._table_name,
            selected_scope.partition_key,
            access_sk,
        )
        current = (
            _stored_access(raw_access, selected_scope, subscription_id)
            if raw_access is not None
            else None
        )
        updated = _updated_access_state(
            selected_scope,
            subscription_id,
            action,
            parsed_input["pausePolicy"],
            current,
            now,
        )
        changed = updated is not None
        selected = updated or current
        result = {
            "subscriptionId": subscription_id,
            "state": selected["state"] if selected is not None else "active",
            "revision": selected["revision"] if selected is not None else 0,
            "changed": changed,
        }
        receipt = {
            "pk": selected_scope.partition_key,
            "sk": receipt_sk,
            "itemType": "SubscriptionAccessIdempotency",
            **_scope_fields(selected_scope),
            "commandId": selected_command_id,
            "action": action,
            "requestHash": request_hash,
            "result": copy.deepcopy(result),
            "createdAt": now,
            "expiresAt": now + IDEMPOTENCY_TTL_SECONDS,
        }
        operations = []
        if updated is not None:
            condition: object = "absent" if current is None else {
                "state": current["state"],
                "billingPauseSuspended": current["billingPauseSuspended"],
                "revision": current["revision"],
            }
            operations.append({
                "kind": "put",
                "table_name": self._table_name,
                "item": updated,
                "condition": condition,
            })
        operations.append({
            "kind": "put",
            "table_name": self._table_name,
            "item": receipt,
            "condition": "absent",
        })
        try:
            self._backend.transact(operations, _client_token(selected_scope, operations))
        except Exception as exc:
            try:
                replay = self._backend.get(
                    self._table_name,
                    selected_scope.partition_key,
                    receipt_sk,
                )
            except Exception:
                raise StorageOutcomeUnknown(
                    "subscription access outcome is unknown"
                ) from None
            if replay is not None:
                return _access_replay(
                    replay,
                    selected_scope,
                    receipt_sk,
                    selected_command_id,
                    action,
                    request_hash,
                    subscription_id,
                )
            if isinstance(exc, ConditionalWriteFailed):
                raise StorageConflict("subscription access state changed") from None
            raise StorageOutcomeUnknown("subscription access outcome is unknown") from None
        return copy.deepcopy(result)

    def _put_with_replay(
        self,
        scope: CommerceScope,
        item: Mapping[str, Any],
        replay_validator: Any,
        label: str,
    ) -> Any:
        operations = [{
            "kind": "put",
            "table_name": self._table_name,
            "item": copy.deepcopy(dict(item)),
            "condition": "absent",
        }]
        try:
            self._backend.transact(operations, _client_token(scope, operations))
        except Exception as exc:
            try:
                replay = self._backend.get(
                    self._table_name,
                    scope.partition_key,
                    item["sk"],
                )
            except Exception:
                raise StorageOutcomeUnknown(f"{label} outcome is unknown") from None
            if replay is not None:
                return replay_validator(replay)
            if isinstance(exc, ConditionalWriteFailed):
                raise StorageConflict(f"{label} changed") from None
            raise StorageOutcomeUnknown(f"{label} outcome is unknown") from None
        return None


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


def _preview_input(operation: object, value: object) -> dict[str, Any]:
    required = {
        "subscriptionId",
        "targetOfferVersionId",
        "expectedRevision",
        "planChangePolicy",
    }
    if operation != "changePlan" or not isinstance(value, Mapping) or set(value) != required:
        raise ValueError("subscription preview input is invalid")
    parsed = copy.deepcopy(dict(value))
    _safe_id(parsed.get("subscriptionId"), "subscription_id")
    _safe_id(parsed.get("targetOfferVersionId"), "target_offer_version_id")
    _positive_int(parsed.get("expectedRevision"), "expected_revision")
    if parsed.get("planChangePolicy") != {"mode": "immediate-prorated"}:
        raise ValueError("subscription preview policy is invalid")
    return parsed


def _access_command(value: object) -> dict[str, Any]:
    required = {"subscriptionId", "expectedRevision", "action", "pausePolicy"}
    if not isinstance(value, Mapping) or set(value) != required:
        raise ValueError("subscription access command is invalid")
    parsed = copy.deepcopy(dict(value))
    _safe_id(parsed.get("subscriptionId"), "subscription_id")
    _positive_int(parsed.get("expectedRevision"), "expected_revision")
    if parsed.get("action") not in {"pause", "resume"}:
        raise ValueError("subscription access action is invalid")
    parsed["pausePolicy"] = _pause_policy(parsed.get("pausePolicy"))
    return parsed


def _pause_policy(value: object) -> dict[str, Any]:
    required = {
        "enabled",
        "newInvoiceBehavior",
        "existingInvoiceBehavior",
        "accessBehavior",
        "resume",
        "onResume",
    }
    if not isinstance(value, Mapping) or set(value) != required or value.get("enabled") is not True:
        raise ValueError("subscription pause policy is invalid")
    if value.get("newInvoiceBehavior") not in {"void", "keep-as-draft", "mark-uncollectible"}:
        raise ValueError("subscription pause policy is invalid")
    if value.get("existingInvoiceBehavior") != "unchanged":
        raise ValueError("subscription pause policy is invalid")
    if value.get("accessBehavior") not in {"retain", "suspend"}:
        raise ValueError("subscription pause policy is invalid")
    if value.get("resume") != {"mode": "manual"} or value.get("onResume") != {
        "collection": "restore",
        "access": "restore-if-suspended",
    }:
        raise ValueError("subscription pause policy is invalid")
    return copy.deepcopy(dict(value))


def _command_digest(operation: object, idempotency_key: object) -> str:
    if operation not in {"changePlan", "pause", "resume"}:
        raise ValueError("subscription command operation is invalid")
    if (
        type(idempotency_key) is not str
        or not 1 <= len(idempotency_key) <= 256
        or any(ord(character) < 32 for character in idempotency_key)
    ):
        raise ValueError("subscription idempotency key is invalid")
    return hashlib.sha256(f"{operation}\0{idempotency_key}".encode("utf-8")).hexdigest()


def _preview_replay(
    item: object,
    scope: CommerceScope,
    sk: str,
    operation: object,
    request_hash: str,
) -> int:
    required = {
        "pk", "sk", "itemType", "environment", "tenantId", "draftId", "domain",
        "operation", "requestHash", "previewTimestamp", "createdAt", "expiresAt",
    }
    if (
        not isinstance(item, Mapping)
        or set(item) != required
        or item.get("pk") != scope.partition_key
        or item.get("sk") != sk
        or item.get("itemType") != "SubscriptionPreviewTimestamp"
        or not _scope_item_matches(item, scope)
        or item.get("operation") != operation
        or item.get("requestHash") != request_hash
    ):
        raise StorageConflict("subscription idempotency key was already used")
    timestamp = _stored_positive_int(item.get("previewTimestamp"))
    if (
        _stored_positive_int(item.get("createdAt")) != timestamp
        or _stored_positive_int(item.get("expiresAt")) != timestamp + IDEMPOTENCY_TTL_SECONDS
    ):
        raise StorageConflict("subscription preview timestamp is invalid")
    return timestamp


def _stored_access(item: object, scope: CommerceScope, subscription_id: str) -> dict[str, Any]:
    required = {
        "pk", "sk", "itemType", "environment", "tenantId", "draftId", "domain",
        "subscriptionId", "state", "billingPauseSuspended", "revision", "updatedAt",
    }
    if (
        not isinstance(item, Mapping)
        or set(item) != required
        or item.get("pk") != scope.partition_key
        or item.get("sk") != f"SUBSCRIPTION_ACCESS#{subscription_id}"
        or item.get("itemType") != "SubscriptionAccessState"
        or not _scope_item_matches(item, scope)
        or item.get("subscriptionId") != subscription_id
        or item.get("state") not in {"active", "suspended"}
        or type(item.get("billingPauseSuspended")) is not bool
        or (item.get("billingPauseSuspended") is True and item.get("state") != "suspended")
    ):
        raise StorageConflict("stored subscription access state is invalid")
    _stored_positive_int(item.get("revision"))
    _stored_positive_int(item.get("updatedAt"))
    return copy.deepcopy(dict(item))


def _updated_access_state(
    scope: CommerceScope,
    subscription_id: str,
    action: str,
    policy: Mapping[str, Any],
    current: Mapping[str, Any] | None,
    now_epoch: int,
) -> dict[str, Any] | None:
    suspend = (
        action == "pause"
        and policy["accessBehavior"] == "suspend"
        and (current is None or current["state"] == "active")
    )
    restore = (
        action == "resume"
        and current is not None
        and current["state"] == "suspended"
        and current["billingPauseSuspended"] is True
    )
    if not suspend and not restore:
        return None
    return {
        "pk": scope.partition_key,
        "sk": f"SUBSCRIPTION_ACCESS#{subscription_id}",
        "itemType": "SubscriptionAccessState",
        **_scope_fields(scope),
        "subscriptionId": subscription_id,
        "state": "suspended" if suspend else "active",
        "billingPauseSuspended": suspend,
        "revision": 1 if current is None else current["revision"] + 1,
        "updatedAt": now_epoch,
    }


def _access_replay(
    item: object,
    scope: CommerceScope,
    sk: str,
    command_id: str,
    action: str,
    request_hash: str,
    subscription_id: str,
) -> dict[str, Any]:
    required = {
        "pk", "sk", "itemType", "environment", "tenantId", "draftId", "domain",
        "commandId", "action", "requestHash", "result", "createdAt", "expiresAt",
    }
    if (
        not isinstance(item, Mapping)
        or set(item) != required
        or item.get("pk") != scope.partition_key
        or item.get("sk") != sk
        or item.get("itemType") != "SubscriptionAccessIdempotency"
        or not _scope_item_matches(item, scope)
        or item.get("commandId") != command_id
        or item.get("action") != action
        or item.get("requestHash") != request_hash
        or not isinstance(item.get("result"), Mapping)
    ):
        raise StorageConflict("subscription access idempotency key was already used")
    created_at = _stored_positive_int(item.get("createdAt"))
    if _stored_positive_int(item.get("expiresAt")) != created_at + IDEMPOTENCY_TTL_SECONDS:
        raise StorageConflict("subscription access receipt is invalid")
    result = dict(item["result"])
    if (
        set(result) != {"subscriptionId", "state", "revision", "changed"}
        or result.get("subscriptionId") != subscription_id
        or result.get("state") not in {"active", "suspended"}
        or type(result.get("revision")) is not int
        or not 0 <= result["revision"] <= 9_999_999_999
        or type(result.get("changed")) is not bool
    ):
        raise StorageConflict("subscription access receipt is invalid")
    _safe_id(result.get("subscriptionId"), "subscription_id")
    return copy.deepcopy(result)


def _scope_fields(scope: CommerceScope) -> dict[str, str]:
    return {
        "environment": scope.environment,
        "tenantId": scope.tenant_id,
        "draftId": scope.draft_id,
        "domain": scope.domain,
    }


def _scope_item_matches(item: Mapping[str, Any], scope: CommerceScope) -> bool:
    return all(item.get(key) == value for key, value in _scope_fields(scope).items())


def _hash_json(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stored_positive_int(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 9_999_999_999:
        raise StorageConflict("stored subscription integer is invalid")
    return value


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
