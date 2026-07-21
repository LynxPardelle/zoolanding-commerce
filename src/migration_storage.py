"""Durable provider-neutral commercial subscription migration requests."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
from typing import Any, Mapping

try:
    from domain.limits import MAX_AMOUNT_MINOR
    from storage import (
        CommerceScope,
        ConditionalWriteFailed,
        StorageConflict,
        StorageOutcomeUnknown,
        _DynamoBackend,
    )
except (ImportError, ModuleNotFoundError):
    from src.domain.limits import MAX_AMOUNT_MINOR
    from src.storage import (
        CommerceScope,
        ConditionalWriteFailed,
        StorageConflict,
        StorageOutcomeUnknown,
        _DynamoBackend,
    )


EVENT_RECEIPT_TTL_SECONDS = 90 * 24 * 60 * 60


_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)
_MIGRATION_ITEM_ID = re.compile(r"migration-item-[a-f0-9]{40}", re.ASCII)
_HASH = re.compile(r"[a-f0-9]{64}", re.ASCII)
_CURRENCY = re.compile(r"[A-Z]{3}", re.ASCII)
_POLICY_MODES = frozenset({"next_renewal", "immediate_prorated"})
_JOB_STATES = frozenset({
    "draft",
    "previewing",
    "awaiting_approval",
    "scheduled",
    "running",
    "paused",
    "cancel_requested",
    "canceling",
    "completed",
    "completed_with_errors",
    "canceled",
})
_TERMINAL_STATES = frozenset({"completed", "completed_with_errors", "canceled"})
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
_CONTROL_STATES = {
    "pause": frozenset({"scheduled", "running"}),
    "resume": frozenset({"paused"}),
    "cancel": frozenset({
        "previewing", "awaiting_approval", "scheduled", "running", "paused",
        "completed", "completed_with_errors",
    }),
}
_TRANSITIONS = {
    "draft": frozenset({"previewing", "canceled"}),
    "previewing": frozenset({"awaiting_approval", "cancel_requested", "canceled"}),
    "awaiting_approval": frozenset({"scheduled", "cancel_requested", "canceled"}),
    "scheduled": frozenset({
        "running", "paused", "cancel_requested", "canceling", "completed",
        "completed_with_errors", "canceled",
    }),
    "running": frozenset({
        "paused", "cancel_requested", "canceling", "completed",
        "completed_with_errors", "canceled",
    }),
    "paused": frozenset({"scheduled", "running", "cancel_requested", "canceling", "canceled"}),
    "cancel_requested": frozenset({"canceling", "canceled"}),
    "canceling": frozenset({"canceled"}),
    "completed": frozenset({"cancel_requested"}),
    "completed_with_errors": frozenset({"cancel_requested"}),
    "canceled": frozenset(),
}
_REQUEST_KEYS = frozenset({
    "pk", "sk", "itemType", "environment", "tenantId", "draftId", "domain",
    "commercialRequestId", "connectionId", "requestHash", "idempotencyDigest",
    "sourceOffer", "targetOffer", "candidateScope", "requestedPolicy",
    "canarySize", "accountConcurrency", "actorHash", "requestId", "createdAt",
    "updatedAt", "state", "revision", "jobId", "dryRunRevision", "dryRunHash",
    "previewExpiresAt", "counts", "approval", "approvalIdempotencyDigest",
    "lastCommand", "lastNeedsReview", "lastEventId", "lastEventHash",
    "lastProgressRevision", "lastProgressHash", "stateRevision", "storageRevision",
})


class MigrationRequestStore:
    def __init__(self, backend: Any, operations_table_name: str) -> None:
        if not hasattr(backend, "get") or not hasattr(backend, "transact"):
            raise ValueError("migration backend is invalid")
        if type(operations_table_name) is not str or not operations_table_name.strip():
            raise ValueError("operations table name is invalid")
        self._backend = backend
        self._table_name = operations_table_name

    @classmethod
    def from_environment(cls) -> "MigrationRequestStore":
        table_name = os.getenv("COMMERCE_OPERATIONS_TABLE_NAME", "").strip()
        if not table_name:
            raise RuntimeError("migration storage is unavailable")
        try:
            import boto3  # type: ignore

            backend = _DynamoBackend(boto3.client("dynamodb"))
        except Exception:
            raise RuntimeError("migration storage is unavailable") from None
        return cls(backend, table_name)

    def prepare_preview(
        self,
        scope: CommerceScope,
        *,
        connection_id: object,
        source_offer: object,
        target_offer: object,
        requested_policy: object,
        candidate_scope: object,
        canary_size: object,
        account_concurrency: object,
        actor_hash: object,
        idempotency_key: object,
        request_id: object,
        now_epoch: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        connection = _safe_id(connection_id, "connection_id")
        source = _offer_binding(source_offer)
        target = _offer_binding(target_offer)
        if not _valid_offer_pair(source, target):
            raise ValueError("migration offer pair is invalid")
        policy = _requested_policy(requested_policy)
        if candidate_scope != {"kind": "all_matching_source_price"}:
            raise ValueError("migration candidate scope is invalid")
        canary = _bounded_int(canary_size, 1, 25, "canary_size")
        concurrency = _bounded_int(account_concurrency, 1, 5, "account_concurrency")
        actor = _actor_hash(actor_hash)
        idempotency_digest = _idempotency_digest(idempotency_key)
        now = _positive_int(now_epoch, "now_epoch")
        selected_request_id = _request_id(request_id)
        request = {
            "scope": _scope_fields(selected_scope),
            "connectionId": connection,
            "sourceOffer": source,
            "targetOffer": target,
            "candidateScope": {"kind": "all_matching_source_price"},
            "requestedPolicy": policy,
            "canarySize": canary,
            "accountConcurrency": concurrency,
            "actorHash": actor,
        }
        request_hash = _hash_json(request)
        commercial_request_id = "migration-" + _hash_json({
            "scope": _scope_fields(selected_scope),
            "idempotencyDigest": idempotency_digest,
        })[:40]
        sk = f"MIGRATION_REQUEST#{commercial_request_id}"
        existing = self._backend.get(self._table_name, selected_scope.partition_key, sk)
        if existing is not None:
            return _preview_replay(
                existing,
                selected_scope,
                commercial_request_id,
                request_hash,
                idempotency_digest,
            )
        item = {
            "pk": selected_scope.partition_key,
            "sk": sk,
            "itemType": "MigrationRequest",
            **_scope_fields(selected_scope),
            "commercialRequestId": commercial_request_id,
            "connectionId": connection,
            "requestHash": request_hash,
            "idempotencyDigest": idempotency_digest,
            "sourceOffer": source,
            "targetOffer": target,
            "candidateScope": {"kind": "all_matching_source_price"},
            "requestedPolicy": policy,
            "canarySize": canary,
            "accountConcurrency": concurrency,
            "actorHash": actor,
            "requestId": selected_request_id,
            "createdAt": now,
            "updatedAt": now,
            "state": "draft",
            "revision": 0,
            "jobId": None,
            "dryRunRevision": None,
            "dryRunHash": None,
            # Deliberately not named expiresAt: MigrationRequest is non-TTL commercial state.
            "previewExpiresAt": None,
            "counts": _zero_counts(),
            "approval": None,
            "approvalIdempotencyDigest": None,
            "lastCommand": None,
            "lastNeedsReview": None,
            "lastEventId": None,
            "lastEventHash": None,
            "lastProgressRevision": None,
            "lastProgressHash": None,
            "stateRevision": 0,
            "storageRevision": 1,
        }
        operation = {
            "kind": "put",
            "table_name": self._table_name,
            "item": item,
            "condition": "absent",
        }
        try:
            self._backend.transact([operation], _client_token(selected_scope, operation))
        except Exception as exc:
            try:
                replay = self._backend.get(
                    self._table_name, selected_scope.partition_key, sk
                )
            except Exception:
                raise StorageOutcomeUnknown("migration request outcome is unknown") from None
            if replay is not None:
                return _preview_replay(
                    replay,
                    selected_scope,
                    commercial_request_id,
                    request_hash,
                    idempotency_digest,
                )
            if isinstance(exc, ConditionalWriteFailed):
                raise StorageConflict("migration request changed") from None
            raise StorageOutcomeUnknown("migration request outcome is unknown") from None
        return copy.deepcopy(item)

    def get_request(
        self, scope: CommerceScope, commercial_request_id: object
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        request_id = _safe_id(commercial_request_id, "commercial_request_id")
        item = self._backend.get(
            self._table_name,
            selected_scope.partition_key,
            f"MIGRATION_REQUEST#{request_id}",
        )
        if item is None:
            raise StorageConflict("migration request is unavailable")
        return _stored_request(item, selected_scope, request_id)

    def replay_command(
        self,
        scope: CommerceScope,
        commercial_request_id: object,
        *,
        operation: object,
        idempotency_key: object,
        request_hash: object,
    ) -> dict[str, Any] | None:
        selected_scope = _scope(scope)
        request_id = _safe_id(commercial_request_id, "commercial_request_id")
        if operation not in {
            "migrationExecute", "migrationPause", "migrationResume", "migrationCancel"
        }:
            raise ValueError("migration command operation is invalid")
        key_digest = _idempotency_digest(idempotency_key)
        command_request_hash = _hash_value(request_hash, "command_request_hash")
        receipt_sk = "MIGRATION_COMMAND#" + _hash_json({
            "operation": operation,
            "idempotencyDigest": key_digest,
        })
        receipt = self._backend.get(
            self._table_name, selected_scope.partition_key, receipt_sk
        )
        if receipt is None:
            return None
        return _command_receipt_lookup(
            receipt,
            selected_scope,
            request_id,
            operation,
            key_digest,
            command_request_hash,
        )

    def record_command_result(
        self,
        scope: CommerceScope,
        commercial_request_id: object,
        *,
        operation: object,
        idempotency_key: object,
        request_hash: object,
        actor_hash: object,
        result: object,
        now_epoch: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        request_id = _safe_id(commercial_request_id, "commercial_request_id")
        if operation not in {
            "migrationPreview", "migrationExecute", "migrationPause",
            "migrationResume", "migrationCancel",
        }:
            raise ValueError("migration command operation is invalid")
        key_digest = _idempotency_digest(idempotency_key)
        command_request_hash = _hash_value(request_hash, "command_request_hash")
        parsed_result = _command_result(result)
        if parsed_result["status"] == "pending":
            raise StorageOutcomeUnknown("migration command dispatch is pending")
        if operation == "migrationPreview" and parsed_result["status"] != "accepted":
            raise StorageConflict("migration preview command is invalid")
        actor = _actor_hash(actor_hash)
        now = _positive_int(now_epoch, "now_epoch")
        receipt_sk = "MIGRATION_COMMAND#" + _hash_json({
            "operation": operation,
            "idempotencyDigest": key_digest,
        })
        command_record = {
            "operation": operation,
            "idempotencyDigest": key_digest,
            "requestHash": command_request_hash,
            "actorHash": actor,
            "result": parsed_result,
        }
        for _attempt in range(8):
            receipt = self._backend.get(
                self._table_name, selected_scope.partition_key, receipt_sk
            )
            if receipt is not None:
                return _command_receipt_replay(
                    receipt,
                    selected_scope,
                    request_id,
                    operation,
                    command_request_hash,
                    parsed_result,
                )
            current = self.get_request(selected_scope, request_id)
            updated, public_result = _command_update(
                current, operation, command_record, parsed_result, now
            )
            receipt_hash = _hash_json({
                "commercialRequestId": request_id,
                "operation": operation,
                "idempotencyDigest": key_digest,
                "requestHash": command_request_hash,
                "actorHash": actor,
                "commandResult": parsed_result,
                "result": public_result,
            })
            receipt_item = {
                "pk": selected_scope.partition_key,
                "sk": receipt_sk,
                "itemType": "MigrationCommandReceipt",
                **_scope_fields(selected_scope),
                "commercialRequestId": request_id,
                "operation": operation,
                "receiptHash": receipt_hash,
                "idempotencyDigest": key_digest,
                "requestHash": command_request_hash,
                "actorHash": actor,
                "commandResult": parsed_result,
                "result": public_result,
                "createdAt": now,
                "expiresAt": now + EVENT_RECEIPT_TTL_SECONDS,
            }
            operations = [
                {
                    "kind": "put",
                    "table_name": self._table_name,
                    "item": updated,
                    "condition": {"storageRevision": current["storageRevision"]},
                },
                {
                    "kind": "put",
                    "table_name": self._table_name,
                    "item": receipt_item,
                    "condition": "absent",
                },
            ]
            try:
                self._backend.transact(
                    operations, _client_token(selected_scope, operations)
                )
            except Exception as exc:
                try:
                    replay = self._backend.get(
                        self._table_name, selected_scope.partition_key, receipt_sk
                    )
                except Exception:
                    raise StorageOutcomeUnknown(
                        "migration command outcome is unknown"
                    ) from None
                if replay is not None:
                    return _command_receipt_replay(
                        replay,
                        selected_scope,
                        request_id,
                        operation,
                        command_request_hash,
                        parsed_result,
                    )
                if isinstance(exc, ConditionalWriteFailed):
                    continue
                raise StorageOutcomeUnknown(
                    "migration command outcome is unknown"
                ) from None
            return copy.deepcopy(public_result)
        raise StorageOutcomeUnknown("migration command outcome is unknown")

    def approve_execution(
        self,
        scope: CommerceScope,
        commercial_request_id: object,
        *,
        dry_run_revision: object,
        dry_run_hash: object,
        actor_hash: object,
        idempotency_key: object,
        now_epoch: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        request_id = _safe_id(commercial_request_id, "commercial_request_id")
        revision = _positive_int(dry_run_revision, "dry_run_revision")
        digest = _hash_value(dry_run_hash, "dry_run_hash")
        actor = _actor_hash(actor_hash)
        key_digest = _idempotency_digest(idempotency_key)
        now = _positive_int(now_epoch, "now_epoch")
        current = self.get_request(selected_scope, request_id)
        approval = {
            "dryRunRevision": revision,
            "dryRunHash": digest,
            "actorHash": actor,
            "approvedAt": now,
        }
        if current["approval"] is not None:
            replay = copy.deepcopy(current)
            expected = copy.deepcopy(approval)
            expected["approvedAt"] = current["approval"].get("approvedAt")
            if (
                current["approval"] == expected
                and current["approvalIdempotencyDigest"] == key_digest
            ):
                return replay
            raise StorageConflict("migration approval changed")
        if (
            current["state"] != "awaiting_approval"
            or current["jobId"] is None
            or current["dryRunRevision"] != revision
            or current["dryRunHash"] != digest
            or type(current["previewExpiresAt"]) is not int
            or now >= current["previewExpiresAt"]
        ):
            raise StorageConflict("migration preview is stale or expired")
        updated = copy.deepcopy(current)
        updated.update({
            "approval": approval,
            "approvalIdempotencyDigest": key_digest,
            "updatedAt": now,
            "storageRevision": current["storageRevision"] + 1,
        })
        return self._replace(
            selected_scope,
            current,
            updated,
            replay=lambda item: _approval_replay(item, approval, key_digest),
            label="migration approval",
        )

    def prepare_control(
        self,
        scope: CommerceScope,
        commercial_request_id: object,
        *,
        action: object,
        expected_revision: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        request_id = _safe_id(commercial_request_id, "commercial_request_id")
        if action not in _CONTROL_STATES:
            raise ValueError("migration control action is invalid")
        revision = _positive_int(expected_revision, "expected_revision")
        current = self.get_request(selected_scope, request_id)
        if (
            current["jobId"] is None
            or current["revision"] != revision
            or current["state"] not in _CONTROL_STATES[action]
        ):
            raise StorageConflict("migration control conflicts with current state")
        return current

    def apply_verified_event(
        self,
        scope: CommerceScope,
        *,
        event_id: object,
        event_type: object,
        occurred_at: object,
        data: object,
        now_epoch: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        selected_event_id = _safe_id(event_id, "event_id")
        if event_type not in {
            "migration.preview_ready.v1",
            "migration.progressed.v1",
            "migration.item_needs_review.v1",
            "migration.completed.v1",
        }:
            raise ValueError("migration event type is invalid")
        occurred = _epoch(occurred_at, "occurred_at")
        now = _epoch(now_epoch, "now_epoch")
        if occurred > now + 300:
            raise ValueError("migration event timestamp is invalid")
        parsed = _event_data(event_type, data)
        current = self.get_request(selected_scope, parsed["commercialRequestId"])
        if (
            current["connectionId"] != parsed["connectionId"]
            or current["jobId"] != parsed["jobId"]
        ):
            raise StorageConflict("migration event binding changed")
        event_hash = _hash_json({
            "eventType": event_type,
            "occurredAt": occurred,
            "data": parsed,
        })
        receipt_sk = f"MIGRATION_EVENT#{parsed['dedupeKey']}"
        receipt = self._backend.get(
            self._table_name, selected_scope.partition_key, receipt_sk
        )
        if receipt is not None:
            return _event_replay(
                receipt,
                selected_scope,
                selected_event_id,
                event_type,
                event_hash,
                parsed["commercialRequestId"],
                parsed["jobId"],
            )
        progress_hash = None
        if event_type == "migration.progressed.v1":
            progress_hash = _hash_json({
                "revision": parsed["revision"],
                "state": parsed["state"],
                "counts": parsed["counts"],
            })
            if (
                current.get("lastProgressRevision") == parsed["revision"]
                and current.get("lastProgressHash") != progress_hash
            ):
                raise StorageConflict("migration progress revision changed")
        same_revision_reconciliation = (
            parsed["revision"] == current["revision"]
            and (
                event_type == "migration.item_needs_review.v1"
                or (
                    event_type == "migration.completed.v1"
                    and parsed.get("state") in _TRANSITIONS[current["state"]]
                    and (
                        parsed["revision"] > current["stateRevision"]
                        or (
                            current["state"] == "cancel_requested"
                            and parsed.get("state") == "canceled"
                        )
                    )
                )
                or (
                    event_type == "migration.progressed.v1"
                    and (
                        parsed.get("state") == current["state"]
                        or (
                            parsed.get("state") in _TRANSITIONS[current["state"]]
                            and parsed["revision"] > current["stateRevision"]
                        )
                    )
                )
            )
        )
        if parsed["revision"] == current["revision"] and not same_revision_reconciliation:
            raise StorageConflict("migration event revision is ambiguous")
        stale = parsed["revision"] < current["revision"]
        if stale:
            result = {**_public_projection(current), "stale": True}
            updated = None
        else:
            updated = _apply_event(
                current,
                event_type,
                parsed,
                selected_event_id,
                event_hash,
                now,
                progress_hash=progress_hash,
            )
            result = {**_public_projection(updated), "stale": False}
        receipt_item = {
            "pk": selected_scope.partition_key,
            "sk": receipt_sk,
            "itemType": "MigrationEventInbox",
            **_scope_fields(selected_scope),
            "eventId": selected_event_id,
            "eventType": event_type,
            "eventHash": event_hash,
            "dedupeKey": parsed["dedupeKey"],
            "commercialRequestId": parsed["commercialRequestId"],
            "jobId": parsed["jobId"],
            "result": result,
            "resultHash": _hash_json({"eventHash": event_hash, "result": result}),
            "processedAt": now,
            "expiresAt": now + EVENT_RECEIPT_TTL_SECONDS,
        }
        operations = []
        if updated is not None:
            operations.append({
                "kind": "put",
                "table_name": self._table_name,
                "item": updated,
                "condition": {"storageRevision": current["storageRevision"]},
            })
        operations.append({
            "kind": "put",
            "table_name": self._table_name,
            "item": receipt_item,
            "condition": "absent",
        })
        try:
            self._backend.transact(operations, _client_token(selected_scope, operations))
        except Exception as exc:
            try:
                replay = self._backend.get(
                    self._table_name, selected_scope.partition_key, receipt_sk
                )
            except Exception:
                raise StorageOutcomeUnknown("migration event outcome is unknown") from None
            if replay is not None:
                return _event_replay(
                    replay,
                    selected_scope,
                    selected_event_id,
                    event_type,
                    event_hash,
                    parsed["commercialRequestId"],
                    parsed["jobId"],
                )
            if isinstance(exc, ConditionalWriteFailed):
                raise StorageConflict("migration event state changed") from None
            raise StorageOutcomeUnknown("migration event outcome is unknown") from None
        return copy.deepcopy(result)

    def _replace(
        self,
        scope: CommerceScope,
        current: Mapping[str, Any],
        updated: Mapping[str, Any],
        *,
        replay: Any,
        label: str,
    ) -> dict[str, Any]:
        operation = {
            "kind": "put",
            "table_name": self._table_name,
            "item": copy.deepcopy(dict(updated)),
            "condition": {"storageRevision": current["storageRevision"]},
        }
        try:
            self._backend.transact([operation], _client_token(scope, operation))
        except Exception as exc:
            try:
                latest = self._backend.get(
                    self._table_name, scope.partition_key, updated["sk"]
                )
            except Exception:
                raise StorageOutcomeUnknown(f"{label} outcome is unknown") from None
            if latest is not None:
                try:
                    return replay(_stored_request(latest, scope, updated["commercialRequestId"]))
                except StorageConflict:
                    pass
            if isinstance(exc, ConditionalWriteFailed):
                raise StorageConflict(f"{label} changed") from None
            raise StorageOutcomeUnknown(f"{label} outcome is unknown") from None
        return copy.deepcopy(dict(updated))


def _command_update(
    current: Mapping[str, Any],
    operation: str,
    command_record: Mapping[str, Any],
    parsed_result: Mapping[str, Any],
    now: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if current["jobId"] not in {None, parsed_result["jobId"]}:
        raise StorageConflict("migration job binding changed")
    if operation == "migrationExecute" and current["approval"] is None:
        raise StorageConflict("migration request is not approved")
    state = current["state"]
    revision = current["revision"]
    state_revision = current["stateRevision"]
    if (
        parsed_result["status"] == "needs_review"
        and parsed_result["revision"] > revision
    ):
        raise StorageConflict("migration command revision changed")
    if parsed_result["revision"] > revision:
        if parsed_result["revision"] != revision + 1:
            raise StorageConflict("migration command revision changed")
        if parsed_result["status"] == "accepted":
            target_state = {
                "migrationPreview": "previewing",
                "migrationExecute": "scheduled",
                "migrationPause": "paused",
                "migrationResume": "running",
                "migrationCancel": "cancel_requested",
            }[operation]
            if target_state != state and target_state not in _TRANSITIONS[state]:
                raise StorageConflict("migration command state changed")
            state = target_state
            state_revision = parsed_result["revision"]
        revision = parsed_result["revision"]
    updated = copy.deepcopy(dict(current))
    updated.update({
        "jobId": parsed_result["jobId"],
        "revision": revision,
        "state": state,
        "stateRevision": state_revision,
        "lastCommand": copy.deepcopy(dict(command_record)),
        "updatedAt": max(now, current["updatedAt"]),
        "storageRevision": current["storageRevision"] + 1,
    })
    return updated, _public_projection(updated)


def public_migration_request(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("migration request is invalid")
    if "previewExpiresAt" in value:
        return _validated_public_projection(_public_projection(value))
    return _validated_public_projection(value)


def _preview_replay(
    item: object,
    scope: CommerceScope,
    commercial_request_id: str,
    request_hash: str,
    idempotency_digest: str,
) -> dict[str, Any]:
    stored = _stored_request(item, scope, commercial_request_id)
    if (
        stored["requestHash"] != request_hash
        or stored["idempotencyDigest"] != idempotency_digest
    ):
        raise StorageConflict("migration idempotency key was already used")
    return stored


def _stored_request(
    item: object, scope: CommerceScope, commercial_request_id: str
) -> dict[str, Any]:
    try:
        return _validated_stored_request(item, scope, commercial_request_id)
    except StorageConflict:
        raise
    except (KeyError, TypeError, ValueError):
        raise StorageConflict("stored migration request is invalid") from None


def _validated_stored_request(
    item: object, scope: CommerceScope, commercial_request_id: str
) -> dict[str, Any]:
    if not isinstance(item, Mapping) or set(item) != _REQUEST_KEYS:
        raise StorageConflict("stored migration request is invalid")
    stored = copy.deepcopy(dict(item))
    if (
        stored.get("pk") != scope.partition_key
        or stored.get("sk") != f"MIGRATION_REQUEST#{commercial_request_id}"
        or stored.get("itemType") != "MigrationRequest"
        or not _scope_matches(stored, scope)
        or stored.get("commercialRequestId") != commercial_request_id
        or stored.get("state") not in _JOB_STATES
    ):
        raise StorageConflict("stored migration request is invalid")
    _safe_id(stored.get("connectionId"), "connection_id")
    _hash_value(stored.get("requestHash"), "request_hash")
    _hash_value(stored.get("idempotencyDigest"), "idempotency_digest")
    source_offer = _offer_binding(stored.get("sourceOffer"))
    target_offer = _offer_binding(stored.get("targetOffer"))
    if not _valid_offer_pair(source_offer, target_offer):
        raise StorageConflict("stored migration offers are invalid")
    _requested_policy(stored.get("requestedPolicy"))
    if stored.get("candidateScope") != {"kind": "all_matching_source_price"}:
        raise StorageConflict("stored migration request is invalid")
    _bounded_int(stored.get("canarySize"), 1, 25, "canary_size")
    _bounded_int(stored.get("accountConcurrency"), 1, 5, "account_concurrency")
    _actor_hash(stored.get("actorHash"))
    _request_id(stored.get("requestId"))
    _positive_int(stored.get("createdAt"), "created_at")
    updated_at = _positive_int(stored.get("updatedAt"), "updated_at")
    if updated_at < stored["createdAt"]:
        raise StorageConflict("stored migration timestamps are invalid")
    revision = _nonnegative_int(stored.get("revision"), "revision")
    state_revision = _nonnegative_int(stored.get("stateRevision"), "state_revision")
    if state_revision > revision:
        raise StorageConflict("stored migration state revision is invalid")
    _positive_int(stored.get("storageRevision"), "storage_revision")
    if stored.get("jobId") is not None:
        _safe_id(stored["jobId"], "job_id")
    if stored.get("dryRunRevision") is not None:
        _positive_int(stored["dryRunRevision"], "dry_run_revision")
    if stored.get("dryRunHash") is not None:
        _hash_value(stored["dryRunHash"], "dry_run_hash")
    if stored.get("previewExpiresAt") is not None:
        _positive_int(stored["previewExpiresAt"], "preview_expires_at")
    dry_run_values = (
        stored.get("dryRunRevision"),
        stored.get("dryRunHash"),
        stored.get("previewExpiresAt"),
    )
    has_dry_run = all(value is not None for value in dry_run_values)
    if any(value is not None for value in dry_run_values) != has_dry_run:
        raise StorageConflict("stored migration preview is incomplete")
    _counts(stored.get("counts"))
    expected_request_hash = _hash_json({
        "scope": _scope_fields(scope),
        "connectionId": stored["connectionId"],
        "sourceOffer": stored["sourceOffer"],
        "targetOffer": stored["targetOffer"],
        "candidateScope": stored["candidateScope"],
        "requestedPolicy": stored["requestedPolicy"],
        "canarySize": stored["canarySize"],
        "accountConcurrency": stored["accountConcurrency"],
        "actorHash": stored["actorHash"],
    })
    if stored["requestHash"] != expected_request_hash:
        raise StorageConflict("stored migration request hash is invalid")
    if stored.get("approval") is not None:
        _approval(stored["approval"])
        _hash_value(stored.get("approvalIdempotencyDigest"), "approval_idempotency")
        if (
            stored["approval"].get("dryRunRevision") != stored.get("dryRunRevision")
            or stored["approval"].get("dryRunHash") != stored.get("dryRunHash")
        ):
            raise StorageConflict("stored migration approval is stale")
    elif stored.get("approvalIdempotencyDigest") is not None:
        raise StorageConflict("stored migration request is invalid")
    last_command = stored.get("lastCommand")
    if last_command is not None:
        if not isinstance(last_command, Mapping) or set(last_command) != {
            "operation", "idempotencyDigest", "requestHash", "actorHash", "result"
        }:
            raise StorageConflict("stored migration command is invalid")
        if last_command.get("operation") not in {
            "migrationPreview", "migrationExecute", "migrationPause",
            "migrationResume", "migrationCancel",
        }:
            raise StorageConflict("stored migration command is invalid")
        _hash_value(last_command.get("idempotencyDigest"), "command_idempotency")
        _hash_value(last_command.get("requestHash"), "command_request_hash")
        _actor_hash(last_command.get("actorHash"))
        command_result = _command_result(last_command.get("result"))
        if (
            command_result["status"] == "pending"
            or command_result["jobId"] != stored.get("jobId")
            or command_result["revision"] > stored.get("revision")
        ):
            raise StorageConflict("stored migration command is invalid")
        if (
            last_command["operation"] in {
                "migrationExecute", "migrationPause", "migrationResume"
            }
            and stored.get("approval") is None
        ):
            raise StorageConflict("stored migration command lacks approval")
    last_needs_review = stored.get("lastNeedsReview")
    if last_needs_review is not None:
        if not isinstance(last_needs_review, Mapping) or set(last_needs_review) != {
            "itemId", "reasonCode"
        }:
            raise StorageConflict("stored migration review item is invalid")
        _migration_item_id(last_needs_review.get("itemId"))
        _migration_reason_code(last_needs_review.get("reasonCode"))
    if (stored.get("lastEventId") is None) != (stored.get("lastEventHash") is None):
        raise StorageConflict("stored migration event is invalid")
    if stored.get("lastEventId") is not None:
        _safe_id(stored["lastEventId"], "event_id")
        _hash_value(stored["lastEventHash"], "event_hash")
    if (stored.get("lastProgressRevision") is None) != (
        stored.get("lastProgressHash") is None
    ):
        raise StorageConflict("stored migration progress is invalid")
    if stored.get("lastProgressRevision") is not None:
        progress_revision = _positive_int(
            stored["lastProgressRevision"], "last_progress_revision"
        )
        _hash_value(stored["lastProgressHash"], "last_progress_hash")
        if progress_revision > stored["revision"]:
            raise StorageConflict("stored migration progress is invalid")
    state = stored["state"]
    if state == "draft":
        if (
            stored["revision"] != 0
            or stored["stateRevision"] != 0
            or stored["jobId"] is not None
            or has_dry_run
            or stored["approval"] is not None
            or stored["lastCommand"] is not None
        ):
            raise StorageConflict("stored migration draft state is invalid")
    elif (
        stored["revision"] == 0
        or stored["stateRevision"] == 0
        or stored["jobId"] is None
    ):
        raise StorageConflict("stored migration job binding is invalid")
    if state == "previewing" and (has_dry_run or stored["approval"] is not None):
        raise StorageConflict("stored migration preview state is invalid")
    if state == "previewing" and (
        stored["lastCommand"] is None
        or stored["lastCommand"].get("operation") != "migrationPreview"
    ):
        raise StorageConflict("stored migration preview command is invalid")
    if state == "awaiting_approval" and not has_dry_run:
        raise StorageConflict("stored migration preview state is invalid")
    if state in {
        "scheduled", "running", "paused", "canceling", "completed",
        "completed_with_errors",
    } and (not has_dry_run or stored["approval"] is None):
        raise StorageConflict("stored migration execution state is invalid")
    return stored


def _offer_binding(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "offerVersionId", "revision", "schemaVersion", "snapshot", "contentHash"
    }:
        raise ValueError("migration offer binding is invalid")
    selected = copy.deepcopy(dict(value))
    _safe_id(selected.get("offerVersionId"), "offer_version_id")
    _positive_int(selected.get("revision"), "offer_revision")
    if selected.get("schemaVersion") != 1 or not isinstance(selected.get("snapshot"), Mapping):
        raise ValueError("migration offer binding is invalid")
    snapshot = selected["snapshot"]
    if set(snapshot) != {
        "schemaVersion", "amountMinor", "billingScheme", "currency",
        "recurrence", "saleType", "taxBehavior",
    }:
        raise ValueError("migration offer binding is invalid")
    if (
        snapshot.get("schemaVersion") != 1
        or snapshot.get("saleType") != "recurring"
        or snapshot.get("billingScheme") != "per_unit"
        or type(snapshot.get("amountMinor")) is not int
        or not 0 <= snapshot["amountMinor"] <= MAX_AMOUNT_MINOR
        or type(snapshot.get("currency")) is not str
        or _CURRENCY.fullmatch(snapshot["currency"]) is None
        or snapshot.get("taxBehavior") not in {"inclusive", "exclusive", "unspecified"}
        or not isinstance(snapshot.get("recurrence"), Mapping)
        or set(snapshot["recurrence"]) != {"interval", "intervalCount", "usageType"}
        or snapshot["recurrence"].get("interval") not in {"month", "year"}
        or snapshot["recurrence"].get("intervalCount") != 1
        or snapshot["recurrence"].get("usageType") != "licensed"
    ):
        raise ValueError("migration offer binding is invalid")
    expected = _hash_json({"schemaVersion": 1, "snapshot": snapshot})
    if selected.get("contentHash") != expected:
        raise ValueError("migration offer binding hash is invalid")
    return selected


def _valid_offer_pair(
    source: Mapping[str, Any], target: Mapping[str, Any]
) -> bool:
    return (
        source["offerVersionId"] != target["offerVersionId"]
        and source["snapshot"]["currency"] == target["snapshot"]["currency"]
        and source["snapshot"]["recurrence"] == target["snapshot"]["recurrence"]
    )


def _command_result(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "commandId", "status", "jobId", "revision"
    }:
        raise ValueError("migration command result is invalid")
    selected = copy.deepcopy(dict(value))
    _safe_id(selected.get("commandId"), "command_id")
    _safe_id(selected.get("jobId"), "job_id")
    _positive_int(selected.get("revision"), "revision")
    if selected.get("status") not in {"accepted", "pending", "needs_review"}:
        raise ValueError("migration command result is invalid")
    return selected


def _event_data(event_type: str, value: object) -> dict[str, Any]:
    common = {"commercialRequestId", "jobId", "connectionId", "revision", "dedupeKey"}
    extra = {
        "migration.preview_ready.v1": {
            "dryRunRevision", "dryRunHash", "expiresAt", "counts"
        },
        "migration.progressed.v1": {"state", "counts"},
        "migration.item_needs_review.v1": {"itemId", "reasonCode"},
        "migration.completed.v1": {"state", "counts"},
    }[event_type]
    if not isinstance(value, Mapping) or set(value) != common | extra:
        raise ValueError("migration event data is invalid")
    selected = copy.deepcopy(dict(value))
    for field in ("commercialRequestId", "jobId", "connectionId", "dedupeKey"):
        _safe_id(selected.get(field), field)
    _positive_int(selected.get("revision"), "revision")
    if event_type == "migration.preview_ready.v1":
        _positive_int(selected.get("dryRunRevision"), "dry_run_revision")
        _hash_value(selected.get("dryRunHash"), "dry_run_hash")
        _positive_int(selected.get("expiresAt"), "expires_at")
        selected["counts"] = _counts(selected.get("counts"))
    elif event_type in {"migration.progressed.v1", "migration.completed.v1"}:
        state = selected.get("state")
        if state not in _JOB_STATES:
            raise ValueError("migration event state is invalid")
        if event_type == "migration.completed.v1" and state not in _TERMINAL_STATES:
            raise ValueError("migration completion state is invalid")
        if event_type == "migration.progressed.v1" and state in _TERMINAL_STATES:
            raise ValueError("migration progress state is invalid")
        selected["counts"] = _counts(selected.get("counts"))
    else:
        selected["itemId"] = _migration_item_id(selected.get("itemId"))
        selected["reasonCode"] = _migration_reason_code(selected.get("reasonCode"))
    return selected


def _apply_event(
    current: Mapping[str, Any],
    event_type: str,
    data: Mapping[str, Any],
    event_id: str,
    event_hash: str,
    now: int,
    *,
    progress_hash: str | None,
) -> dict[str, Any]:
    updated = copy.deepcopy(dict(current))
    if event_type == "migration.preview_ready.v1":
        target_state = "awaiting_approval"
        if current["state"] != "previewing":
            raise StorageConflict("migration preview event conflicts with current state")
        updated.update({
            "dryRunRevision": data["dryRunRevision"],
            "dryRunHash": data["dryRunHash"],
            "previewExpiresAt": data["expiresAt"],
            "counts": copy.deepcopy(data["counts"]),
        })
    elif event_type == "migration.progressed.v1":
        target_state = data["state"]
        if target_state in {"scheduled", "running"} and current["approval"] is None:
            raise StorageConflict("migration progress lacks commercial approval")
        updated["counts"] = copy.deepcopy(data["counts"])
    elif event_type == "migration.completed.v1":
        target_state = data["state"]
        updated["counts"] = copy.deepcopy(data["counts"])
    else:
        target_state = current["state"]
        updated["lastNeedsReview"] = {
            "itemId": data["itemId"],
            "reasonCode": data["reasonCode"],
        }
    if target_state != current["state"] and target_state not in _TRANSITIONS[current["state"]]:
        raise StorageConflict("migration event transition is invalid")
    state_revision = (
        data["revision"]
        if target_state != current["state"]
        else current["stateRevision"]
    )
    updated.update({
        "state": target_state,
        "revision": data["revision"],
        "stateRevision": state_revision,
        "updatedAt": now,
        "lastEventId": event_id,
        "lastEventHash": event_hash,
        "storageRevision": current["storageRevision"] + 1,
    })
    if event_type == "migration.progressed.v1":
        if progress_hash is None:
            raise StorageConflict("migration progress is invalid")
        updated.update({
            "lastProgressRevision": data["revision"],
            "lastProgressHash": progress_hash,
        })
    return updated


def _public_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    last_command = value.get("lastCommand")
    command_status = (
        None
        if last_command is None
        else last_command["result"]["status"]
    )
    return {
        "commercialRequestId": value["commercialRequestId"],
        "jobId": value["jobId"],
        "state": value["state"],
        "revision": value["revision"],
        "commandStatus": command_status,
        "dryRunRevision": value["dryRunRevision"],
        "dryRunHash": value["dryRunHash"],
        "expiresAt": value["previewExpiresAt"],
        "counts": copy.deepcopy(value["counts"]),
    }


def _validated_public_projection(value: object) -> dict[str, Any]:
    keys = {
        "commercialRequestId", "jobId", "state", "revision", "dryRunRevision",
        "dryRunHash", "expiresAt", "counts", "commandStatus",
    }
    if not isinstance(value, Mapping) or set(value) != keys:
        raise StorageConflict("stored migration result is invalid")
    selected = copy.deepcopy(dict(value))
    _safe_id(selected.get("commercialRequestId"), "commercial_request_id")
    _safe_id(selected.get("jobId"), "job_id")
    if selected.get("state") not in _JOB_STATES:
        raise StorageConflict("stored migration result is invalid")
    if selected.get("commandStatus") not in {None, "accepted", "needs_review"}:
        raise StorageConflict("stored migration result is invalid")
    _positive_int(selected.get("revision"), "revision")
    if selected.get("dryRunRevision") is not None:
        _positive_int(selected["dryRunRevision"], "dry_run_revision")
    if selected.get("dryRunHash") is not None:
        _hash_value(selected["dryRunHash"], "dry_run_hash")
    if selected.get("expiresAt") is not None:
        _positive_int(selected["expiresAt"], "expires_at")
    selected["counts"] = _counts(selected.get("counts"))
    return selected


def _event_replay(
    item: object,
    scope: CommerceScope,
    event_id: str,
    event_type: str,
    event_hash: str,
    commercial_request_id: str,
    job_id: str,
) -> dict[str, Any]:
    expected_keys = {
        "pk", "sk", "itemType", "environment", "tenantId", "draftId", "domain",
        "eventId", "eventType", "eventHash", "dedupeKey", "commercialRequestId",
        "jobId", "result", "resultHash", "processedAt", "expiresAt",
    }
    if (
        not isinstance(item, Mapping)
        or set(item) != expected_keys
        or item.get("pk") != scope.partition_key
        or item.get("sk") != f"MIGRATION_EVENT#{item.get('dedupeKey')}"
        or item.get("itemType") != "MigrationEventInbox"
        or not _scope_matches(item, scope)
        or item.get("eventId") != event_id
        or item.get("eventType") != event_type
        or item.get("eventHash") != event_hash
        or item.get("commercialRequestId") != commercial_request_id
        or item.get("jobId") != job_id
        or not isinstance(item.get("result"), Mapping)
    ):
        raise StorageConflict("migration event dedupe collision")
    try:
        processed_at = _epoch(item.get("processedAt"), "processed_at")
        if item.get("expiresAt") != processed_at + EVENT_RECEIPT_TTL_SECONDS:
            raise ValueError
        raw_result = dict(item["result"])
        if set(raw_result) != {
            "commercialRequestId", "jobId", "state", "revision", "dryRunRevision",
            "dryRunHash", "expiresAt", "counts", "commandStatus", "stale",
        } or type(raw_result.get("stale")) is not bool:
            raise ValueError
        stale = raw_result.pop("stale")
        projection = _validated_public_projection(raw_result)
        result = {**projection, "stale": stale}
        if (
            projection["commercialRequestId"] != commercial_request_id
            or projection["jobId"] != job_id
            or item.get("resultHash") != _hash_json({
                "eventHash": event_hash,
                "result": result,
            })
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise StorageConflict("migration event dedupe collision") from None
    return result


def _command_receipt_replay(
    item: object,
    scope: CommerceScope,
    commercial_request_id: str,
    operation: str,
    request_hash: str,
    expected_command_result: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise StorageConflict("migration command idempotency collision")
    result = _command_receipt_lookup(
        item,
        scope,
        commercial_request_id,
        operation,
        item.get("idempotencyDigest"),
        request_hash,
    )
    if item.get("commandResult") != expected_command_result:
        raise StorageConflict("migration command idempotency collision")
    return result


def _command_receipt_lookup(
    item: object,
    scope: CommerceScope,
    commercial_request_id: str,
    operation: str,
    idempotency_digest: object,
    request_hash: object,
) -> dict[str, Any]:
    expected_keys = {
        "pk", "sk", "itemType", "environment", "tenantId", "draftId", "domain",
        "commercialRequestId", "operation", "receiptHash", "idempotencyDigest",
        "requestHash", "actorHash", "commandResult", "result", "createdAt",
        "expiresAt",
    }
    if (
        not isinstance(item, Mapping)
        or set(item) != expected_keys
        or item.get("pk") != scope.partition_key
        or item.get("itemType") != "MigrationCommandReceipt"
        or not _scope_matches(item, scope)
        or item.get("commercialRequestId") != commercial_request_id
        or item.get("operation") != operation
        or item.get("idempotencyDigest") != idempotency_digest
        or item.get("requestHash") != request_hash
        or not isinstance(item.get("result"), Mapping)
    ):
        raise StorageConflict("migration command idempotency collision")
    try:
        digest = _hash_value(idempotency_digest, "idempotency_digest")
        selected_request_hash = _hash_value(request_hash, "command_request_hash")
        actor = _actor_hash(item.get("actorHash"))
        command_result = _command_result(item.get("commandResult"))
        created_at = _positive_int(item.get("createdAt"), "created_at")
        if item.get("expiresAt") != created_at + EVENT_RECEIPT_TTL_SECONDS:
            raise ValueError
    except (TypeError, ValueError):
        raise StorageConflict("migration command idempotency collision") from None
    public_result = _validated_public_projection(item["result"])
    if (
        public_result["commercialRequestId"] != commercial_request_id
        or public_result["jobId"] != command_result["jobId"]
        or public_result["revision"] < command_result["revision"]
    ):
        raise StorageConflict("migration command idempotency collision")
    expected_hash = _hash_json({
        "commercialRequestId": commercial_request_id,
        "operation": operation,
        "idempotencyDigest": digest,
        "requestHash": selected_request_hash,
        "actorHash": actor,
        "commandResult": command_result,
        "result": public_result,
    })
    if item.get("receiptHash") != expected_hash:
        raise StorageConflict("migration command idempotency collision")
    return public_result


def _approval_replay(
    item: Mapping[str, Any], approval: Mapping[str, Any], key_digest: str
) -> dict[str, Any]:
    expected = copy.deepcopy(dict(approval))
    expected["approvedAt"] = item.get("approval", {}).get("approvedAt")
    if item.get("approval") != expected or item.get("approvalIdempotencyDigest") != key_digest:
        raise StorageConflict("migration approval changed")
    return copy.deepcopy(dict(item))


def _approval(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "dryRunRevision", "dryRunHash", "actorHash", "approvedAt"
    }:
        raise StorageConflict("stored migration approval is invalid")
    _positive_int(value.get("dryRunRevision"), "dry_run_revision")
    _hash_value(value.get("dryRunHash"), "dry_run_hash")
    _actor_hash(value.get("actorHash"))
    _positive_int(value.get("approvedAt"), "approved_at")
    return copy.deepcopy(dict(value))


def _requested_policy(value: object) -> dict[str, str]:
    if (
        not isinstance(value, Mapping)
        or set(value) != {"mode"}
        or value.get("mode") not in _POLICY_MODES
    ):
        raise ValueError("migration policy is invalid")
    return {"mode": value["mode"]}


def _counts(value: object) -> dict[str, int]:
    keys = {"total", "pending", "applied", "needsReview", "failed"}
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError("migration counts are invalid")
    selected = {key: _nonnegative_int(value[key], key) for key in keys}
    if selected["total"] != sum(selected[key] for key in keys - {"total"}):
        raise ValueError("migration counts are inconsistent")
    return selected


def _zero_counts() -> dict[str, int]:
    return {"total": 0, "pending": 0, "applied": 0, "needsReview": 0, "failed": 0}


def _scope(value: object) -> CommerceScope:
    if type(value) is not CommerceScope:
        raise ValueError("scope must be an immutable CommerceScope")
    return value


def _scope_fields(scope: CommerceScope) -> dict[str, str]:
    return {
        "environment": scope.environment,
        "tenantId": scope.tenant_id,
        "draftId": scope.draft_id,
        "domain": scope.domain,
    }


def _scope_matches(item: Mapping[str, Any], scope: CommerceScope) -> bool:
    return all(item.get(key) == value for key, value in _scope_fields(scope).items())


def _safe_id(value: object, field_name: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field_name} is invalid")
    return value


def _migration_item_id(value: object) -> str:
    if type(value) is not str or _MIGRATION_ITEM_ID.fullmatch(value) is None:
        raise ValueError("migration item id is invalid")
    return value


def _migration_reason_code(value: object) -> str:
    if type(value) is not str or value not in _MIGRATION_REASON_CODES:
        raise ValueError("migration reason code is invalid")
    return value


def _hash_value(value: object, field_name: str) -> str:
    if type(value) is not str or _HASH.fullmatch(value) is None:
        raise ValueError(f"{field_name} is invalid")
    return value


def _actor_hash(value: object) -> str:
    return _hash_value(value, "actor_hash")


def _request_id(value: object) -> str:
    if type(value) is not str or not 1 <= len(value) <= 128 or any(ord(c) < 33 for c in value):
        raise ValueError("request_id is invalid")
    return value


def _idempotency_digest(value: object) -> str:
    if type(value) is not str or not 1 <= len(value) <= 256 or any(ord(c) < 32 for c in value):
        raise ValueError("idempotency key is invalid")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _bounded_int(value: object, minimum: int, maximum: int, field_name: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field_name} is invalid")
    return value


def _positive_int(value: object, field_name: str) -> int:
    return _bounded_int(value, 1, 9_999_999_999, field_name)


def _nonnegative_int(value: object, field_name: str) -> int:
    return _bounded_int(value, 0, 9_999_999_999, field_name)


def _epoch(value: object, field_name: str) -> int:
    return _nonnegative_int(value, field_name)


def _hash_json(value: object) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError("migration value is invalid") from None
    return hashlib.sha256(encoded).hexdigest()


def _client_token(scope: CommerceScope, operation: object) -> str:
    return _hash_json({"scope": scope.partition_key, "operation": operation})[:36]
