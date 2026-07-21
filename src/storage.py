"""Conditional cross-table persistence for Commerce inventory transactions."""

from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import json
import os
import re
from typing import Any, Mapping

try:  # Lambda packages the contents of src/ at the import root.
    from domain.inventory import RECONCILER_INTERVAL_SECONDS, StockState, reservation_timing
    from domain.orders import PendingOrder
except ModuleNotFoundError:  # Repository-root unit tests use src.*.
    from src.domain.inventory import RECONCILER_INTERVAL_SECONDS, StockState, reservation_timing
    from src.domain.orders import PendingOrder


IDEMPOTENCY_TTL_SECONDS = 90 * 24 * 60 * 60
MAX_TRANSACTION_ACTIONS = 100
MAX_TRANSACTION_BYTES = 4 * 1024 * 1024
MAX_DUE_PAGE_SIZE = 100

_COMMIT_REASONS = frozenset({"canonical_paid"})
_RELEASE_REASONS = frozenset(
    {
        "canonical_not_created",
        "canonical_terminal_unpaid",
        "confirmed_not_created",
        "confirmed_expiry_precondition_not_created",
    }
)

_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)
_ACTOR_HASH = re.compile(r"[a-f0-9]{64}", re.ASCII)


class StorageError(RuntimeError):
    pass


class StorageConflict(StorageError):
    pass


class StorageNotFound(StorageError):
    pass


class StorageLimitExceeded(StorageError):
    pass


class StorageOutcomeUnknown(StorageError):
    """The caller must reconcile or safely retry the exact same request."""


class ConditionalWriteFailed(RuntimeError):
    """Backend-neutral confirmed conditional transaction failure."""


@dataclass(frozen=True, slots=True)
class CommerceScope:
    environment: str
    tenant_id: str
    draft_id: str

    def __post_init__(self) -> None:
        if self.environment not in {"test", "production"}:
            raise ValueError("environment must be test or production")
        _safe_id(self.tenant_id, "tenant_id")
        _safe_id(self.draft_id, "draft_id")

    @property
    def partition_key(self) -> str:
        return (
            f"ENV#{self.environment}#TENANT#{self.tenant_id}#"
            f"DRAFT#{self.draft_id}"
        )


class CommerceStore:
    """Builds bounded, atomic inventory writes from trusted server snapshots."""

    def __init__(self, backend: Any, catalog_table_name: str, operations_table_name: str) -> None:
        self.backend = backend
        self.catalog_table_name = _table_name(catalog_table_name)
        self.operations_table_name = _table_name(operations_table_name)
        if self.catalog_table_name == self.operations_table_name:
            raise ValueError("catalog and operations tables must be distinct")

    @classmethod
    def from_environment(cls) -> "CommerceStore":
        catalog = os.environ.get("COMMERCE_CATALOG_TABLE_NAME", "").strip()
        operations = os.environ.get("COMMERCE_OPERATIONS_TABLE_NAME", "").strip()
        if not catalog or not operations:
            raise RuntimeError("Commerce table names are required")
        import boto3  # Runtime dependency; tests inject a fake backend.

        return cls(_DynamoBackend(boto3.client("dynamodb")), catalog, operations)

    def adjust_stock(
        self,
        scope: CommerceScope,
        stock_id: str,
        delta: int,
        expected_revision: int,
        *,
        location_id: str,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str | None,
        now_epoch: int,
    ) -> dict[str, Any]:
        scope = _scope(scope)
        stock_id = _safe_id(stock_id, "stock_id")
        location_id = _safe_id(location_id, "location_id")
        if type(delta) is not int or delta == 0:
            raise ValueError("delta must be a non-zero integer")
        expected_revision = _epoch(expected_revision, "expected_revision")
        metadata = _metadata(request_id, correlation_id, actor_hash, now_epoch)
        request = {
            "action": "adjust",
            "stockId": stock_id,
            "locationId": location_id,
            "delta": delta,
            "expectedRevision": expected_revision,
        }
        replay, receipt = self._replay(scope, idempotency_key, request)
        if replay is not None:
            return replay

        current_item = self.backend.get(
            self.catalog_table_name,
            scope.partition_key,
            f"STOCK#{location_id}#{stock_id}",
        )
        if current_item is None:
            if expected_revision != 0 or delta < 1:
                raise StorageConflict("new tracked stock requires a positive initial adjustment from revision zero")
            current = None
            updated = StockState(stock_id, location_id, True, delta, 0, 1)
            stock_condition: Any = "absent"
        else:
            current = _stored_stock(current_item, scope, location_id, stock_id)
            if expected_revision == 0 or current.revision != expected_revision:
                raise StorageConflict("stock revision changed")
            try:
                updated = current.adjust(delta)
            except ValueError as exc:
                raise StorageConflict(str(exc)) from None
            stock_condition = _stock_condition(current)
        result = _stock_result(updated, "adjust")
        movement = self._movement(
            scope,
            stock_id,
            location_id,
            "adjust",
            abs(delta),
            delta,
            0,
            delta,
            receipt,
            metadata,
        )
        operations = [
            self._put(
                self.catalog_table_name,
                _stock_item(scope, updated),
                condition=stock_condition,
            ),
            self._put(self.catalog_table_name, movement, condition="absent"),
            self._put(
                self.operations_table_name,
                _receipt_item(scope, receipt, result, metadata),
                condition="absent",
            ),
        ]
        concurrent = self._execute(scope, operations, receipt)
        return concurrent if concurrent is not None else result

    def reserve_checkout(
        self,
        scope: CommerceScope,
        order: PendingOrder,
        reservation_id: str,
        *,
        location_id: str,
        created_at_epoch: int,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str | None,
        now_epoch: int,
    ) -> dict[str, Any]:
        scope = _scope(scope)
        if type(order) is not PendingOrder:
            raise ValueError("order must be an immutable PendingOrder")
        reservation_id = _safe_id(reservation_id, "reservation_id")
        location_id = _safe_id(location_id, "location_id")
        timing = reservation_timing(created_at_epoch)
        metadata = _metadata(request_id, correlation_id, actor_hash, now_epoch)
        if timing.reservation_created_at != metadata["now_epoch"]:
            raise ValueError("created_at_epoch and now_epoch must use one server timestamp")
        request = {
            "action": "reserve",
            "reservationId": reservation_id,
            "order": _order_snapshot(order),
            "locationId": location_id,
            "createdAt": timing.reservation_created_at,
        }
        replay, receipt = self._replay(scope, idempotency_key, request)
        if replay is not None:
            return replay

        requirements: dict[str, dict[str, Any]] = {}
        for line in order.lines:
            if line.stock_id is None:
                continue
            requirement = requirements.setdefault(
                line.stock_id,
                {"stockId": line.stock_id, "locationId": location_id, "quantity": 0, "lineIds": []},
            )
            requirement["quantity"] += line.quantity
            requirement["lineIds"].append(line.line_id)

        transitions: list[tuple[StockState, StockState, dict[str, Any]]] = []
        allocations: list[dict[str, Any]] = []
        for stock_id in sorted(requirements):
            requirement = requirements[stock_id]
            current = self._stock(scope, location_id, stock_id)
            try:
                updated = current.reserve(requirement["quantity"])
            except ValueError as exc:
                raise StorageConflict(str(exc)) from None
            allocation = {
                "stockId": stock_id,
                "locationId": location_id,
                "quantity": requirement["quantity"],
                "lineIds": sorted(requirement["lineIds"]),
            }
            transitions.append((current, updated, allocation))
            allocations.append(allocation)

        allocation_hash = _hash_json({"allocations": allocations})
        due_sk = _due_key(timing.reconcile_after, reservation_id)
        result = {
            "reservationId": reservation_id,
            "orderId": order.order_id,
            "paymentAttemptId": order.payment_attempt_id,
            "status": "reserved",
            "reservationCreatedAt": timing.reservation_created_at,
            "checkoutExpiresAt": timing.checkout_expires_at,
            "reconcileAfter": timing.reconcile_after,
        }
        operations: list[dict[str, Any]] = []
        for current, updated, allocation in transitions:
            operations.append(
                self._put(
                    self.catalog_table_name,
                    _stock_item(scope, updated),
                    condition=_stock_condition(current),
                )
            )
            operations.append(
                self._put(
                    self.catalog_table_name,
                    self._movement(
                        scope,
                        allocation["stockId"],
                        location_id,
                        "reserve",
                        allocation["quantity"],
                        0,
                        allocation["quantity"],
                        -allocation["quantity"],
                        receipt,
                        metadata,
                        reservation_id=reservation_id,
                        line_ids=allocation["lineIds"],
                    ),
                    condition="absent",
                )
            )
        operations.extend(
            [
                self._put(
                    self.catalog_table_name,
                    {
                        "pk": scope.partition_key,
                        "sk": f"RESERVATION#{reservation_id}",
                        "itemType": "Reservation",
                        **_scope_fields(scope),
                        "reservationId": reservation_id,
                        "orderId": order.order_id,
                        "paymentAttemptId": order.payment_attempt_id,
                        "status": "reserved",
                        "revision": 1,
                        "allocations": allocations,
                        "allocationHash": allocation_hash,
                        "reservationCreatedAt": timing.reservation_created_at,
                        "checkoutExpiresAt": timing.checkout_expires_at,
                        "reconcileAfter": timing.reconcile_after,
                        "initialReconcileAfter": timing.reconcile_after,
                        "dueKey": due_sk,
                        **_operation_fields(metadata),
                    },
                    condition="absent",
                ),
                self._put(
                    self.catalog_table_name,
                    {
                        "pk": scope.partition_key,
                        "sk": due_sk,
                        "itemType": "ReservationDue",
                        **_scope_fields(scope),
                        "reservationId": reservation_id,
                        "reconcileAfter": timing.reconcile_after,
                    },
                    condition="absent",
                ),
                self._put(
                    self.operations_table_name,
                    {
                        "pk": scope.partition_key,
                        "sk": f"ORDER#{order.order_id}",
                        "itemType": "Order",
                        **_scope_fields(scope),
                        "orderId": order.order_id,
                        "reservationId": reservation_id,
                        "paymentAttemptId": order.payment_attempt_id,
                        "status": "pending_checkout",
                        "revision": 1,
                        "lines": _order_snapshot(order)["lines"],
                        "amountMinor": order.total.amount_minor,
                        "currency": order.total.currency,
                        "createdAt": timing.reservation_created_at,
                        "checkoutExpiresAt": timing.checkout_expires_at,
                        **_operation_fields(metadata),
                    },
                    condition="absent",
                ),
                self._put(
                    self.operations_table_name,
                    {
                        "pk": scope.partition_key,
                        "sk": f"PAYMENT_ATTEMPT#{order.payment_attempt_id}",
                        "itemType": "PaymentAttemptBinding",
                        **_scope_fields(scope),
                        "paymentAttemptId": order.payment_attempt_id,
                        "orderId": order.order_id,
                        "reservationId": reservation_id,
                        "requestHash": receipt["requestHash"],
                        "createdAt": timing.reservation_created_at,
                        **_operation_fields(metadata),
                    },
                    condition="absent",
                ),
                self._put(
                    self.operations_table_name,
                    _receipt_item(scope, receipt, result, metadata),
                    condition="absent",
                ),
            ]
        )
        concurrent = self._execute(scope, operations, receipt)
        return concurrent if concurrent is not None else result

    def commit_reservation(
        self,
        scope: CommerceScope,
        reservation_id: str,
        *,
        completion_reason: str,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str | None,
        now_epoch: int,
    ) -> dict[str, Any]:
        return self._finish_reservation(
            scope,
            reservation_id,
            "committed",
            completion_reason=completion_reason,
            idempotency_key=idempotency_key,
            request_id=request_id,
            correlation_id=correlation_id,
            actor_hash=actor_hash,
            now_epoch=now_epoch,
        )

    def release_reservation(
        self,
        scope: CommerceScope,
        reservation_id: str,
        *,
        completion_reason: str,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str | None,
        now_epoch: int,
    ) -> dict[str, Any]:
        return self._finish_reservation(
            scope,
            reservation_id,
            "released",
            completion_reason=completion_reason,
            idempotency_key=idempotency_key,
            request_id=request_id,
            correlation_id=correlation_id,
            actor_hash=actor_hash,
            now_epoch=now_epoch,
        )

    def list_due_reservations(
        self,
        scope: CommerceScope,
        through_epoch: int,
        *,
        limit: int = 25,
    ) -> list[str]:
        scope = _scope(scope)
        through_epoch = _epoch(through_epoch, "through_epoch")
        if type(limit) is not int or not 1 <= limit <= MAX_DUE_PAGE_SIZE:
            raise ValueError("limit must be between 1 and 100")
        items = self.backend.query_due(
            self.catalog_table_name,
            scope.partition_key,
            through_epoch,
            limit,
        )
        reservation_ids = []
        for item in items:
            if (
                not isinstance(item, Mapping)
                or item.get("pk") != scope.partition_key
                or not _scope_matches(item, scope)
                or item.get("itemType") != "ReservationDue"
                or type(item.get("reconcileAfter")) is not int
                or item["reconcileAfter"] > through_epoch
            ):
                raise StorageConflict("invalid reservation due marker")
            reservation_ids.append(_safe_id(item.get("reservationId"), "reservation_id"))
        return reservation_ids

    def defer_reservation(
        self,
        scope: CommerceScope,
        reservation_id: str,
        next_reconcile_at: int,
        *,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str | None,
        now_epoch: int,
    ) -> dict[str, Any]:
        scope = _scope(scope)
        reservation_id = _safe_id(reservation_id, "reservation_id")
        next_reconcile_at = _epoch(next_reconcile_at, "next_reconcile_at")
        metadata = _metadata(request_id, correlation_id, actor_hash, now_epoch)
        if next_reconcile_at != metadata["now_epoch"] + RECONCILER_INTERVAL_SECONDS:
            raise ValueError("next_reconcile_at must be exactly five minutes after now_epoch")
        request = {
            "action": "defer_reconciliation",
            "reservationId": reservation_id,
            "nextReconcileAt": next_reconcile_at,
        }
        replay, receipt = self._replay(scope, idempotency_key, request)
        if replay is not None:
            return replay
        reservation = _reservation(
            self.backend.get(
                self.catalog_table_name,
                scope.partition_key,
                f"RESERVATION#{reservation_id}",
            ),
            scope,
            reservation_id,
        )
        if reservation["status"] != "reserved":
            raise StorageConflict("only an active reservation can be deferred")
        if metadata["now_epoch"] < reservation["reconcileAfter"]:
            raise StorageConflict("reservation is not due for reconciliation")
        if next_reconcile_at <= reservation["reconcileAfter"]:
            raise StorageConflict("reconciliation must move forward")
        new_due_key = _due_key(next_reconcile_at, reservation_id)
        result = {
            "reservationId": reservation_id,
            "status": "reserved",
            "reconcileAfter": next_reconcile_at,
        }
        updated_reservation = copy.deepcopy(reservation)
        updated_reservation.update(
            {
                "revision": reservation["revision"] + 1,
                "reconcileAfter": next_reconcile_at,
                "dueKey": new_due_key,
                "lastReconciledAt": metadata["now_epoch"],
                **_operation_fields(metadata),
            }
        )
        operations = [
            self._put(
                self.catalog_table_name,
                updated_reservation,
                condition={
                    "status": "reserved",
                    "revision": reservation["revision"],
                    "dueKey": reservation["dueKey"],
                },
            ),
            {
                "kind": "delete",
                "table_name": self.catalog_table_name,
                "pk": scope.partition_key,
                "sk": reservation["dueKey"],
                "condition": {"reservationId": reservation_id},
            },
            self._put(
                self.catalog_table_name,
                {
                    "pk": scope.partition_key,
                    "sk": new_due_key,
                    "itemType": "ReservationDue",
                    **_scope_fields(scope),
                    "reservationId": reservation_id,
                    "reconcileAfter": next_reconcile_at,
                },
                condition="absent",
            ),
            self._put(
                self.operations_table_name,
                _receipt_item(scope, receipt, result, metadata),
                condition="absent",
            ),
        ]
        concurrent = self._execute(scope, operations, receipt)
        return concurrent if concurrent is not None else result

    def _finish_reservation(
        self,
        scope: CommerceScope,
        reservation_id: str,
        target_status: str,
        *,
        completion_reason: str,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str | None,
        now_epoch: int,
    ) -> dict[str, Any]:
        scope = _scope(scope)
        reservation_id = _safe_id(reservation_id, "reservation_id")
        completion_reason = _completion_reason(target_status, completion_reason)
        metadata = _metadata(request_id, correlation_id, actor_hash, now_epoch)
        request = {
            "action": target_status,
            "reservationId": reservation_id,
            "completionReason": completion_reason,
        }
        replay, receipt = self._replay(scope, idempotency_key, request)
        if replay is not None:
            return replay
        reservation = self.backend.get(
            self.catalog_table_name,
            scope.partition_key,
            f"RESERVATION#{reservation_id}",
        )
        reservation = _reservation(reservation, scope, reservation_id)
        if reservation["status"] == target_status:
            if reservation.get("completionReason") != completion_reason:
                raise StorageConflict("reservation already completed for a different reason")
            return _terminal_result(reservation)
        if reservation["status"] != "reserved":
            raise StorageConflict("reservation already reached a different terminal state")
        order = self.backend.get(
            self.operations_table_name,
            scope.partition_key,
            f"ORDER#{reservation['orderId']}",
        )
        order = _pending_order(order, scope, reservation)

        transitions: list[tuple[StockState, StockState, Mapping[str, Any]]] = []
        for allocation in reservation["allocations"]:
            current = self._stock(scope, allocation["locationId"], allocation["stockId"])
            try:
                updated = (
                    current.commit(allocation["quantity"])
                    if target_status == "committed"
                    else current.release(allocation["quantity"])
                )
            except ValueError as exc:
                raise StorageConflict(str(exc)) from None
            transitions.append((current, updated, allocation))

        result = {
            "reservationId": reservation_id,
            "orderId": reservation["orderId"],
            "paymentAttemptId": reservation["paymentAttemptId"],
            "status": target_status,
            "reservationCreatedAt": reservation["reservationCreatedAt"],
            "checkoutExpiresAt": reservation["checkoutExpiresAt"],
            "reconcileAfter": reservation["reconcileAfter"],
            "completedAt": metadata["now_epoch"],
            "completionReason": completion_reason,
        }
        movement_type = "commit" if target_status == "committed" else "release"
        operations: list[dict[str, Any]] = []
        for current, updated, allocation in transitions:
            quantity = allocation["quantity"]
            on_hand_delta = -quantity if target_status == "committed" else 0
            available_delta = 0 if target_status == "committed" else quantity
            operations.append(
                self._put(
                    self.catalog_table_name,
                    _stock_item(scope, updated),
                    condition=_stock_condition(current),
                )
            )
            operations.append(
                self._put(
                    self.catalog_table_name,
                    self._movement(
                        scope,
                        allocation["stockId"],
                        allocation["locationId"],
                        movement_type,
                        quantity,
                        on_hand_delta,
                        -quantity,
                        available_delta,
                        receipt,
                        metadata,
                        reservation_id=reservation_id,
                        line_ids=allocation["lineIds"],
                        completion_reason=completion_reason,
                    ),
                    condition="absent",
                )
            )
        updated_reservation = copy.deepcopy(reservation)
        updated_reservation.update(
            {
                "status": target_status,
                "revision": reservation["revision"] + 1,
                "completedAt": metadata["now_epoch"],
                "completionReason": completion_reason,
                **_operation_fields(metadata),
            }
        )
        updated_order = copy.deepcopy(order)
        updated_order.update(
            {
                "status": "paid" if target_status == "committed" else "payment_not_completed",
                "revision": order["revision"] + 1,
                "completedAt": metadata["now_epoch"],
                "completionReason": completion_reason,
                **_operation_fields(metadata),
            }
        )
        operations.extend(
            [
                self._put(
                    self.catalog_table_name,
                    updated_reservation,
                    condition={
                        "status": "reserved",
                        "revision": reservation["revision"],
                        "allocationHash": reservation["allocationHash"],
                    },
                ),
                {
                    "kind": "delete",
                    "table_name": self.catalog_table_name,
                    "pk": scope.partition_key,
                    "sk": reservation["dueKey"],
                    "condition": {"reservationId": reservation_id},
                },
                self._put(
                    self.operations_table_name,
                    updated_order,
                    condition={
                        "status": "pending_checkout",
                        "reservationId": reservation_id,
                        "paymentAttemptId": reservation["paymentAttemptId"],
                        "revision": order["revision"],
                    },
                ),
                self._put(
                    self.operations_table_name,
                    _receipt_item(scope, receipt, result, metadata),
                    condition="absent",
                ),
            ]
        )
        concurrent = self._execute(scope, operations, receipt)
        return concurrent if concurrent is not None else result

    def _stock(self, scope: CommerceScope, location_id: str, stock_id: str) -> StockState:
        item = self.backend.get(
            self.catalog_table_name,
            scope.partition_key,
            f"STOCK#{location_id}#{stock_id}",
        )
        if item is None:
            raise StorageNotFound("tracked stock was not found")
        return _stored_stock(item, scope, location_id, stock_id)

    def _replay(self, scope: CommerceScope, idempotency_key: str, request: Mapping[str, Any]):
        digest = _idempotency_digest(idempotency_key)
        request_hash = _hash_json(request)
        receipt = {"sk": f"IDEMPOTENCY#{digest}", "digest": digest, "requestHash": request_hash}
        existing = self.backend.get(self.operations_table_name, scope.partition_key, receipt["sk"])
        if existing is None:
            return None, receipt
        return _validated_replay(existing, request_hash, scope), receipt

    def _execute(
        self,
        scope: CommerceScope,
        operations: list[dict[str, Any]],
        receipt: Mapping[str, str],
    ) -> dict[str, Any] | None:
        _validate_transaction_plan(operations)
        try:
            self.backend.transact(operations, _client_request_token(scope, operations))
        except ConditionalWriteFailed:
            try:
                existing = self.backend.get(
                    self.operations_table_name,
                    scope.partition_key,
                    receipt["sk"],
                )
            except Exception:
                raise StorageOutcomeUnknown("transaction outcome is unknown") from None
            if existing is not None:
                return _validated_replay(existing, receipt["requestHash"], scope)
            raise StorageConflict("conditional transaction failed") from None
        except Exception:
            try:
                existing = self.backend.get(
                    self.operations_table_name,
                    scope.partition_key,
                    receipt["sk"],
                )
            except Exception:
                raise StorageOutcomeUnknown("transaction outcome is unknown") from None
            if existing is not None:
                return _validated_replay(existing, receipt["requestHash"], scope)
            raise StorageOutcomeUnknown("transaction outcome is unknown") from None
        return None

    def _movement(
        self,
        scope: CommerceScope,
        stock_id: str,
        location_id: str,
        movement_type: str,
        quantity: int,
        on_hand_delta: int,
        reserved_delta: int,
        available_delta: int,
        receipt: Mapping[str, str],
        metadata: Mapping[str, Any],
        *,
        reservation_id: str | None = None,
        line_ids: list[str] | None = None,
        completion_reason: str | None = None,
    ) -> dict[str, Any]:
        movement_id = hashlib.sha256(
            f"{receipt['digest']}:{movement_type}:{location_id}:{stock_id}".encode("utf-8")
        ).hexdigest()
        item = {
            "pk": scope.partition_key,
            "sk": f"STOCK_MOVEMENT#{stock_id}#{movement_id}",
            "itemType": "StockMovement",
            **_scope_fields(scope),
            "movementId": movement_id,
            "movementType": movement_type,
            "stockId": stock_id,
            "locationId": location_id,
            "quantity": quantity,
            "onHandDelta": on_hand_delta,
            "reservedDelta": reserved_delta,
            "availableDelta": available_delta,
            "occurredAt": metadata["now_epoch"],
            **_operation_fields(metadata),
        }
        if reservation_id is not None:
            item["reservationId"] = reservation_id
        if line_ids:
            item["lineIds"] = list(line_ids)
        if completion_reason is not None:
            item["completionReason"] = completion_reason
        return item

    @staticmethod
    def _put(table_name: str, item: Mapping[str, Any], *, condition: Any) -> dict[str, Any]:
        return {
            "kind": "put",
            "table_name": table_name,
            "item": copy.deepcopy(dict(item)),
            "condition": condition,
        }


class _DynamoBackend:
    """Small boto3 adapter; deterministic fakes exercise domain transactions."""

    def __init__(self, client: Any) -> None:
        self.client = client

    def get(self, table_name: str, pk: str, sk: str):
        response = self.client.get_item(
            TableName=table_name,
            Key={"pk": {"S": pk}, "sk": {"S": sk}},
            ConsistentRead=True,
        )
        item = response.get("Item")
        return _from_item(item) if item else None

    def query_due(self, table_name: str, pk: str, through_epoch: int, limit: int):
        prefix = "RESERVATION_DUE#"
        response = self.client.query(
            TableName=table_name,
            KeyConditionExpression="#pk = :pk AND #sk BETWEEN :start AND :end",
            ExpressionAttributeNames={"#pk": "pk", "#sk": "sk"},
            ExpressionAttributeValues={
                ":pk": {"S": pk},
                ":start": {"S": prefix},
                ":end": {"S": f"{prefix}{through_epoch:020d}#\uffff"},
            },
            Limit=limit,
            ConsistentRead=True,
        )
        return [_from_item(item) for item in response.get("Items", [])]

    def transact(self, operations: list[dict[str, Any]], client_token: str) -> None:
        _validate_transaction_plan(operations)
        if type(client_token) is not str or not 1 <= len(client_token) <= 36:
            raise ValueError("client_token must contain 1 to 36 characters")
        transact_items = [_transaction_item(operation) for operation in operations]
        try:
            self.client.transact_write_items(
                TransactItems=transact_items,
                ClientRequestToken=client_token,
            )
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            reasons = getattr(exc, "response", {}).get("CancellationReasons", [])
            if code == "ConditionalCheckFailedException" or (
                code == "TransactionCanceledException"
                and any(
                    isinstance(reason, Mapping) and reason.get("Code") == "ConditionalCheckFailed"
                    for reason in reasons
                )
            ):
                raise ConditionalWriteFailed() from None
            raise


def _validate_transaction_plan(operations: object) -> None:
    if type(operations) is not list or not operations:
        raise StorageLimitExceeded("transaction must contain at least one action")
    if len(operations) > MAX_TRANSACTION_ACTIONS:
        raise StorageLimitExceeded("transaction exceeds the DynamoDB action limit")
    targets = set()
    transact_items = []
    for operation in operations:
        if not isinstance(operation, Mapping):
            raise ValueError("transaction operation is invalid")
        item = operation.get("item", {})
        table_name = _table_name(operation.get("table_name"))
        pk = operation.get("pk") or item.get("pk")
        sk = operation.get("sk") or item.get("sk")
        if type(pk) is not str or not pk or type(sk) is not str or not sk:
            raise ValueError("transaction target is invalid")
        target = (table_name, pk, sk)
        if target in targets:
            raise StorageLimitExceeded("transaction targets one item more than once")
        targets.add(target)
        transact_items.append(_transaction_item(operation))
    encoded = json.dumps(
        {"TransactItems": transact_items},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > MAX_TRANSACTION_BYTES:
        raise StorageLimitExceeded("transaction exceeds the DynamoDB size limit")


def _transaction_item(operation: Mapping[str, Any]) -> dict[str, Any]:
    table_name = _table_name(operation.get("table_name"))
    kind = operation.get("kind")
    if kind == "put":
        if not isinstance(operation.get("item"), Mapping):
            raise ValueError("put item is invalid")
        put = {"TableName": table_name, "Item": _to_item(operation["item"])}
        _apply_condition(put, operation.get("condition"))
        return {"Put": put}
    if kind == "delete":
        delete = {
            "TableName": table_name,
            "Key": {
                "pk": {"S": operation.get("pk")},
                "sk": {"S": operation.get("sk")},
            },
        }
        _apply_condition(delete, operation.get("condition"))
        return {"Delete": delete}
    raise ValueError("unsupported transaction operation")


def _apply_condition(operation: dict[str, Any], condition: Any) -> None:
    if condition == "absent":
        operation["ConditionExpression"] = "attribute_not_exists(#pk) AND attribute_not_exists(#sk)"
        operation["ExpressionAttributeNames"] = {"#pk": "pk", "#sk": "sk"}
        return
    if isinstance(condition, Mapping) and condition:
        names = {}
        values = {}
        clauses = []
        for index, (field, expected) in enumerate(condition.items()):
            name, value = f"#field{index}", f":expected{index}"
            names[name] = str(field)
            values[value] = _to_attribute(expected)
            clauses.append(f"{name} = {value}")
        operation["ConditionExpression"] = " AND ".join(clauses)
        operation["ExpressionAttributeNames"] = names
        operation["ExpressionAttributeValues"] = values


def _to_item(item: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _to_attribute(value) for key, value in item.items()}


def _to_attribute(value: Any) -> dict[str, Any]:
    if value is None:
        return {"NULL": True}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, str):
        return {"S": value}
    if type(value) is int:
        return {"N": str(value)}
    if isinstance(value, list):
        return {"L": [_to_attribute(item) for item in value]}
    if isinstance(value, Mapping):
        return {"M": {str(key): _to_attribute(item) for key, item in value.items()}}
    raise TypeError(f"unsupported DynamoDB value: {type(value).__name__}")


def _from_item(item: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _from_attribute(value) for key, value in item.items()}


def _from_attribute(value: Mapping[str, Any]) -> Any:
    if "NULL" in value:
        return None
    if "BOOL" in value:
        return value["BOOL"]
    if "S" in value:
        return value["S"]
    if "N" in value:
        return int(value["N"])
    if "L" in value:
        return [_from_attribute(item) for item in value["L"]]
    if "M" in value:
        return {key: _from_attribute(item) for key, item in value["M"].items()}
    raise TypeError("unsupported DynamoDB attribute")


def _scope(value: object) -> CommerceScope:
    if type(value) is not CommerceScope:
        raise ValueError("scope must be an immutable CommerceScope")
    return value


def _safe_id(value: object, field_name: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe canonical identifier")
    return value


def _table_name(value: object) -> str:
    if type(value) is not str or not value.strip() or any(ord(character) < 33 for character in value):
        raise ValueError("table_name is invalid")
    return value


def _epoch(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _positive_integer(value: object, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _metadata(request_id: object, correlation_id: object, actor_hash: object, now_epoch: object):
    result = {
        "request_id": _safe_id(request_id, "request_id"),
        "correlation_id": _safe_id(correlation_id, "correlation_id"),
        "now_epoch": _epoch(now_epoch, "now_epoch"),
        "actor_hash": None,
    }
    if actor_hash is not None:
        if type(actor_hash) is not str or _ACTOR_HASH.fullmatch(actor_hash) is None:
            raise ValueError("actor_hash must be a lowercase SHA-256 digest")
        result["actor_hash"] = actor_hash
    return result


def _operation_fields(metadata: Mapping[str, Any]) -> dict[str, Any]:
    fields = {
        "requestId": metadata["request_id"],
        "correlationId": metadata["correlation_id"],
    }
    if metadata["actor_hash"] is not None:
        fields["actorHash"] = metadata["actor_hash"]
    return fields


def _scope_fields(scope: CommerceScope) -> dict[str, str]:
    return {
        "environment": scope.environment,
        "tenantId": scope.tenant_id,
        "draftId": scope.draft_id,
    }


def _scope_matches(item: Mapping[str, Any], scope: CommerceScope) -> bool:
    return (
        item.get("pk") == scope.partition_key
        and item.get("environment") == scope.environment
        and item.get("tenantId") == scope.tenant_id
        and item.get("draftId") == scope.draft_id
    )


def _stock_item(scope: CommerceScope, stock: StockState) -> dict[str, Any]:
    return {
        "pk": scope.partition_key,
        "sk": f"STOCK#{stock.location_id}#{stock.stock_id}",
        "itemType": "Stock",
        **_scope_fields(scope),
        "stockId": stock.stock_id,
        "locationId": stock.location_id,
        "tracked": stock.tracked,
        "onHand": stock.on_hand,
        "reserved": stock.reserved,
        "available": stock.available,
        "revision": stock.revision,
    }


def _stored_stock(
    item: Any,
    scope: CommerceScope,
    location_id: str,
    stock_id: str,
) -> StockState:
    if not isinstance(item, Mapping) or item.get("itemType") != "Stock":
        raise StorageConflict("stored stock is invalid")
    try:
        state = StockState(
            stock_id=item.get("stockId"),
            location_id=item.get("locationId"),
            tracked=item.get("tracked"),
            on_hand=item.get("onHand"),
            reserved=item.get("reserved"),
            revision=item.get("revision"),
        )
    except ValueError:
        raise StorageConflict("stored stock is invalid") from None
    if not _scope_matches(item, scope) or state.stock_id != stock_id or state.location_id != location_id:
        raise StorageConflict("stored stock scope is invalid")
    if item.get("available") != state.available:
        raise StorageConflict("stored stock invariant is invalid")
    if not state.tracked:
        raise StorageConflict("referenced stock is not tracked")
    return state


def _stock_condition(stock: StockState) -> dict[str, Any]:
    return {
        "tracked": True,
        "onHand": stock.on_hand,
        "reserved": stock.reserved,
        "available": stock.available,
        "revision": stock.revision,
    }


def _stock_result(stock: StockState, action: str) -> dict[str, Any]:
    return {
        "action": action,
        "stockId": stock.stock_id,
        "locationId": stock.location_id,
        "onHand": stock.on_hand,
        "reserved": stock.reserved,
        "available": stock.available,
        "revision": stock.revision,
    }


def _order_snapshot(order: PendingOrder) -> dict[str, Any]:
    return {
        "orderId": order.order_id,
        "paymentAttemptId": order.payment_attempt_id,
        "lines": [
            {
                "lineId": line.line_id,
                "offerVersionId": line.offer_version_id,
                "quantity": line.quantity,
                "amountMinor": line.unit_price.amount_minor,
                "currency": line.unit_price.currency,
                "stockId": line.stock_id,
            }
            for line in order.lines
        ],
    }


def _receipt_item(
    scope: CommerceScope,
    receipt: Mapping[str, str],
    result: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "pk": scope.partition_key,
        "sk": receipt["sk"],
        "itemType": "IdempotencyReceipt",
        **_scope_fields(scope),
        "requestHash": receipt["requestHash"],
        "result": copy.deepcopy(dict(result)),
        "createdAt": metadata["now_epoch"],
        "expiresAt": metadata["now_epoch"] + IDEMPOTENCY_TTL_SECONDS,
        **_operation_fields(metadata),
    }


def _validated_replay(
    existing: Mapping[str, Any],
    request_hash: str,
    scope: CommerceScope,
) -> dict[str, Any]:
    if (
        existing.get("itemType") != "IdempotencyReceipt"
        or existing.get("requestHash") != request_hash
        or not _scope_matches(existing, scope)
    ):
        raise StorageConflict("idempotency key was already used")
    result = existing.get("result")
    if not isinstance(result, Mapping):
        raise StorageConflict("idempotency receipt is invalid")
    return copy.deepcopy(dict(result))


def _reservation(item: Any, scope: CommerceScope, reservation_id: str) -> dict[str, Any]:
    if not isinstance(item, Mapping) or item.get("itemType") != "Reservation":
        raise StorageNotFound("reservation was not found")
    reservation = copy.deepcopy(dict(item))
    if not _scope_matches(reservation, scope) or reservation.get("reservationId") != reservation_id:
        raise StorageConflict("stored reservation scope is invalid")
    if reservation.get("status") not in {"reserved", "committed", "released"}:
        raise StorageConflict("stored reservation status is invalid")
    if type(reservation.get("revision")) is not int or reservation["revision"] <= 0:
        raise StorageConflict("stored reservation revision is invalid")
    allocations = reservation.get("allocations")
    if type(allocations) is not list:
        raise StorageConflict("stored reservation allocations are invalid")
    seen = set()
    for allocation in allocations:
        if not isinstance(allocation, Mapping):
            raise StorageConflict("stored reservation allocation is invalid")
        stock_id = _safe_id(allocation.get("stockId"), "stock_id")
        _safe_id(allocation.get("locationId"), "location_id")
        _positive_integer(allocation.get("quantity"), "quantity")
        line_ids = allocation.get("lineIds")
        if type(line_ids) is not list or not line_ids or any(
            _SAFE_ID.fullmatch(value) is None for value in line_ids if type(value) is str
        ) or any(type(value) is not str for value in line_ids):
            raise StorageConflict("stored reservation line IDs are invalid")
        if stock_id in seen:
            raise StorageConflict("stored reservation repeats a stock target")
        seen.add(stock_id)
    expected_hash = _hash_json({"allocations": allocations})
    if reservation.get("allocationHash") != expected_hash:
        raise StorageConflict("stored reservation allocation hash is invalid")
    for field in ("orderId", "paymentAttemptId"):
        _safe_id(reservation.get(field), field)
    for field in (
        "reservationCreatedAt",
        "checkoutExpiresAt",
        "initialReconcileAfter",
        "reconcileAfter",
    ):
        _epoch(reservation.get(field), field)
    timing = reservation_timing(reservation["reservationCreatedAt"])
    if (
        reservation["checkoutExpiresAt"] != timing.checkout_expires_at
        or reservation["initialReconcileAfter"] != timing.reconcile_after
        or reservation["reconcileAfter"] < reservation["initialReconcileAfter"]
    ):
        raise StorageConflict("stored reservation timing is invalid")
    due_key = reservation.get("dueKey")
    if due_key != _due_key(reservation["reconcileAfter"], reservation_id):
        raise StorageConflict("stored reservation due key is invalid")
    return reservation


def _pending_order(
    item: Any,
    scope: CommerceScope,
    reservation: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(item, Mapping) or item.get("itemType") != "Order":
        raise StorageNotFound("reservation order was not found")
    order = copy.deepcopy(dict(item))
    if (
        not _scope_matches(order, scope)
        or order.get("orderId") != reservation["orderId"]
        or order.get("reservationId") != reservation["reservationId"]
        or order.get("paymentAttemptId") != reservation["paymentAttemptId"]
        or order.get("status") != "pending_checkout"
        or type(order.get("revision")) is not int
        or order["revision"] <= 0
    ):
        raise StorageConflict("stored reservation order is invalid")
    return order


def _terminal_result(reservation: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        "reservationId": reservation["reservationId"],
        "orderId": reservation["orderId"],
        "paymentAttemptId": reservation["paymentAttemptId"],
        "status": reservation["status"],
        "reservationCreatedAt": reservation["reservationCreatedAt"],
        "checkoutExpiresAt": reservation["checkoutExpiresAt"],
        "reconcileAfter": reservation["reconcileAfter"],
    }
    if type(reservation.get("completedAt")) is int:
        result["completedAt"] = reservation["completedAt"]
    if type(reservation.get("completionReason")) is str:
        result["completionReason"] = reservation["completionReason"]
    return result


def _due_key(reconcile_after: int, reservation_id: str) -> str:
    return f"RESERVATION_DUE#{reconcile_after:020d}#{reservation_id}"


def _idempotency_digest(value: object) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 256
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError("idempotency_key is invalid")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _completion_reason(target_status: str, value: object) -> str:
    allowed = _COMMIT_REASONS if target_status == "committed" else _RELEASE_REASONS
    if type(value) is not str or value not in allowed:
        raise ValueError("completion_reason is not valid for this terminal transition")
    return value


def _client_request_token(
    scope: CommerceScope,
    operations: list[dict[str, Any]],
) -> str:
    return _hash_json(
        {
            "scope": scope.partition_key,
            "operations": operations,
        }
    )[:36]


def _hash_json(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
