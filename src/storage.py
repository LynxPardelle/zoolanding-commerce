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
MAX_DUE_QUERY_PAGES = 5
RESERVATION_DUE_INDEX_NAME = "ReservationDueIndex"

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
_REQUEST_IDENTIFIER = re.compile(r"[A-Za-z0-9._:-]{1,128}", re.ASCII)
_ACTOR_HASH = re.compile(r"[a-f0-9]{64}", re.ASCII)
_DOMAIN = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+",
    re.ASCII,
)
_VERSION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", re.ASCII)
_NOTIFICATION_TYPE_TEMPLATES = {
    "payment-failed": "payment-failed-v1",
    "payment-succeeded": "payment-succeeded-v1",
}


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
    domain: str

    def __post_init__(self) -> None:
        if self.environment not in {"test", "production"}:
            raise ValueError("environment must be test or production")
        _safe_id(self.tenant_id, "tenant_id")
        _safe_id(self.draft_id, "draft_id")
        if (
            type(self.domain) is not str
            or not 4 <= len(self.domain) <= 253
            or _DOMAIN.fullmatch(self.domain) is None
        ):
            raise ValueError("domain must be a canonical lowercase domain")

    @property
    def partition_key(self) -> str:
        return (
            f"ENV#{self.environment}#TENANT#{self.tenant_id}#"
            f"DRAFT#{self.draft_id}"
        )


@dataclass(frozen=True, slots=True)
class DueReservation:
    scope: CommerceScope
    reservation_id: str
    order_id: str
    payment_attempt_id: str
    reconcile_after: int
    marker_key: str


@dataclass(frozen=True, slots=True)
class InvalidDueMarker:
    partition_key: str | None
    marker_key: str | None
    due_partition: str | None
    due_key: str | None


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
        notification_target: Mapping[str, Any] | None = None,
        fiscal_access: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        scope = _scope(scope)
        if type(order) is not PendingOrder:
            raise ValueError("order must be an immutable PendingOrder")
        reservation_id = _safe_id(reservation_id, "reservation_id")
        location_id = _safe_id(location_id, "location_id")
        timing = reservation_timing(created_at_epoch)
        metadata = _metadata(request_id, correlation_id, actor_hash, now_epoch)
        notification_target = _notification_target(notification_target)
        fiscal_access = _fiscal_access(fiscal_access)
        if timing.reservation_created_at != metadata["now_epoch"]:
            raise ValueError("created_at_epoch and now_epoch must use one server timestamp")
        request = {
            "action": "reserve",
            "reservationId": reservation_id,
            "order": _order_snapshot(order),
            "locationId": location_id,
            "notificationTarget": notification_target,
            "fiscalEnabled": fiscal_access is not None,
        }
        replay, receipt = self._replay(scope, idempotency_key, request)
        if replay is not None:
            if fiscal_access is not None and replay.get("fiscalAccessHash") is not None:
                return self._rotate_fiscal_access_on_checkout_replay(
                    scope,
                    order.order_id,
                    fiscal_access["proofHash"],
                    metadata["now_epoch"],
                    replay,
                )
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
        if fiscal_access is not None:
            result["fiscalAccessHash"] = fiscal_access["proofHash"]
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
        order_item = {
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
        }
        if notification_target is not None:
            order_item["notificationTarget"] = notification_target
        fiscal_access_item = None
        if fiscal_access is not None:
            fiscal_access_item = {
                "pk": scope.partition_key,
                "sk": f"FISCAL_ACCESS#{order.order_id}",
                "itemType": "FiscalOrderAccess",
                **_scope_fields(scope),
                "orderId": order.order_id,
                "proofHash": fiscal_access["proofHash"],
                "state": "pending_payment",
                "attempts": 0,
                "revision": 1,
                "requestWindowSeconds": fiscal_access["windowSeconds"],
                "createdAt": timing.reservation_created_at,
                "expiresAt": timing.reconcile_after + fiscal_access["windowSeconds"],
            }
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
                        "orderId": order.order_id,
                        "paymentAttemptId": order.payment_attempt_id,
                        "reconcileAfter": timing.reconcile_after,
                        "duePartition": _due_partition(scope.environment),
                        "dueKey": _due_index_key(scope, timing.reconcile_after, reservation_id),
                    },
                    condition="absent",
                ),
                self._put(
                    self.operations_table_name,
                    order_item,
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
        if fiscal_access_item is not None:
            operations.insert(
                -1,
                self._put(self.operations_table_name, fiscal_access_item, condition="absent"),
            )
        concurrent = self._execute(scope, operations, receipt)
        return concurrent if concurrent is not None else result

    def _rotate_fiscal_access_on_checkout_replay(
        self,
        scope: CommerceScope,
        order_id: str,
        replacement_hash: str,
        now_epoch: int,
        replay: Mapping[str, Any],
    ) -> dict[str, Any]:
        current = self.backend.get(
            self.operations_table_name,
            scope.partition_key,
            f"FISCAL_ACCESS#{order_id}",
        )
        access = _stored_fiscal_access(current, scope, order_id)
        if access["state"] != "pending_payment" or now_epoch >= access["expiresAt"]:
            return copy.deepcopy(dict(replay))
        if access["proofHash"] == replacement_hash:
            rotated_result = copy.deepcopy(dict(replay))
            rotated_result["fiscalAccessHash"] = replacement_hash
            return rotated_result
        updated = copy.deepcopy(access)
        updated.update({
            "proofHash": replacement_hash,
            "revision": access["revision"] + 1,
            "rotatedAt": now_epoch,
        })
        operation = self._put(
            self.operations_table_name,
            updated,
            condition={
                "state": "pending_payment",
                "proofHash": access["proofHash"],
                "revision": access["revision"],
                "expiresAt": access["expiresAt"],
            },
        )
        try:
            self.backend.transact([operation], _client_request_token(scope, [operation]))
        except Exception as exc:
            latest = self.backend.get(
                self.operations_table_name,
                scope.partition_key,
                f"FISCAL_ACCESS#{order_id}",
            )
            latest = _stored_fiscal_access(latest, scope, order_id)
            if latest["state"] == "pending_payment" and latest["proofHash"] == replacement_hash:
                pass
            elif isinstance(exc, ConditionalWriteFailed):
                return copy.deepcopy(dict(replay))
            else:
                raise StorageOutcomeUnknown("fiscal proof rotation outcome is unknown") from None
        rotated_result = copy.deepcopy(dict(replay))
        rotated_result["fiscalAccessHash"] = replacement_hash
        return rotated_result

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

    def apply_payment_event(
        self,
        scope: CommerceScope,
        *,
        event_id: str,
        event_type: str,
        reservation_id: str,
        order_id: str,
        payment_attempt_id: str,
        occurred_at: int,
        now_epoch: int,
    ) -> dict[str, Any]:
        scope = _scope(scope)
        event_id = _safe_id(event_id, "event_id")
        reservation_id = _safe_id(reservation_id, "reservation_id")
        order_id = _safe_id(order_id, "order_id")
        payment_attempt_id = _safe_id(payment_attempt_id, "payment_attempt_id")
        occurred_at = _epoch(occurred_at, "occurred_at")
        if event_type == "commerce.payment.succeeded.v1":
            target_status, completion_reason, outcome = "committed", "canonical_paid", "payment_succeeded"
        elif event_type == "commerce.payment.terminal_unpaid.v1":
            target_status, completion_reason, outcome = (
                "released",
                "canonical_terminal_unpaid",
                "payment_terminal_unpaid",
            )
        else:
            raise ValueError("unsupported payment event type")
        event_snapshot = {
            "eventId": event_id,
            "eventType": event_type,
            "occurredAt": occurred_at,
            **_scope_fields(scope),
            "reservationId": reservation_id,
            "orderId": order_id,
            "paymentAttemptId": payment_attempt_id,
        }
        event_hash = _hash_json(event_snapshot)
        replay = self._event_replay(scope, event_id, event_hash)
        if replay is not None:
            return replay
        binding = self.backend.get(
            self.operations_table_name,
            scope.partition_key,
            f"PAYMENT_ATTEMPT#{payment_attempt_id}",
        )
        _payment_binding(binding, scope, reservation_id, order_id, payment_attempt_id)
        return self._finish_reservation(
            scope,
            reservation_id,
            target_status,
            completion_reason=completion_reason,
            idempotency_key=f"integration:{event_id}",
            request_id=event_id,
            correlation_id=event_id,
            actor_hash=None,
            now_epoch=now_epoch,
            integration_event={
                **event_snapshot,
                "eventHash": event_hash,
                "outcome": outcome,
            },
        )

    def record_refund_event(
        self,
        scope: CommerceScope,
        *,
        event_id: str,
        order_id: str,
        refund_id: str,
        amount_minor: int,
        currency: str,
        occurred_at: int,
        now_epoch: int,
    ) -> dict[str, Any]:
        scope = _scope(scope)
        event_id = _safe_id(event_id, "event_id")
        order_id = _safe_id(order_id, "order_id")
        refund_id = _safe_id(refund_id, "refund_id")
        if type(amount_minor) is not int or amount_minor <= 0:
            raise ValueError("amount_minor must be a positive integer")
        if type(currency) is not str or re.fullmatch(r"[A-Z]{3}", currency, re.ASCII) is None:
            raise ValueError("currency must be an uppercase three-letter code")
        occurred_at = _epoch(occurred_at, "occurred_at")
        metadata = _metadata(event_id, event_id, None, now_epoch)
        request = {
            "action": "record_refund",
            "eventId": event_id,
            "orderId": order_id,
            "refundId": refund_id,
            "amountMinor": amount_minor,
            "currency": currency,
            "occurredAt": occurred_at,
        }
        replay, receipt = self._replay(scope, f"integration:{event_id}", request)
        if replay is not None:
            return replay
        event_hash = _hash_json({**request, **_scope_fields(scope)})
        replay = self._event_replay(scope, event_id, event_hash)
        if replay is not None:
            return replay
        order = self.backend.get(
            self.operations_table_name,
            scope.partition_key,
            f"ORDER#{order_id}",
        )
        if (
            not isinstance(order, Mapping)
            or not _scope_matches(order, scope)
            or order.get("itemType") != "Order"
            or order.get("orderId") != order_id
            or order.get("status") not in {"pending_checkout", "paid"}
            or order.get("currency") != currency
            or type(order.get("amountMinor")) is not int
            or amount_minor > order["amountMinor"]
        ):
            raise StorageConflict("refund order binding is invalid")
        result = {
            "refundId": refund_id,
            "orderId": order_id,
            "status": "recorded",
            "amountMinor": amount_minor,
            "currency": currency,
            "confirmedAt": occurred_at,
        }
        event_context = {
            "eventId": event_id,
            "eventType": "commerce.refund.confirmed.v1",
            "eventHash": event_hash,
            "occurredAt": occurred_at,
            "orderId": order_id,
            "outcome": "refund_confirmed",
        }
        if order["status"] == "pending_checkout":
            return self._record_refund_from_pending(
                scope,
                order,
                result,
                event_context,
                receipt,
                metadata,
            )
        updated_order = copy.deepcopy(dict(order))
        updated_order.update({
            "status": "refunded",
            "revision": order["revision"] + 1,
            "refundId": refund_id,
            "refundedAt": metadata["now_epoch"],
            **_operation_fields(metadata),
        })
        operations = [
            self._put(
                self.operations_table_name,
                updated_order,
                condition={
                    "itemType": "Order",
                    "status": "paid",
                    "orderId": order_id,
                    "currency": currency,
                    "amountMinor": order["amountMinor"],
                    "revision": order["revision"],
                },
            ),
            self._put(
                self.operations_table_name,
                {
                    "pk": scope.partition_key,
                    "sk": f"REFUND#{refund_id}",
                    "itemType": "RefundProjection",
                    **_scope_fields(scope),
                    **result,
                    **_operation_fields(metadata),
                },
                condition="absent",
            ),
            *self._event_operations(scope, event_context, result, metadata, order=order),
            self._put(
                self.operations_table_name,
                _receipt_item(scope, receipt, result, metadata),
                condition="absent",
            ),
        ]
        concurrent = self._execute(scope, operations, receipt)
        return concurrent if concurrent is not None else result

    def _record_refund_from_pending(
        self,
        scope: CommerceScope,
        order: Mapping[str, Any],
        result: Mapping[str, Any],
        event_context: Mapping[str, Any],
        receipt: Mapping[str, str],
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        reservation_id = _safe_id(order.get("reservationId"), "reservation_id")
        payment_attempt_id = _safe_id(order.get("paymentAttemptId"), "payment_attempt_id")
        reservation = _reservation(
            self.backend.get(
                self.catalog_table_name,
                scope.partition_key,
                f"RESERVATION#{reservation_id}",
            ),
            scope,
            reservation_id,
        )
        pending_order = _pending_order(order, scope, reservation)
        transitions: list[tuple[StockState, StockState, Mapping[str, Any]]] = []
        for allocation in reservation["allocations"]:
            current = self._stock(scope, allocation["locationId"], allocation["stockId"])
            try:
                updated = current.commit(allocation["quantity"])
            except ValueError as exc:
                raise StorageConflict(str(exc)) from None
            transitions.append((current, updated, allocation))

        operations: list[dict[str, Any]] = []
        for current, updated, allocation in transitions:
            quantity = allocation["quantity"]
            operations.extend([
                self._put(
                    self.catalog_table_name,
                    _stock_item(scope, updated),
                    condition=_stock_condition(current),
                ),
                self._put(
                    self.catalog_table_name,
                    self._movement(
                        scope,
                        allocation["stockId"],
                        allocation["locationId"],
                        "commit",
                        quantity,
                        -quantity,
                        -quantity,
                        0,
                        receipt,
                        metadata,
                        reservation_id=reservation_id,
                        line_ids=allocation["lineIds"],
                        completion_reason="canonical_paid",
                    ),
                    condition="absent",
                ),
            ])
        updated_reservation = copy.deepcopy(reservation)
        updated_reservation.update({
            "status": "committed",
            "revision": reservation["revision"] + 1,
            "completedAt": metadata["now_epoch"],
            "completionReason": "canonical_paid",
            **_operation_fields(metadata),
        })
        updated_order = copy.deepcopy(pending_order)
        updated_order.update({
            "status": "refunded",
            "revision": pending_order["revision"] + 1,
            "completedAt": metadata["now_epoch"],
            "completionReason": "canonical_paid",
            "refundId": result["refundId"],
            "refundedAt": metadata["now_epoch"],
            **_operation_fields(metadata),
        })
        binding_context = {
            "reservationId": reservation_id,
            "orderId": pending_order["orderId"],
            "paymentAttemptId": payment_attempt_id,
        }
        operations.extend([
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
                    "paymentAttemptId": payment_attempt_id,
                    "revision": pending_order["revision"],
                },
            ),
            self._payment_binding_check(scope, binding_context),
            *self._fiscal_access_operations(
                scope,
                pending_order,
                "commerce.payment.succeeded.v1",
                metadata["now_epoch"],
            ),
            self._put(
                self.operations_table_name,
                {
                    "pk": scope.partition_key,
                    "sk": f"REFUND#{result['refundId']}",
                    "itemType": "RefundProjection",
                    **_scope_fields(scope),
                    **copy.deepcopy(dict(result)),
                    **_operation_fields(metadata),
                },
                condition="absent",
            ),
            *self._event_operations(
                scope,
                event_context,
                result,
                metadata,
                order=updated_order,
            ),
            self._put(
                self.operations_table_name,
                _receipt_item(scope, receipt, result, metadata),
                condition="absent",
            ),
        ])
        concurrent = self._execute(scope, operations, receipt)
        return concurrent if concurrent is not None else copy.deepcopy(dict(result))

    def get_outbox(self, scope: CommerceScope, event_id: str) -> dict[str, Any]:
        scope = _scope(scope)
        event_id = _safe_id(event_id, "event_id")
        item = self.backend.get(
            self.operations_table_name,
            scope.partition_key,
            f"OUTBOX#{event_id}",
        )
        return _outbox(item, scope, event_id)

    def mark_outbox_delivered(
        self,
        scope: CommerceScope,
        event_id: str,
        *,
        now_epoch: int,
    ) -> dict[str, Any]:
        scope = _scope(scope)
        event_id = _safe_id(event_id, "event_id")
        now_epoch = _epoch(now_epoch, "now_epoch")
        current = self.get_outbox(scope, event_id)
        if current["deliveryStatus"] == "delivered":
            return current
        updated = copy.deepcopy(current)
        updated.update(
            {
                "deliveryStatus": "delivered",
                "deliveredAt": now_epoch,
                "expiresAt": now_epoch + IDEMPOTENCY_TTL_SECONDS,
                "revision": current["revision"] + 1,
            }
        )
        operations = [
            self._put(
                self.operations_table_name,
                updated,
                condition={"deliveryStatus": "pending", "revision": current["revision"]},
            )
        ]
        _validate_transaction_plan(operations)
        try:
            self.backend.transact(operations, _client_request_token(scope, operations))
        except Exception:
            try:
                reread = self.get_outbox(scope, event_id)
            except Exception:
                raise StorageOutcomeUnknown("outbox delivery outcome is unknown") from None
            if reread["deliveryStatus"] == "delivered":
                return reread
            raise StorageOutcomeUnknown("outbox delivery outcome is unknown") from None
        return updated

    def list_due_reservations(
        self,
        environment: str,
        through_epoch: int,
        *,
        limit: int = 25,
        max_pages: int = 1,
    ) -> list[DueReservation | InvalidDueMarker]:
        environment = _environment(environment)
        through_epoch = _epoch(through_epoch, "through_epoch")
        if type(limit) is not int or not 1 <= limit <= MAX_DUE_PAGE_SIZE:
            raise ValueError("limit must be between 1 and 100")
        if type(max_pages) is not int or not 1 <= max_pages <= MAX_DUE_QUERY_PAGES:
            raise ValueError("max_pages must be between 1 and 5")
        items = []
        cursor = None
        for _page in range(max_pages):
            page, cursor = self.backend.query_due(
                self.catalog_table_name,
                _due_partition(environment),
                through_epoch,
                limit,
                cursor,
            )
            items.extend(page)
            if cursor is None:
                break
        due_reservations = []
        for item in items:
            if not isinstance(item, Mapping):
                due_reservations.append(InvalidDueMarker(None, None, None, None))
                continue
            if item.get("duePartition") != _due_partition(environment):
                continue
            projected_pk = item.get("pk")
            marker_key = item.get("sk")
            due_partition = item.get("duePartition")
            due_key = item.get("dueKey")
            invalid = InvalidDueMarker(
                projected_pk if type(projected_pk) is str else None,
                marker_key if type(marker_key) is str else None,
                due_partition if type(due_partition) is str else None,
                due_key if type(due_key) is str else None,
            )
            if (
                type(projected_pk) is not str
                or type(marker_key) is not str
                or not marker_key.startswith("RESERVATION_DUE#")
                or type(due_key) is not str
            ):
                due_reservations.append(invalid)
                continue
            current = self.backend.get(self.catalog_table_name, projected_pk, marker_key)
            if current is None:
                continue  # A GSI may briefly return a marker already deleted from the base table.
            try:
                scope = CommerceScope(
                    current.get("environment"),
                    current.get("tenantId"),
                    current.get("draftId"),
                    current.get("domain"),
                )
                expected_due_key = _due_index_key(
                    scope,
                    current.get("reconcileAfter"),
                    current.get("reservationId"),
                )
                expected_marker_key = _due_key(
                    current.get("reconcileAfter"),
                    current.get("reservationId"),
                )
            except (AttributeError, TypeError, ValueError):
                due_reservations.append(invalid)
                continue
            if (
                current.get("pk") != projected_pk
                or current.get("sk") != marker_key
                or marker_key != expected_marker_key
                or item.get("dueKey") != current.get("dueKey")
                or current.get("duePartition") != _due_partition(environment)
                or item.get("dueKey") != expected_due_key
                or not _scope_matches(current, scope)
                or current.get("itemType") != "ReservationDue"
                or type(current.get("reconcileAfter")) is not int
                or current["reconcileAfter"] > through_epoch
            ):
                due_reservations.append(invalid)
                continue
            try:
                due = DueReservation(
                    scope,
                    _safe_id(current.get("reservationId"), "reservation_id"),
                    _safe_id(current.get("orderId"), "order_id"),
                    _safe_id(current.get("paymentAttemptId"), "payment_attempt_id"),
                    current["reconcileAfter"],
                    marker_key,
                )
            except ValueError:
                due_reservations.append(invalid)
                continue
            due_reservations.append(due)
        return due_reservations

    def quarantine_due_marker(self, marker: InvalidDueMarker, *, now_epoch: int) -> bool:
        if type(marker) is not InvalidDueMarker:
            raise ValueError("marker must be an InvalidDueMarker")
        now_epoch = _epoch(now_epoch, "now_epoch")
        if None in (marker.partition_key, marker.marker_key, marker.due_partition, marker.due_key):
            return False
        current = self.backend.get(
            self.catalog_table_name,
            marker.partition_key,
            marker.marker_key,
        )
        if (
            not isinstance(current, Mapping)
            or current.get("duePartition") != marker.due_partition
            or current.get("dueKey") != marker.due_key
        ):
            return False
        updated = copy.deepcopy(dict(current))
        updated.pop("duePartition", None)
        updated.pop("dueKey", None)
        updated.update(
            {
                "reconciliationStatus": "quarantined",
                "quarantinedAt": now_epoch,
            }
        )
        condition = {
            "duePartition": marker.due_partition,
            "dueKey": marker.due_key,
        }
        if type(current.get("revision")) is int:
            condition["revision"] = current["revision"]
        operations = [self._put(self.catalog_table_name, updated, condition=condition)]
        _validate_transaction_plan(operations)
        token = _hash_json(
            {
                "action": "quarantine_due_marker",
                "table": self.catalog_table_name,
                "partitionKey": marker.partition_key,
                "markerKey": marker.marker_key,
                "duePartition": marker.due_partition,
                "dueKey": marker.due_key,
            }
        )[:36]
        try:
            self.backend.transact(operations, token)
        except Exception:
            try:
                reread = self.backend.get(
                    self.catalog_table_name,
                    marker.partition_key,
                    marker.marker_key,
                )
            except Exception:
                raise StorageOutcomeUnknown("due marker quarantine outcome is unknown") from None
            if not isinstance(reread, Mapping) or (
                reread.get("duePartition") != marker.due_partition
                or reread.get("dueKey") != marker.due_key
            ):
                return True
            raise StorageOutcomeUnknown("due marker quarantine outcome is unknown") from None
        return True

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
                    "orderId": reservation["orderId"],
                    "paymentAttemptId": reservation["paymentAttemptId"],
                    "reconcileAfter": next_reconcile_at,
                    "duePartition": _due_partition(scope.environment),
                    "dueKey": _due_index_key(scope, next_reconcile_at, reservation_id),
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
        integration_event: Mapping[str, Any] | None = None,
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
        if integration_event is not None:
            request.update(
                {
                    "eventId": integration_event.get("eventId"),
                    "eventHash": integration_event.get("eventHash"),
                }
            )
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
            result = _terminal_result(reservation)
            if integration_event is None:
                return result
            stored_order = self.backend.get(
                self.operations_table_name,
                scope.partition_key,
                f"ORDER#{reservation['orderId']}",
            )
            expected_order_status = (
                "refunded"
                if target_status == "committed"
                and isinstance(stored_order, Mapping)
                and stored_order.get("status") == "refunded"
                else "paid" if target_status == "committed" else "payment_not_completed"
            )
            stored_order = _reservation_order(
                stored_order,
                scope,
                reservation,
                expected_order_status,
            )
            operations = [
                self._payment_binding_check(scope, integration_event),
                *self._fiscal_access_operations(
                    scope,
                    stored_order,
                    integration_event.get("eventType"),
                    metadata["now_epoch"],
                ),
                *self._event_operations(
                    scope,
                    integration_event,
                    result,
                    metadata,
                    order=stored_order,
                ),
                self._put(
                    self.operations_table_name,
                    _receipt_item(scope, receipt, result, metadata),
                    condition="absent",
                ),
            ]
            concurrent = self._execute(scope, operations, receipt)
            return concurrent if concurrent is not None else result
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
        if integration_event is not None:
            operations[-1:-1] = [
                self._payment_binding_check(scope, integration_event),
                *self._fiscal_access_operations(
                    scope,
                    order,
                    integration_event.get("eventType"),
                    metadata["now_epoch"],
                ),
                *self._event_operations(
                    scope,
                    integration_event,
                    result,
                    metadata,
                    order=order,
                ),
            ]
        concurrent = self._execute(scope, operations, receipt)
        return concurrent if concurrent is not None else result

    def _fiscal_access_operations(
        self,
        scope: CommerceScope,
        order: Mapping[str, Any],
        event_type: object,
        now_epoch: int,
    ) -> list[dict[str, Any]]:
        order_id = _safe_id(order.get("orderId"), "order_id")
        current = self.backend.get(
            self.operations_table_name,
            scope.partition_key,
            f"FISCAL_ACCESS#{order_id}",
        )
        if current is None:
            return []
        access = _stored_fiscal_access(current, scope, order_id)
        if event_type == "commerce.payment.succeeded.v1" and access["state"] in {"eligible", "consumed"}:
            return []
        if event_type == "commerce.payment.terminal_unpaid.v1" and access["state"] == "ineligible":
            return []
        if access["state"] != "pending_payment":
            raise StorageConflict("fiscal order access state changed")
        updated = copy.deepcopy(access)
        if event_type == "commerce.payment.succeeded.v1":
            updated.update({
                "state": "eligible",
                "eligibleAt": now_epoch,
                "expiresAt": now_epoch + access["requestWindowSeconds"],
            })
        elif event_type == "commerce.payment.terminal_unpaid.v1":
            updated.update({"state": "ineligible", "ineligibleAt": now_epoch})
        else:
            raise ValueError("unsupported fiscal payment transition")
        updated["revision"] = access["revision"] + 1
        return [
            self._put(
                self.operations_table_name,
                updated,
                condition={
                    "state": "pending_payment",
                    "proofHash": access["proofHash"],
                    "revision": access["revision"],
                    "expiresAt": access["expiresAt"],
                },
            )
        ]

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

    def _event_replay(
        self,
        scope: CommerceScope,
        event_id: str,
        event_hash: str,
    ) -> dict[str, Any] | None:
        existing = self.backend.get(
            self.operations_table_name,
            scope.partition_key,
            f"EVENT_INBOX#{event_id}",
        )
        if existing is None:
            return None
        if (
            existing.get("itemType") != "IntegrationEventInbox"
            or not _scope_matches(existing, scope)
            or existing.get("eventId") != event_id
            or existing.get("eventHash") != event_hash
            or not isinstance(existing.get("result"), Mapping)
        ):
            raise StorageConflict("integration event id was already used")
        return copy.deepcopy(dict(existing["result"]))

    def _event_operations(
        self,
        scope: CommerceScope,
        event: Mapping[str, Any],
        result: Mapping[str, Any],
        metadata: Mapping[str, Any],
        *,
        order: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        event_id = _safe_id(event.get("eventId"), "event_id")
        event_type = event.get("eventType")
        event_hash = event.get("eventHash")
        if (
            event_type not in {
                "commerce.payment.succeeded.v1",
                "commerce.payment.terminal_unpaid.v1",
                "commerce.refund.confirmed.v1",
            }
            or type(event_hash) is not str
            or _ACTOR_HASH.fullmatch(event_hash) is None
        ):
            raise ValueError("integration event context is invalid")
        inbox = self._put(
            self.operations_table_name,
            {
                "pk": scope.partition_key,
                "sk": f"EVENT_INBOX#{event_id}",
                "itemType": "IntegrationEventInbox",
                **_scope_fields(scope),
                "schemaVersion": 1,
                "eventId": event_id,
                "eventType": event_type,
                "eventHash": event_hash,
                "occurredAt": _epoch(event.get("occurredAt"), "occurred_at"),
                "processedAt": metadata["now_epoch"],
                "expiresAt": metadata["now_epoch"] + IDEMPOTENCY_TTL_SECONDS,
                "result": copy.deepcopy(dict(result)),
                **_operation_fields(metadata),
            },
            condition="absent",
        )
        if event_type == "commerce.refund.confirmed.v1":
            return [inbox]
        if event_type == "commerce.payment.succeeded.v1" and order.get("status") == "refunded":
            return [inbox]
        target = _notification_target(order.get("notificationTarget"))
        if target is None:
            return [inbox]
        if event_type == "commerce.payment.succeeded.v1":
            notification_type, template_id = "payment-succeeded", "payment-succeeded-v1"
        else:
            notification_type, template_id = "payment-failed", "payment-failed-v1"
        if target["notificationTypeTemplates"].get(notification_type) != template_id:
            return [inbox]
        payload = {
            "notificationPolicyId": target["notificationPolicyId"],
            "notificationType": notification_type,
            "publishedVersionId": target["publishedVersionId"],
            "templateId": template_id,
            "recipientSetId": target["recipientSetId"],
            "recipientSetVersion": target["recipientSetVersion"],
            "recipientMemberId": target["recipientMemberId"],
            "source": {
                "type": "commerce-order",
                "id": _safe_id(order.get("orderId"), "order_id"),
            },
            "variables": {
                "orderId": {"type": "safe-id", "value": order["orderId"]},
                "amountMinor": {"type": "integer", "value": order["amountMinor"]},
                "currency": {"type": "currency", "value": order["currency"]},
            },
        }
        outbox_event_id = _notification_event_id(scope, event_id, payload)
        payload["dedupeKey"] = outbox_event_id
        return [
            inbox,
            self._put(
                self.operations_table_name,
                {
                    "pk": scope.partition_key,
                    "sk": f"OUTBOX#{outbox_event_id}",
                    "itemType": "Outbox",
                    **_scope_fields(scope),
                    "schemaVersion": 1,
                    "eventId": outbox_event_id,
                    "eventType": "notification.requested.v1",
                    "sourceEventId": event_id,
                    "payload": payload,
                    "deliveryStatus": "pending",
                    "revision": 1,
                    "createdAt": metadata["now_epoch"],
                    **_operation_fields(metadata),
                },
                condition="absent",
            ),
        ]

    def _payment_binding_check(
        self,
        scope: CommerceScope,
        event: Mapping[str, Any],
    ) -> dict[str, Any]:
        payment_attempt_id = _safe_id(event.get("paymentAttemptId"), "payment_attempt_id")
        return self._check(
            self.operations_table_name,
            scope.partition_key,
            f"PAYMENT_ATTEMPT#{payment_attempt_id}",
            {
                "itemType": "PaymentAttemptBinding",
                "reservationId": _safe_id(event.get("reservationId"), "reservation_id"),
                "orderId": _safe_id(event.get("orderId"), "order_id"),
                "paymentAttemptId": payment_attempt_id,
                **_scope_fields(scope),
            },
        )

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

    @staticmethod
    def _check(
        table_name: str,
        pk: str,
        sk: str,
        condition: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "kind": "check",
            "table_name": table_name,
            "pk": pk,
            "sk": sk,
            "condition": copy.deepcopy(dict(condition)),
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

    def query_due(
        self,
        table_name: str,
        due_partition: str,
        through_epoch: int,
        limit: int,
        cursor: Any = None,
    ):
        request = dict(
            TableName=table_name,
            IndexName=RESERVATION_DUE_INDEX_NAME,
            KeyConditionExpression="#pk = :pk AND #sk BETWEEN :start AND :end",
            ExpressionAttributeNames={"#pk": "duePartition", "#sk": "dueKey"},
            ExpressionAttributeValues={
                ":pk": {"S": due_partition},
                ":start": {"S": "00000000000000000000#"},
                ":end": {"S": f"{through_epoch:020d}#\uffff"},
            },
            Limit=limit,
            ConsistentRead=False,
        )
        if cursor is not None:
            request["ExclusiveStartKey"] = cursor
        response = self.client.query(**request)
        return (
            [_from_item(item) for item in response.get("Items", [])],
            response.get("LastEvaluatedKey"),
        )

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
    if kind == "check":
        check = {
            "TableName": table_name,
            "Key": {
                "pk": {"S": operation.get("pk")},
                "sk": {"S": operation.get("sk")},
            },
        }
        _apply_condition(check, operation.get("condition"))
        if "ConditionExpression" not in check:
            raise ValueError("condition check requires a condition")
        return {"ConditionCheck": check}
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


def decode_dynamodb_item(item: object) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise ValueError("DynamoDB item is invalid")
    try:
        return _from_item(item)
    except (KeyError, TypeError, ValueError):
        raise ValueError("DynamoDB item is invalid") from None


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


def _request_identifier(value: object, field_name: str) -> str:
    if type(value) is not str or _REQUEST_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe transport identifier")
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
        "request_id": _request_identifier(request_id, "request_id"),
        "correlation_id": _request_identifier(correlation_id, "correlation_id"),
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
        "domain": scope.domain,
    }


def _fiscal_access(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {"proofHash", "windowSeconds"}:
        raise ValueError("fiscal access is invalid")
    proof_hash = value.get("proofHash")
    window_seconds = value.get("windowSeconds")
    if type(proof_hash) is not str or _ACTOR_HASH.fullmatch(proof_hash) is None:
        raise ValueError("fiscal access is invalid")
    if type(window_seconds) is not int or not 1 <= window_seconds <= 720 * 60 * 60:
        raise ValueError("fiscal access is invalid")
    return {"proofHash": proof_hash, "windowSeconds": window_seconds}


def _stored_fiscal_access(
    item: object,
    scope: CommerceScope,
    order_id: str,
) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise StorageConflict("stored fiscal order access is invalid")
    access = copy.deepcopy(dict(item))
    if (
        access.get("pk") != scope.partition_key
        or access.get("sk") != f"FISCAL_ACCESS#{order_id}"
        or access.get("itemType") != "FiscalOrderAccess"
        or not _scope_matches(access, scope)
        or access.get("orderId") != order_id
        or type(access.get("proofHash")) is not str
        or _ACTOR_HASH.fullmatch(access["proofHash"]) is None
        or access.get("state") not in {"pending_payment", "eligible", "ineligible", "consumed"}
        or type(access.get("attempts")) is not int
        or not 0 <= access["attempts"] <= 5
        or type(access.get("requestWindowSeconds")) is not int
        or not 1 <= access["requestWindowSeconds"] <= 720 * 60 * 60
    ):
        raise StorageConflict("stored fiscal order access is invalid")
    _epoch(access.get("createdAt"), "created_at")
    _epoch(access.get("expiresAt"), "expires_at")
    _positive_integer(access.get("revision"), "revision")
    return access


def _notification_target(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    keys = {
        "notificationPolicyId",
        "publishedVersionId",
        "recipientSetId",
        "recipientSetVersion",
        "recipientMemberId",
        "notificationTypeTemplates",
    }
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError("notification_target is invalid")
    version_id = value.get("publishedVersionId")
    version = value.get("recipientSetVersion")
    type_templates = value.get("notificationTypeTemplates")
    if type(version_id) is not str or _VERSION_ID.fullmatch(version_id) is None:
        raise ValueError("notification_target is invalid")
    if type(version) is not int or not 1 <= version <= 2_147_483_647:
        raise ValueError("notification_target is invalid")
    if (
        not isinstance(type_templates, Mapping)
        or not 1 <= len(type_templates) <= len(_NOTIFICATION_TYPE_TEMPLATES)
        or any(
            key not in _NOTIFICATION_TYPE_TEMPLATES
            or type(template_id) is not str
            or template_id != _NOTIFICATION_TYPE_TEMPLATES[key]
            for key, template_id in type_templates.items()
        )
    ):
        raise ValueError("notification_target is invalid")
    return {
        "notificationPolicyId": _safe_id(value.get("notificationPolicyId"), "notification_policy_id"),
        "publishedVersionId": version_id,
        "recipientSetId": _safe_id(value.get("recipientSetId"), "recipient_set_id"),
        "recipientSetVersion": version,
        "recipientMemberId": _safe_id(value.get("recipientMemberId"), "recipient_member_id"),
        "notificationTypeTemplates": {
            key: type_templates[key] for key in sorted(type_templates)
        },
    }


def _scope_matches(item: Mapping[str, Any], scope: CommerceScope) -> bool:
    return (
        item.get("pk") == scope.partition_key
        and item.get("environment") == scope.environment
        and item.get("tenantId") == scope.tenant_id
        and item.get("draftId") == scope.draft_id
        and item.get("domain") == scope.domain
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
    return _reservation_order(item, scope, reservation, "pending_checkout")


def _reservation_order(
    item: Any,
    scope: CommerceScope,
    reservation: Mapping[str, Any],
    expected_status: str,
) -> dict[str, Any]:
    if expected_status not in {"pending_checkout", "paid", "payment_not_completed", "refunded"}:
        raise ValueError("expected order status is invalid")
    if not isinstance(item, Mapping) or item.get("itemType") != "Order":
        raise StorageNotFound("reservation order was not found")
    order = copy.deepcopy(dict(item))
    if (
        not _scope_matches(order, scope)
        or order.get("orderId") != reservation["orderId"]
        or order.get("reservationId") != reservation["reservationId"]
        or order.get("paymentAttemptId") != reservation["paymentAttemptId"]
        or order.get("status") != expected_status
        or type(order.get("revision")) is not int
        or order["revision"] <= 0
        or type(order.get("amountMinor")) is not int
        or order["amountMinor"] < 0
        or type(order.get("currency")) is not str
        or re.fullmatch(r"[A-Z]{3}", order["currency"], re.ASCII) is None
    ):
        raise StorageConflict("stored reservation order is invalid")
    try:
        _notification_target(order.get("notificationTarget"))
    except ValueError:
        raise StorageConflict("stored reservation order is invalid") from None
    return order


def _payment_binding(
    item: Any,
    scope: CommerceScope,
    reservation_id: str,
    order_id: str,
    payment_attempt_id: str,
) -> dict[str, Any]:
    if (
        not isinstance(item, Mapping)
        or item.get("itemType") != "PaymentAttemptBinding"
        or not _scope_matches(item, scope)
        or item.get("reservationId") != reservation_id
        or item.get("orderId") != order_id
        or item.get("paymentAttemptId") != payment_attempt_id
    ):
        raise StorageConflict("payment event binding is invalid")
    return copy.deepcopy(dict(item))


def _outbox(item: Any, scope: CommerceScope, event_id: str) -> dict[str, Any]:
    if (
        not isinstance(item, Mapping)
        or item.get("itemType") != "Outbox"
        or not _scope_matches(item, scope)
        or item.get("schemaVersion") != 1
        or item.get("pk") != scope.partition_key
        or item.get("sk") != f"OUTBOX#{event_id}"
        or item.get("eventId") != event_id
        or item.get("eventType") != "notification.requested.v1"
        or item.get("deliveryStatus") not in {"pending", "delivered"}
        or type(item.get("revision")) is not int
        or item["revision"] <= 0
        or not isinstance(item.get("payload"), Mapping)
        or type(item.get("createdAt")) is not int
        or item["createdAt"] < 0
    ):
        raise StorageConflict("outbox event is invalid")
    if item["deliveryStatus"] == "pending" and (
        "deliveredAt" in item or "expiresAt" in item
    ):
        raise StorageConflict("outbox event is invalid")
    if item["deliveryStatus"] == "delivered":
        delivered_at = item.get("deliveredAt")
        expires_at = item.get("expiresAt")
        if (
            type(delivered_at) is not int
            or delivered_at < 0
            or expires_at != delivered_at + IDEMPOTENCY_TTL_SECONDS
        ):
            raise StorageConflict("outbox event is invalid")
    payload = item["payload"]
    expected_payload_keys = {
        "notificationPolicyId",
        "notificationType",
        "publishedVersionId",
        "templateId",
        "recipientSetId",
        "recipientSetVersion",
        "recipientMemberId",
        "source",
        "dedupeKey",
        "variables",
    }
    if set(payload) != expected_payload_keys:
        raise StorageConflict("outbox payload is invalid")
    expected_template = {
        "payment-succeeded": "payment-succeeded-v1",
        "payment-failed": "payment-failed-v1",
    }.get(payload.get("notificationType"))
    source = payload.get("source")
    variables = payload.get("variables")
    if (
        expected_template is None
        or payload.get("templateId") != expected_template
        or payload.get("dedupeKey") != event_id
        or not isinstance(source, Mapping)
        or set(source) != {"type", "id"}
        or source.get("type") != "commerce-order"
        or not isinstance(variables, Mapping)
        or set(variables) != {"orderId", "amountMinor", "currency"}
    ):
        raise StorageConflict("outbox payload is invalid")
    try:
        target = _notification_target(
            {
                "notificationPolicyId": payload.get("notificationPolicyId"),
                "publishedVersionId": payload.get("publishedVersionId"),
                "recipientSetId": payload.get("recipientSetId"),
                "recipientSetVersion": payload.get("recipientSetVersion"),
                "recipientMemberId": payload.get("recipientMemberId"),
                "notificationTypeTemplates": {
                    payload.get("notificationType"): payload.get("templateId"),
                },
            }
        )
        order_id = _safe_id(source.get("id"), "order_id")
        source_event_id = _safe_id(item.get("sourceEventId"), "source_event_id")
        _request_identifier(item.get("requestId"), "request_id")
        _request_identifier(item.get("correlationId"), "correlation_id")
    except ValueError:
        raise StorageConflict("outbox payload is invalid") from None
    if target is None:
        raise StorageConflict("outbox payload is invalid")
    order_variable = variables.get("orderId")
    amount_variable = variables.get("amountMinor")
    currency_variable = variables.get("currency")
    if (
        order_variable != {"type": "safe-id", "value": order_id}
        or not isinstance(amount_variable, Mapping)
        or set(amount_variable) != {"type", "value"}
        or amount_variable.get("type") != "integer"
        or type(amount_variable.get("value")) is not int
        or amount_variable["value"] < 0
        or not isinstance(currency_variable, Mapping)
        or set(currency_variable) != {"type", "value"}
        or currency_variable.get("type") != "currency"
        or type(currency_variable.get("value")) is not str
        or re.fullmatch(r"[A-Z]{3}", currency_variable["value"], re.ASCII) is None
    ):
        raise StorageConflict("outbox payload is invalid")
    canonical_payload = {
        key: copy.deepcopy(value)
        for key, value in payload.items()
        if key != "dedupeKey"
    }
    if event_id != _notification_event_id(scope, source_event_id, canonical_payload):
        raise StorageConflict("outbox event is invalid")
    return copy.deepcopy(dict(item))


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


def _due_partition(environment: object) -> str:
    return f"ENV#{_environment(environment)}"


def _due_index_key(scope: CommerceScope, reconcile_after: object, reservation_id: object) -> str:
    scope = _scope(scope)
    reconcile_after = _epoch(reconcile_after, "reconcile_after")
    reservation_id = _safe_id(reservation_id, "reservation_id")
    return (
        f"{reconcile_after:020d}#TENANT#{scope.tenant_id}#DRAFT#{scope.draft_id}#"
        f"DOMAIN#{scope.domain}#RESERVATION#{reservation_id}"
    )


def _environment(value: object) -> str:
    if value not in {"test", "production"}:
        raise ValueError("environment must be test or production")
    return value


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


def _notification_event_id(
    scope: CommerceScope,
    source_event_id: str,
    payload: Mapping[str, Any],
) -> str:
    return _hash_json({
        "scope": _scope_fields(_scope(scope)),
        "sourceEventId": _safe_id(source_event_id, "source_event_id"),
        "eventType": "notification.requested.v1",
        "payload": payload,
    })


def _hash_json(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
