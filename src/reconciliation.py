"""Five-minute reservation reconciliation driven by canonical status evidence."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Callable

try:
    from domain.inventory import RECONCILER_INTERVAL_SECONDS, reconciliation_outcome
    from storage import CommerceStore, DueReservation, InvalidDueMarker
except ModuleNotFoundError:
    from src.domain.inventory import RECONCILER_INTERVAL_SECONDS, reconciliation_outcome
    from src.storage import CommerceStore, DueReservation, InvalidDueMarker


class ReconciliationError(RuntimeError):
    pass


RECONCILIATION_WORK_LIMIT = 25
MINIMUM_REMAINING_TIME_MS = 1_500
_SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


class ReservationReconciler:
    def __init__(self, store: CommerceStore, policy_resolver: Any, status_gateway: Any = None) -> None:
        if type(store) is not CommerceStore:
            raise ValueError("store must be a CommerceStore")
        if not callable(policy_resolver):
            raise ValueError("policy_resolver must be callable")
        if status_gateway is not None and not hasattr(status_gateway, "lookup_status"):
            raise ValueError("status_gateway must expose lookup_status")
        self.store = store
        self.policy_resolver = policy_resolver
        self.status_gateway = status_gateway

    def run(
        self,
        *,
        environment: str,
        now_epoch: int,
        remaining_time_ms: Callable[[], int] | None = None,
    ) -> dict[str, int]:
        if environment not in {"test", "production"}:
            raise ValueError("environment must be test or production")
        if type(now_epoch) is not int or now_epoch < 0:
            raise ValueError("now_epoch must be a non-negative integer")
        if remaining_time_ms is not None and not callable(remaining_time_ms):
            raise ValueError("remaining_time_ms must be callable")
        result = {"processed": 0, "committed": 0, "released": 0, "deferred": 0, "failed": 0}
        if not _has_time_budget(remaining_time_ms):
            return result
        for due in self.store.list_due_reservations(
            environment,
            now_epoch,
            limit=RECONCILIATION_WORK_LIMIT,
            max_pages=1,
        ):
            if not _has_time_budget(remaining_time_ms):
                break
            if type(due) is InvalidDueMarker:
                try:
                    self.store.quarantine_due_marker(due, now_epoch=now_epoch)
                except Exception:
                    pass
                result["failed"] += 1
                continue
            try:
                policy = self._revalidate(due)
                if not _has_time_budget(remaining_time_ms):
                    break
                status = self._status(due, policy)
                if not _has_time_budget(remaining_time_ms):
                    break
                decision = reconciliation_outcome(status, now_epoch)
                key = hashlib.sha256(
                    f"{due.marker_key}:{status}".encode("utf-8")
                ).hexdigest()
                if status in {"paid", "terminal_unpaid"}:
                    self.store.apply_payment_event(
                        due.scope,
                        event_id=f"reconcile-{key[:40]}",
                        event_type=(
                            "commerce.payment.succeeded.v1"
                            if status == "paid"
                            else "commerce.payment.terminal_unpaid.v1"
                        ),
                        reservation_id=due.reservation_id,
                        order_id=due.order_id,
                        payment_attempt_id=due.payment_attempt_id,
                        occurred_at=now_epoch,
                        now_epoch=now_epoch,
                    )
                    result["committed" if status == "paid" else "released"] += 1
                    result["processed"] += 1
                    continue
                metadata = {
                    "idempotency_key": f"reconcile:{key}",
                    "request_id": f"reconcile-{key[:32]}",
                    "correlation_id": f"reconcile-{key[32:]}",
                    "actor_hash": None,
                    "now_epoch": now_epoch,
                }
                if decision.action == "release":
                    self.store.release_reservation(
                        due.scope,
                        due.reservation_id,
                        completion_reason=decision.completion_reason,
                        **metadata,
                    )
                    result["released"] += 1
                else:
                    self.store.defer_reservation(
                        due.scope,
                        due.reservation_id,
                        decision.next_reconcile_at,
                        **metadata,
                    )
                    result["deferred"] += 1
                result["processed"] += 1
            except Exception:
                result["failed"] += 1
                if not _has_time_budget(remaining_time_ms):
                    break
                failure_key = hashlib.sha256(
                    f"{due.marker_key}:failure".encode("utf-8")
                ).hexdigest()
                try:
                    self.store.defer_reservation(
                        due.scope,
                        due.reservation_id,
                        now_epoch + RECONCILER_INTERVAL_SECONDS,
                        idempotency_key=f"reconcile-failure:{failure_key}",
                        request_id=f"reconcile-{failure_key[:32]}",
                        correlation_id=f"reconcile-{failure_key[32:]}",
                        actor_hash=None,
                        now_epoch=now_epoch,
                    )
                    result["deferred"] += 1
                except Exception:
                    pass
        return result

    def _revalidate(self, due: DueReservation) -> Any:
        try:
            policy = self.policy_resolver(
                domain=due.scope.domain,
                environment=due.scope.environment,
                tenant_id=due.scope.tenant_id,
                draft_id=due.scope.draft_id,
            )
        except Exception:
            raise ReconciliationError("published Commerce policy is unavailable") from None
        if (
            getattr(policy, "domain", None) != due.scope.domain
            or getattr(policy, "environment", None) != due.scope.environment
            or getattr(policy, "tenant_id", None) != due.scope.tenant_id
            or getattr(policy, "draft_id", None) != due.scope.draft_id
        ):
            raise ReconciliationError("published Commerce policy scope does not match")
        return policy

    def _status(self, due: DueReservation, policy: Any) -> str:
        if self.status_gateway is None:
            return "lookup_failure"
        descriptor = getattr(policy, "commerce", None)
        expected_scope = {
            "environment": due.scope.environment,
            "tenantId": due.scope.tenant_id,
            "draftId": due.scope.draft_id,
            "domain": due.scope.domain,
        }
        commerce = descriptor.get("commerce") if isinstance(descriptor, dict) else None
        payments = commerce.get("payments") if isinstance(commerce, dict) else None
        connection_id = payments.get("bindingId") if isinstance(payments, dict) else None
        if (
            not isinstance(descriptor, dict)
            or set(descriptor) != {"version", "scope", "commerce"}
            or descriptor.get("version") != 1
            or descriptor.get("scope") != expected_scope
            or type(connection_id) is not str
            or _SAFE_ID_RE.fullmatch(connection_id) is None
        ):
            raise ReconciliationError("published Commerce payment binding is invalid")
        try:
            return self.status_gateway.lookup_status(
                due.scope,
                connection_id,
                due.order_id,
                due.payment_attempt_id,
                1,
            )
        except Exception:
            return "lookup_failure"


def _has_time_budget(remaining_time_ms: Callable[[], int] | None) -> bool:
    if remaining_time_ms is None:
        return True
    remaining = remaining_time_ms()
    if type(remaining) is not int or remaining < 0:
        raise ValueError("remaining time must be a non-negative integer")
    return remaining > MINIMUM_REMAINING_TIME_MS
