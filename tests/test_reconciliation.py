from types import SimpleNamespace
import unittest

from src.domain.offers import Money
from src.domain.orders import CheckoutLine, PendingOrder
from src.reconciliation import ReconciliationError, ReservationReconciler
from src.storage import CommerceScope, CommerceStore
from tests.test_storage import CATALOG_TABLE, OPERATIONS_TABLE, FakeBackend


CREATED_AT = 1_800_000_000
DUE_AT = CREATED_AT + 2_400
SCOPE = CommerceScope("test", "tenant-a", "draft-a", "draft-a.example.test")


class Policies:
    def __init__(self, *, mismatch=False):
        self.calls = []
        self.mismatch = mismatch

    def __call__(self, *, domain, environment, tenant_id, draft_id):
        self.calls.append((domain, environment, tenant_id, draft_id))
        return SimpleNamespace(
            domain="other.example.test" if self.mismatch else domain,
            environment=environment,
            tenant_id=tenant_id,
            draft_id=draft_id,
        )


class Statuses:
    def __init__(self, status=None, error=None):
        self.status = status
        self.error = error
        self.calls = []

    def lookup_status(self, scope, payment_attempt_id):
        self.calls.append((scope, payment_attempt_id))
        if self.error:
            raise self.error
        return self.status


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.backend = FakeBackend()
        self.store = CommerceStore(self.backend, CATALOG_TABLE, OPERATIONS_TABLE)
        stock = {
            "pk": SCOPE.partition_key,
            "sk": "STOCK#primary#landing",
            "itemType": "Stock",
            "environment": SCOPE.environment,
            "tenantId": SCOPE.tenant_id,
            "draftId": SCOPE.draft_id,
            "domain": SCOPE.domain,
            "stockId": "landing",
            "locationId": "primary",
            "tracked": True,
            "onHand": 10,
            "reserved": 0,
            "available": 10,
            "revision": 1,
        }
        self.backend.items[(CATALOG_TABLE, stock["pk"], stock["sk"])] = stock
        order = PendingOrder(
            "order-1",
            "attempt-1",
            (CheckoutLine("line-1", "offer-1", 2, Money(90_000, "MXN", frozenset({"MXN"})), "landing"),),
        )
        self.store.reserve_checkout(
            SCOPE,
            order,
            "reservation-1",
            location_id="primary",
            created_at_epoch=CREATED_AT,
            idempotency_key="reserve",
            request_id="request-reserve",
            correlation_id="correlation-reserve",
            actor_hash=None,
            now_epoch=CREATED_AT,
            notification_target={
                "publishedVersionId": "version-1",
                "recipientSetId": "billing-operators",
                "recipientSetVersion": 1,
                "recipientMemberId": "primary",
            },
        )

    def reconcile(self, status=None, *, gateway_error=None, policies=None, gateway=True):
        policy = policies or Policies()
        statuses = Statuses(status, gateway_error) if gateway else None
        result = ReservationReconciler(self.store, policy, statuses).run(
            environment="test",
            now_epoch=DUE_AT,
        )
        return result, policy, statuses

    def test_paid_and_terminal_unpaid_are_the_only_terminal_statuses(self):
        paid, policy, statuses = self.reconcile("paid")
        self.assertEqual(paid, {"processed": 1, "committed": 1, "released": 0, "deferred": 0, "failed": 0})
        self.assertEqual(policy.calls, [(SCOPE.domain, "test", SCOPE.tenant_id, SCOPE.draft_id)])
        self.assertEqual(statuses.calls, [(SCOPE, "attempt-1")])
        stock = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"]), (8, 0))
        self.assertTrue(any(
            table == OPERATIONS_TABLE and item.get("itemType") == "IntegrationEventInbox"
            for (table, _pk, _sk), item in self.backend.items.items()
        ))
        self.assertTrue(any(
            table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
            for (table, _pk, _sk), item in self.backend.items.items()
        ))

        other = ReconciliationTests(methodName="runTest")
        other.setUp()
        released, _policy, _statuses = other.reconcile("terminal_unpaid")
        self.assertEqual(released["released"], 1)
        stock = other.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"]), (10, 0))

    def test_unknown_unavailable_or_missing_adapter_defers_exactly_five_minutes(self):
        cases = (
            ("unknown", None, True),
            (None, RuntimeError("provider detail"), True),
            (None, None, False),
        )
        for status, error, gateway in cases:
            with self.subTest(status=status, error=error, gateway=gateway):
                fixture = ReconciliationTests(methodName="runTest")
                fixture.setUp()
                result, _policy, _statuses = fixture.reconcile(status, gateway_error=error, gateway=gateway)
                self.assertEqual(result["deferred"], 1)
                reservation = fixture.backend.get(
                    CATALOG_TABLE,
                    SCOPE.partition_key,
                    "RESERVATION#reservation-1",
                )
                self.assertEqual(reservation["status"], "reserved")
                self.assertEqual(reservation["reconcileAfter"], DUE_AT + 300)

    def test_policy_mismatch_fails_closed_before_status_or_stock_mutation(self):
        policies = Policies(mismatch=True)
        statuses = Statuses("paid")
        reconciler = ReservationReconciler(self.store, policies, statuses)

        self.assertEqual(
            reconciler.run(environment="test", now_epoch=DUE_AT),
            {"processed": 0, "committed": 0, "released": 0, "deferred": 1, "failed": 1},
        )

        self.assertEqual(statuses.calls, [])
        stock = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"]), (10, 2))
        reservation = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "RESERVATION#reservation-1")
        self.assertEqual(reservation["reconcileAfter"], DUE_AT + 300)

    def test_wrong_environment_never_discovers_or_processes_the_reservation(self):
        result = ReservationReconciler(self.store, Policies(), Statuses("paid")).run(
            environment="production",
            now_epoch=DUE_AT,
        )

        self.assertEqual(result, {"processed": 0, "committed": 0, "released": 0, "deferred": 0, "failed": 0})
        stock = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"]), (10, 2))

    def test_one_poison_draft_is_counted_and_does_not_block_other_due_drafts(self):
        other_scope = CommerceScope("test", "tenant-a", "draft-b", "draft-b.example.test")
        stock = {
            "pk": other_scope.partition_key,
            "sk": "STOCK#primary#landing-b",
            "itemType": "Stock",
            "environment": other_scope.environment,
            "tenantId": other_scope.tenant_id,
            "draftId": other_scope.draft_id,
            "domain": other_scope.domain,
            "stockId": "landing-b",
            "locationId": "primary",
            "tracked": True,
            "onHand": 3,
            "reserved": 0,
            "available": 3,
            "revision": 1,
        }
        self.backend.items[(CATALOG_TABLE, stock["pk"], stock["sk"])] = stock
        self.store.reserve_checkout(
            other_scope,
            PendingOrder(
                "order-b",
                "attempt-b",
                (CheckoutLine("line-b", "offer-b", 1, Money(90_000, "MXN", frozenset({"MXN"})), "landing-b"),),
            ),
            "reservation-b",
            location_id="primary",
            created_at_epoch=CREATED_AT,
            idempotency_key="reserve-b",
            request_id="request-b",
            correlation_id="correlation-b",
            actor_hash=None,
            now_epoch=CREATED_AT,
        )

        def policies(*, domain, environment, tenant_id, draft_id):
            if draft_id == "draft-a":
                raise RuntimeError("private policy error")
            return SimpleNamespace(
                domain=domain,
                environment=environment,
                tenant_id=tenant_id,
                draft_id=draft_id,
            )

        result = ReservationReconciler(self.store, policies, Statuses("paid")).run(
            environment="test",
            now_epoch=DUE_AT,
        )

        self.assertEqual(
            result,
            {"processed": 1, "committed": 1, "released": 0, "deferred": 1, "failed": 1},
        )
        self.assertIsNone(
            self.backend.get(
                CATALOG_TABLE,
                SCOPE.partition_key,
                f"RESERVATION_DUE#{DUE_AT:020d}#reservation-1",
            )
        )
        self.assertIsNotNone(
            self.backend.get(
                CATALOG_TABLE,
                SCOPE.partition_key,
                f"RESERVATION_DUE#{DUE_AT + 300:020d}#reservation-1",
            )
        )
        poisoned_stock = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((poisoned_stock["onHand"], poisoned_stock["reserved"]), (10, 2))
        stock_b = self.backend.get(CATALOG_TABLE, other_scope.partition_key, "STOCK#primary#landing-b")
        self.assertEqual((stock_b["onHand"], stock_b["reserved"]), (2, 0))

    def test_corrupt_due_marker_is_quarantined_without_releasing_or_blocking_another_draft(self):
        other_scope = CommerceScope("test", "tenant-a", "draft-b", "draft-b.example.test")
        stock = {
            "pk": other_scope.partition_key,
            "sk": "STOCK#primary#landing-b",
            "itemType": "Stock",
            "environment": other_scope.environment,
            "tenantId": other_scope.tenant_id,
            "draftId": other_scope.draft_id,
            "domain": other_scope.domain,
            "stockId": "landing-b",
            "locationId": "primary",
            "tracked": True,
            "onHand": 3,
            "reserved": 0,
            "available": 3,
            "revision": 1,
        }
        self.backend.items[(CATALOG_TABLE, stock["pk"], stock["sk"])] = stock
        self.store.reserve_checkout(
            other_scope,
            PendingOrder(
                "order-b",
                "attempt-b",
                (CheckoutLine("line-b", "offer-b", 1, Money(90_000, "MXN", frozenset({"MXN"})), "landing-b"),),
            ),
            "reservation-b",
            location_id="primary",
            created_at_epoch=CREATED_AT,
            idempotency_key="reserve-b",
            request_id="request-b",
            correlation_id="correlation-b",
            actor_hash=None,
            now_epoch=CREATED_AT,
        )
        corrupt_key = (
            CATALOG_TABLE,
            SCOPE.partition_key,
            f"RESERVATION_DUE#{DUE_AT:020d}#reservation-1",
        )
        self.backend.items[corrupt_key]["reconcileAfter"] = "corrupt"

        result = ReservationReconciler(self.store, Policies(), Statuses("unknown")).run(
            environment="test",
            now_epoch=DUE_AT,
        )

        self.assertEqual(
            result,
            {"processed": 1, "committed": 0, "released": 0, "deferred": 1, "failed": 1},
        )
        quarantined = self.backend.items[corrupt_key]
        self.assertNotIn("duePartition", quarantined)
        self.assertNotIn("dueKey", quarantined)
        self.assertEqual(quarantined["reconciliationStatus"], "quarantined")
        poisoned_stock = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((poisoned_stock["onHand"], poisoned_stock["reserved"]), (10, 2))
        healthy_reservation = self.backend.get(
            CATALOG_TABLE,
            other_scope.partition_key,
            "RESERVATION#reservation-b",
        )
        self.assertEqual(healthy_reservation["reconcileAfter"], DUE_AT + 300)

    def test_time_budget_stops_before_mutation_and_leaves_the_reservation_due(self):
        remaining = iter((2_000, 1_500))

        result = ReservationReconciler(self.store, Policies(), Statuses("paid")).run(
            environment="test",
            now_epoch=DUE_AT,
            remaining_time_ms=lambda: next(remaining),
        )

        self.assertEqual(
            result,
            {"processed": 0, "committed": 0, "released": 0, "deferred": 0, "failed": 0},
        )
        self.assertEqual(len(self.backend.queries), 1)
        reservation = self.backend.get(
            CATALOG_TABLE,
            SCOPE.partition_key,
            "RESERVATION#reservation-1",
        )
        self.assertEqual((reservation["status"], reservation["reconcileAfter"]), ("reserved", DUE_AT))
        self.assertIsNotNone(
            self.backend.get(
                CATALOG_TABLE,
                SCOPE.partition_key,
                f"RESERVATION_DUE#{DUE_AT:020d}#reservation-1",
            )
        )

    def test_one_hundred_one_failed_drafts_do_not_starve_a_healthy_successive_run(self):
        self.backend = FakeBackend()
        self.store = CommerceStore(self.backend, CATALOG_TABLE, OPERATIONS_TABLE)

        def reserve(scope, index):
            reservation_id = f"reservation-{index}"
            self.store.reserve_checkout(
                scope,
                PendingOrder(
                    f"order-{index}",
                    f"attempt-{index}",
                    (
                        CheckoutLine(
                            f"line-{index}",
                            f"offer-{index}",
                            1,
                            Money(1, "MXN", frozenset({"MXN"})),
                            None,
                        ),
                    ),
                ),
                reservation_id,
                location_id="primary",
                created_at_epoch=CREATED_AT,
                idempotency_key=f"reserve-{index}",
                request_id=f"request-{index}",
                correlation_id=f"correlation-{index}",
                actor_hash=None,
                now_epoch=CREATED_AT,
            )
            return reservation_id

        failed_scopes = []
        for index in range(101):
            draft_id = f"draft-fail-{index:03d}"
            scope = CommerceScope("test", "tenant-a", draft_id, f"{draft_id}.example.test")
            failed_scopes.append((scope, reserve(scope, f"fail-{index:03d}")))
        healthy_scope = CommerceScope("test", "tenant-a", "draft-healthy", "draft-healthy.example.test")
        healthy_reservation_id = reserve(healthy_scope, "healthy")

        def policies(*, domain, environment, tenant_id, draft_id):
            if draft_id.startswith("draft-fail-"):
                raise RuntimeError("private policy error")
            return SimpleNamespace(
                domain=domain,
                environment=environment,
                tenant_id=tenant_id,
                draft_id=draft_id,
            )

        reconciler = ReservationReconciler(self.store, policies, Statuses("unknown"))
        totals = {"processed": 0, "committed": 0, "released": 0, "deferred": 0, "failed": 0}
        for _invocation in range(5):
            result = reconciler.run(environment="test", now_epoch=DUE_AT)
            self.assertLessEqual(result["processed"] + result["failed"], 25)
            for key, value in result.items():
                totals[key] += value

        self.assertEqual(
            totals,
            {"processed": 1, "committed": 0, "released": 0, "deferred": 102, "failed": 101},
        )
        self.assertEqual(len(self.backend.queries), 5)
        self.assertTrue(all(query[3] == 25 for query in self.backend.queries))
        healthy = self.backend.get(
            CATALOG_TABLE,
            healthy_scope.partition_key,
            f"RESERVATION#{healthy_reservation_id}",
        )
        self.assertEqual((healthy["status"], healthy["reconcileAfter"]), ("reserved", DUE_AT + 300))
        failed_scope, failed_reservation_id = failed_scopes[0]
        failed = self.backend.get(
            CATALOG_TABLE,
            failed_scope.partition_key,
            f"RESERVATION#{failed_reservation_id}",
        )
        self.assertEqual((failed["status"], failed["reconcileAfter"]), ("reserved", DUE_AT + 300))


if __name__ == "__main__":
    unittest.main()
