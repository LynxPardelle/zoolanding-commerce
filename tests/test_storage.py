import copy
import unittest

from src.domain.offers import Money
from src.domain.orders import CheckoutLine, PendingOrder
from src.storage import (
    ConditionalWriteFailed,
    CommerceScope,
    CommerceStore,
    InvalidDueMarker,
    StorageConflict,
    StorageLimitExceeded,
    StorageOutcomeUnknown,
    _DynamoBackend,
    _validate_transaction_plan,
)


CATALOG_TABLE = "catalog-table"
OPERATIONS_TABLE = "operations-table"
ACTOR_HASH = "a" * 64
NOW = 1_800_000_000
SUPPORTED_CURRENCIES = frozenset({"MXN"})


class FakeBackend:
    def __init__(self):
        self.items = {}
        self.transactions = []
        self.queries = []
        self.before_transact = None
        self.before_commit_error = None
        self.after_commit_error = None
        self.fail_receipt_read_after_error = False
        self.next_get_error = None

    def get(self, table_name, pk, sk):
        if self.next_get_error is not None:
            error, self.next_get_error = self.next_get_error, None
            raise error
        return copy.deepcopy(self.items.get((table_name, pk, sk)))

    def query_due(self, table_name, due_partition, through_epoch, limit, cursor=None):
        self.queries.append((table_name, due_partition, through_epoch, limit, cursor))
        maximum = f"{through_epoch:020d}#\uffff"
        rows = [
            {
                key: copy.deepcopy(item[key])
                for key in ("pk", "sk", "duePartition", "dueKey")
            }
            for (table, _item_pk, _sk), item in sorted(self.items.items())
            if table == table_name
            and item.get("duePartition") == due_partition
            and item.get("dueKey", "") <= maximum
            and (cursor is None or item.get("dueKey", "") > cursor)
        ]
        page = rows[:limit]
        next_cursor = page[-1]["dueKey"] if len(rows) > limit else None
        return page, next_cursor

    def transact(self, operations, client_token):
        if self.before_transact:
            callback, self.before_transact = self.before_transact, None
            callback()
        if self.before_commit_error is not None:
            error, self.before_commit_error = self.before_commit_error, None
            if self.fail_receipt_read_after_error:
                self.next_get_error = RuntimeError("simulated receipt read failure")
            raise error
        _validate_transaction_plan(operations)
        candidate = copy.deepcopy(self.items)
        targets = set()
        for operation in operations:
            item = operation.get("item", {})
            key = (
                operation["table_name"],
                operation.get("pk") or item.get("pk"),
                operation.get("sk") or item.get("sk"),
            )
            if key in targets:
                raise AssertionError("duplicate transaction target")
            targets.add(key)
            current = candidate.get(key)
            condition = operation.get("condition")
            if condition == "absent" and current is not None:
                raise ConditionalWriteFailed()
            if isinstance(condition, dict) and (
                current is None
                or any(current.get(field) != expected for field, expected in condition.items())
            ):
                raise ConditionalWriteFailed()
            if operation["kind"] == "check":
                continue
            if operation["kind"] == "put":
                candidate[key] = copy.deepcopy(operation["item"])
            elif operation["kind"] == "delete":
                candidate.pop(key, None)
            else:
                raise AssertionError("unsupported fake operation")
        self.items = candidate
        self.transactions.append((copy.deepcopy(operations), client_token))
        if self.after_commit_error is not None:
            error, self.after_commit_error = self.after_commit_error, None
            raise error


class CommerceStorageTests(unittest.TestCase):
    def setUp(self):
        self.backend = FakeBackend()
        self.store = CommerceStore(self.backend, CATALOG_TABLE, OPERATIONS_TABLE)
        self.scope = CommerceScope("test", "tenant-a", "draft-a", "draft-a.example.test")

    def seed_stock(self, stock_id, *, on_hand=10, reserved=0, revision=1, scope=None):
        scope = scope or self.scope
        item = {
            "pk": scope.partition_key,
            "sk": f"STOCK#primary#{stock_id}",
            "itemType": "Stock",
            "environment": scope.environment,
            "tenantId": scope.tenant_id,
            "draftId": scope.draft_id,
            "domain": scope.domain,
            "stockId": stock_id,
            "locationId": "primary",
            "tracked": True,
            "onHand": on_hand,
            "reserved": reserved,
            "available": on_hand - reserved,
            "revision": revision,
        }
        self.backend.items[(CATALOG_TABLE, item["pk"], item["sk"])] = item

    def order(self, *, order_id="order-1", attempt_id="attempt-1", stock_ids=("landing",)):
        lines = tuple(
            CheckoutLine(
                f"line-{index}",
                f"offer-{index}",
                2,
                Money(90_000 + index, "MXN", SUPPORTED_CURRENCIES),
                stock_id,
            )
            for index, stock_id in enumerate(stock_ids, start=1)
        )
        return PendingOrder(order_id, attempt_id, lines)

    def metadata(self, suffix):
        return {
            "idempotency_key": f"idem-{suffix}",
            "request_id": f"request-{suffix}",
            "correlation_id": f"correlation-{suffix}",
            "actor_hash": ACTOR_HASH,
            "now_epoch": NOW,
        }

    def reserve(self, order=None, **overrides):
        values = self.metadata("reserve")
        values.update(overrides)
        return self.store.reserve_checkout(
            self.scope,
            order or self.order(),
            "reservation-1",
            location_id="primary",
            created_at_epoch=NOW,
            **values,
        )

    def test_scope_requires_a_canonical_domain(self):
        for value in ("", "HTTPS://EXAMPLE.COM", "bad domain", "localhost"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                CommerceScope("test", "tenant-a", "draft-a", value)

    def test_adjust_is_one_atomic_conditional_movement_with_durable_receipt(self):
        self.seed_stock("landing", on_hand=10, reserved=2, revision=4)

        result = self.store.adjust_stock(
            self.scope,
            "landing",
            -3,
            4,
            location_id="primary",
            **self.metadata("adjust"),
        )

        self.assertEqual((result["onHand"], result["reserved"], result["available"], result["revision"]), (7, 2, 5, 5))
        operations, token = self.backend.transactions[-1]
        self.assertEqual(len(operations), 3)
        self.assertEqual(len(token), 36)
        movement = next(operation["item"] for operation in operations if operation["item"]["itemType"] == "StockMovement")
        receipt = next(operation["item"] for operation in operations if operation["item"]["itemType"] == "IdempotencyReceipt")
        self.assertEqual((movement["onHandDelta"], movement["reservedDelta"], movement["availableDelta"]), (-3, 0, -3))
        self.assertEqual(receipt["expiresAt"], NOW + 90 * 24 * 60 * 60)
        self.assertNotIn("expiresAt", movement)
        self.assertTrue(all(operation["item"]["pk"].startswith("ENV#test#TENANT#tenant-a#DRAFT#draft-a") for operation in operations))

        before = copy.deepcopy(self.backend.items)
        with self.assertRaises(StorageConflict):
            self.store.adjust_stock(
                self.scope,
                "landing",
                -6,
                5,
                location_id="primary",
                **self.metadata("invalid-adjust"),
            )
        self.assertEqual(self.backend.items, before)

    def test_adjust_can_initialize_tracked_stock_only_from_revision_zero(self):
        result = self.store.adjust_stock(
            self.scope,
            "new-stock",
            5,
            0,
            location_id="primary",
            **self.metadata("initialize"),
        )

        self.assertEqual(
            (result["onHand"], result["reserved"], result["available"], result["revision"]),
            (5, 0, 5, 1),
        )
        operations, _ = self.backend.transactions[-1]
        stock_put = next(operation for operation in operations if operation.get("item", {}).get("itemType") == "Stock")
        self.assertEqual(stock_put["condition"], "absent")
        with self.assertRaises(StorageConflict):
            self.store.adjust_stock(
                self.scope,
                "missing-negative",
                -1,
                0,
                location_id="primary",
                **self.metadata("initialize-negative"),
            )
        with self.assertRaises(StorageConflict):
            self.store.adjust_stock(
                self.scope,
                "new-stock",
                1,
                0,
                location_id="primary",
                **self.metadata("initialize-again"),
            )

    def test_transport_request_identifiers_persist_without_resource_id_restrictions(self):
        identifiers = ("AbCDef123", "req:abc", "R" * 128)

        for index, identifier in enumerate(identifiers):
            with self.subTest(identifier=identifier):
                self.store.adjust_stock(
                    self.scope,
                    f"transport-{index}",
                    1,
                    0,
                    location_id="primary",
                    idempotency_key=f"transport-{index}",
                    request_id=identifier,
                    correlation_id=identifier,
                    actor_hash=None,
                    now_epoch=NOW,
                )
                movement = next(
                    item
                    for (table, _pk, _sk), item in self.backend.items.items()
                    if table == CATALOG_TABLE
                    and item.get("itemType") == "StockMovement"
                    and item.get("stockId") == f"transport-{index}"
                )
                self.assertEqual(movement["requestId"], identifier)
                self.assertEqual(movement["correlationId"], identifier)

    def test_reserve_creates_order_reservation_due_marker_and_stock_atomically(self):
        self.seed_stock("landing", on_hand=5)
        order = PendingOrder(
            "order-1",
            "attempt-1",
            (
                CheckoutLine("line-1", "offer-1", 2, Money(90_000, "MXN", SUPPORTED_CURRENCIES), "landing"),
                CheckoutLine("line-2", "offer-2", 1, Money(10_000, "MXN", SUPPORTED_CURRENCIES), None),
            ),
        )

        result = self.reserve(
            order,
            notification_target={
                "notificationPolicyId": "payment-status",
                "publishedVersionId": "version-1",
                "recipientSetId": "billing-operators",
                "recipientSetVersion": 1,
                "recipientMemberId": "primary",
                "notificationTypeTemplates": {
                    "payment-failed": "payment-failed-v1",
                    "payment-succeeded": "payment-succeeded-v1",
                },
            },
        )

        self.assertEqual(result["checkoutExpiresAt"], NOW + 2_100)
        self.assertEqual(result["reconcileAfter"], NOW + 2_400)
        operations, _ = self.backend.transactions[-1]
        self.assertEqual(len(operations), 7)
        self.assertEqual({operation["table_name"] for operation in operations}, {CATALOG_TABLE, OPERATIONS_TABLE})
        item_types = [operation["item"]["itemType"] for operation in operations if operation["kind"] == "put"]
        self.assertEqual(item_types.count("StockMovement"), 1)
        self.assertIn("Order", item_types)
        self.assertIn("PaymentAttemptBinding", item_types)
        self.assertIn("Reservation", item_types)
        self.assertIn("ReservationDue", item_types)
        order_item = next(operation["item"] for operation in operations if operation.get("item", {}).get("itemType") == "Order")
        self.assertEqual(order_item["paymentAttemptId"], "attempt-1")
        self.assertEqual(order_item["notificationTarget"]["notificationPolicyId"], "payment-status")
        self.assertEqual(order_item["notificationTarget"]["publishedVersionId"], "version-1")
        self.assertNotIn("expiresAt", order_item)
        self.assertNotIn("provider", repr(operations).lower())
        stock = self.backend.get(CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"], stock["available"]), (5, 2, 3))

        due = self.store.list_due_reservations("test", NOW + 2_399)
        self.assertEqual(due, [])
        due = self.store.list_due_reservations("test", NOW + 2_400)
        self.assertEqual([item.reservation_id for item in due], ["reservation-1"])
        self.assertEqual(due[0].scope, self.scope)
        self.assertEqual(due[0].order_id, "order-1")
        self.assertEqual(due[0].payment_attempt_id, "attempt-1")
        self.assertEqual(self.backend.queries[-1][0:2], (CATALOG_TABLE, "ENV#test"))

    def test_reserve_rejects_untrusted_notification_target_fields_before_writing(self):
        target = {
            "notificationPolicyId": "payment-status",
            "publishedVersionId": "version-1",
            "recipientSetId": "billing-operators",
            "recipientSetVersion": 1,
            "recipientMemberId": "primary",
            "notificationTypeTemplates": {
                "payment-failed": "payment-failed-v1",
                "payment-succeeded": "payment-succeeded-v1",
            },
        }
        invalid_targets = (
            {key: value for key, value in target.items() if key != "notificationPolicyId"},
            {key: value for key, value in target.items() if key != "notificationTypeTemplates"},
            {**target, "email": "attacker@example.test"},
            {**target, "recipientSetVersion": 0},
            {**target, "recipientSetVersion": True},
            {**target, "notificationPolicyId": "INVALID"},
            {**target, "notificationTypeTemplates": {}},
            {**target, "notificationTypeTemplates": {"payment-succeeded": "payment-failed-v1"}},
            {**target, "notificationTypeTemplates": {"custom": "custom-v1"}},
        )

        for notification_target in invalid_targets:
            with self.subTest(notification_target=notification_target), self.assertRaises(ValueError):
                self.reserve(notification_target=notification_target)

        self.assertEqual(self.backend.transactions, [])
        self.assertEqual(self.backend.items, {})

    def test_checkout_idempotency_binds_the_exact_notification_policy_target(self):
        self.seed_stock("landing", on_hand=10)
        target = {
            "notificationPolicyId": "payment-status",
            "publishedVersionId": "version-1",
            "recipientSetId": "billing-operators",
            "recipientSetVersion": 1,
            "recipientMemberId": "primary",
            "notificationTypeTemplates": {
                "payment-failed": "payment-failed-v1",
                "payment-succeeded": "payment-succeeded-v1",
            },
        }

        self.reserve(notification_target=target)

        with self.assertRaises(StorageConflict):
            self.reserve(notification_target={**target, "notificationPolicyId": "other-policy"})
        with self.assertRaises(StorageConflict):
            self.reserve(notification_target={
                **target,
                "notificationTypeTemplates": {"payment-succeeded": "payment-succeeded-v1"},
            })
        self.assertEqual(len(self.backend.transactions), 1)

    def test_due_reservation_can_be_deferred_without_starving_the_next_item(self):
        self.seed_stock("first", on_hand=2)
        self.seed_stock("second", on_hand=2)
        for index, stock_id in enumerate(("first", "second"), start=1):
            self.store.reserve_checkout(
                self.scope,
                self.order(
                    order_id=f"order-{index}",
                    attempt_id=f"attempt-{index}",
                    stock_ids=(stock_id,),
                ),
                f"reservation-{index}",
                location_id="primary",
                created_at_epoch=NOW,
                **self.metadata(f"reserve-{index}"),
            )
        due_at = NOW + 2_400

        self.assertEqual(
            [item.reservation_id for item in self.store.list_due_reservations("test", due_at, limit=1)],
            ["reservation-1"],
        )
        defer_metadata = self.metadata("defer-1")
        defer_metadata["now_epoch"] = due_at
        self.store.defer_reservation(
            self.scope,
            "reservation-1",
            due_at + 300,
            **defer_metadata,
        )

        self.assertEqual(
            [item.reservation_id for item in self.store.list_due_reservations("test", due_at, limit=1)],
            ["reservation-2"],
        )
        self.assertEqual(
            [item.reservation_id for item in self.store.list_due_reservations("test", due_at + 300)],
            ["reservation-2", "reservation-1"],
        )

    def test_due_reservations_can_read_a_bounded_second_gsi_page(self):
        self.seed_stock("first", on_hand=2)
        self.seed_stock("second", on_hand=2)
        for index, stock_id in enumerate(("first", "second"), start=1):
            self.store.reserve_checkout(
                self.scope,
                self.order(
                    order_id=f"page-order-{index}",
                    attempt_id=f"page-attempt-{index}",
                    stock_ids=(stock_id,),
                ),
                f"page-reservation-{index}",
                location_id="primary",
                created_at_epoch=NOW,
                **self.metadata(f"page-reserve-{index}"),
            )

        due = self.store.list_due_reservations(
            "test",
            NOW + 2_400,
            limit=1,
            max_pages=2,
        )

        self.assertEqual(
            [item.reservation_id for item in due],
            ["page-reservation-1", "page-reservation-2"],
        )
        self.assertEqual(len(self.backend.queries), 2)

    def test_due_gsi_results_are_environment_scoped_and_strongly_rechecked(self):
        self.seed_stock("landing", on_hand=2)
        self.reserve()
        due_at = NOW + 2_400
        marker_key = (
            CATALOG_TABLE,
            self.scope.partition_key,
            f"RESERVATION_DUE#{due_at:020d}#reservation-1",
        )
        stale = copy.deepcopy(self.backend.items[marker_key])
        del self.backend.items[marker_key]
        self.backend.query_due = lambda *_args: ([stale], None)

        self.assertEqual(self.store.list_due_reservations("test", due_at), [])
        self.assertEqual(self.store.list_due_reservations("production", due_at), [])

    def test_corrupt_due_gsi_item_is_returned_as_an_isolated_marker(self):
        self.seed_stock("landing", on_hand=2)
        self.reserve()
        due_at = NOW + 2_400
        marker = next(
            item for (_table, _pk, _sk), item in self.backend.items.items()
            if item.get("itemType") == "ReservationDue"
        )
        marker["reconcileAfter"] = "not-an-epoch"
        self.backend.query_due = lambda *_args: ([copy.deepcopy(marker)], None)

        due = self.store.list_due_reservations("test", due_at)

        self.assertEqual(len(due), 1)
        self.assertIs(type(due[0]), InvalidDueMarker)
        self.assertEqual(due[0].partition_key, self.scope.partition_key)

    def test_due_marker_primary_sort_key_must_match_its_business_identity(self):
        self.seed_stock("landing", on_hand=2)
        self.reserve()
        due_at = NOW + 2_400
        marker = next(
            item for (_table, _pk, _sk), item in self.backend.items.items()
            if item.get("itemType") == "ReservationDue"
        )
        old_key = (CATALOG_TABLE, self.scope.partition_key, marker["sk"])
        forged = copy.deepcopy(marker)
        forged["sk"] = f"RESERVATION_DUE#{due_at:020d}#alternate"
        del self.backend.items[old_key]
        self.backend.items[(CATALOG_TABLE, self.scope.partition_key, forged["sk"])] = forged

        due = self.store.list_due_reservations("test", due_at)

        self.assertEqual(len(due), 1)
        self.assertIs(type(due[0]), InvalidDueMarker)

    def test_reserve_aggregates_shared_stock_and_checks_limits_before_writing(self):
        self.seed_stock("landing", on_hand=10)
        shared = self.order(stock_ids=("landing", "landing"))

        self.reserve(shared)

        operations, _ = self.backend.transactions[-1]
        stock_puts = [operation for operation in operations if operation.get("item", {}).get("itemType") == "Stock"]
        movements = [operation for operation in operations if operation.get("item", {}).get("itemType") == "StockMovement"]
        self.assertEqual(len(stock_puts), 1)
        self.assertEqual(len(movements), 1)
        self.assertEqual(movements[0]["item"]["quantity"], 4)
        self.assertEqual(len(operations), 7)

        line = CheckoutLine("line", "offer", 1, Money(1, "MXN", SUPPORTED_CURRENCIES), None)
        with self.assertRaises(ValueError):
            PendingOrder("too-many", "attempt", tuple(
                CheckoutLine(f"line-{index}", f"offer-{index}", 1, line.unit_price, None)
                for index in range(21)
            ))
        self.assertEqual(len(self.backend.transactions), 1)
        with self.assertRaises(StorageLimitExceeded):
            _validate_transaction_plan([
                {
                    "kind": "put",
                    "table_name": CATALOG_TABLE,
                    "item": {"pk": "p", "sk": f"s-{index}"},
                }
                for index in range(101)
            ])

    def test_twenty_tracked_lines_use_one_bounded_45_action_transaction(self):
        scope = CommerceScope("test", "tenant-a", "draft-max", "draft-max.example.test")
        stock_ids = tuple(f"stock-{index}" for index in range(20))
        for stock_id in stock_ids:
            self.seed_stock(stock_id, on_hand=1, scope=scope)
        order = PendingOrder(
            "order-max",
            "attempt-max",
            tuple(
                CheckoutLine(
                    f"line-{index}",
                    f"offer-{index}",
                    1,
                    Money(1, "MXN", SUPPORTED_CURRENCIES),
                    stock_id,
                )
                for index, stock_id in enumerate(stock_ids)
            ),
        )

        self.store.reserve_checkout(
            scope,
            order,
            "reservation-max",
            location_id="primary",
            created_at_epoch=NOW,
            **self.metadata("reserve-max"),
        )

        operations, _ = self.backend.transactions[-1]
        self.assertEqual(len(operations), 45)
        self.assertLessEqual(len(operations), 100)
        self.assertEqual(
            len({
                (
                    operation["table_name"],
                    operation.get("pk") or operation["item"]["pk"],
                    operation.get("sk") or operation["item"]["sk"],
                )
                for operation in operations
            }),
            45,
        )

    def test_conditional_contention_rolls_back_every_planned_write(self):
        self.seed_stock("first", on_hand=5)
        self.seed_stock("second", on_hand=5)

        def competing_reservation():
            key = (CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#second")
            self.backend.items[key]["reserved"] = 5
            self.backend.items[key]["available"] = 0
            self.backend.items[key]["revision"] = 2

        self.backend.before_transact = competing_reservation
        with self.assertRaises(StorageConflict):
            self.reserve(self.order(stock_ids=("first", "second")))

        self.assertIsNone(self.backend.get(OPERATIONS_TABLE, self.scope.partition_key, "ORDER#order-1"))
        self.assertIsNone(self.backend.get(CATALOG_TABLE, self.scope.partition_key, "RESERVATION#reservation-1"))
        first = self.backend.get(CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#first")
        self.assertEqual((first["reserved"], first["revision"]), (0, 1))
        self.assertEqual(self.backend.transactions, [])

    def test_application_idempotency_replays_and_is_isolated_by_scope(self):
        self.seed_stock("landing", on_hand=10)

        first = self.reserve()
        second = self.reserve()

        self.assertEqual(second, first)
        self.assertEqual(len(self.backend.transactions), 1)
        changed = self.order(order_id="order-2")
        with self.assertRaises(StorageConflict):
            self.reserve(changed)

        other = CommerceScope("test", "tenant-a", "draft-b", "draft-b.example.test")
        self.seed_stock("landing", on_hand=10, scope=other)
        self.store.reserve_checkout(
            other,
            self.order(order_id="order-2", attempt_id="attempt-1"),
            "reservation-1",
            location_id="primary",
            created_at_epoch=NOW,
            **self.metadata("reserve"),
        )
        self.assertEqual(len(self.backend.transactions), 2)
        self.assertNotEqual(self.scope.partition_key, other.partition_key)
        self.assertNotEqual(
            self.backend.transactions[0][1],
            self.backend.transactions[1][1],
        )

    def test_concurrent_identical_idempotency_request_applies_once(self):
        self.seed_stock("landing", on_hand=10)
        winner = {}

        def competing_identical_request():
            winner["result"] = self.reserve()

        self.backend.before_transact = competing_identical_request
        replay = self.reserve()

        self.assertEqual(replay, winner["result"])
        self.assertEqual(len(self.backend.transactions), 1)
        stock = self.backend.get(CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["reserved"], stock["revision"]), (2, 2))

    def test_concurrent_different_payload_for_same_idempotency_key_conflicts(self):
        self.seed_stock("landing", on_hand=10)

        def competing_different_request():
            self.store.reserve_checkout(
                self.scope,
                self.order(order_id="order-2", attempt_id="attempt-2"),
                "reservation-2",
                location_id="primary",
                created_at_epoch=NOW,
                **self.metadata("reserve"),
            )

        self.backend.before_transact = competing_different_request
        with self.assertRaises(StorageConflict):
            self.reserve()

        self.assertEqual(len(self.backend.transactions), 1)
        self.assertIsNotNone(
            self.backend.get(OPERATIONS_TABLE, self.scope.partition_key, "ORDER#order-2")
        )
        self.assertIsNone(
            self.backend.get(OPERATIONS_TABLE, self.scope.partition_key, "ORDER#order-1")
        )

    def test_payment_attempt_binding_is_unique_inside_each_draft(self):
        self.seed_stock("landing", on_hand=10)
        self.reserve()

        with self.assertRaises(StorageConflict):
            self.store.reserve_checkout(
                self.scope,
                self.order(order_id="order-2", attempt_id="attempt-1"),
                "reservation-2",
                location_id="primary",
                created_at_epoch=NOW,
                **self.metadata("attempt-reuse"),
            )
        self.assertIsNone(
            self.backend.get(OPERATIONS_TABLE, self.scope.partition_key, "ORDER#order-2")
        )

    def test_ambiguous_write_reads_the_durable_receipt_before_retrying(self):
        self.seed_stock("landing", on_hand=10)
        self.backend.after_commit_error = RuntimeError("simulated lost response")

        result = self.reserve()

        self.assertEqual(result["status"], "reserved")
        self.assertEqual(len(self.backend.transactions), 1)
        self.assertEqual(self.reserve(), result)
        self.assertEqual(len(self.backend.transactions), 1)

    def test_ambiguous_write_without_receipt_is_an_unknown_outcome(self):
        self.seed_stock("landing", on_hand=10)
        self.backend.before_commit_error = RuntimeError("simulated transport loss")

        with self.assertRaises(StorageOutcomeUnknown) as unknown:
            self.reserve()

        self.assertIsNone(unknown.exception.__cause__)
        stock = self.backend.get(CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["reserved"], stock["revision"]), (0, 1))
        self.assertEqual(self.backend.transactions, [])

    def test_ambiguous_write_and_receipt_read_failure_stays_sanitized_unknown(self):
        self.seed_stock("landing", on_hand=10)
        self.backend.before_commit_error = RuntimeError("sensitive provider detail")
        self.backend.fail_receipt_read_after_error = True

        with self.assertRaises(StorageOutcomeUnknown) as unknown:
            self.reserve()

        self.assertIsNone(unknown.exception.__cause__)
        self.assertNotIn("sensitive", str(unknown.exception))

    def test_conditional_conflict_and_receipt_read_failure_stays_unknown(self):
        self.seed_stock("landing", on_hand=10)

        def competing_write():
            key = (CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#landing")
            self.backend.items[key]["revision"] = 2
            self.backend.next_get_error = RuntimeError("sensitive receipt detail")

        self.backend.before_transact = competing_write
        with self.assertRaises(StorageOutcomeUnknown) as unknown:
            self.reserve()

        self.assertIsNone(unknown.exception.__cause__)
        self.assertNotIn("sensitive", str(unknown.exception))

    def test_commit_and_release_are_atomic_idempotent_terminal_transitions(self):
        self.seed_stock("landing", on_hand=10)
        self.reserve()

        with self.assertRaises(ValueError):
            self.store.release_reservation(
                self.scope,
                "reservation-1",
                completion_reason="timeout",
                **self.metadata("unsafe-release"),
            )

        committed = self.store.commit_reservation(
            self.scope,
            "reservation-1",
            completion_reason="canonical_paid",
            **self.metadata("commit"),
        )

        self.assertEqual(committed["status"], "committed")
        committed_order = self.backend.get(OPERATIONS_TABLE, self.scope.partition_key, "ORDER#order-1")
        self.assertEqual(committed_order["status"], "paid")
        stock = self.backend.get(CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"], stock["available"]), (8, 0, 8))
        transaction_count = len(self.backend.transactions)
        self.assertEqual(
            self.store.commit_reservation(
                self.scope,
                "reservation-1",
                completion_reason="canonical_paid",
                **self.metadata("commit-retry"),
            ),
            committed,
        )
        self.assertEqual(len(self.backend.transactions), transaction_count)
        with self.assertRaises(StorageConflict):
            self.store.release_reservation(
                self.scope,
                "reservation-1",
                completion_reason="canonical_terminal_unpaid",
                **self.metadata("release-after-commit"),
            )

        scope = CommerceScope("test", "tenant-a", "draft-release", "draft-release.example.test")
        self.seed_stock("landing", on_hand=10, scope=scope)
        self.store.reserve_checkout(
            scope,
            self.order(order_id="order-release", attempt_id="attempt-release"),
            "reservation-release",
            location_id="primary",
            created_at_epoch=NOW,
            **self.metadata("reserve-release"),
        )
        released = self.store.release_reservation(
            scope,
            "reservation-release",
            completion_reason="canonical_terminal_unpaid",
            **self.metadata("release"),
        )
        self.assertEqual(released["status"], "released")
        released_order = self.backend.get(OPERATIONS_TABLE, scope.partition_key, "ORDER#order-release")
        self.assertEqual(released_order["status"], "payment_not_completed")
        self.assertEqual(released_order["completionReason"], "canonical_terminal_unpaid")
        self.assertEqual(released_order["revision"], 2)
        released_stock = self.backend.get(CATALOG_TABLE, scope.partition_key, "STOCK#primary#landing")
        self.assertEqual((released_stock["onHand"], released_stock["reserved"], released_stock["available"]), (10, 0, 10))

    def test_corrupt_reservation_timing_fails_before_terminal_stock_mutation(self):
        self.seed_stock("landing", on_hand=10)
        self.reserve()
        reservation_key = (CATALOG_TABLE, self.scope.partition_key, "RESERVATION#reservation-1")
        self.backend.items[reservation_key]["checkoutExpiresAt"] += 1

        with self.assertRaises(StorageConflict):
            self.store.commit_reservation(
                self.scope,
                "reservation-1",
                completion_reason="canonical_paid",
                **self.metadata("commit-corrupt"),
            )

        stock = self.backend.get(CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"], stock["available"]), (10, 2, 8))

    def test_commit_release_race_has_one_terminal_winner(self):
        self.seed_stock("landing", on_hand=10)
        self.reserve()

        def competing_release():
            self.store.release_reservation(
                self.scope,
                "reservation-1",
                completion_reason="canonical_terminal_unpaid",
                **self.metadata("race-release"),
            )

        self.backend.before_transact = competing_release
        with self.assertRaises(StorageConflict):
            self.store.commit_reservation(
                self.scope,
                "reservation-1",
                completion_reason="canonical_paid",
                **self.metadata("race-commit"),
            )

        reservation = self.backend.get(
            CATALOG_TABLE,
            self.scope.partition_key,
            "RESERVATION#reservation-1",
        )
        stock = self.backend.get(CATALOG_TABLE, self.scope.partition_key, "STOCK#primary#landing")
        self.assertEqual(reservation["status"], "released")
        self.assertEqual((stock["onHand"], stock["reserved"], stock["available"]), (10, 0, 10))

    def test_dynamo_adapter_emits_one_cross_table_request_and_preserves_unknown_errors(self):
        class ProviderError(RuntimeError):
            def __init__(self, code, reasons=()):
                self.response = {"Error": {"Code": code}, "CancellationReasons": list(reasons)}

        class Client:
            request = None
            error = None

            def transact_write_items(self, **kwargs):
                self.request = kwargs
                if self.error:
                    raise self.error

        client = Client()
        backend = _DynamoBackend(client)
        operations = [
            {
                "kind": "put",
                "table_name": CATALOG_TABLE,
                "item": {"pk": "scope", "sk": "stock", "revision": 2},
                "condition": {"revision": 1},
            },
            {
                "kind": "put",
                "table_name": OPERATIONS_TABLE,
                "item": {"pk": "scope", "sk": "order"},
                "condition": "absent",
            },
        ]

        backend.transact(operations, "a" * 36)

        self.assertEqual(len(client.request["TransactItems"]), 2)
        self.assertEqual(client.request["ClientRequestToken"], "a" * 36)
        self.assertEqual(
            {entry["Put"]["TableName"] for entry in client.request["TransactItems"]},
            {CATALOG_TABLE, OPERATIONS_TABLE},
        )
        client.error = ProviderError(
            "TransactionCanceledException",
            ({"Code": "ConditionalCheckFailed"},),
        )
        with self.assertRaises(ConditionalWriteFailed) as conflict:
            backend.transact(operations, "b" * 36)
        self.assertIsNone(conflict.exception.__cause__)
        client.error = ProviderError("RequestTimeout")
        with self.assertRaises(ProviderError):
            backend.transact(operations, "c" * 36)

    def test_dynamo_due_query_is_scoped_bounded_consistent_and_returns_items(self):
        class Client:
            request = None

            def query(self, **kwargs):
                self.request = kwargs
                return {
                    "Items": [
                        {
                            "pk": {"S": "scope"},
                            "sk": {"S": "RESERVATION_DUE#00000000000000000010#reservation"},
                            "itemType": {"S": "ReservationDue"},
                            "reservationId": {"S": "reservation"},
                            "reconcileAfter": {"N": "10"},
                        }
                    ],
                    "LastEvaluatedKey": {"pk": {"S": "next-scope"}, "sk": {"S": "next-marker"}},
                }

        client = Client()
        start = {"pk": {"S": "start-scope"}, "sk": {"S": "start-marker"}}
        items, cursor = _DynamoBackend(client).query_due(CATALOG_TABLE, "ENV#test", 10, 25, start)

        self.assertEqual(items[0]["reservationId"], "reservation")
        self.assertEqual(cursor, {"pk": {"S": "next-scope"}, "sk": {"S": "next-marker"}})
        self.assertEqual(client.request["TableName"], CATALOG_TABLE)
        self.assertEqual(client.request["IndexName"], "ReservationDueIndex")
        self.assertEqual(client.request["Limit"], 25)
        self.assertFalse(client.request["ConsistentRead"])
        self.assertEqual(client.request["ExclusiveStartKey"], start)
        self.assertEqual(client.request["ExpressionAttributeValues"][":pk"], {"S": "ENV#test"})
        self.assertEqual(
            client.request["ExpressionAttributeValues"][":end"],
            {"S": "00000000000000000010#\uffff"},
        )


if __name__ == "__main__":
    unittest.main()
