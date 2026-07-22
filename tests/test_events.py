import copy
import json
import unittest

from src.domain.offers import Money
from src.domain.orders import CheckoutLine, PendingOrder
from src.events import (
    IntegrationEventProcessor,
    IntegrationEventValidationError,
    parse_integration_event,
)
from src.handlers.integration_event_worker import process_batch as process_sqs_batch
from src.handlers.integration_event_worker import runtime_environment as integration_runtime_environment
from src.handlers.outbox_relay import process_batch as process_stream_batch
from src.handlers.outbox_relay import runtime_environment as outbox_runtime_environment
from src.outbox import OutboxRelay
from src.storage import CommerceScope, CommerceStore, StorageConflict
from src.subscription_storage import SubscriptionProjectionStore
from tests.test_storage import CATALOG_TABLE, OPERATIONS_TABLE, FakeBackend


NOW = 1_800_000_000
SCOPE = CommerceScope("test", "tenant-a", "draft-a", "draft-a.example.test")


def envelope(event_type, data, *, event_id="event-1", **overrides):
    value = {
        "schemaVersion": 1,
        "eventId": event_id,
        "eventType": event_type,
        "occurredAt": NOW,
        "environment": SCOPE.environment,
        "tenantId": SCOPE.tenant_id,
        "draftId": SCOPE.draft_id,
        "domain": SCOPE.domain,
        "data": data,
    }
    value.update(overrides)
    return value


def payment_data():
    return {
        "reservationId": "reservation-1",
        "orderId": "order-1",
        "paymentAttemptId": "attempt-1",
    }


class Projector:
    def __init__(self):
        self.calls = []

    def apply_verified_event(self, scope, event, *, now_epoch):
        self.calls.append((scope, copy.deepcopy(event), now_epoch))
        return {"status": "projected"}


class Publisher:
    def __init__(self):
        self.calls = []

    def publish(self, *, TopicArn, Message):
        self.calls.append((TopicArn, json.loads(Message)))
        return {"MessageId": "published-1"}


class FailingPublisher(Publisher):
    def publish(self, **_kwargs):
        raise RuntimeError("sensitive provider detail")


class CommerceEventTests(unittest.TestCase):
    def setUp(self):
        self.backend = FakeBackend()
        self.store = CommerceStore(self.backend, CATALOG_TABLE, OPERATIONS_TABLE)
        self.projector = Projector()
        self.processor = IntegrationEventProcessor(self.store, self.projector)
        self._seed_reserved_checkout()

    def _seed_reserved_checkout(self):
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
            created_at_epoch=NOW,
            idempotency_key="reserve",
            request_id="request-reserve",
            correlation_id="correlation-reserve",
            actor_hash=None,
            now_epoch=NOW,
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

    def process(self, event_type, data=None, **overrides):
        event = parse_integration_event(envelope(event_type, data or payment_data(), **overrides))
        return self.processor.process(event, now_epoch=NOW + 60)

    def test_parser_is_closed_versioned_bounded_and_provider_neutral(self):
        parsed = parse_integration_event(
            envelope("commerce.payment.succeeded.v1", payment_data())
        )
        self.assertEqual(parsed.scope, SCOPE)
        self.assertEqual(parsed.data["reservationId"], "reservation-1")

        invalid = [
            envelope("stripe.payment_intent.succeeded", payment_data()),
            envelope("commerce.payment.succeeded.v2", payment_data()),
            envelope("commerce.payment.succeeded.v1", {**payment_data(), "raw": {}}),
            envelope("commerce.payment.succeeded.v1", payment_data(), providerAccountId="acct"),
            envelope("commerce.payment.succeeded.v1", payment_data(), event_id="x" * 65),
        ]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(IntegrationEventValidationError):
                parse_integration_event(value)

    def test_verified_payment_success_commits_with_inbox_and_stable_outbox_atomically(self):
        result = self.process("commerce.payment.succeeded.v1")

        self.assertEqual(result["status"], "committed")
        stock = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"], stock["available"]), (8, 0, 8))
        inbox = self.backend.get(OPERATIONS_TABLE, SCOPE.partition_key, "EVENT_INBOX#event-1")
        self.assertEqual(inbox["eventType"], "commerce.payment.succeeded.v1")
        self.assertEqual(inbox["expiresAt"], NOW + 60 + 90 * 24 * 60 * 60)
        outboxes = [
            item for (table, _pk, _sk), item in self.backend.items.items()
            if table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
        ]
        self.assertEqual(len(outboxes), 1)
        self.assertEqual(outboxes[0]["eventType"], "notification.requested.v1")
        self.assertEqual(
            outboxes[0]["payload"],
            {
                "notificationPolicyId": "payment-status",
                "notificationType": "payment-succeeded",
                "publishedVersionId": "version-1",
                "templateId": "payment-succeeded-v1",
                "recipientSetId": "billing-operators",
                "recipientSetVersion": 1,
                "recipientMemberId": "primary",
                "source": {"type": "commerce-order", "id": "order-1"},
                "dedupeKey": outboxes[0]["eventId"],
                "variables": {
                    "orderId": {"type": "safe-id", "value": "order-1"},
                    "amountMinor": {"type": "integer", "value": 180_000},
                    "currency": {"type": "currency", "value": "MXN"},
                },
            },
        )
        self.assertNotIn("provider", repr(outboxes).lower())
        transaction_count = len(self.backend.transactions)

        replay = self.process("commerce.payment.succeeded.v1")
        self.assertEqual(replay, result)
        self.assertEqual(len(self.backend.transactions), transaction_count)

    def test_same_event_id_with_changed_payload_conflicts_without_mutation(self):
        self.process("commerce.payment.succeeded.v1")
        before = copy.deepcopy(self.backend.items)
        changed = payment_data()
        changed["paymentAttemptId"] = "attempt-other"

        with self.assertRaises(StorageConflict):
            self.process("commerce.payment.succeeded.v1", changed)
        self.assertEqual(self.backend.items, before)

    def test_future_skew_is_rejected_before_every_event_family_dispatch(self):
        before = copy.deepcopy(self.backend.items)
        future = envelope(
            "commerce.payment.succeeded.v1",
            payment_data(),
            occurredAt=NOW + 361,
        )
        with self.assertRaises(IntegrationEventValidationError):
            self.processor.process(parse_integration_event(future), now_epoch=NOW + 60)
        self.assertEqual(self.backend.items, before)

    def test_ambiguous_payment_write_replays_from_the_atomic_receipt(self):
        self.backend.after_commit_error = RuntimeError("simulated lost response")

        result = self.process("commerce.payment.succeeded.v1")

        self.assertEqual(result["status"], "committed")
        self.assertIsNotNone(
            self.backend.get(OPERATIONS_TABLE, SCOPE.partition_key, "EVENT_INBOX#event-1")
        )
        self.assertEqual(self.process("commerce.payment.succeeded.v1"), result)

    def test_payment_binding_and_scope_must_match_server_state(self):
        for index, changed in enumerate((
            {**payment_data(), "orderId": "order-other"},
            {**payment_data(), "paymentAttemptId": "attempt-other"},
            {**payment_data(), "reservationId": "reservation-other"},
        ), start=1):
            with self.subTest(changed=changed), self.assertRaises(StorageConflict):
                self.process("commerce.payment.succeeded.v1", changed, event_id=f"event-mismatch-{index}")
        other_scope = envelope(
            "commerce.payment.succeeded.v1",
            payment_data(),
            event_id="event-other-scope",
            draftId="draft-b",
            domain="draft-b.example.test",
        )
        with self.assertRaises(StorageConflict):
            self.processor.process(parse_integration_event(other_scope), now_epoch=NOW + 60)

    def test_terminal_unpaid_releases_and_refund_never_restocks(self):
        released = self.process("commerce.payment.terminal_unpaid.v1")
        self.assertEqual(released["status"], "released")
        stock = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock["onHand"], stock["reserved"], stock["available"]), (10, 0, 10))
        failed_outbox = next(
            item for (table, _pk, _sk), item in self.backend.items.items()
            if table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
        )
        self.assertEqual(failed_outbox["payload"]["notificationType"], "payment-failed")
        self.assertEqual(failed_outbox["payload"]["templateId"], "payment-failed-v1")

        second = CommerceEventTests(methodName="runTest")
        second.setUp()
        second.process("commerce.payment.succeeded.v1")
        paid_stock = second.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        refund = {
            "orderId": "order-1",
            "refundId": "refund-1",
            "amountMinor": 90_000,
            "currency": "MXN",
        }
        result = second.process("commerce.refund.confirmed.v1", refund, event_id="event-refund")
        self.assertEqual(result["status"], "recorded")
        self.assertEqual(
            second.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing"),
            paid_stock,
        )
        self.assertEqual(
            sum(
                1 for (table, _pk, _sk), item in second.backend.items.items()
                if table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
            ),
            1,
        )
        paid_order = second.backend.get(OPERATIONS_TABLE, SCOPE.partition_key, "ORDER#order-1")
        self.assertEqual(paid_order["status"], "refunded")

    def test_refund_before_payment_commits_once_and_later_payment_converges_without_restock(self):
        refund = {
            "orderId": "order-1",
            "refundId": "refund-early",
            "amountMinor": 90_000,
            "currency": "MXN",
        }
        first = self.process("commerce.refund.confirmed.v1", refund, event_id="event-refund-early")
        self.assertEqual(first["status"], "recorded")
        stock_after_refund = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual((stock_after_refund["onHand"], stock_after_refund["reserved"]), (8, 0))
        self.assertEqual(
            self.backend.get(OPERATIONS_TABLE, SCOPE.partition_key, "ORDER#order-1")["status"],
            "refunded",
        )

        paid = self.process("commerce.payment.succeeded.v1", event_id="event-payment-late")
        replay = self.process("commerce.refund.confirmed.v1", refund, event_id="event-refund-early")
        self.assertEqual(paid["status"], "committed")
        self.assertEqual(replay, first)
        stock_after_late_payment = self.backend.get(CATALOG_TABLE, SCOPE.partition_key, "STOCK#primary#landing")
        self.assertEqual(stock_after_late_payment, stock_after_refund)
        self.assertEqual(
            self.backend.get(OPERATIONS_TABLE, SCOPE.partition_key, "ORDER#order-1")["status"],
            "refunded",
        )

    def test_payment_without_a_server_resolved_notification_target_emits_no_outbox(self):
        fixture = CommerceEventTests(methodName="runTest")
        fixture.setUp()
        order = fixture.backend.items[(OPERATIONS_TABLE, SCOPE.partition_key, "ORDER#order-1")]
        order.pop("notificationTarget")

        fixture.process("commerce.payment.succeeded.v1")

        self.assertFalse(any(
            table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
            for (table, _pk, _sk), item in fixture.backend.items.items()
        ))

    def test_payment_type_not_enabled_by_the_pinned_policy_emits_no_outbox(self):
        order_key = (OPERATIONS_TABLE, SCOPE.partition_key, "ORDER#order-1")
        self.backend.items[order_key]["notificationTarget"]["notificationTypeTemplates"] = {
            "payment-succeeded": "payment-succeeded-v1",
        }

        result = self.process("commerce.payment.terminal_unpaid.v1")

        self.assertEqual(result["status"], "released")
        self.assertFalse(any(
            table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
            for (table, _pk, _sk), item in self.backend.items.items()
        ))

    def test_notification_dedupe_key_is_isolated_by_draft_scope(self):
        event = {
            "eventId": "event-1",
            "eventType": "commerce.payment.succeeded.v1",
            "eventHash": "a" * 64,
            "occurredAt": NOW,
        }
        metadata = {
            "request_id": "event-1",
            "correlation_id": "event-1",
            "actor_hash": None,
            "now_epoch": NOW,
        }
        order = {
            "orderId": "order-1",
            "amountMinor": 180_000,
            "currency": "MXN",
            "notificationTarget": {
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
        }
        other_scope = CommerceScope("test", "tenant-a", "draft-b", "draft-b.example.test")

        first = self.store._event_operations(SCOPE, event, {}, metadata, order=order)[1]["item"]
        second = self.store._event_operations(other_scope, event, {}, metadata, order=order)[1]["item"]

        self.assertNotEqual(first["eventId"], second["eventId"])
        self.assertEqual(first["payload"]["dedupeKey"], first["eventId"])
        self.assertEqual(second["payload"]["dedupeKey"], second["eventId"])

    def test_notification_event_identity_is_stable_for_replay_and_binds_the_complete_payload(self):
        event = {
            "eventId": "event-1",
            "eventType": "commerce.payment.succeeded.v1",
            "eventHash": "a" * 64,
            "occurredAt": NOW,
        }
        metadata = {
            "request_id": "event-1",
            "correlation_id": "event-1",
            "actor_hash": None,
            "now_epoch": NOW,
        }
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
        order = {
            "orderId": "order-1",
            "amountMinor": 180_000,
            "currency": "MXN",
            "notificationTarget": target,
        }

        first = self.store._event_operations(SCOPE, event, {}, metadata, order=order)[1]["item"]
        replay = self.store._event_operations(SCOPE, event, {}, metadata, order=order)[1]["item"]
        changed_amount = self.store._event_operations(
            SCOPE,
            event,
            {},
            metadata,
            order={**order, "amountMinor": 1},
        )[1]["item"]
        failed = self.store._event_operations(
            SCOPE,
            {**event, "eventType": "commerce.payment.terminal_unpaid.v1"},
            {},
            metadata,
            order=order,
        )[1]["item"]

        self.assertEqual(first["eventId"], replay["eventId"])
        self.assertNotEqual(first["eventId"], changed_amount["eventId"])
        self.assertNotEqual(first["eventId"], failed["eventId"])

    def test_subscription_event_uses_only_the_verified_projection_boundary(self):
        data = {
            "subscriptionId": "subscription-1",
            "offerVersionId": "offer-v1",
            "status": "active",
            "currentPeriodEnd": NOW + 30 * 24 * 60 * 60,
            "sourceRevision": 1,
        }
        result = self.process("commerce.subscription.updated.v1", data, event_id="event-subscription")

        self.assertEqual(result, {"status": "projected"})
        self.assertEqual(self.projector.calls[0][0], SCOPE)
        self.assertEqual(self.projector.calls[0][1]["eventId"], "event-subscription")
        self.assertEqual(self.projector.calls[0][2], NOW + 60)

    def test_subscription_receipt_uses_processing_time_and_rejects_far_future_events(self):
        projector = SubscriptionProjectionStore(self.backend, OPERATIONS_TABLE)
        processor = IntegrationEventProcessor(self.store, projector)
        data = {
            "subscriptionId": "subscription-1",
            "offerVersionId": "offer-v1",
            "status": "active",
            "currentPeriodEnd": NOW + 30 * 24 * 60 * 60,
            "sourceRevision": 1,
        }
        processor.process(
            parse_integration_event(envelope("commerce.subscription.updated.v1", data)),
            now_epoch=NOW + 60,
        )
        inbox = self.backend.get(OPERATIONS_TABLE, SCOPE.partition_key, "EVENT_INBOX#event-1")
        self.assertEqual(inbox["processedAt"], NOW + 60)
        self.assertEqual(inbox["expiresAt"], NOW + 60 + 90 * 24 * 60 * 60)

        future = envelope(
            "commerce.subscription.updated.v1",
            data,
            event_id="event-future",
            occurredAt=NOW + 361,
        )
        with self.assertRaises(IntegrationEventValidationError):
            processor.process(parse_integration_event(future), now_epoch=NOW + 60)

        for source_revision in (0, True):
            with self.subTest(source_revision=source_revision):
                invalid = envelope(
                    "commerce.subscription.updated.v1",
                    {**data, "sourceRevision": source_revision},
                )
                with self.assertRaises(IntegrationEventValidationError):
                    parse_integration_event(invalid)

    def test_sqs_batch_reports_only_malformed_or_failed_records(self):
        good = envelope("commerce.payment.succeeded.v1", payment_data())
        bad = envelope("unknown.v1", {})
        batch = {
            "Records": [
                {"messageId": "message-good", "body": json.dumps(good)},
                {"messageId": "message-json", "body": "{"},
                {"messageId": "message-unknown", "body": json.dumps(bad)},
            ]
        }

        result = process_sqs_batch(batch, self.processor, now_epoch=NOW + 60)

        self.assertEqual(
            result,
            {"batchItemFailures": [
                {"itemIdentifier": "message-json"},
                {"itemIdentifier": "message-unknown"},
            ]},
        )

        duplicate_json = '{"schemaVersion":1,"schemaVersion":1}'
        self.assertEqual(
            process_sqs_batch(
                {"Records": [{"messageId": "message-duplicate", "body": duplicate_json}]},
                self.processor,
                now_epoch=NOW + 60,
            ),
            {"batchItemFailures": [{"itemIdentifier": "message-duplicate"}]},
        )

    def test_sqs_worker_rejects_wrong_runtime_environment_before_mutation(self):
        batch = {
            "Records": [{
                "messageId": "message-wrong-env",
                "body": json.dumps(envelope("commerce.payment.succeeded.v1", payment_data())),
            }]
        }
        before = copy.deepcopy(self.backend.items)
        metrics = []
        self.assertEqual(
            process_sqs_batch(
                batch,
                self.processor,
                now_epoch=NOW + 60,
                expected_environment="production",
                metric_emitter=lambda name, value, **dimensions: metrics.append(
                    (name, value, dimensions)
                ),
            ),
            {"batchItemFailures": [{"itemIdentifier": "message-wrong-env"}]},
        )
        self.assertEqual(self.backend.items, before)
        self.assertEqual(
            metrics,
            [("TestLiveMismatch", 1, {"environment": "production"})],
        )

    def test_sqs_worker_emits_only_aggregate_migration_metrics_after_processing(self):
        data = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "dedupeKey": "migration-dedupe-1",
            "state": "running",
            "counts": {
                "total": 30,
                "pending": 25,
                "applied": 2,
                "needsReview": 1,
                "failed": 2,
            },
        }
        batch = {
            "Records": [{
                "messageId": "message-migration",
                "body": json.dumps(envelope("migration.progressed.v1", data)),
            }]
        }

        class AcceptingProcessor:
            def __init__(self):
                self.calls = []

            def process(self, parsed, *, now_epoch):
                self.calls.append((parsed, now_epoch))
                return {"status": "accepted"}

        processor = AcceptingProcessor()
        metrics = []
        result = process_sqs_batch(
            batch,
            processor,
            now_epoch=NOW + 60,
            expected_environment="test",
            metric_emitter=lambda name, value, **dimensions: metrics.append(
                (name, value, dimensions)
            ),
        )

        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(len(processor.calls), 1)
        self.assertEqual(
            metrics,
            [
                ("MigrationBacklog", 25, {"environment": "test"}),
                ("MigrationFailures", 3, {"environment": "test"}),
            ],
        )

    def test_runtime_environment_accepts_only_test_and_production(self):
        self.assertEqual(integration_runtime_environment("test"), "test")
        self.assertEqual(integration_runtime_environment("production"), "production")
        self.assertEqual(outbox_runtime_environment("production"), "production")
        for value in ("", "dev", "prod", "TEST"):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                integration_runtime_environment(value)

    def test_outbox_relay_uses_fixed_topic_marks_once_and_rejects_arbitrary_events(self):
        self.process("commerce.payment.succeeded.v1")
        outbox = next(
            item for (table, _pk, _sk), item in self.backend.items.items()
            if table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
        )
        publisher = Publisher()
        relay = OutboxRelay(
            self.store,
            publisher,
            "arn:aws:sns:us-east-1:111122223333:fixed",
            "test",
        )
        record = {
            "eventID": "stream-1",
            "eventName": "INSERT",
            "dynamodb": {"SequenceNumber": "100000000000000000001", "NewImage": self._to_ddb(outbox)},
        }

        self.assertEqual(process_stream_batch({"Records": [record]}, relay, now_epoch=NOW + 90), {"batchItemFailures": []})
        self.assertEqual(len(publisher.calls), 1)
        delivered = self.store.get_outbox(SCOPE, outbox["eventId"])
        self.assertEqual(delivered["expiresAt"], NOW + 90 + 90 * 24 * 60 * 60)

        pending_fixture = CommerceEventTests(methodName="runTest")
        pending_fixture.setUp()
        pending_fixture.process("commerce.payment.succeeded.v1")
        pending = next(
            item for (table, _pk, _sk), item in pending_fixture.backend.items.items()
            if table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
        )
        failing_relay = OutboxRelay(
            pending_fixture.store,
            FailingPublisher(),
            "arn:aws:sns:us-east-1:111122223333:fixed",
            "test",
        )
        failed_publish = {
            "eventID": "stream-publish-failed",
            "eventName": "INSERT",
            "dynamodb": {"SequenceNumber": "100000000000000000002", "NewImage": self._to_ddb(pending)},
        }
        self.assertEqual(
            process_stream_batch({"Records": [failed_publish]}, failing_relay, now_epoch=NOW + 92),
            {"batchItemFailures": [{"itemIdentifier": "100000000000000000002"}]},
        )
        self.assertEqual(
            pending_fixture.store.get_outbox(SCOPE, pending["eventId"])["deliveryStatus"],
            "pending",
        )
        self.assertEqual(publisher.calls[0][0], "arn:aws:sns:us-east-1:111122223333:fixed")
        self.assertEqual(
            publisher.calls[0][1],
            {
                "schemaVersion": 1,
                "eventId": outbox["eventId"],
                "eventType": "notification.requested.v1",
                "occurredAt": NOW + 60,
                "environment": SCOPE.environment,
                "tenantId": SCOPE.tenant_id,
                "draftId": SCOPE.draft_id,
                "domain": SCOPE.domain,
                "data": outbox["payload"],
            },
        )
        serialized = json.dumps(publisher.calls[0][1], sort_keys=True).lower()
        for forbidden in (
            "address", "body", "customer", "email", "fiscal", "paymentattempt",
            "provider", "secret", "stripe",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, serialized)
        self.assertEqual(process_stream_batch({"Records": [record]}, relay, now_epoch=NOW + 91), {"batchItemFailures": []})
        self.assertEqual(len(publisher.calls), 1)

    def test_outbox_relay_rejects_unknown_sensitive_or_invalid_policy_payload_before_publish(self):
        cases = (
            ("unknown", lambda payload: payload.update({"body": "forbidden"})),
            ("missing-policy", lambda payload: payload.pop("notificationPolicyId")),
            ("forged-policy", lambda payload: payload.update({"notificationPolicyId": "INVALID"})),
            ("wrong-template", lambda payload: payload.update({"templateId": "payment-failed-v1"})),
            ("paired-type-template", lambda payload: payload.update({
                "notificationType": "payment-failed",
                "templateId": "payment-failed-v1",
            })),
            ("changed-amount", lambda payload: payload["variables"]["amountMinor"].update({"value": 1})),
            ("changed-order", lambda payload: (
                payload["source"].update({"id": "order-other"}),
                payload["variables"]["orderId"].update({"value": "order-other"}),
            )),
        )
        for name, mutate in cases:
            with self.subTest(name=name):
                fixture = CommerceEventTests(methodName="runTest")
                fixture.setUp()
                fixture.process("commerce.payment.succeeded.v1")
                outbox = next(
                    item for (table, _pk, _sk), item in fixture.backend.items.items()
                    if table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
                )
                mutate(outbox["payload"])
                key = (OPERATIONS_TABLE, SCOPE.partition_key, outbox["sk"])
                fixture.backend.items[key] = copy.deepcopy(outbox)
                publisher = Publisher()
                relay = OutboxRelay(
                    fixture.store,
                    publisher,
                    "arn:aws:sns:us-east-1:111122223333:fixed",
                    "test",
                )

                with self.assertRaises(StorageConflict):
                    relay.relay(outbox, now_epoch=NOW + 90)
                self.assertEqual(publisher.calls, [])

    def test_outbox_relay_rejects_wrong_environment_and_malformed_canonical_fields_before_publish(self):
        self.process("commerce.payment.succeeded.v1")
        outbox = next(
            item for (table, _pk, _sk), item in self.backend.items.items()
            if table == OPERATIONS_TABLE and item.get("itemType") == "Outbox"
        )
        publisher = Publisher()
        relay = OutboxRelay(
            self.store,
            publisher,
            "arn:aws:sns:us-east-1:111122223333:fixed",
            "test",
        )

        wrong_environment = copy.deepcopy(outbox)
        wrong_environment["environment"] = "production"
        with self.assertRaises(StorageConflict):
            relay.relay(wrong_environment, now_epoch=NOW + 90)

        key = (OPERATIONS_TABLE, SCOPE.partition_key, outbox["sk"])
        malformed = copy.deepcopy(outbox)
        malformed["createdAt"] = -1
        self.backend.items[key] = malformed
        with self.assertRaises(StorageConflict):
            relay.relay(malformed, now_epoch=NOW + 90)
        self.assertEqual(publisher.calls, [])

        malicious = copy.deepcopy(outbox)
        malicious["eventType"] = "arbitrary.v1"
        malicious["topicArn"] = "arn:aws:sns:us-east-1:111122223333:attacker"
        failed = {
            "eventID": "stream-2",
            "eventName": "INSERT",
            "dynamodb": {"SequenceNumber": "100000000000000000003", "NewImage": self._to_ddb(malicious)},
        }
        self.assertEqual(
            process_stream_batch({"Records": [failed]}, relay, now_epoch=NOW + 92),
            {"batchItemFailures": [{"itemIdentifier": "100000000000000000003"}]},
        )
        self.assertEqual(len(publisher.calls), 0)

    @classmethod
    def _to_ddb(cls, item):
        def encode(value):
            if isinstance(value, bool):
                return {"BOOL": value}
            if isinstance(value, str):
                return {"S": value}
            if type(value) is int:
                return {"N": str(value)}
            if isinstance(value, dict):
                return {"M": {key: encode(child) for key, child in value.items()}}
            raise AssertionError(type(value))

        return {key: encode(value) for key, value in item.items()}


if __name__ == "__main__":
    unittest.main()
