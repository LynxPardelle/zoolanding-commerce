import copy
import json
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.common.auth_admin import AuthenticationError, AuthorizationError, AuthorizedContext
from src.common.published_policy import ResolvedPolicies
from src.handlers import subscription_action
from src.storage import ConditionalWriteFailed, CommerceScope, StorageConflict
import src.subscription_storage as subscription_storage
from src.subscription_storage import SubscriptionProjectionStore


DOMAIN = "example.com"
TENANT_ID = "tenant-a"
DRAFT_ID = "draft-a"
OPERATIONS_TABLE = "operations-table"
NOW = 1_800_000_000


def pause_policy():
    return {
        "enabled": True,
        "newInvoiceBehavior": "void",
        "existingInvoiceBehavior": "unchanged",
        "accessBehavior": "suspend",
        "resume": {"mode": "manual"},
        "onResume": {
            "collection": "restore",
            "access": "restore-if-suspended",
        },
    }


def policies():
    commerce = {
        "status": "active",
        "adminAccess": {
            "mode": "auth-profile",
            "authProfileId": "staff",
            "capabilities": ["commerce:subscription:manage"],
        },
        "sellableTypes": ["service", "subscription"],
        "payments": {
            "bindingId": "payments-primary",
            "supportedCurrencies": ["MXN"],
            "oneTime": True,
            "subscriptions": True,
            "editablePrices": True,
            "coupons": True,
            "planChangePolicy": {"mode": "immediate-prorated"},
            "pausePolicy": pause_policy(),
        },
        "inventory": {"enabled": False, "tracked": False, "backorders": False, "locationId": "primary"},
        "shipping": {"enabled": False, "methods": ["free"]},
        "fiscal": {"enabled": False},
        "checkout": {
            "successPath": "/success",
            "cancelPath": "/cancel",
            "termsPath": "/terms",
            "privacyPath": "/privacy",
            "refundPolicyPath": "/refunds",
            "supportPath": "/support",
        },
    }
    return ResolvedPolicies(
        environment="test",
        tenant_id=TENANT_ID,
        draft_id=DRAFT_ID,
        domain=DOMAIN,
        version_id="version-1",
        prefix=f"sites/{DOMAIN}/versions/version-1/",
        commerce={
            "version": 1,
            "scope": {"environment": "test", "tenantId": TENANT_ID, "draftId": DRAFT_ID, "domain": DOMAIN},
            "commerce": commerce,
        },
        auth_registry={"version": 1, "profiles": []},
    )


def auth_context(resolved):
    return AuthorizedContext(
        environment=resolved.environment,
        tenant_id=resolved.tenant_id,
        draft_id=resolved.draft_id,
        domain=resolved.domain,
        subject="operator-subject",
        roles=("accountant",),
        profile={"authProfileId": "staff"},
        commerce=resolved.commerce["commerce"],
        session={"subject": "operator-subject"},
    )


def api_event(payload, *, path="/features/commerce/subscription/action"):
    return {
        "httpMethod": "POST",
        "path": path,
        "headers": {
            "X-ZLP-Domain": DOMAIN,
            "X-ZLP-Auth-Profile-Id": "staff",
            "Idempotency-Key": "subscription-operation-1",
        },
        "body": json.dumps(payload, separators=(",", ":")),
        "isBase64Encoded": False,
        "requestContext": {"requestId": "request-subscription-1"},
    }


class FakeGateway:
    def __init__(self):
        self.calls = []

    def execute(self, operation, scope, input_value, **metadata):
        self.calls.append((operation, scope, input_value, metadata))
        return {"commandId": "command-1", "status": "accepted"}


class FakeBackend:
    def __init__(self):
        self.items = {}

    def get(self, table_name, pk, sk):
        return copy.deepcopy(self.items.get((table_name, pk, sk)))

    def transact(self, operations, client_token):
        del client_token
        candidate = copy.deepcopy(self.items)
        for operation in operations:
            item = operation["item"]
            key = (operation["table_name"], item["pk"], item["sk"])
            current = candidate.get(key)
            condition = operation.get("condition")
            if condition == "absent" and current is not None:
                raise ConditionalWriteFailed()
            if isinstance(condition, dict) and (
                current is None
                or any(current.get(field) != expected for field, expected in condition.items())
            ):
                raise ConditionalWriteFailed()
            candidate[key] = copy.deepcopy(item)
        self.items = candidate


class SubscriptionBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.policies = policies()
        self.context = auth_context(self.policies)
        self.gateway = FakeGateway()

    def call(self, payload, *, authorize_error=None, use_gateway=True):
        auth = authorize_error if authorize_error is not None else self.context
        with (
            patch.object(subscription_action, "resolve_policies", return_value=self.policies),
            patch.object(subscription_action, "authorize_request", side_effect=auth if isinstance(auth, Exception) else None, return_value=None if isinstance(auth, Exception) else auth) as authorize,
            patch.object(subscription_action, "_gateway", return_value=self.gateway if use_gateway else subscription_action.UnavailableSubscriptionGateway()),
            patch("time.time", return_value=NOW),
        ):
            return subscription_action.lambda_handler(api_event(payload), None), authorize

    def test_exact_operations_are_authenticated_csrf_protected_and_provider_neutral(self):
        cases = (
            (
                "changePlan",
                {"subscriptionId": "subscription-1", "targetOfferVersionId": "offer-2", "expectedRevision": 1},
                {
                    "subscriptionId": "subscription-1",
                    "targetOfferVersionId": "offer-2",
                    "expectedRevision": 1,
                    "planChangePolicy": {"mode": "immediate-prorated"},
                    "previewTimestamp": NOW,
                },
            ),
            (
                "applyDiscount",
                {"subscriptionId": "subscription-1", "discountVersionId": "discount-1", "expectedRevision": 1},
                {"subscriptionId": "subscription-1", "discountVersionId": "discount-1", "expectedRevision": 1},
            ),
            (
                "pause",
                {"subscriptionId": "subscription-1", "expectedRevision": 1},
                {"subscriptionId": "subscription-1", "expectedRevision": 1, "pausePolicy": pause_policy()},
            ),
            (
                "resume",
                {"subscriptionId": "subscription-1", "expectedRevision": 2},
                {"subscriptionId": "subscription-1", "expectedRevision": 2, "pausePolicy": pause_policy()},
            ),
        )
        for operation, input_value, expected_forwarded in cases:
            with self.subTest(operation=operation):
                response, authorize = self.call({"operation": operation, "input": input_value})
                body = json.loads(response["body"])
                self.assertEqual(response["statusCode"], 200, body)
                self.assertEqual(body["data"], {"commandId": "command-1", "status": "accepted"})
                self.assertEqual(authorize.call_args.kwargs["capability"], "commerce:subscription:manage")
                self.assertTrue(authorize.call_args.kwargs["mutation"])
                _, scope, forwarded, metadata = self.gateway.calls[-1]
                self.assertEqual((scope.environment, scope.tenant_id, scope.draft_id, scope.domain), ("test", TENANT_ID, DRAFT_ID, DOMAIN))
                self.assertEqual(forwarded, expected_forwarded)
                self.assertEqual(metadata["idempotency_key"], "subscription-operation-1")
                self.assertNotIn("provider", repr(self.gateway.calls[-1]).lower())

    def test_unknown_browser_coordinates_policy_disabled_and_unavailable_gateway_fail_closed(self):
        invalid = {
            "operation": "pause",
            "input": {"subscriptionId": "subscription-1", "expectedRevision": 1},
            "tenantId": "other",
        }
        response, _ = self.call(invalid)
        self.assertEqual(response["statusCode"], 400)
        self.assertEqual(self.gateway.calls, [])

        self.policies.commerce["commerce"]["payments"]["pausePolicy"] = {"enabled": False}
        response, _ = self.call({"operation": "pause", "input": {"subscriptionId": "subscription-1", "expectedRevision": 1}})
        self.assertEqual(response["statusCode"], 403)
        self.assertEqual(self.gateway.calls, [])

        self.policies = policies()
        response, _ = self.call(
            {"operation": "resume", "input": {"subscriptionId": "subscription-1", "expectedRevision": 1}},
            use_gateway=False,
        )
        self.assertEqual(response["statusCode"], 503)
        self.assertNotIn("endpoint", response["body"].lower())

    def test_authentication_csrf_and_gateway_errors_are_sanitized(self):
        payload = {"operation": "resume", "input": {"subscriptionId": "subscription-1", "expectedRevision": 1}}
        for error, expected in ((AuthenticationError("detail"), 401), (AuthorizationError("csrf secret"), 403)):
            with self.subTest(expected=expected):
                response, _ = self.call(payload, authorize_error=error)
                self.assertEqual(response["statusCode"], expected)
                self.assertNotIn("detail", response["body"])
                self.assertNotIn("secret", response["body"])
                self.assertEqual(self.gateway.calls, [])

    def test_authentication_runs_before_policy_feature_disclosure(self):
        self.policies.commerce["commerce"]["payments"]["pausePolicy"] = {"enabled": False}
        response, _ = self.call(
            {"operation": "pause", "input": {"subscriptionId": "subscription-1", "expectedRevision": 1}},
            authorize_error=AuthenticationError("private"),
        )
        self.assertEqual(response["statusCode"], 401)

    def test_invalid_gateway_result_is_a_safe_error(self):
        self.gateway.execute = lambda *args, **kwargs: {"commandId": "HTTPS://provider.invalid", "status": "accepted"}
        response, _ = self.call({"operation": "resume", "input": {"subscriptionId": "subscription-1", "expectedRevision": 1}})
        self.assertEqual(response["statusCode"], 503)
        self.assertNotIn("provider.invalid", response["body"])

    def test_browser_cannot_inject_billing_provider_or_cross_draft_fields(self):
        forbidden_inputs = (
            ("changePlan", {"subscriptionId": "subscription-1", "targetOfferVersionId": "offer-2", "expectedRevision": 1, "proration": "prorate"}),
            ("changePlan", {"subscriptionId": "subscription-1", "targetOfferVersionId": "offer-2", "expectedRevision": 1, "previewTimestamp": NOW}),
            ("changePlan", {"subscriptionId": "subscription-1", "targetOfferVersionId": "offer-2", "expectedRevision": 1, "stripePriceId": "price-synthetic"}),
            ("pause", {"subscriptionId": "subscription-1", "expectedRevision": 1, "billingBehavior": "void"}),
            ("pause", {"subscriptionId": "subscription-1", "expectedRevision": 1, "accessBehavior": "suspend"}),
            ("pause", {"subscriptionId": "subscription-1", "expectedRevision": 1, "resumeAt": NOW + 3600}),
            ("resume", {"subscriptionId": "subscription-1", "expectedRevision": 1, "provider": "stripe"}),
            ("resume", {"subscriptionId": "subscription-1", "expectedRevision": 1, "stripeSubscriptionId": "sub-synthetic"}),
            ("resume", {"subscriptionId": "subscription-1", "expectedRevision": 1, "draftId": "draft-other"}),
        )
        for operation, input_value in forbidden_inputs:
            with self.subTest(operation=operation, field=set(input_value) - {"subscriptionId", "targetOfferVersionId", "expectedRevision"}):
                self.gateway.calls.clear()
                response, _ = self.call({"operation": operation, "input": input_value})
                self.assertEqual(response["statusCode"], 400)
                self.assertEqual(self.gateway.calls, [])

    def test_missing_invalid_and_disabled_policies_fail_closed_without_gateway(self):
        cases = (
            ("changePlan", "planChangePolicy", None, 503),
            ("changePlan", "planChangePolicy", {"mode": "operator-selectable"}, 503),
            ("changePlan", "planChangePolicy", {"mode": "disabled"}, 403),
            ("pause", "pausePolicy", None, 503),
            ("pause", "pausePolicy", {**pause_policy(), "newInvoiceBehavior": "draft"}, 503),
            ("pause", "pausePolicy", {"enabled": False}, 403),
            ("resume", "pausePolicy", {"enabled": False}, 403),
        )
        for operation, policy_name, policy_value, expected_status in cases:
            with self.subTest(operation=operation, policy=policy_value):
                self.policies = policies()
                payments = self.policies.commerce["commerce"]["payments"]
                if policy_value is None:
                    del payments[policy_name]
                else:
                    payments[policy_name] = policy_value
                self.gateway.calls.clear()
                input_value = {"subscriptionId": "subscription-1", "expectedRevision": 1}
                if operation == "changePlan":
                    input_value["targetOfferVersionId"] = "offer-2"
                response, _ = self.call({"operation": operation, "input": input_value})
                self.assertEqual(response["statusCode"], expected_status)
                self.assertEqual(self.gateway.calls, [])

    def test_next_renewal_does_not_create_a_preview_timestamp(self):
        self.policies.commerce["commerce"]["payments"]["planChangePolicy"] = {"mode": "next-renewal"}
        response, _ = self.call({
            "operation": "changePlan",
            "input": {"subscriptionId": "subscription-1", "targetOfferVersionId": "offer-2", "expectedRevision": 1},
        })
        self.assertEqual(response["statusCode"], 200)
        forwarded = self.gateway.calls[0][2]
        self.assertEqual(forwarded["planChangePolicy"], {"mode": "next-renewal"})
        self.assertNotIn("previewTimestamp", forwarded)

    def test_verified_subscription_projection_is_idempotent_scope_bound_and_contains_no_pii(self):
        backend = FakeBackend()
        store = SubscriptionProjectionStore(backend, OPERATIONS_TABLE)
        scope = CommerceScope("test", TENANT_ID, DRAFT_ID, DOMAIN)
        event = {
            "eventId": "event-1",
            "subscriptionId": "subscription-1",
            "offerVersionId": "offer-1",
            "status": "active",
            "currentPeriodEnd": NOW + 30 * 24 * 60 * 60,
            "sourceRevision": 2,
            "occurredAt": NOW,
        }

        first = store.apply_verified_event(scope, event, now_epoch=NOW + 60)
        replay = store.apply_verified_event(scope, event, now_epoch=NOW + 60)
        self.assertEqual(first, replay)
        self.assertEqual(first["revision"], 1)
        self.assertNotIn("tenant", repr(first).lower())
        self.assertNotIn("email", repr(backend.items).lower())
        inbox = backend.items[(OPERATIONS_TABLE, scope.partition_key, "EVENT_INBOX#event-1")]
        self.assertEqual(inbox["itemType"], "IntegrationEventInbox")
        self.assertEqual(inbox["eventType"], "commerce.subscription.updated.v1")
        self.assertNotIn("data", inbox)

        other = CommerceScope("test", TENANT_ID, "draft-b", "other.example.com")
        self.assertIsNone(store.get_projection(other, "subscription-1"))
        with self.assertRaises(ValueError):
            store.apply_verified_event(
                scope,
                {**event, "status": "provider_specific_state"},
                now_epoch=NOW + 60,
            )
        for index, field in enumerate(("sourceRevision", "occurredAt", "currentPeriodEnd")):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    store.apply_verified_event(
                        scope,
                        {
                            **event,
                            "eventId": f"event-limit-{index}",
                            field: 10_000_000_000,
                        },
                        now_epoch=NOW + 60,
                    )

        stale = store.apply_verified_event(
            scope,
            {
                **event,
                "eventId": "event-0",
                "status": "past_due",
                "sourceRevision": 1,
                "occurredAt": NOW - 1,
            },
            now_epoch=NOW + 60,
        )
        self.assertTrue(stale["stale"])
        self.assertEqual(store.get_projection(scope, "subscription-1")["status"], "active")
        self.assertIn((OPERATIONS_TABLE, scope.partition_key, "EVENT_INBOX#event-0"), backend.items)

        same_second = store.apply_verified_event(
            scope,
            {
                **event,
                "eventId": "event-2",
                "status": "past_due",
                "sourceRevision": 3,
            },
            now_epoch=NOW + 60,
        )
        self.assertEqual(same_second["revision"], 2)
        self.assertEqual(store.get_projection(scope, "subscription-1")["status"], "past_due")
        with self.assertRaises(StorageConflict):
            store.apply_verified_event(
                scope,
                {
                    **event,
                    "eventId": "event-ambiguous",
                    "status": "paused",
                    "sourceRevision": 3,
                },
                now_epoch=NOW + 60,
            )
        self.assertEqual(
            store.apply_verified_event(
                scope,
                {
                    **event,
                    "eventId": "event-late",
                    "status": "paused",
                    "sourceRevision": 2,
                    "occurredAt": NOW + 1,
                },
                now_epoch=NOW + 60,
            )["stale"],
            True,
        )
        self.assertEqual(
            store.apply_verified_event(
                scope,
                {
                    **event,
                    "eventId": "event-2",
                    "status": "past_due",
                    "sourceRevision": 3,
                },
                now_epoch=NOW + 60,
            ),
            same_second,
        )

    def test_projection_store_uses_the_canonical_operations_table_environment_name(self):
        backend = FakeBackend()
        with (
            patch.dict(os.environ, {"COMMERCE_OPERATIONS_TABLE_NAME": OPERATIONS_TABLE}, clear=True),
            patch.dict(sys.modules, {"boto3": SimpleNamespace(client=lambda _name: object())}),
            patch.object(subscription_storage, "_DynamoBackend", return_value=backend),
        ):
            store = SubscriptionProjectionStore.from_environment()
        self.assertEqual(store._table_name, OPERATIONS_TABLE)


if __name__ == "__main__":
    unittest.main()
