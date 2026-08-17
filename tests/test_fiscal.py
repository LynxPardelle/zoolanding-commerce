import copy
import hashlib
import json
import os
import unittest
from unittest.mock import patch

from src.common.auth_admin import AuthenticationError, AuthorizationError, AuthorizedContext
from src.common.published_policy import ResolvedPolicies
from src.domain.offers import Money
from src.domain.orders import CheckoutLine, PendingOrder
from src.events import IntegrationEventProcessor, parse_integration_event
from src.fiscal_storage import (
    MAX_CLAIM_ATTEMPTS,
    FiscalCaptureDisabled,
    FiscalScope,
    FiscalStore,
    fiscal_request_window_seconds,
    new_fiscal_access_proof,
)
from src.handlers import fiscal_admin, fiscal_request
from src.storage import CommerceScope, CommerceStore, StorageConflict, StorageNotFound
from tests.test_storage import CATALOG_TABLE, OPERATIONS_TABLE, FakeBackend


FISCAL_TABLE = "fiscal-table"
NOW = 1_800_000_000
ACTOR_HASH = "a" * 64


def fiscal_details():
    return {
        "rfc": "XXX000000XXX",
        "legalName": "Synthetic Example",
        "postalCode": "00000",
        "fiscalRegime": "000",
        "cfdiUse": "X00",
        "contactEmail": "billing-contact@example.invalid",
    }


def fiscal_policy(*, request_window_hours=24):
    return {
        "enabled": True,
        "manual": True,
        "disclosureId": "manual-invoice-v1",
        "taxBehavior": "exclusive",
        "retentionDays": 90,
        "requestWindowHours": request_window_hours,
        "accountantApprovalId": "approval-1",
    }


def resolved_policies(*, environment="test", enabled=True, request_window_hours=24):
    fiscal = fiscal_policy(request_window_hours=request_window_hours) if enabled else {"enabled": False}
    commerce = {
        "status": "active",
        "adminAccess": {
            "mode": "auth-profile",
            "authProfileId": "staff",
            "capabilities": ["commerce:fiscal:manage"],
        },
        "fiscal": fiscal,
    }
    return ResolvedPolicies(
        environment=environment,
        tenant_id="tenant-a",
        draft_id="draft-a",
        domain="example.com",
        version_id="version-1",
        prefix="sites/example.com/versions/version-1/",
        commerce={
            "version": 1,
            "scope": {
                "environment": environment,
                "tenantId": "tenant-a",
                "draftId": "draft-a",
                "domain": "example.com",
            },
            "commerce": commerce,
        },
        auth_registry={"version": 1, "profiles": []},
    )


def auth_context(policies):
    return AuthorizedContext(
        environment=policies.environment,
        tenant_id=policies.tenant_id,
        draft_id=policies.draft_id,
        domain=policies.domain,
        subject="accountant-subject",
        roles=("accountant",),
        profile={"authProfileId": "staff"},
        commerce=policies.commerce["commerce"],
        session={"subject": "accountant-subject"},
    )


def api_event(path, payload, *, origin="https://example.com", idempotency_key="fixture"):
    headers = {
        "X-ZLP-Domain": "example.com",
        "X-ZLP-Auth-Profile-Id": "staff",
        "Cookie": "__Host-zlp_session=session-value",
    }
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    if origin is not None:
        headers["Origin"] = origin
    return {
        "httpMethod": "POST",
        "path": path,
        "headers": headers,
        "body": json.dumps(payload, separators=(",", ":")),
        "isBase64Encoded": False,
        "requestContext": {"requestId": "request-fiscal-1"},
    }


def response_body(response):
    return json.loads(response["body"])


class FiscalScenario(unittest.TestCase):
    def setUp(self):
        self.backend = FakeBackend()
        self.commerce_scope = CommerceScope("test", "tenant-a", "draft-a", "example.com")
        self.fiscal_scope = FiscalScope("test", "tenant-a", "draft-a", "example.com")
        self.commerce = CommerceStore(self.backend, CATALOG_TABLE, OPERATIONS_TABLE)
        self.fiscal = FiscalStore(self.backend, FISCAL_TABLE, OPERATIONS_TABLE)
        self.proof, proof_hash = new_fiscal_access_proof()
        self.order = PendingOrder(
            "order-1",
            "attempt-1",
            (CheckoutLine("line-1", "offer-1", 1, Money(90_000, "MXN", frozenset({"MXN"})), None),),
        )
        self.commerce.reserve_checkout(
            self.commerce_scope,
            self.order,
            "reservation-1",
            location_id="primary",
            created_at_epoch=NOW,
            idempotency_key="checkout-1",
            request_id="checkout-1",
            correlation_id="checkout-1",
            actor_hash=None,
            now_epoch=NOW,
            fiscal_access={"proofHash": proof_hash, "windowSeconds": 24 * 60 * 60},
        )

    def payment_event(self, event_type, *, event_id="event-payment", now_epoch=NOW + 60):
        event = parse_integration_event({
            "schemaVersion": 1,
            "eventId": event_id,
            "eventType": event_type,
            "occurredAt": NOW + 30,
            "environment": "test",
            "tenantId": "tenant-a",
            "draftId": "draft-a",
            "domain": "example.com",
            "data": {
                "reservationId": "reservation-1",
                "orderId": "order-1",
                "paymentAttemptId": "attempt-1",
            },
        })
        return IntegrationEventProcessor(self.commerce).process(event, now_epoch=now_epoch)

    def redeem(self, *, details=None, proof=None, order_id="order-1", key="fiscal-submit-1", now=NOW + 61):
        return self.fiscal.redeem_claim(
            self.fiscal_scope,
            order_id,
            proof or self.proof,
            details or fiscal_details(),
            idempotency_key=key,
            now_epoch=now,
        )


class FiscalCheckoutHandoffTests(FiscalScenario):
    def test_lost_checkout_response_can_rotate_the_pending_proof_without_persisting_it(self):
        replacement, replacement_hash = new_fiscal_access_proof()
        replay = self.commerce.reserve_checkout(
            self.commerce_scope,
            self.order,
            "reservation-1",
            location_id="primary",
            created_at_epoch=NOW + 10,
            idempotency_key="checkout-1",
            request_id="checkout-retry",
            correlation_id="checkout-retry",
            actor_hash=None,
            now_epoch=NOW + 10,
            fiscal_access={"proofHash": replacement_hash, "windowSeconds": 24 * 60 * 60},
        )
        self.assertEqual(replay["fiscalAccessHash"], replacement_hash)
        self.assertNotIn(replacement, repr(self.backend.items))

        self.payment_event("commerce.payment.succeeded.v1")
        with self.assertRaises(StorageConflict):
            self.redeem(proof=self.proof, key="old-proof")
        accepted = self.redeem(proof=replacement, key="replacement-proof")
        self.assertEqual(accepted["status"], "requested")

    def test_proof_is_hash_only_and_has_no_authority_until_verified_payment(self):
        self.assertNotIn(self.proof, repr(self.backend.items))
        with self.assertRaises(StorageConflict):
            self.redeem(now=NOW + 1)

        self.payment_event("commerce.payment.succeeded.v1")
        result = self.redeem()

        self.assertEqual(result["status"], "requested")
        self.assertNotIn("details", result)
        self.assertNotIn(self.proof, repr(self.backend.items))
        access = self.backend.get(OPERATIONS_TABLE, self.commerce_scope.partition_key, "FISCAL_ACCESS#order-1")
        self.assertEqual(access["state"], "consumed")
        self.assertEqual(access["expiresAt"], NOW + 60 + 24 * 60 * 60)
        request = self.fiscal.get_request(self.fiscal_scope, result["requestId"])
        self.assertEqual(request["details"], fiscal_details())

    def test_terminal_unpaid_wrong_scope_wrong_proof_and_expiry_never_authorize(self):
        self.payment_event("commerce.payment.terminal_unpaid.v1")
        with self.assertRaises(StorageConflict):
            self.redeem()

        fresh = FiscalScenario(methodName="runTest")
        fresh.setUp()
        fresh.payment_event("commerce.payment.succeeded.v1")
        with self.assertRaises(StorageNotFound):
            fresh.fiscal.redeem_claim(
                FiscalScope("test", "tenant-a", "draft-b", "other.example.com"),
                "order-1",
                fresh.proof,
                fiscal_details(),
                idempotency_key="scope-mismatch",
                now_epoch=NOW + 61,
            )
        wrong, _ = new_fiscal_access_proof()
        with self.assertRaises(StorageConflict):
            fresh.redeem(proof=wrong)
        with self.assertRaises(StorageConflict):
            fresh.redeem(now=NOW + 60 + 24 * 60 * 60)

    def test_exact_replay_is_stable_but_key_or_payload_reuse_is_rejected(self):
        self.payment_event("commerce.payment.succeeded.v1")
        first = self.redeem()
        transaction_count = len(self.backend.transactions)
        self.assertEqual(self.redeem(), first)
        self.assertEqual(len(self.backend.transactions), transaction_count)

        with self.assertRaises(StorageConflict):
            self.redeem(details={**fiscal_details(), "fiscalRegime": "001"})
        with self.assertRaises(StorageConflict):
            self.redeem(key="different-key")

    def test_ambiguous_commit_rereads_receipt_and_conditional_race_creates_no_second_request(self):
        self.payment_event("commerce.payment.succeeded.v1")
        self.backend.after_commit_error = RuntimeError("simulated lost response")
        result = self.redeem()
        self.assertEqual(result["status"], "requested")
        self.assertEqual(
            sum(item.get("itemType") == "FiscalRequest" for item in self.backend.items.values()),
            1,
        )

        other = FiscalScenario(methodName="runTest")
        other.setUp()
        other.payment_event("commerce.payment.succeeded.v1")
        key = (OPERATIONS_TABLE, other.commerce_scope.partition_key, "FISCAL_ACCESS#order-1")
        other.backend.before_transact = lambda: other.backend.items[key].update({"state": "consumed"})
        with self.assertRaises(StorageConflict):
            other.redeem()
        self.assertFalse(any(item.get("itemType") == "FiscalRequest" for item in other.backend.items.values()))

    def test_invalid_payload_consumes_bounded_attempts_without_persisting_it(self):
        self.payment_event("commerce.payment.succeeded.v1")
        invalid = {**fiscal_details(), "contactEmail": "invalid\naddress"}
        for _ in range(MAX_CLAIM_ATTEMPTS):
            with self.assertRaises(ValueError):
                self.redeem(details=invalid)
        self.assertNotIn("invalid\naddress", repr(self.backend.items))
        with self.assertRaises(StorageConflict):
            self.redeem()

    def test_manual_correction_ready_delivery_is_conditional_and_keeps_pii_in_fiscal(self):
        self.payment_event("commerce.payment.succeeded.v1")
        request_id = self.redeem()["requestId"]
        with self.assertRaises(StorageConflict):
            self.fiscal.transition_request(
                self.fiscal_scope,
                request_id,
                "markDelivered",
                expected_revision=1,
                actor_hash=ACTOR_HASH,
                now_epoch=NOW + 62,
            )
        correction = self.fiscal.transition_request(
            self.fiscal_scope,
            request_id,
            "markNeedsCorrection",
            expected_revision=1,
            actor_hash=ACTOR_HASH,
            now_epoch=NOW + 62,
            reason_code="invalid_tax_profile",
        )
        corrected = self.fiscal.correct_request(
            self.fiscal_scope,
            request_id,
            {**fiscal_details(), "fiscalRegime": "001"},
            expected_revision=correction["revision"],
            actor_hash=ACTOR_HASH,
            now_epoch=NOW + 63,
        )
        ready = self.fiscal.transition_request(
            self.fiscal_scope,
            request_id,
            "markReady",
            expected_revision=corrected["revision"],
            actor_hash=ACTOR_HASH,
            now_epoch=NOW + 64,
        )
        delivered = self.fiscal.transition_request(
            self.fiscal_scope,
            request_id,
            "markDelivered",
            expected_revision=ready["revision"],
            actor_hash=ACTOR_HASH,
            now_epoch=NOW + 65,
            delivery_reference_id="delivery-1",
        )
        self.assertEqual(delivered["status"], "delivered")
        self.assertNotIn("pac", repr(delivered).lower())
        operations_only = [item for (table, _pk, _sk), item in self.backend.items.items() if table == OPERATIONS_TABLE]
        for value in (
            fiscal_details()["rfc"],
            fiscal_details()["legalName"],
            fiscal_details()["contactEmail"],
        ):
            self.assertNotIn(value, repr(operations_only))


class FiscalGateTests(unittest.TestCase):
    def test_test_window_is_24_hours_and_production_is_hard_blocked_even_with_all_parameters(self):
        self.assertEqual(fiscal_request_window_seconds(fiscal_policy(), "test", {}), 24 * 60 * 60)
        with self.assertRaises(FiscalCaptureDisabled):
            fiscal_request_window_seconds(fiscal_policy(request_window_hours=12), "test", {})
        gates = {
            "FISCAL_PRODUCTION_APPROVED": "true",
            "FISCAL_RETENTION_APPROVAL_ID": "retention-1",
            "FISCAL_ACCESS_APPROVAL_ID": "access-1",
        }
        with self.assertRaises(FiscalCaptureDisabled):
            fiscal_request_window_seconds(fiscal_policy(request_window_hours=48), "production", gates)


class FiscalHandlerTests(FiscalScenario):
    def setUp(self):
        super().setUp()
        self.policies = resolved_policies()
        self.payment_event("commerce.payment.succeeded.v1")

    def test_admin_missing_session_is_rejected_before_published_policy_io(self):
        request = api_event(
            "/features/commerce/fiscal/admin",
            {"operation": "getRequest", "input": {"requestId": "request-1"}},
        )
        request["headers"].pop("Cookie")
        with patch.object(fiscal_admin, "resolve_policies") as resolver:
            response = fiscal_admin.lambda_handler(request, None)

        self.assertEqual(response["statusCode"], 401)
        resolver.assert_not_called()

    def public_request(self, payload, *, origin="https://example.com", key="fixture", policies=None):
        with (
            patch.object(fiscal_request, "resolve_commerce_policy", return_value=policies or self.policies) as resolver,
            patch.object(fiscal_request, "_store", return_value=self.fiscal),
            patch.object(fiscal_request.time, "time", return_value=NOW + 61),
            patch.dict(os.environ, {}, clear=True),
        ):
            response = fiscal_request.lambda_handler(
                api_event("/features/commerce/fiscal/request", payload, origin=origin, idempotency_key=key),
                None,
            )
        return response, resolver

    def admin_request(self, payload, *, auth_error=None):
        context = auth_context(self.policies)
        with (
            patch.object(fiscal_admin, "resolve_policies", return_value=self.policies),
            patch.object(
                fiscal_admin,
                "authorize_request",
                side_effect=auth_error,
                return_value=None if auth_error else context,
            ) as authorize,
            patch.object(fiscal_admin, "_store", return_value=self.fiscal),
            patch.object(fiscal_admin.time, "time", return_value=NOW + 62),
            patch.dict(os.environ, {}, clear=True),
        ):
            response = fiscal_admin.lambda_handler(
                api_event("/features/commerce/fiscal/admin", payload),
                None,
            )
        return response, authorize

    def submission_payload(self):
        return {
            "operation": "submitRequest",
            "orderId": "order-1",
            "fiscalAccessProof": self.proof,
            "input": fiscal_details(),
        }

    def test_public_request_requires_same_origin_and_idempotency_without_echoing_pii(self):
        response, resolver = self.public_request(self.submission_payload())
        self.assertEqual(response["statusCode"], 200, response_body(response))
        resolver.assert_called_once_with("example.com")
        for sensitive in fiscal_details().values():
            self.assertNotIn(sensitive, response["body"])

        for origin in (None, "null", "http://example.com", "https://attacker.invalid"):
            with self.subTest(origin=origin):
                blocked, resolver = self.public_request(self.submission_payload(), origin=origin, key="other-key")
                self.assertEqual(blocked["statusCode"], 403)
                resolver.assert_not_called()
        missing, _ = self.public_request(self.submission_payload(), key=None)
        self.assertEqual(missing["statusCode"], 400)

    def test_public_request_policy_and_production_gates_fail_closed(self):
        disabled, _ = self.public_request(self.submission_payload(), policies=resolved_policies(enabled=False))
        self.assertEqual(disabled["statusCode"], 404)
        production, _ = self.public_request(
            self.submission_payload(),
            policies=resolved_policies(environment="production", request_window_hours=48),
        )
        self.assertEqual(production["statusCode"], 503)

    def test_admin_capability_read_and_csrf_mutation_are_isolated(self):
        request_id = response_body(self.public_request(self.submission_payload())[0])["data"]["requestId"]
        read, authorize = self.admin_request({"operation": "getRequest", "input": {"requestId": request_id}})
        self.assertEqual(response_body(read)["data"]["details"], fiscal_details())
        self.assertEqual(authorize.call_args.kwargs["capability"], "commerce:fiscal:manage")
        self.assertFalse(authorize.call_args.kwargs["mutation"])

        payload = {"operation": "markReady", "input": {"requestId": request_id, "expectedRevision": 1}}
        for error, status in ((AuthenticationError("private"), 401), (AuthorizationError("private"), 403)):
            with self.subTest(status=status):
                blocked, _ = self.admin_request(payload, auth_error=error)
                self.assertEqual(blocked["statusCode"], status)
                self.assertNotIn("private", blocked["body"])


if __name__ == "__main__":
    unittest.main()
