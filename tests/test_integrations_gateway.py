import importlib.util
import json
import socket
import unittest
from types import SimpleNamespace
from urllib.error import HTTPError

from src.domain.offers import Money, OfferRecurrence, OfferVersion
from src.storage import CommerceScope


SCOPE = CommerceScope("test", "tenant-a", "draft-a", "example.com")
CONNECTION_ID = "stripe-main"


class FakeResponse:
    def __init__(self, payload, *, status=200, url=None):
        self.status = status
        self._body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self._url = url

    def read(self, amount):
        return self._body[:amount]

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class FakeOpener:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def open(self, request, timeout):
        self.calls.append((request, timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if outcome._url is None:
            outcome._url = request.full_url
        return outcome


class FakeSigner:
    def sign(self, method, url, body, headers):
        del method, url, body
        return {
            **headers,
            "Authorization": "AWS4-HMAC-SHA256 Credential=access/test",
            "X-Amz-Date": "20260721T120000Z",
            "X-Amz-Security-Token": "session",
        }


class RecordingTransport:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def request(self, method, path, payload):
        self.calls.append((method, path, payload))
        return self.result(payload) if callable(self.result) else self.result


def offer():
    return OfferVersion(
        version_id="offer-1",
        catalog_item_id="item-1",
        variant_id=None,
        revision=3,
        sellable_type="subscription",
        unit_price=Money(90_000, "MXN", frozenset({"MXN"})),
        tax_behavior="exclusive",
        recurrence=OfferRecurrence("month"),
        lifecycle_state="provisioning",
        lifecycle_revision=2,
        presentation_revision=4,
        display_name="Plan mensual",
        display_description="Servicio administrado",
    )


class SigV4TransportTests(unittest.TestCase):
    def transport(self, opener):
        from src.integrations_gateway import SigV4ExecuteApiTransport

        return SigV4ExecuteApiTransport(
            api_id="abcdefghij",
            stage="test",
            region="us-east-1",
            credentials=SimpleNamespace(
                access_key="access", secret_key="secret", token="session"
            ),
            opener=opener,
            signer=FakeSigner(),
        )

    def test_signs_exact_execute_api_method_path_and_returns_only_json(self):
        opener = FakeOpener([FakeResponse({"commandId": "command-1", "status": "accepted"})])
        result = self.transport(opener).request(
            "POST",
            "/internal/v1/stripe/offer",
            {"version": 1, "idempotencyKey": "same-command"},
        )

        request, timeout = opener.calls[0]
        self.assertEqual(
            request.full_url,
            "https://abcdefghij.execute-api.us-east-1.amazonaws.com/test/internal/v1/stripe/offer",
        )
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), {"version": 1, "idempotencyKey": "same-command"})
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertTrue(headers["authorization"].startswith("AWS4-HMAC-SHA256 "))
        self.assertIn("x-amz-date", headers)
        self.assertEqual(headers["x-amz-security-token"], "session")
        self.assertLessEqual(timeout, 3)
        self.assertEqual(result, {"commandId": "command-1", "status": "accepted"})
        self.assertNotIn("authorization", repr(result).lower())

    @unittest.skipUnless(
        importlib.util.find_spec("botocore") is not None,
        "botocore is supplied by the Lambda/SAM runtime",
    )
    def test_runtime_botocore_signer_emits_execute_api_sigv4_headers(self):
        from botocore.credentials import Credentials
        from src.integrations_gateway import SigV4ExecuteApiTransport

        opener = FakeOpener([
            FakeResponse({"commandId": "command-1", "status": "accepted"})
        ])
        transport = SigV4ExecuteApiTransport(
            api_id="abcdefghij",
            stage="test",
            region="us-east-1",
            credentials=Credentials("access", "secret", "session"),
            opener=opener,
        )

        transport.request(
            "POST", "/internal/v1/stripe/offer", {"version": 1}
        )

        headers = {
            key.lower(): value for key, value in opener.calls[0][0].header_items()
        }
        self.assertRegex(
            headers["authorization"],
            r"^AWS4-HMAC-SHA256 Credential=access/.+/us-east-1/execute-api/aws4_request,",
        )
        self.assertEqual(headers["x-amz-security-token"], "session")

    def test_rejects_arbitrary_api_ids_stages_methods_and_paths_before_network(self):
        from src.integrations_gateway import GatewayConfigurationError, SigV4ExecuteApiTransport

        credentials = SimpleNamespace(
            access_key="access", secret_key="secret", token=None
        )
        opener = FakeOpener([])
        invalid_configs = (
            {"api_id": "https://evil.example", "stage": "test", "region": "us-east-1"},
            {"api_id": "abcdefghij", "stage": "dev", "region": "us-east-1"},
            {"api_id": "abcdefghij", "stage": "test/other", "region": "us-east-1"},
            {"api_id": "abcdefghij", "stage": "test", "region": "https://evil.example"},
        )
        for config in invalid_configs:
            with self.subTest(config=config), self.assertRaises(GatewayConfigurationError):
                SigV4ExecuteApiTransport(
                    **config,
                    credentials=credentials,
                    opener=opener,
                    signer=FakeSigner(),
                )

        transport = self.transport(opener)
        for method, path in (
            ("DELETE", "/internal/v1/stripe/offer"),
            ("POST", "https://evil.example/internal/v1/stripe/offer"),
            ("POST", "/internal/v1/stripe/offer/../checkout"),
            ("POST", "/internal/v1/stripe/not-approved"),
        ):
            with self.subTest(method=method, path=path), self.assertRaises(GatewayConfigurationError):
                transport.request(method, path, {"version": 1})
        self.assertEqual(opener.calls, [])

    def test_denies_redirects_and_sanitizes_timeout_or_retryable_http_failures(self):
        from src.integrations_gateway import IntegrationsUnavailable

        redirect = HTTPError(
            "https://abcdefghij.execute-api.us-east-1.amazonaws.com/test/internal/v1/stripe/offer",
            302,
            "redirect",
            {"Location": "https://evil.example/private"},
            None,
        )
        opener = FakeOpener([redirect])
        with self.assertRaisesRegex(IntegrationsUnavailable, "unavailable") as raised:
            self.transport(opener).request("POST", "/internal/v1/stripe/offer", {"secret": "never-echo"})
        self.assertNotIn("evil", str(raised.exception))
        self.assertNotIn("secret", str(raised.exception))
        self.assertEqual(len(opener.calls), 1)

        retrying = FakeOpener([
            HTTPError("https://example.invalid", 429, "quota", {}, None),
            HTTPError("https://example.invalid", 503, "provider detail", {}, None),
        ])
        with self.assertRaisesRegex(IntegrationsUnavailable, "unavailable"):
            self.transport(retrying).request(
                "POST", "/internal/v1/stripe/checkout", {"idempotencyKey": "stable"}
            )
        self.assertEqual(len(retrying.calls), 2)
        self.assertEqual(retrying.calls[0][0].data, retrying.calls[1][0].data)

        timed_out = FakeOpener([socket.timeout("private network detail"), socket.timeout("again")])
        with self.assertRaisesRegex(IntegrationsUnavailable, "unavailable") as timeout_error:
            self.transport(timed_out).request(
                "GET", "/internal/v1/stripe/checkout-status", {"idempotencyKey": "stable"}
            )
        self.assertNotIn("private", str(timeout_error.exception))
        self.assertEqual(len(timed_out.calls), 2)

    def test_final_provider_failure_emits_one_environment_only_metric(self):
        from src.integrations_gateway import IntegrationsUnavailable, SigV4ExecuteApiTransport

        opener = FakeOpener([
            HTTPError("https://example.invalid", 503, "provider detail", {}, None),
            socket.timeout("private network detail"),
        ])
        metrics = []
        transport = SigV4ExecuteApiTransport(
            api_id="abcdefghij",
            stage="test",
            region="us-east-1",
            credentials=SimpleNamespace(
                access_key="access", secret_key="secret", token="session"
            ),
            opener=opener,
            signer=FakeSigner(),
            metric_emitter=lambda name, value, **dimensions: metrics.append(
                (name, value, dimensions)
            ),
        )

        with self.assertRaisesRegex(IntegrationsUnavailable, "unavailable"):
            transport.request(
                "POST",
                "/internal/v1/stripe/checkout",
                {"idempotencyKey": "stable"},
            )

        self.assertEqual(
            metrics,
            [("ProviderFailures", 1, {"environment": "test"})],
        )


class IntegrationsGatewayContractTests(unittest.TestCase):
    def test_offer_command_uses_exact_snapshot_hash_scope_route_and_idempotency(self):
        from src.integrations_gateway import InternalIntegrationsGateway, canonical_hash

        transport = RecordingTransport(
            lambda payload: {"commandId": payload["commandId"], "status": "accepted"}
        )
        gateway = InternalIntegrationsGateway(transport)
        result = gateway.provision_offer(SCOPE, CONNECTION_ID, offer())

        self.assertEqual(result["status"], "accepted")
        method, path, payload = transport.calls[0]
        self.assertEqual((method, path), ("POST", "/internal/v1/stripe/offer"))
        snapshot = offer().provider_snapshot()
        content_hash = canonical_hash({"schemaVersion": 1, "snapshot": snapshot})
        self.assertEqual(payload["scope"], {
            "environment": "test",
            "tenantId": "tenant-a",
            "draftId": "draft-a",
            "domain": "example.com",
        })
        self.assertEqual(payload["connectionId"], CONNECTION_ID)
        self.assertEqual(payload["input"], {
            "resourceId": "offer-1",
            "revision": 3,
            "schemaVersion": 1,
            "snapshot": snapshot,
            "contentHash": content_hash,
            "operation": "provision",
        })
        expected_key = "integrations-command-v1:" + canonical_hash({
            "scope": payload["scope"],
            "connectionId": CONNECTION_ID,
            "operation": "provision",
            "resourceId": "offer-1",
            "revision": 3,
            "contentHash": content_hash,
        })
        self.assertEqual(payload["idempotencyKey"], expected_key)
        self.assertRegex(payload["commandId"], r"^command-[a-f0-9]{40}$")

    def test_discount_presentation_uses_its_exact_operation_revision_hash_and_route(self):
        from src.domain.offers import DiscountVersion
        from src.integrations_gateway import InternalIntegrationsGateway, canonical_hash

        transport = RecordingTransport(
            lambda payload: {"commandId": payload["commandId"], "status": "accepted"}
        )
        discount = DiscountVersion(
            "discount-1",
            2,
            "once",
            percentage_basis_points=1_000,
            lifecycle_state="active",
            lifecycle_revision=3,
            presentation_revision=4,
            display_name="Descuento recurrente",
            display_description="Beneficio del plan",
        )

        result = InternalIntegrationsGateway(transport).update_discount_presentation(
            SCOPE,
            CONNECTION_ID,
            discount,
        )

        self.assertEqual(result["status"], "accepted")
        method, path, payload = transport.calls[0]
        self.assertEqual((method, path), ("POST", "/internal/v1/stripe/discount"))
        snapshot = {
            "displayName": "Descuento recurrente",
            "displayDescription": "Beneficio del plan",
        }
        content_hash = canonical_hash({"schemaVersion": 1, "snapshot": snapshot})
        self.assertEqual(payload["input"], {
            "resourceId": "discount-1",
            "revision": 4,
            "schemaVersion": 1,
            "snapshot": snapshot,
            "contentHash": content_hash,
            "operation": "presentation",
        })
        self.assertEqual(
            payload["idempotencyKey"],
            "integrations-command-v1:" + canonical_hash({
                "scope": payload["scope"],
                "connectionId": CONNECTION_ID,
                "operation": "discount-presentation",
                "resourceId": "discount-1",
                "revision": 4,
                "contentHash": content_hash,
            }),
        )

    def test_checkout_and_status_commands_are_closed_and_never_return_provider_ids(self):
        from src.integrations_gateway import InternalIntegrationsGateway, IntegrationsUnavailable

        transport = RecordingTransport(lambda payload: {
            "commandId": payload["commandId"],
            "status": "accepted",
            "redirectUrl": "https://checkout.stripe.com/c/pay/cs_test_safe",
            "expiresAt": 1_800_002_100,
        })
        gateway = InternalIntegrationsGateway(transport)
        checkout_input = {
            "orderId": "order-1",
            "paymentAttemptId": "attempt-1",
            "revision": 1,
            "reservationIds": ["reservation-1"],
            "checkoutExpiresAt": 1_800_002_100,
            "offerBindings": [{
                "offerVersionId": "offer-1",
                "revision": 3,
                "quantity": 1,
                "sellableType": "subscription",
                "snapshot": offer().provider_snapshot(),
                "contentHash": "a" * 64,
            }],
            "taxPolicy": {"mode": "disabled"},
            "shippingPolicy": {"collection": "none"},
            "paymentCollection": "immediate_card_link",
        }
        result = gateway.create_checkout(SCOPE, CONNECTION_ID, checkout_input)
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(result["redirectUrl"], "https://checkout.stripe.com/c/pay/cs_test_safe")
        self.assertEqual(transport.calls[0][:2], ("POST", "/internal/v1/stripe/checkout"))
        self.assertNotIn("stripeId", repr(transport.calls[0][2]))
        self.assertNotIn("email", repr(transport.calls[0][2]).lower())

        status_transport = RecordingTransport({
            "orderId": "order-1",
            "paymentAttemptId": "attempt-1",
            "revision": 1,
            "status": "pending",
        })
        status = InternalIntegrationsGateway(status_transport).lookup_status(
            SCOPE, CONNECTION_ID, "order-1", "attempt-1", 1
        )
        self.assertEqual(status, "pending")
        self.assertEqual(status_transport.calls[0][:2], ("GET", "/internal/v1/stripe/checkout-status"))
        self.assertEqual(set(status_transport.calls[0][2]["input"]), {
            "orderId", "paymentAttemptId", "revision"
        })

        bad = RecordingTransport(lambda payload: {
            "commandId": payload["commandId"],
            "status": "accepted",
            "redirectUrl": "https://evil.example/collect",
            "expiresAt": 1_800_002_100,
        })
        with self.assertRaises(IntegrationsUnavailable):
            InternalIntegrationsGateway(bad).create_checkout(SCOPE, CONNECTION_ID, checkout_input)

    def test_subscription_commands_use_exact_routes_and_accept_needs_review(self):
        from src.integrations_gateway import InternalIntegrationsGateway, canonical_hash

        transport = RecordingTransport(
            lambda payload: {"commandId": payload["commandId"], "status": "needs_review"}
        )
        gateway = InternalIntegrationsGateway(transport)
        result = gateway.execute_subscription(
            "changePlan",
            SCOPE,
            CONNECTION_ID,
            {
                "subscriptionId": "subscription-1",
                "targetOfferVersionId": "offer-2",
                "expectedRevision": 4,
                "planChangePolicy": {"mode": "immediate-prorated"},
                "previewTimestamp": 1_800_000_000,
            },
            idempotency_key="browser-key",
        )
        self.assertEqual(result["status"], "needs_review")
        self.assertEqual(transport.calls[0][:2], ("POST", "/internal/v1/stripe/subscription/change"))
        forwarded = transport.calls[0][2]
        self.assertEqual(forwarded["input"]["previewTimestamp"], 1_800_000_000)
        self.assertTrue(forwarded["idempotencyKey"].startswith("integrations-command-v1:"))
        self.assertNotIn("browser-key", repr(forwarded))

        portal_transport = RecordingTransport(lambda payload: {
            "commandId": payload["commandId"],
            "status": "accepted",
            "redirectUrl": "https://billing.stripe.com/p/session/test_safe",
            "expiresAt": 1_800_000_600,
        })
        portal = InternalIntegrationsGateway(portal_transport).execute_subscription(
            "openPortal",
            SCOPE,
            CONNECTION_ID,
            {"subscriptionId": "subscription-1"},
            idempotency_key="browser-key",
        )
        self.assertEqual(portal["status"], "accepted")
        self.assertEqual(portal_transport.calls[0][:2], ("POST", "/internal/v1/stripe/customer-portal"))
        portal_payload = portal_transport.calls[0][2]
        portal_input = portal_payload["input"]
        self.assertEqual(set(portal_input), {"subscriptionId", "portalAttemptId"})
        self.assertEqual(portal_input["subscriptionId"], "subscription-1")
        self.assertRegex(portal_input["portalAttemptId"], r"^portal-[a-f0-9]{56}$")
        content_hash = canonical_hash(portal_input)
        self.assertEqual(
            portal_payload["idempotencyKey"],
            "integrations-command-v1:" + canonical_hash({
                "scope": portal_payload["scope"],
                "connectionId": CONNECTION_ID,
                "operation": "customer-portal",
                "resourceId": "subscription-1",
                "revision": 1,
                "contentHash": content_hash,
            }),
        )
        self.assertNotIn("browser-key", repr(portal_payload))

        InternalIntegrationsGateway(portal_transport).execute_subscription(
            "openPortal",
            SCOPE,
            CONNECTION_ID,
            {"subscriptionId": "subscription-1"},
            idempotency_key="browser-key",
        )
        replay_payload = portal_transport.calls[1][2]
        self.assertEqual(replay_payload, portal_payload)

        InternalIntegrationsGateway(portal_transport).execute_subscription(
            "openPortal",
            SCOPE,
            CONNECTION_ID,
            {"subscriptionId": "subscription-1"},
            idempotency_key="new-browser-key",
        )
        new_attempt_payload = portal_transport.calls[2][2]
        self.assertNotEqual(
            new_attempt_payload["input"]["portalAttemptId"],
            portal_input["portalAttemptId"],
        )
        self.assertNotEqual(new_attempt_payload["commandId"], portal_payload["commandId"])
        self.assertNotIn("new-browser-key", repr(new_attempt_payload))

        other_scope = CommerceScope("test", "tenant-a", "draft-b", "example.com")
        InternalIntegrationsGateway(portal_transport).execute_subscription(
            "openPortal",
            other_scope,
            CONNECTION_ID,
            {"subscriptionId": "subscription-1"},
            idempotency_key="browser-key",
        )
        self.assertNotEqual(
            portal_transport.calls[3][2]["input"]["portalAttemptId"],
            portal_input["portalAttemptId"],
        )

    def test_portal_redirect_rejects_expired_or_wrong_host_handoffs(self):
        from src.integrations_gateway import InternalIntegrationsGateway, IntegrationsUnavailable

        for redirect_url, expires_at in (
            ("https://billing.stripe.com/p/session/expired", 1),
            ("https://billing.stripe.com.evil.example/p/session", 1_900_000_000),
        ):
            with self.subTest(redirect_url=redirect_url):
                transport = RecordingTransport(lambda payload: {
                    "commandId": payload["commandId"],
                    "status": "accepted",
                    "redirectUrl": redirect_url,
                    "expiresAt": expires_at,
                })
                with self.assertRaises(IntegrationsUnavailable):
                    InternalIntegrationsGateway(transport).execute_subscription(
                        "openPortal",
                        SCOPE,
                        CONNECTION_ID,
                        {"subscriptionId": "subscription-1"},
                        idempotency_key="browser-key",
                    )

    def test_gateway_rejects_integer_overflow_before_transport(self):
        from src.integrations_gateway import InternalIntegrationsGateway

        transport = RecordingTransport({
            "orderId": "order-1",
            "paymentAttemptId": "attempt-1",
            "revision": 10_000_000_000,
            "status": "pending",
        })
        with self.assertRaises(ValueError):
            InternalIntegrationsGateway(transport).lookup_status(
                SCOPE,
                CONNECTION_ID,
                "order-1",
                "attempt-1",
                10_000_000_000,
            )
        self.assertEqual(transport.calls, [])


if __name__ == "__main__":
    unittest.main()
