import base64
from dataclasses import replace
import importlib.util
import os
import unittest
from unittest.mock import patch

from tests.test_catalog_handlers import api_event, resolved_policies, response_body


PUBLIC_CHECKOUT_RECOVERY_KEY = base64.urlsafe_b64encode(b"r" * 32).decode("ascii").rstrip("=")


def checkout_event(payload, *, headers=None):
    merged_headers = {
        "idempotency-key": PUBLIC_CHECKOUT_RECOVERY_KEY,
        "origin": "https://test.zoolandingpage.com.mx",
    }
    merged_headers.update(headers or {})
    event = api_event(
        "/features/commerce/public-action",
        payload,
        headers=merged_headers,
    )
    event["queryStringParameters"] = {"draftDomain": "example.com"}
    return event


class FakeCatalog:
    def __init__(self, offers):
        self.offers = offers
        self.calls = []

    def get_checkout_offer(self, scope, version_id, supported_currencies):
        self.calls.append((scope, version_id, supported_currencies))
        return self.offers[version_id]


class FakeCommerce:
    def __init__(self):
        self.calls = []

    def reserve_checkout(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        order = args[1]
        result = {
            "reservationId": args[2],
            "orderId": order.order_id,
            "paymentAttemptId": order.payment_attempt_id,
            "status": "reserved",
            "reservationCreatedAt": kwargs["created_at_epoch"],
            "checkoutExpiresAt": kwargs["created_at_epoch"] + 2_100,
            "reconcileAfter": kwargs["created_at_epoch"] + 2_400,
        }
        fiscal_access = kwargs.get("fiscal_access")
        if fiscal_access is not None:
            result["fiscalAccessHash"] = fiscal_access["proofHash"]
        return result


class FakeCheckoutGateway:
    def __init__(self):
        self.calls = []

    def create_checkout(self, scope, connection_id, command_input):
        self.calls.append((scope, connection_id, command_input))
        return {
            "commandId": "command-checkout",
            "status": "accepted",
            "redirectUrl": "https://checkout.stripe.com/c/pay/test",
            "expiresAt": command_input["checkoutExpiresAt"],
        }


def offer_and_item(*, sellable_type="service", recurring=False, variant_id=None):
    from src.domain.catalog import CatalogItem, CatalogVariant
    from src.domain.offers import Money, OfferRecurrence, OfferVersion

    variants = () if variant_id is None else (CatalogVariant(variant_id, "SKU-1"),)
    return (
        OfferVersion(
            "offer-1",
            "item-1",
            variant_id,
            1,
            sellable_type,
            Money(90_000, "MXN", frozenset({"MXN"})),
            "exclusive",
            recurrence=OfferRecurrence("month") if recurring else None,
            lifecycle_state="active",
            lifecycle_revision=3,
        ),
        CatalogItem("item-1", sellable_type, variants),
    )


@patch.dict(
    os.environ,
    {
        "ENVIRONMENT_NAME": "test",
        "TEST_PREVIEW_ORIGIN": "https://test.zoolandingpage.com.mx",
    },
)
class CheckoutHandlerContractTests(unittest.TestCase):
    def setUp(self):
        from src.handlers import checkout

        self.gateway = FakeCheckoutGateway()
        gateway_patch = patch.object(checkout, "_gateway", return_value=self.gateway)
        gateway_patch.start()
        self.addCleanup(gateway_patch.stop)

    def test_checkout_handler_exists(self):
        self.assertIsNotNone(importlib.util.find_spec("src.handlers.checkout"))

    def test_checkout_api_exists(self):
        from src.handlers import checkout

        self.assertTrue(hasattr(checkout, "lambda_handler"))

    def test_admission_resolves_server_price_scope_location_ids_and_time(self):
        from src.handlers import checkout

        policies = resolved_policies()
        policies.notification_policies.update({
            "version": 1,
            "scope": policies.scope,
            "policies": [{
                "id": "payment-status",
                "status": "active",
                "provider": "email.smtp",
                "connectionId": "billing-mailbox",
                "notificationTypes": ["payment-succeeded", "payment-failed"],
                "templateIds": ["payment-succeeded-v1", "payment-failed-v1"],
                "recipientSets": [{
                    "id": "billing-operators", "version": 1, "members": [{"id": "primary"}],
                }],
                "retryPolicy": {"maxAttempts": 5},
                "acceptanceStatus": "accepted_by_smtp",
            }],
        })
        catalog = FakeCatalog({"offer-1": offer_and_item()})
        commerce = FakeCommerce()
        with (
            patch.object(checkout, "resolve_checkout_policy", return_value=policies) as resolver,
            patch.object(checkout, "_catalog_store", return_value=catalog),
            patch.object(checkout, "_commerce_store", return_value=commerce),
            patch.object(checkout.time, "time", return_value=1_800_000_000),
            patch.object(
                checkout,
                "_new_id",
                side_effect=["order-generated", "attempt-generated", "reservation-generated", "line-generated"],
            ),
        ):
            response = checkout.lambda_handler(
                checkout_event(
                    {"operation": "admitCheckout", "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 2}]}},
                ),
                None,
            )

        self.assertEqual(response["statusCode"], 200)
        resolver.assert_called_once_with("example.com")
        self.assertEqual(catalog.calls[0][2], frozenset({"MXN"}))
        args, kwargs = commerce.calls[0]
        scope, order, reservation_id = args
        self.assertEqual((scope.environment, scope.tenant_id, scope.draft_id, scope.domain), (
            "test", "tenant-example", "draft-example", "example.com"
        ))
        self.assertEqual(reservation_id, "reservation-generated")
        self.assertEqual(order.order_id, "order-generated")
        self.assertEqual(order.payment_attempt_id, "attempt-generated")
        self.assertEqual(order.lines[0].unit_price.amount_minor, 90_000)
        self.assertEqual(order.lines[0].quantity, 2)
        self.assertIsNone(order.lines[0].stock_id)
        self.assertEqual(kwargs["location_id"], "primary")
        self.assertEqual(kwargs["created_at_epoch"], 1_800_000_000)
        self.assertEqual(kwargs["now_epoch"], 1_800_000_000)
        self.assertEqual(
            kwargs["idempotency_key"],
            f"public-checkout-recovery-v1:{PUBLIC_CHECKOUT_RECOVERY_KEY}",
        )
        self.assertEqual(kwargs["notification_target"], {
            "publishedVersionId": "version-1",
            "recipientSetId": "billing-operators",
            "recipientSetVersion": 1,
            "recipientMemberId": "primary",
        })
        self.assertNotIn(PUBLIC_CHECKOUT_RECOVERY_KEY, response["body"])
        self.assertEqual(
            response_body(response)["data"],
            {
                "commandId": "command-checkout",
                "status": "accepted",
                "redirectUrl": "https://checkout.stripe.com/c/pay/test",
                "expiresAt": 1_800_002_100,
            },
        )
        self.assertNotIn("reservation", response["body"].lower())
        self.assertNotIn("paymentattempt", response["body"].lower())

    def test_physical_stock_target_is_server_derived(self):
        from src.handlers import checkout

        policies = resolved_policies()
        policies.commerce["commerce"]["notificationPolicyIds"] = []
        policies.commerce["commerce"]["shipping"]["allowedCountries"] = ["MX"]
        catalog = FakeCatalog({"offer-1": offer_and_item(sellable_type="physical", variant_id="blue")})
        commerce = FakeCommerce()
        with (
            patch.object(checkout, "resolve_checkout_policy", return_value=policies),
            patch.object(checkout, "_catalog_store", return_value=catalog),
            patch.object(checkout, "_commerce_store", return_value=commerce),
            patch.object(checkout.time, "time", return_value=1_800_000_000),
            patch.object(checkout, "_new_id", side_effect=["order-1", "attempt-1", "reservation-1", "line-1"]),
        ):
            response = checkout.lambda_handler(
                checkout_event(
                    {"operation": "admitCheckout", "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]}},
                ),
                None,
            )
        self.assertEqual(response["statusCode"], 200)
        order = commerce.calls[0][0][1]
        self.assertEqual(order.lines[0].stock_id, "item-1.blue")

    def test_missing_physical_shipping_policy_fails_before_reservation(self):
        from src.handlers import checkout

        policies = resolved_policies()
        policies.commerce["commerce"]["notificationPolicyIds"] = []
        catalog = FakeCatalog({
            "offer-1": offer_and_item(sellable_type="physical", variant_id="blue")
        })
        commerce = FakeCommerce()
        with (
            patch.object(checkout, "resolve_checkout_policy", return_value=policies),
            patch.object(checkout, "_catalog_store", return_value=catalog),
            patch.object(checkout, "_commerce_store", return_value=commerce),
        ):
            response = checkout.lambda_handler(
                checkout_event({
                    "operation": "admitCheckout",
                    "input": {
                        "lines": [{"offerVersionId": "offer-1", "quantity": 1}]
                    },
                }),
                None,
            )

        self.assertEqual(response["statusCode"], 503)
        self.assertEqual(commerce.calls, [])

    def test_checkout_rejects_an_offer_currency_outside_the_published_allowlist(self):
        from src.handlers import checkout

        policies = resolved_policies()
        policies.commerce["commerce"]["payments"]["supportedCurrencies"] = ["USD"]
        policies.commerce["commerce"]["notificationPolicyIds"] = []
        catalog = FakeCatalog({"offer-1": offer_and_item()})
        commerce = FakeCommerce()
        with (
            patch.object(checkout, "resolve_checkout_policy", return_value=policies),
            patch.object(checkout, "_catalog_store", return_value=catalog),
            patch.object(checkout, "_commerce_store", return_value=commerce),
        ):
            response = checkout.lambda_handler(
                checkout_event(
                    {"operation": "admitCheckout", "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]}},
                ),
                None,
            )

        self.assertEqual(response["statusCode"], 400)
        self.assertEqual(commerce.calls, [])

    def test_rejects_browser_authority_fields_before_policy_or_storage(self):
        from src.handlers import checkout

        forbidden_payloads = (
            {
                "operation": "admitCheckout",
                "tenantId": "other",
                "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]},
            },
            {
                "operation": "admitCheckout",
                "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1, "amountMinor": 1}]},
            },
            {
                "operation": "admitCheckout",
                "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}], "locationId": "other"},
            },
        )
        for payload in forbidden_payloads:
            with self.subTest(payload=payload), patch.object(
                checkout, "resolve_checkout_policy"
            ) as resolver:
                response = checkout.lambda_handler(
                    checkout_event(payload), None
                )
                self.assertEqual(response["statusCode"], 400)
                resolver.assert_not_called()

    def test_checkout_requires_the_owned_test_front_door_before_policy_or_storage(self):
        from src.handlers import checkout

        cases = (
            None,
            "null",
            "http://test.zoolandingpage.com.mx",
            "https://example.com",
            "https://attacker.invalid",
            "http://127.0.0.1:4200",
            "https://localhost",
            "https://test.zoolandingpage.com.mx https://attacker.invalid",
        )
        for origin in cases:
            with self.subTest(origin=origin), patch.object(checkout, "resolve_checkout_policy") as resolver:
                event = checkout_event(
                    {"operation": "admitCheckout", "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]}},
                )
                if origin is None:
                    event["headers"].pop("origin")
                else:
                    event["headers"]["origin"] = origin
                response = checkout.lambda_handler(event, None)

            self.assertEqual(response["statusCode"], 403)
            resolver.assert_not_called()

    def test_test_front_door_requires_the_exact_expected_draft_domain_binding(self):
        from src.handlers import checkout

        payload = {
            "operation": "admitCheckout",
            "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]},
        }
        for query in (
            None,
            {},
            {"draftDomain": "other.example.com"},
            {"draftDomain": "example.com", "other": "unexpected"},
        ):
            with self.subTest(query=query), patch.object(checkout, "resolve_checkout_policy") as resolver:
                event = checkout_event(payload)
                event["queryStringParameters"] = query
                response = checkout.lambda_handler(event, None)
            self.assertEqual(response["statusCode"], 403)
            resolver.assert_not_called()

    def test_test_front_door_rejects_multivalue_origin_and_domain_pollution(self):
        from src.handlers import checkout

        payload = {
            "operation": "admitCheckout",
            "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]},
        }
        polluted_events = []
        for values in (["other.example.com"], ["example.com", "other.example.com"]):
            event = checkout_event(payload)
            event["multiValueQueryStringParameters"] = {"draftDomain": values}
            polluted_events.append(event)
        for values in (
            ["https://evil.example"],
            ["https://test.zoolandingpage.com.mx", "https://evil.example"],
        ):
            event = checkout_event(payload)
            event["multiValueHeaders"] = {"origin": values}
            polluted_events.append(event)

        for event in polluted_events:
            with self.subTest(event=event), patch.object(checkout, "resolve_checkout_policy") as resolver:
                response = checkout.lambda_handler(event, None)
            self.assertEqual(response["statusCode"], 403)
            resolver.assert_not_called()

        checkout._validate_origin_binding(
            {
                **checkout_event(payload),
                "multiValueHeaders": {"Origin": ["https://test.zoolandingpage.com.mx"]},
                "multiValueQueryStringParameters": {"draftDomain": ["example.com"]},
            },
            "example.com",
        )

    def test_production_accepts_only_the_exact_canonical_origin_and_no_preview_binding(self):
        from src.handlers import checkout

        payload = {
            "operation": "admitCheckout",
            "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]},
        }
        policies = resolved_policies()
        policies.commerce["scope"]["environment"] = "production"
        policies.commerce["commerce"]["notificationPolicyIds"] = []
        policies = replace(policies, environment="production")
        catalog = FakeCatalog({"offer-1": offer_and_item()})
        commerce = FakeCommerce()
        with (
            patch.dict(os.environ, {"ENVIRONMENT_NAME": "prod"}),
            patch.object(checkout, "resolve_checkout_policy", return_value=policies),
            patch.object(checkout, "_catalog_store", return_value=catalog),
            patch.object(checkout, "_commerce_store", return_value=commerce),
        ):
            valid = checkout_event(payload, headers={"origin": "https://example.com"})
            valid["queryStringParameters"] = None
            accepted = checkout.lambda_handler(valid, None)

            arbitrary = checkout_event(payload, headers={"origin": "https://example.com"})
            arbitrary["queryStringParameters"] = {"draftDomain": "other.example.com"}
            rejected_query = checkout.lambda_handler(arbitrary, None)

            wrong_origin = checkout_event(payload, headers={"origin": "https://test.zoolandingpage.com.mx"})
            wrong_origin["queryStringParameters"] = None
            rejected_origin = checkout.lambda_handler(wrong_origin, None)

        self.assertEqual(accepted["statusCode"], 200)
        self.assertEqual(rejected_query["statusCode"], 403)
        self.assertEqual(rejected_origin["statusCode"], 403)
        self.assertEqual(len(commerce.calls), 1)

    def test_fiscal_enabled_checkout_returns_one_opaque_proof_and_persists_only_its_hash(self):
        from src.handlers import checkout

        policies = resolved_policies()
        policies.commerce["commerce"]["fiscal"] = {
            "enabled": True,
            "manual": True,
            "disclosureId": "manual-invoice-v1",
            "taxBehavior": "exclusive",
            "retentionDays": 90,
            "requestWindowHours": 24,
            "accountantApprovalId": "approval-1",
        }
        policies.commerce["commerce"]["notificationPolicyIds"] = []
        catalog = FakeCatalog({"offer-1": offer_and_item()})
        commerce = FakeCommerce()

        with (
            patch.object(checkout, "resolve_checkout_policy", return_value=policies),
            patch.object(checkout, "_catalog_store", return_value=catalog),
            patch.object(checkout, "_commerce_store", return_value=commerce),
            patch.object(checkout.time, "time", return_value=1_800_000_000),
            patch.object(checkout, "_new_id", side_effect=["order-1", "attempt-1", "reservation-1", "line-1"]),
        ):
            response = checkout.lambda_handler(
                checkout_event(
                    {"operation": "admitCheckout", "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]}},
                ),
                None,
            )

        self.assertEqual(response["statusCode"], 200)
        body = response_body(response)["data"]
        proof = body["fiscalAccessProof"]
        self.assertGreaterEqual(len(proof), 43)
        fiscal_access = commerce.calls[0][1]["fiscal_access"]
        self.assertEqual(fiscal_access["proofHash"], __import__("hashlib").sha256(proof.encode("ascii")).hexdigest())
        self.assertEqual(fiscal_access["windowSeconds"], 24 * 60 * 60)
        self.assertNotIn(proof, repr(commerce.calls))
        self.assertNotIn("fiscalAccessHash", body)

    def test_checkout_recovery_key_is_one_canonical_256_bit_capability(self):
        from src.handlers import checkout

        payload = {
            "operation": "admitCheckout",
            "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 1}]},
        }
        invalid_keys = (
            "operation-1",
            "A" * 42,
            "A" * 44,
            "A" * 42 + "B",
            "*" * 43,
        )
        for raw_key in invalid_keys:
            with self.subTest(raw_key=raw_key), patch.object(
                checkout, "resolve_checkout_policy"
            ) as resolver:
                response = checkout.lambda_handler(
                    checkout_event(payload, headers={"idempotency-key": raw_key}),
                    None,
                )
            self.assertEqual(response["statusCode"], 400)
            resolver.assert_not_called()

    def test_checkout_line_quantity_has_a_code_owned_upper_bound_before_policy_reads(self):
        from src.handlers import checkout

        payload = {
            "operation": "admitCheckout",
            "input": {
                "lines": [{
                    "offerVersionId": "offer-1",
                    "quantity": checkout.MAX_CHECKOUT_LINE_QUANTITY + 1,
                }],
            },
        }
        with patch.object(checkout, "resolve_checkout_policy") as resolver:
            response = checkout.lambda_handler(checkout_event(payload), None)

        self.assertEqual(response["statusCode"], 400)
        resolver.assert_not_called()


if __name__ == "__main__":
    unittest.main()
