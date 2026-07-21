import base64
import json
import os
import unittest
from unittest.mock import Mock, patch

from tests.test_catalog_handlers import api_event, resolved_policies, response_body
from tests.test_checkout_handler import FakeCommerce, checkout_event, offer_and_item


PUBLIC_CHECKOUT_RECOVERY_KEY = base64.urlsafe_b64encode(b"r" * 32).decode("ascii").rstrip("=")


class CatalogPhase4Store:
    def __init__(self, *, offer=None, discount=None, sequence=None):
        self.offer = offer
        self.discount = discount
        self.calls = []
        self.sequence = sequence if sequence is not None else []

    def get_offer_version(self, scope, version_id, supported_currencies):
        self.calls.append(("get_offer_version", scope, version_id, supported_currencies))
        return self.offer

    def get_discount_version(self, scope, version_id, supported_currencies):
        self.calls.append(("get_discount_version", scope, version_id, supported_currencies))
        return self.discount

    def replay_mutation(self, scope, idempotency_key, request):
        self.calls.append(("replay_mutation", scope, idempotency_key, request))
        return None

    def advance_offer_lifecycle(self, scope, version_id, target_state, expected_revision, currencies, **metadata):
        self.sequence.append("store")
        self.calls.append(("advance_offer_lifecycle", scope, version_id, target_state, expected_revision, currencies, metadata))
        return {"versionId": version_id, "lifecycleState": target_state, "lifecycleRevision": expected_revision + 1}

    def update_offer_presentation(self, scope, version_id, expected_revision, currencies, **metadata):
        self.sequence.append("store")
        self.calls.append(("update_offer_presentation", scope, version_id, expected_revision, currencies, metadata))
        return {"versionId": version_id, "presentationRevision": expected_revision + 1}

    def advance_discount_lifecycle(self, scope, version_id, target_state, expected_revision, currencies, **metadata):
        self.sequence.append("store")
        self.calls.append(("advance_discount_lifecycle", scope, version_id, target_state, expected_revision, currencies, metadata))
        return {"versionId": version_id, "lifecycleState": target_state, "lifecycleRevision": expected_revision + 1}

    def update_discount_presentation(self, scope, version_id, expected_revision, currencies, **metadata):
        self.sequence.append("store")
        self.calls.append(("update_discount_presentation", scope, version_id, expected_revision, currencies, metadata))
        return {"versionId": version_id, "presentationRevision": expected_revision + 1}


class CatalogPhase4Gateway:
    def __init__(self, *, status="accepted", sequence=None, error=None):
        self.status = status
        self.error = error
        self.calls = []
        self.sequence = sequence if sequence is not None else []

    def provision_offer(self, scope, connection_id, offer):
        self.sequence.append("gateway")
        self.calls.append(("provision_offer", scope, connection_id, offer))
        return {"commandId": "command-offer", "status": self.status}

    def deactivate_offer(self, scope, connection_id, resource_id, lifecycle_revision):
        self.sequence.append("gateway")
        self.calls.append(("deactivate_offer", scope, connection_id, resource_id, lifecycle_revision))
        return {"commandId": "command-offer-retire", "status": self.status}

    def update_offer_presentation(self, scope, connection_id, offer):
        self.sequence.append("gateway")
        self.calls.append(("update_offer_presentation", scope, connection_id, offer))
        return {"commandId": "command-presentation", "status": self.status}

    def provision_discount(self, scope, connection_id, discount):
        self.sequence.append("gateway")
        self.calls.append(("provision_discount", scope, connection_id, discount))
        return {"commandId": "command-discount", "status": self.status}

    def update_discount_lifecycle(self, scope, connection_id, discount):
        self.sequence.append("gateway")
        self.calls.append(("update_discount_lifecycle", scope, connection_id, discount))
        return {"commandId": "command-discount-state", "status": self.status}

    def update_discount_presentation(self, scope, connection_id, discount):
        self.sequence.append("gateway")
        self.calls.append(("update_discount_presentation", scope, connection_id, discount))
        if self.error is not None:
            raise self.error
        return {"commandId": "command-discount-presentation", "status": self.status}


class CheckoutCatalog:
    def __init__(self, offer, item, discount=None):
        self.offer = offer
        self.item = item
        self.discount = discount
        self.calls = []

    def get_checkout_offer(self, scope, version_id, currencies):
        self.calls.append(("offer", scope, version_id, currencies))
        return self.offer, self.item

    def get_checkout_discount(self, scope, version_id, currencies):
        self.calls.append(("discount", scope, version_id, currencies))
        return self.discount


class CheckoutGateway:
    def __init__(self, *, error=None, status="accepted"):
        self.error = error
        self.status = status
        self.calls = []

    def create_checkout(self, scope, connection_id, command_input):
        self.calls.append((scope, connection_id, command_input))
        if self.error:
            raise self.error
        if self.status != "accepted":
            return {"commandId": "command-checkout", "status": self.status}
        return {
            "commandId": "command-checkout",
            "status": "accepted",
            "redirectUrl": "https://checkout.stripe.com/c/pay/cs_test_safe",
            "expiresAt": command_input["checkoutExpiresAt"],
        }


def active_discount():
    from src.domain.offers import DiscountVersion

    return DiscountVersion(
        version_id="discount-1",
        revision=2,
        duration="once",
        percentage_basis_points=1_000,
        eligible_offer_version_ids=frozenset({"offer-1"}),
        lifecycle_state="active",
        lifecycle_revision=3,
    )


@patch.dict(os.environ, {"ENVIRONMENT_NAME": "test"}, clear=False)
class CatalogIntegrationsWiringTests(unittest.TestCase):
    def invoke(self, payload, store, gateway):
        from src.handlers import catalog_action

        with (
            patch.object(catalog_action, "resolve_policies", return_value=resolved_policies()),
            patch.object(catalog_action, "authorize_request", return_value=Mock(subject="operator-1")),
            patch.object(catalog_action, "_store", return_value=store),
            patch.object(catalog_action, "_gateway", return_value=gateway),
            patch.object(catalog_action.time, "time", return_value=1_800_000_000),
        ):
            return catalog_action.lambda_handler(
                api_event("/features/commerce/catalog/action", payload), None
            )

    def test_offer_activation_calls_integrations_before_local_activation(self):
        from src.domain.offers import Money, OfferVersion

        sequence = []
        current = OfferVersion(
            "offer-1", "item-1", None, 3, "service",
            Money(90_000, "MXN", frozenset({"MXN"})), "exclusive",
            lifecycle_state="provisioning", lifecycle_revision=2,
            display_name="Servicio",
        )
        store = CatalogPhase4Store(offer=current, sequence=sequence)
        gateway = CatalogPhase4Gateway(sequence=sequence)
        response = self.invoke({
            "operation": "advanceOfferLifecycle",
            "input": {"versionId": "offer-1", "targetState": "active", "expectedRevision": 2},
        }, store, gateway)

        self.assertEqual(response["statusCode"], 200, response["body"])
        self.assertEqual(sequence, ["gateway", "gateway", "store"])
        name, scope, connection_id, forwarded = gateway.calls[0]
        self.assertEqual(name, "provision_offer")
        self.assertEqual(connection_id, "stripe-main")
        self.assertEqual((scope.tenant_id, scope.draft_id), ("tenant-example", "draft-example"))
        self.assertEqual(forwarded.provider_snapshot(), current.provider_snapshot())
        self.assertEqual(gateway.calls[1][0], "update_offer_presentation")

    def test_pending_offer_activation_never_fakes_a_local_success(self):
        from src.domain.offers import Money, OfferVersion

        current = OfferVersion(
            "offer-1", "item-1", None, 1, "service",
            Money(100, "MXN", frozenset({"MXN"})), "exclusive",
            lifecycle_state="provisioning", lifecycle_revision=2,
        )
        store = CatalogPhase4Store(offer=current)
        gateway = CatalogPhase4Gateway(status="pending")
        response = self.invoke({
            "operation": "advanceOfferLifecycle",
            "input": {"versionId": "offer-1", "targetState": "active", "expectedRevision": 2},
        }, store, gateway)

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response_body(response)["data"], {
            "commandId": "command-offer", "status": "pending"
        })
        self.assertFalse(any(call[0] == "advance_offer_lifecycle" for call in store.calls))

    def test_exact_offer_activation_retry_replays_before_revision_or_provider_checks(self):
        from src.catalog_storage import CatalogStore
        from src.domain.catalog import CatalogItem
        from src.domain.offers import Money, OfferVersion
        from src.storage import CommerceScope
        from tests.test_catalog_handlers import FakeCatalogBackend

        backend = FakeCatalogBackend()
        store = CatalogStore(backend, "Catalog", "Operations")
        scope = CommerceScope("test", "tenant-example", "draft-example", "example.com")
        metadata = {
            "request_id": "seed-request",
            "correlation_id": "seed-request",
            "actor_hash": "a" * 64,
            "now_epoch": 1_799_999_900,
        }
        store.create_item(
            scope,
            CatalogItem("item-1", "service"),
            idempotency_key="seed-item",
            **metadata,
        )
        store.create_offer(
            scope,
            OfferVersion(
                "offer-1",
                "item-1",
                None,
                3,
                "service",
                Money(90_000, "MXN", frozenset({"MXN"})),
                "exclusive",
                display_name="Servicio",
            ),
            supported_currencies=frozenset({"MXN"}),
            idempotency_key="seed-offer",
            **metadata,
        )
        store.advance_offer_lifecycle(
            scope,
            "offer-1",
            "provisioning",
            1,
            frozenset({"MXN"}),
            idempotency_key="seed-provisioning",
            **metadata,
        )
        gateway = CatalogPhase4Gateway()
        payload = {
            "operation": "advanceOfferLifecycle",
            "input": {
                "versionId": "offer-1",
                "targetState": "active",
                "expectedRevision": 2,
            },
        }

        first = self.invoke(payload, store, gateway)
        replay = self.invoke(payload, store, gateway)

        self.assertEqual(first["statusCode"], 200, first["body"])
        self.assertEqual(replay, first)
        self.assertEqual(
            [call[0] for call in gateway.calls],
            ["provision_offer", "update_offer_presentation"],
        )

    def test_all_provider_backed_catalog_replays_short_circuit_revision_and_gateway_checks(self):
        cases = (
            (
                "advanceOfferLifecycle",
                {"versionId": "offer-1", "targetState": "active", "expectedRevision": 2},
                {
                    "action": "advanceOfferLifecycle",
                    "versionId": "offer-1",
                    "targetState": "active",
                    "expectedRevision": 2,
                },
            ),
            (
                "updateOfferPresentation",
                {"versionId": "offer-1", "expectedRevision": 3, "displayName": "Oferta"},
                {
                    "action": "updateOfferPresentation",
                    "versionId": "offer-1",
                    "expectedRevision": 3,
                    "displayName": "Oferta",
                    "displayDescription": None,
                },
            ),
            (
                "advanceDiscountLifecycle",
                {"versionId": "discount-1", "targetState": "active", "expectedRevision": 2},
                {
                    "action": "advanceDiscountLifecycle",
                    "versionId": "discount-1",
                    "targetState": "active",
                    "expectedRevision": 2,
                },
            ),
            (
                "updateDiscountPresentation",
                {
                    "versionId": "discount-1",
                    "expectedRevision": 4,
                    "displayName": "Promoción",
                    "displayDescription": "Beneficio",
                },
                {
                    "action": "updateDiscountPresentation",
                    "versionId": "discount-1",
                    "expectedRevision": 4,
                    "displayName": "Promoción",
                    "displayDescription": "Beneficio",
                },
            ),
        )
        for operation, input_value, expected_request in cases:
            with self.subTest(operation=operation):
                replay = {"versionId": input_value["versionId"], "replayed": True}
                store = CatalogPhase4Store()
                store.replay_mutation = Mock(return_value=replay)
                gateway = CatalogPhase4Gateway()

                response = self.invoke(
                    {"operation": operation, "input": input_value},
                    store,
                    gateway,
                )

                self.assertEqual(response["statusCode"], 200, response["body"])
                self.assertEqual(response_body(response)["data"], replay)
                store.replay_mutation.assert_called_once()
                self.assertEqual(store.replay_mutation.call_args.args[2], expected_request)
                self.assertEqual(gateway.calls, [])
                self.assertFalse(any(call[0].startswith("get_") for call in store.calls))

    def test_offer_retirement_and_presentation_use_their_exact_provider_seams(self):
        from src.domain.offers import Money, OfferVersion

        retiring = OfferVersion(
            "offer-1", "item-1", None, 1, "service",
            Money(100, "MXN", frozenset({"MXN"})), "exclusive",
            lifecycle_state="existing_only", lifecycle_revision=4,
            display_name="Anterior", presentation_revision=2,
        )
        store = CatalogPhase4Store(offer=retiring)
        gateway = CatalogPhase4Gateway()
        retired = self.invoke({
            "operation": "advanceOfferLifecycle",
            "input": {"versionId": "offer-1", "targetState": "retired", "expectedRevision": 4},
        }, store, gateway)
        self.assertEqual(retired["statusCode"], 200)
        self.assertEqual(gateway.calls[0][0], "deactivate_offer")
        self.assertEqual(gateway.calls[0][-1], 5)

        active = OfferVersion(
            "offer-1", "item-1", None, 1, "service",
            Money(100, "MXN", frozenset({"MXN"})), "exclusive",
            lifecycle_state="active", lifecycle_revision=3,
            display_name="Anterior", presentation_revision=2,
        )
        presentation_store = CatalogPhase4Store(offer=active)
        presentation_gateway = CatalogPhase4Gateway()
        updated = self.invoke({
            "operation": "updateOfferPresentation",
            "input": {
                "versionId": "offer-1", "expectedRevision": 2,
                "displayName": "Nuevo", "displayDescription": "Descripción",
            },
        }, presentation_store, presentation_gateway)
        self.assertEqual(updated["statusCode"], 200)
        forwarded = presentation_gateway.calls[0][-1]
        self.assertEqual((forwarded.presentation_revision, forwarded.display_name), (3, "Nuevo"))

    def test_discount_activation_and_deactivation_use_server_snapshots(self):
        from src.domain.offers import DiscountVersion

        current = DiscountVersion(
            "discount-1", 2, "once", percentage_basis_points=1_000,
            lifecycle_state="provisioning", lifecycle_revision=2,
            display_name="Promoción",
        )
        store = CatalogPhase4Store(discount=current)
        gateway = CatalogPhase4Gateway()
        active = self.invoke({
            "operation": "advanceDiscountLifecycle",
            "input": {"versionId": "discount-1", "targetState": "active", "expectedRevision": 2},
        }, store, gateway)
        self.assertEqual(active["statusCode"], 200)
        self.assertEqual(gateway.calls[0][0], "provision_discount")
        self.assertEqual(gateway.calls[0][-1].provider_snapshot(), current.provider_snapshot())
        self.assertEqual(gateway.calls[1][0], "update_discount_presentation")

        existing = DiscountVersion(
            "discount-1", 2, "once", percentage_basis_points=1_000,
            lifecycle_state="active", lifecycle_revision=3,
        )
        state_store = CatalogPhase4Store(discount=existing)
        state_gateway = CatalogPhase4Gateway(status="needs_review")
        held = self.invoke({
            "operation": "advanceDiscountLifecycle",
            "input": {"versionId": "discount-1", "targetState": "existing_only", "expectedRevision": 3},
        }, state_store, state_gateway)
        self.assertEqual(response_body(held)["data"]["status"], "needs_review")
        self.assertFalse(any(call[0] == "advance_discount_lifecycle" for call in state_store.calls))

    def test_active_discount_presentation_reaches_integrations_before_local_success(self):
        from src.domain.offers import DiscountVersion

        sequence = []
        current = DiscountVersion(
            "discount-1",
            2,
            "once",
            percentage_basis_points=1_000,
            lifecycle_state="active",
            lifecycle_revision=3,
            presentation_revision=2,
            display_name="Anterior",
        )
        store = CatalogPhase4Store(discount=current, sequence=sequence)
        gateway = CatalogPhase4Gateway(sequence=sequence)

        response = self.invoke({
            "operation": "updateDiscountPresentation",
            "input": {
                "versionId": "discount-1",
                "expectedRevision": 2,
                "displayName": "Nueva promoción",
                "displayDescription": "Descripción segura",
            },
        }, store, gateway)

        self.assertEqual(response["statusCode"], 200, response["body"])
        self.assertEqual(sequence, ["gateway", "store"])
        forwarded = gateway.calls[0][-1]
        self.assertEqual(
            (forwarded.presentation_revision, forwarded.display_name),
            (3, "Nueva promoción"),
        )

    def test_nonaccepted_discount_presentation_never_persists_local_success(self):
        from src.domain.offers import DiscountVersion

        current = DiscountVersion(
            "discount-1",
            2,
            "once",
            percentage_basis_points=1_000,
            lifecycle_state="active",
            lifecycle_revision=3,
            presentation_revision=2,
            display_name="Anterior",
        )
        for status in ("pending", "needs_review"):
            with self.subTest(status=status):
                store = CatalogPhase4Store(discount=current)
                gateway = CatalogPhase4Gateway(status=status)

                response = self.invoke({
                    "operation": "updateDiscountPresentation",
                    "input": {
                        "versionId": "discount-1",
                        "expectedRevision": 2,
                        "displayName": "Nueva promoción",
                    },
                }, store, gateway)

                self.assertEqual(response_body(response)["data"], {
                    "commandId": "command-discount-presentation",
                    "status": status,
                })
                self.assertFalse(
                    any(call[0] == "update_discount_presentation" for call in store.calls)
                )

    def test_failed_discount_presentation_never_persists_local_success(self):
        from src.domain.offers import DiscountVersion
        from src.integrations_gateway import IntegrationsUnavailable

        current = DiscountVersion(
            "discount-1",
            2,
            "once",
            percentage_basis_points=1_000,
            lifecycle_state="active",
            lifecycle_revision=3,
            presentation_revision=2,
            display_name="Anterior",
        )
        store = CatalogPhase4Store(discount=current)
        gateway = CatalogPhase4Gateway(
            error=IntegrationsUnavailable("provider detail must not escape")
        )

        response = self.invoke({
            "operation": "updateDiscountPresentation",
            "input": {
                "versionId": "discount-1",
                "expectedRevision": 2,
                "displayName": "Nueva promoción",
            },
        }, store, gateway)

        self.assertEqual(response["statusCode"], 503)
        self.assertNotIn("provider detail", response["body"])
        self.assertFalse(
            any(call[0] == "update_discount_presentation" for call in store.calls)
        )


@patch.dict(
    os.environ,
    {"ENVIRONMENT_NAME": "test", "TEST_PREVIEW_ORIGIN": "https://test.zoolandingpage.com.mx"},
    clear=False,
)
class CheckoutIntegrationsWiringTests(unittest.TestCase):
    def invoke(self, payload, catalog, commerce, gateway):
        from src.handlers import checkout

        policies = resolved_policies()
        policies.commerce["commerce"]["notificationPolicyIds"] = []
        with (
            patch.object(checkout, "resolve_checkout_policy", return_value=policies),
            patch.object(checkout, "_catalog_store", return_value=catalog),
            patch.object(checkout, "_commerce_store", return_value=commerce),
            patch.object(checkout, "_gateway", return_value=gateway),
            patch.object(checkout.time, "time", return_value=1_800_000_000),
            patch.object(
                checkout,
                "_new_id",
                side_effect=["order-1", "attempt-1", "reservation-1", "line-1"],
            ),
        ):
            return checkout.lambda_handler(checkout_event(payload), None)

    def test_checkout_reserves_first_then_returns_only_ephemeral_handoff(self):
        offer, item = offer_and_item()
        catalog = CheckoutCatalog(offer, item)
        commerce = FakeCommerce()
        gateway = CheckoutGateway()
        response = self.invoke({
            "operation": "admitCheckout",
            "input": {"lines": [{"offerVersionId": "offer-1", "quantity": 2}]},
        }, catalog, commerce, gateway)

        self.assertEqual(response["statusCode"], 200, response["body"])
        data = response_body(response)["data"]
        self.assertEqual(set(data), {"commandId", "status", "redirectUrl", "expiresAt"})
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertNotIn("reservation", repr(data).lower())
        self.assertNotIn("paymentattempt", repr(data).lower())
        self.assertEqual(len(commerce.calls), 1)
        scope, connection_id, command = gateway.calls[0]
        self.assertEqual(connection_id, "stripe-main")
        self.assertEqual((scope.tenant_id, scope.draft_id), ("tenant-example", "draft-example"))
        self.assertEqual(command["orderId"], "order-1")
        self.assertEqual(command["paymentAttemptId"], "attempt-1")
        self.assertEqual(command["reservationIds"], ["reservation-1"])
        self.assertEqual(command["revision"], 1)
        self.assertEqual(command["offerBindings"][0]["snapshot"], offer.provider_snapshot())
        self.assertEqual(command["taxPolicy"], {"mode": "disabled"})
        self.assertEqual(command["shippingPolicy"], {"collection": "none"})
        self.assertEqual(command["paymentCollection"], "immediate_card_link")

    def test_checkout_resolves_internal_discount_and_provider_failure_preserves_reservation(self):
        from src.integrations_gateway import IntegrationsUnavailable

        offer, item = offer_and_item()
        catalog = CheckoutCatalog(offer, item, active_discount())
        commerce = FakeCommerce()
        gateway = CheckoutGateway(error=IntegrationsUnavailable("private provider detail"))
        response = self.invoke({
            "operation": "admitCheckout",
            "input": {
                "lines": [{"offerVersionId": "offer-1", "quantity": 1}],
                "discountVersionId": "discount-1",
            },
        }, catalog, commerce, gateway)

        self.assertEqual(response["statusCode"], 503)
        self.assertEqual(response_body(response)["code"], "upstream_unavailable")
        self.assertNotIn("private", response["body"])
        self.assertEqual(len(commerce.calls), 1)
        self.assertEqual(gateway.calls[0][2]["discountVersionId"], "discount-1")
        self.assertTrue(any(call[0] == "discount" for call in catalog.calls))


class SubscriptionIntegrationsWiringTests(unittest.TestCase):
    @patch.dict(os.environ, {"ENVIRONMENT_NAME": "test"}, clear=False)
    def test_portal_uses_published_binding_and_returns_only_provider_handoff(self):
        from src.handlers import subscription_action

        policies = resolved_policies()
        gateway = Mock()
        gateway.execute.return_value = {
            "commandId": "command-portal",
            "status": "accepted",
            "redirectUrl": "https://billing.stripe.com/p/session/test_safe",
            "expiresAt": 1_800_000_600,
        }
        with (
            patch.object(subscription_action, "resolve_policies", return_value=policies),
            patch.object(subscription_action, "authorize_request", return_value=Mock(subject="operator-1")),
            patch.object(subscription_action, "_gateway", return_value=gateway),
        ):
            response = subscription_action.lambda_handler(api_event(
                "/features/commerce/subscription/action",
                {"operation": "openPortal", "input": {"subscriptionId": "subscription-1"}},
            ), None)

        self.assertEqual(response["statusCode"], 200, response["body"])
        self.assertEqual(set(response_body(response)["data"]), {
            "commandId", "status", "redirectUrl", "expiresAt"
        })
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        call = gateway.execute.call_args
        self.assertEqual(call.args[0], "openPortal")
        self.assertEqual(call.args[2], {"subscriptionId": "subscription-1"})
        self.assertEqual(call.kwargs["connection_id"], "stripe-main")


if __name__ == "__main__":
    unittest.main()
