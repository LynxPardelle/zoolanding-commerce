import base64
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from unittest.mock import Mock, patch
import unittest


DOMAIN = "example.com"
SUPPORTED_CURRENCIES = frozenset({"MXN"})
CURSOR_SIGNING_KEY = base64.urlsafe_b64encode(b"k" * 32).decode("ascii").rstrip("=")


def resolved_policies():
    from src.common.published_policy import ResolvedPolicies
    from tests.test_published_policy import auth_registry, commerce_policy

    return ResolvedPolicies(
        environment="test",
        tenant_id="tenant-example",
        draft_id="draft-example",
        domain=DOMAIN,
        version_id="version-1",
        prefix="sites/example.com/versions/version-1/",
        commerce=commerce_policy(),
        auth_registry=auth_registry(),
    )


def api_event(path, payload, *, headers=None, method="POST"):
    base_headers = {
        "x-zlp-domain": DOMAIN,
        "x-zlp-auth-profile-id": "staff",
        "idempotency-key": "operation-1",
        "origin": f"https://{DOMAIN}",
    }
    base_headers.update(headers or {})
    return {
        "rawPath": path,
        "headers": base_headers,
        "body": json.dumps(payload),
        "isBase64Encoded": False,
        "requestContext": {"requestId": "request-1", "http": {"method": method}},
    }


def response_body(response):
    return json.loads(response["body"])


class FakeCatalogBackend:
    def __init__(self):
        self.items = {}
        self.transactions = []
        self.before_transact = None
        self.queries = []

    def get(self, table, pk, sk):
        value = self.items.get((table, pk, sk))
        return dict(value) if value else None

    def query_prefix(self, table, pk, prefix, limit, cursor=None):
        self.queries.append((table, pk, prefix, limit, cursor))
        items = [
            dict(item)
            for (stored_table, stored_pk, sk), item in sorted(self.items.items())
            if (
                stored_table == table
                and stored_pk == pk
                and sk.startswith(prefix)
                and (cursor is None or sk > cursor)
            )
        ]
        page = items[:limit]
        next_cursor = page[-1]["sk"] if len(items) > limit else None
        return page, next_cursor

    def transact(self, operations, client_token):
        from src.storage import ConditionalWriteFailed

        del client_token
        if self.before_transact is not None:
            callback, self.before_transact = self.before_transact, None
            callback()
        for operation in operations:
            item = operation["item"]
            key = (operation["table_name"], item["pk"], item["sk"])
            current = self.items.get(key)
            condition = operation["condition"]
            if condition == "absent" and current is not None:
                raise ConditionalWriteFailed()
            if isinstance(condition, dict) and (
                current is None
                or any(current.get(field) != expected for field, expected in condition.items())
            ):
                raise ConditionalWriteFailed()
        self.transactions.append(list(operations))
        for operation in operations:
            item = operation["item"]
            self.items[(operation["table_name"], item["pk"], item["sk"])] = dict(item)


class HandlerStore:
    def __init__(self):
        self.calls = []

    def list_public_offers(self, scope, limit, cursor, supported_currencies):
        self.calls.append(("list_public_offers", scope, limit, cursor, supported_currencies))
        return (
            [{"offerVersionId": "offer-1", "amountMinor": 90000, "currency": "MXN"}],
            None if cursor else "OFFER#offer-1",
        )

    def list_catalog(self, scope, kind, limit, cursor, supported_currencies):
        self.calls.append(("list_catalog", scope, kind, limit, cursor, supported_currencies))
        prefixes = {"items": "CATALOG_ITEM#", "offers": "OFFER#", "discounts": "DISCOUNT#"}
        return ([{"itemType": kind}], None if cursor else f"{prefixes[kind]}resource-1")

    def create_item(self, scope, item, **metadata):
        self.calls.append(("create_item", scope, item, metadata))
        return {"itemId": item.item_id, "revision": 1}

    def create_offer(self, scope, item, **metadata):
        self.calls.append(("create_offer", scope, item, metadata))
        return {"versionId": item.version_id, "revision": 1}

    def get_offer_version(self, scope, version_id, supported_currencies):
        from src.domain.offers import Money, OfferVersion

        self.calls.append(
            ("get_offer_version", scope, version_id, supported_currencies)
        )
        return OfferVersion(
            version_id,
            "item-1",
            None,
            1,
            "service",
            Money(90_000, "MXN", frozenset({"MXN"})),
            "exclusive",
            lifecycle_state="provisioning",
            lifecycle_revision=2,
        )

    def replay_mutation(self, scope, idempotency_key, request):
        self.calls.append(("replay_mutation", scope, idempotency_key, request))
        return None

    def adjust_stock(self, *args, **kwargs):
        self.calls.append(("adjust_stock", args, kwargs))
        return {"action": "adjust", "revision": 1}


class CatalogHandlerContractTests(unittest.TestCase):
    def setUp(self):
        cursor_environment = patch.dict(
            os.environ,
            {"COMMERCE_CURSOR_SIGNING_KEY": CURSOR_SIGNING_KEY},
            clear=False,
        )
        cursor_environment.start()
        self.addCleanup(cursor_environment.stop)

    @staticmethod
    def mutation_metadata(key="catalog-operation-1", now_epoch=100):
        return {
            "idempotency_key": key,
            "request_id": "request-1",
            "correlation_id": "request-1",
            "actor_hash": "a" * 64,
            "now_epoch": now_epoch,
        }

    def test_catalog_transport_storage_and_handlers_exist(self):
        for module in (
            "src.common.http",
            "src.catalog_storage",
            "src.handlers.catalog_public_read",
            "src.handlers.catalog_read",
            "src.handlers.catalog_action",
            "src.handlers.inventory_action",
        ):
            with self.subTest(module=module):
                self.assertIsNotNone(importlib.util.find_spec(module))

    def test_catalog_storage_api_exists(self):
        import src.catalog_storage as catalog_storage

        self.assertTrue(hasattr(catalog_storage, "CatalogStore"))

    def test_catalog_handler_apis_exist(self):
        from src.handlers import catalog_action, catalog_public_read, catalog_read, inventory_action

        for module in (catalog_action, catalog_public_read, catalog_read, inventory_action):
            with self.subTest(module=module.__name__):
                self.assertTrue(hasattr(module, "lambda_handler"))

    def test_catalog_store_uses_conditional_immutable_versions_and_public_projection(self):
        from src.catalog_storage import CatalogStore
        from src.domain.catalog import CatalogItem, CatalogVariant
        from src.domain.offers import Money, OfferVersion
        from src.storage import CommerceScope, StorageConflict

        backend = FakeCatalogBackend()
        store = CatalogStore(backend, "Catalog", "Operations")
        scope = CommerceScope("test", "tenant-example", "draft-example", DOMAIN)
        item = CatalogItem("landing", "service", (CatalogVariant("base", "LANDING-BASE"),))
        offer = OfferVersion(
            "offer-1", "landing", "base", 1, "service",
            Money(90_000, "MXN", frozenset({"MXN"})), "exclusive",
            display_name="Landing page",
        )

        self.assertEqual(store.create_item(scope, item, **self.mutation_metadata("create-item"))["revision"], 1)
        self.assertEqual(store.create_offer(
            scope,
            offer,
            supported_currencies=SUPPORTED_CURRENCIES,
            **self.mutation_metadata("create-offer"),
        )["lifecycleState"], "draft")
        self.assertEqual(
            store.advance_offer_lifecycle(
                scope,
                "offer-1",
                "provisioning",
                1,
                SUPPORTED_CURRENCIES,
                **self.mutation_metadata("provision-offer", 101),
            )["lifecycleRevision"],
            2,
        )
        active = store.advance_offer_lifecycle(
            scope,
            "offer-1",
            "active",
            2,
            SUPPORTED_CURRENCIES,
            **self.mutation_metadata("activate-offer", 102),
        )
        self.assertEqual(active["lifecycleState"], "active")
        with self.assertRaises(StorageConflict):
            store.advance_offer_lifecycle(
                scope,
                "offer-1",
                "existing_only",
                1,
                SUPPORTED_CURRENCIES,
                **self.mutation_metadata("stale-offer", 103),
            )

        public, cursor = store.list_public_offers(scope, 25, None, SUPPORTED_CURRENCIES)
        self.assertIsNone(cursor)
        self.assertEqual(public, [{
            "offerVersionId": "offer-1",
            "catalogItemId": "landing",
            "variantId": "base",
            "sellableType": "service",
            "saleType": "one_time",
            "amountMinor": 90_000,
            "currency": "MXN",
            "taxBehavior": "exclusive",
            "recurrence": None,
            "displayName": "Landing page",
            "displayDescription": None,
        }])
        serialized = json.dumps(public)
        for forbidden in ("pk", "tenantId", "draftId", "actorHash", "providerFingerprint"):
            self.assertNotIn(forbidden, serialized)

    def test_catalog_mutations_have_durable_exact_replay_and_reject_key_reuse(self):
        from src.catalog_storage import CatalogStore
        from src.domain.catalog import CatalogItem
        from src.domain.offers import Money, OfferVersion
        from src.storage import CommerceScope, StorageConflict

        backend = FakeCatalogBackend()
        store = CatalogStore(backend, "Catalog", "Operations")
        scope = CommerceScope("test", "tenant-example", "draft-example", DOMAIN)
        item = CatalogItem("landing", "service")
        first = store.create_item(
            scope,
            item,
            **self.mutation_metadata("same-operation", 100),
        )
        replay = store.create_item(
            scope,
            item,
            **{
                **self.mutation_metadata("same-operation", 999),
                "request_id": "request-2",
                "correlation_id": "request-2",
            },
        )

        self.assertEqual(replay, first)
        self.assertEqual(len(backend.transactions), 1)
        receipt = next(
            item
            for (table, _pk, _sk), item in backend.items.items()
            if table == "Operations" and item.get("itemType") == "IdempotencyReceipt"
        )
        self.assertEqual(receipt["requestId"], "request-1")
        self.assertEqual(receipt["correlationId"], "request-1")
        self.assertEqual(receipt["actorHash"], "a" * 64)
        self.assertEqual(receipt["expiresAt"], 100 + 90 * 24 * 60 * 60)
        self.assertNotIn("same-operation", json.dumps(receipt))

        with self.assertRaises(StorageConflict):
            store.create_item(
                scope,
                CatalogItem("other", "service"),
                **self.mutation_metadata("same-operation", 101),
            )

        offer = OfferVersion(
            "offer-1",
            "landing",
            None,
            1,
            "service",
            Money(90_000, "MXN", frozenset({"MXN"})),
            "exclusive",
        )
        store.create_offer(
            scope,
            offer,
            supported_currencies=SUPPORTED_CURRENCIES,
            **self.mutation_metadata("create-offer", 102),
        )
        transitioned = store.advance_offer_lifecycle(
            scope,
            "offer-1",
            "provisioning",
            1,
            SUPPORTED_CURRENCIES,
            **self.mutation_metadata("transition-offer", 103),
        )
        transaction_count = len(backend.transactions)
        transition_replay = store.advance_offer_lifecycle(
            scope,
            "offer-1",
            "provisioning",
            1,
            SUPPORTED_CURRENCIES,
            **self.mutation_metadata("transition-offer", 999),
        )
        self.assertEqual(transition_replay, transitioned)
        self.assertEqual(len(backend.transactions), transaction_count)

        replay_request = {
            "action": "advanceOfferLifecycle",
            "versionId": "offer-1",
            "targetState": "provisioning",
            "expectedRevision": 1,
        }
        self.assertEqual(
            store.replay_mutation(
                scope,
                "transition-offer",
                replay_request,
            ),
            transitioned,
        )
        with self.assertRaises(StorageConflict):
            store.replay_mutation(
                scope,
                "transition-offer",
                {**replay_request, "targetState": "active"},
            )

    def test_offer_and_discount_revision_dimensions_cannot_overwrite_each_other(self):
        from src.catalog_storage import CatalogStore
        from src.domain.catalog import CatalogItem
        from src.domain.offers import DiscountVersion, Money, OfferVersion
        from src.storage import CommerceScope, StorageConflict

        scope = CommerceScope("test", "tenant-example", "draft-example", DOMAIN)

        def seeded_store(resource):
            backend = FakeCatalogBackend()
            store = CatalogStore(backend, "Catalog", "Operations")
            store.create_item(
                scope,
                CatalogItem("landing", "service"),
                **self.mutation_metadata("create-item"),
            )
            if resource == "offer":
                store.create_offer(
                    scope,
                    OfferVersion(
                        "offer-1",
                        "landing",
                        None,
                        1,
                        "service",
                        Money(90_000, "MXN", frozenset({"MXN"})),
                        "exclusive",
                    ),
                    supported_currencies=SUPPORTED_CURRENCIES,
                    **self.mutation_metadata("create-offer"),
                )
                return backend, store, "OFFER#offer-1"
            store.create_discount(
                scope,
                DiscountVersion("discount-1", 1, "once", percentage_basis_points=1_000),
                supported_currencies=SUPPORTED_CURRENCIES,
                **self.mutation_metadata("create-discount"),
            )
            return backend, store, "DISCOUNT#discount-1"

        for resource in ("offer", "discount"):
            with self.subTest(resource=resource, winner="presentation"):
                backend, store, sk = seeded_store(resource)

                def concurrent_presentation():
                    item = backend.items[("Catalog", scope.partition_key, sk)]
                    item.update({"presentationRevision": 2, "displayName": "Concurrent"})

                backend.before_transact = concurrent_presentation
                lifecycle = (
                    store.advance_offer_lifecycle
                    if resource == "offer"
                    else store.advance_discount_lifecycle
                )
                with self.assertRaises(StorageConflict):
                    lifecycle(
                        scope,
                        f"{resource}-1" if resource == "offer" else "discount-1",
                        "provisioning",
                        1,
                        SUPPORTED_CURRENCIES,
                        **self.mutation_metadata(f"{resource}-lifecycle"),
                    )
                current = backend.items[("Catalog", scope.partition_key, sk)]
                self.assertEqual(current["presentationRevision"], 2)
                self.assertEqual(current["displayName"], "Concurrent")
                self.assertEqual(current["lifecycleRevision"], 1)

            with self.subTest(resource=resource, winner="lifecycle"):
                backend, store, sk = seeded_store(resource)

                def concurrent_lifecycle():
                    item = backend.items[("Catalog", scope.partition_key, sk)]
                    item.update({"lifecycleRevision": 2, "lifecycleState": "provisioning"})

                backend.before_transact = concurrent_lifecycle
                presentation = (
                    store.update_offer_presentation
                    if resource == "offer"
                    else store.update_discount_presentation
                )
                with self.assertRaises(StorageConflict):
                    presentation(
                        scope,
                        f"{resource}-1" if resource == "offer" else "discount-1",
                        1,
                        SUPPORTED_CURRENCIES,
                        display_name="Operator",
                        display_description=None,
                        **self.mutation_metadata(f"{resource}-presentation"),
                    )
                current = backend.items[("Catalog", scope.partition_key, sk)]
                self.assertEqual(current["lifecycleRevision"], 2)
                self.assertEqual(current["lifecycleState"], "provisioning")
                self.assertEqual(current["presentationRevision"], 1)

    def test_policy_currency_allowlist_is_closed_and_enforced_for_browser_and_storage_reads(self):
        from src.catalog_storage import CatalogStore
        from src.common.http import HttpError, supported_currencies
        from src.domain.catalog import CatalogItem
        from src.domain.offers import DiscountVersion, Money, OfferVersion
        from src.handlers import catalog_action
        from src.storage import CommerceScope, StorageConflict

        resolved = resolved_policies()
        commerce = resolved.commerce["commerce"]
        self.assertEqual(supported_currencies(commerce), SUPPORTED_CURRENCIES)

        invalid_values = (
            None,
            [],
            ["MXN", "MXN"],
            ["mxn"],
            [f"AA{chr(65 + index)}" for index in range(17)],
        )
        for value in invalid_values:
            with self.subTest(value=value):
                invalid = json.loads(json.dumps(commerce))
                if value is None:
                    del invalid["payments"]["supportedCurrencies"]
                else:
                    invalid["payments"]["supportedCurrencies"] = value
                with self.assertRaises(HttpError) as caught:
                    supported_currencies(invalid)
                self.assertEqual(caught.exception.code, "upstream_unavailable")

        store = HandlerStore()
        with (
            patch.object(catalog_action, "resolve_policies", return_value=resolved),
            patch.object(catalog_action, "authorize_request", return_value=Mock(subject="operator-1")),
            patch.object(catalog_action, "_store", return_value=store),
        ):
            unsupported = catalog_action.lambda_handler(
                api_event(
                    "/features/commerce/catalog/action",
                    {
                        "operation": "createOfferVersion",
                        "input": {
                            "versionId": "offer-usd",
                            "catalogItemId": "landing",
                            "revision": 1,
                            "sellableType": "service",
                            "unitPrice": {"amountMinor": 100, "currency": "USD"},
                            "taxBehavior": "exclusive",
                        },
                    },
                ),
                None,
            )
        self.assertEqual(unsupported["statusCode"], 400)
        self.assertEqual(store.calls, [])

        backend = FakeCatalogBackend()
        catalog = CatalogStore(backend, "Catalog", "Operations")
        scope = CommerceScope("test", "tenant-example", "draft-example", DOMAIN)
        catalog.create_item(
            scope,
            CatalogItem("landing", "service"),
            **self.mutation_metadata("create-currency-item"),
        )
        with self.assertRaises(ValueError):
            catalog.create_offer(
                scope,
                OfferVersion(
                    "offer-usd",
                    "landing",
                    None,
                    1,
                    "service",
                    Money(100, "USD", frozenset({"USD"})),
                    "exclusive",
                ),
                supported_currencies=SUPPORTED_CURRENCIES,
                **self.mutation_metadata("reject-currency-offer"),
            )
        with self.assertRaises(ValueError):
            catalog.create_discount(
                scope,
                DiscountVersion(
                    "discount-usd",
                    1,
                    "once",
                    fixed_amount=Money(100, "USD", frozenset({"USD"})),
                ),
                supported_currencies=SUPPORTED_CURRENCIES,
                **self.mutation_metadata("reject-currency-discount"),
            )
        catalog.create_offer(
            scope,
            OfferVersion(
                "offer-mxn",
                "landing",
                None,
                1,
                "service",
                Money(90_000, "MXN", SUPPORTED_CURRENCIES),
                "exclusive",
            ),
            supported_currencies=SUPPORTED_CURRENCIES,
            **self.mutation_metadata("create-currency-offer"),
        )
        with self.assertRaises(StorageConflict):
            catalog.get_checkout_offer(scope, "offer-mxn", frozenset({"USD"}))
        self.assertEqual(
            catalog.get_catalog(scope, "offers", "offer-mxn", SUPPORTED_CURRENCIES)["currency"],
            "MXN",
        )

    def test_catalog_lists_page_without_inactive_offers_hiding_active_offers(self):
        from src.catalog_storage import CatalogStore
        from src.domain.catalog import CatalogItem
        from src.domain.offers import Money, OfferVersion
        from src.storage import CommerceScope

        backend = FakeCatalogBackend()
        store = CatalogStore(backend, "Catalog", "Operations")
        scope = CommerceScope("test", "tenant-example", "draft-example", DOMAIN)
        store.create_item(
            scope,
            CatalogItem("landing", "service"),
            **self.mutation_metadata("create-page-item"),
        )
        for index in range(103):
            version_id = f"offer-{index:03d}"
            store.create_offer(
                scope,
                OfferVersion(
                    version_id,
                    "landing",
                    None,
                    1,
                    "service",
                    Money(90_000 + index, "MXN", SUPPORTED_CURRENCIES),
                    "exclusive",
                ),
                supported_currencies=SUPPORTED_CURRENCIES,
                **self.mutation_metadata(f"create-page-{index}"),
            )
            if index >= 101:
                backend.items[("Catalog", scope.partition_key, f"OFFER#{version_id}")]["lifecycleState"] = "active"

        admin_first, admin_cursor = store.list_catalog(
            scope, "offers", 2, None, SUPPORTED_CURRENCIES
        )
        admin_second, _ = store.list_catalog(
            scope, "offers", 2, admin_cursor, SUPPORTED_CURRENCIES
        )
        self.assertEqual([item["versionId"] for item in admin_first], ["offer-000", "offer-001"])
        self.assertEqual([item["versionId"] for item in admin_second], ["offer-002", "offer-003"])

        backend.queries.clear()
        public_first, public_cursor = store.list_public_offers(
            scope, 1, None, SUPPORTED_CURRENCIES
        )
        self.assertEqual([item["offerVersionId"] for item in public_first], ["offer-101"])
        self.assertEqual(len(backend.queries), 2)
        self.assertIsNotNone(public_cursor)
        public_second, final_cursor = store.list_public_offers(
            scope, 1, public_cursor, SUPPORTED_CURRENCIES
        )
        self.assertEqual([item["offerVersionId"] for item in public_second], ["offer-102"])
        self.assertIsNone(final_cursor)

    def test_public_catalog_scan_is_bounded_and_empty_page_advances_from_last_inspected_offer(self):
        from src.catalog_storage import CatalogStore
        from src.domain.catalog import CatalogItem
        from src.domain.offers import Money, OfferVersion
        from src.storage import CommerceScope

        backend = FakeCatalogBackend()
        store = CatalogStore(backend, "Catalog", "Operations")
        scope = CommerceScope("test", "tenant-example", "draft-example", DOMAIN)
        store.create_item(
            scope,
            CatalogItem("landing", "service"),
            **self.mutation_metadata("create-bounded-item"),
        )
        for index in range(205):
            version_id = f"bounded-{index:03d}"
            store.create_offer(
                scope,
                OfferVersion(
                    version_id,
                    "landing",
                    None,
                    1,
                    "service",
                    Money(90_000 + index, "MXN", SUPPORTED_CURRENCIES),
                    "exclusive",
                ),
                supported_currencies=SUPPORTED_CURRENCIES,
                **self.mutation_metadata(f"create-bounded-{index}"),
            )
        backend.items[("Catalog", scope.partition_key, "OFFER#bounded-204")]["lifecycleState"] = "active"

        backend.queries.clear()
        first, cursor = store.list_public_offers(scope, 1, None, SUPPORTED_CURRENCIES)

        self.assertEqual(first, [])
        self.assertEqual(len(backend.queries), 2)
        self.assertEqual(cursor, "OFFER#bounded-199")

        second, final_cursor = store.list_public_offers(
            scope, 1, cursor, SUPPORTED_CURRENCIES
        )
        self.assertEqual([item["offerVersionId"] for item in second], ["bounded-204"])
        self.assertIsNone(final_cursor)

    def test_catalog_cursor_is_opaque_scope_and_kind_bound_and_rejects_malformed_json(self):
        from dataclasses import replace

        from src.handlers import catalog_public_read, catalog_read

        resolved = resolved_policies()
        store = HandlerStore()
        first_event = api_event(
            "/features/commerce/public-read",
            {"operation": "offerList", "input": {"limit": 1}},
        )
        with (
            patch.object(catalog_public_read, "resolve_commerce_policy", return_value=resolved),
            patch.object(catalog_public_read, "_store", return_value=store),
        ):
            first = catalog_public_read.lambda_handler(first_event, None)
        self.assertEqual(first["statusCode"], 200)
        token = response_body(first)["data"]["cursor"]
        decoded = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode("utf-8")
        for forbidden in ("tenant-example", "draft-example", "ENV#", "OFFER#"):
            self.assertNotIn(forbidden, decoded)

        store.calls.clear()
        with (
            patch.object(catalog_public_read, "resolve_commerce_policy", return_value=resolved),
            patch.object(catalog_public_read, "_store", return_value=store),
        ):
            second = catalog_public_read.lambda_handler(
                api_event(
                    "/features/commerce/public-read",
                    {"operation": "offerList", "input": {"limit": 1, "cursor": token}},
                ),
                None,
            )
        self.assertEqual(second["statusCode"], 200)
        self.assertEqual(store.calls[0][3], "OFFER#offer-1")

        duplicate = base64.urlsafe_b64encode(b'{"v":1,"v":1}').decode("ascii").rstrip("=")
        for malformed in ("not+a+cursor", duplicate):
            with self.subTest(malformed=malformed), patch.object(
                catalog_public_read, "resolve_commerce_policy", return_value=resolved
            ), patch.object(catalog_public_read, "_store", return_value=store):
                rejected = catalog_public_read.lambda_handler(
                    api_event(
                        "/features/commerce/public-read",
                        {"operation": "offerList", "input": {"cursor": malformed}},
                    ),
                    None,
                )
            self.assertEqual(rejected["statusCode"], 400)

        other_commerce = json.loads(json.dumps(resolved.commerce))
        other_commerce["scope"]["draftId"] = "draft-other"
        other = replace(resolved, draft_id="draft-other", commerce=other_commerce)
        with (
            patch.object(catalog_public_read, "resolve_commerce_policy", return_value=other),
            patch.object(catalog_public_read, "_store", return_value=store),
        ):
            cross_scope = catalog_public_read.lambda_handler(
                api_event(
                    "/features/commerce/public-read",
                    {"operation": "offerList", "input": {"cursor": token}},
                ),
                None,
            )
        self.assertEqual(cross_scope["statusCode"], 400)

        with (
            patch.object(catalog_read, "resolve_policies", return_value=resolved),
            patch.object(catalog_read, "authorize_request", return_value=Mock()),
            patch.object(catalog_read, "_store", return_value=store),
        ):
            cross_kind = catalog_read.lambda_handler(
                api_event(
                    "/features/commerce/read",
                    {"operation": "offerList", "input": {"cursor": token}},
                ),
                None,
            )
            self.assertEqual(cross_kind["statusCode"], 400)

    def test_public_catalog_cursor_rejects_last_id_tampering(self):
        from src.handlers import catalog_public_read

        resolved = resolved_policies()
        store = HandlerStore()
        with (
            patch.dict(
                os.environ,
                {"COMMERCE_CURSOR_SIGNING_KEY": CURSOR_SIGNING_KEY},
                clear=False,
            ),
            patch.object(catalog_public_read, "resolve_commerce_policy", return_value=resolved),
            patch.object(catalog_public_read, "_store", return_value=store),
        ):
            first = catalog_public_read.lambda_handler(
                api_event(
                    "/features/commerce/public-read",
                    {"operation": "offerList", "input": {"limit": 1}},
                ),
                None,
            )
            token = response_body(first)["data"]["cursor"]
            self.assertLessEqual(len(token), 1024)
            payload = json.loads(
                base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
            )
            payload["l"] = "offer-999"
            forged = base64.urlsafe_b64encode(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).decode("ascii").rstrip("=")

            rejected = catalog_public_read.lambda_handler(
                api_event(
                    "/features/commerce/public-read",
                    {"operation": "offerList", "input": {"limit": 1, "cursor": forged}},
                ),
                None,
            )

        self.assertEqual(rejected["statusCode"], 400)

    def test_catalog_cursor_rejects_signature_tampering_and_key_rotation(self):
        from src.handlers import catalog_public_read

        resolved = resolved_policies()
        store = HandlerStore()
        with (
            patch.object(catalog_public_read, "resolve_commerce_policy", return_value=resolved),
            patch.object(catalog_public_read, "_store", return_value=store),
        ):
            first = catalog_public_read.lambda_handler(
                api_event(
                    "/features/commerce/public-read",
                    {"operation": "offerList", "input": {"limit": 1}},
                ),
                None,
            )
            token = response_body(first)["data"]["cursor"]
            payload = json.loads(
                base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
            )
            self.assertEqual(set(payload), {"v", "k", "l", "s", "m"})
            payload["m"] = "A" * 43
            tampered = base64.urlsafe_b64encode(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).decode("ascii").rstrip("=")
            bad_signature = catalog_public_read.lambda_handler(
                api_event(
                    "/features/commerce/public-read",
                    {"operation": "offerList", "input": {"cursor": tampered}},
                ),
                None,
            )
            with patch.dict(
                os.environ,
                {
                    "COMMERCE_CURSOR_SIGNING_KEY": base64.urlsafe_b64encode(b"r" * 32)
                    .decode("ascii")
                    .rstrip("=")
                },
                clear=False,
            ):
                rotated = catalog_public_read.lambda_handler(
                    api_event(
                        "/features/commerce/public-read",
                        {"operation": "offerList", "input": {"cursor": token}},
                    ),
                    None,
                )

        self.assertEqual(bad_signature["statusCode"], 400)
        self.assertEqual(rotated["statusCode"], 400)

    def test_catalog_list_handlers_fail_sanitized_when_cursor_key_is_missing_or_malformed(self):
        from src.handlers import catalog_public_read, catalog_read

        resolved = resolved_policies()
        store = HandlerStore()
        cases = (
            (catalog_public_read, "resolve_commerce_policy", "/features/commerce/public-read"),
            (catalog_read, "resolve_policies", "/features/commerce/read"),
        )
        for module, resolver_name, path in cases:
            for raw_key in (None, "A" * 31, "A" * 33, "*" * 43, "A" * 129):
                with self.subTest(handler=module.__name__, raw_key=raw_key):
                    environment = {} if raw_key is None else {"COMMERCE_CURSOR_SIGNING_KEY": raw_key}
                    output = io.StringIO()
                    with (
                        patch.dict(os.environ, environment, clear=True),
                        patch.object(module, resolver_name, return_value=resolved),
                        patch.object(module, "_store", return_value=store),
                        patch.object(module, "authorize_request", return_value=Mock(), create=True),
                        redirect_stdout(output),
                        redirect_stderr(output),
                    ):
                        response = module.lambda_handler(
                            api_event(path, {"operation": "offerList", "input": {"limit": 1}}),
                            None,
                        )
                    serialized = json.dumps(response)
                    self.assertEqual(response["statusCode"], 503)
                    if raw_key is not None:
                        self.assertNotIn(raw_key, serialized)
                        self.assertNotIn(raw_key, output.getvalue())

    def test_catalog_cursor_accepts_canonical_key_length_boundaries(self):
        from src.handlers import catalog_public_read

        resolved = resolved_policies()
        for raw_key in ("A" * 32, "A" * 128):
            with (
                self.subTest(length=len(raw_key)),
                patch.dict(
                    os.environ,
                    {"COMMERCE_CURSOR_SIGNING_KEY": raw_key},
                    clear=False,
                ),
                patch.object(
                    catalog_public_read,
                    "resolve_commerce_policy",
                    return_value=resolved,
                ),
                patch.object(catalog_public_read, "_store", return_value=HandlerStore()),
            ):
                response = catalog_public_read.lambda_handler(
                    api_event(
                        "/features/commerce/public-read",
                        {"operation": "offerList", "input": {"limit": 1}},
                    ),
                    None,
                )
            self.assertEqual(response["statusCode"], 200)

    def test_catalog_cursor_never_returns_or_logs_the_signing_key(self):
        from src.handlers import catalog_public_read

        resolved = resolved_policies()
        output = io.StringIO()
        with (
            patch.object(catalog_public_read, "resolve_commerce_policy", return_value=resolved),
            patch.object(catalog_public_read, "_store", return_value=HandlerStore()),
            redirect_stdout(output),
            redirect_stderr(output),
        ):
            response = catalog_public_read.lambda_handler(
                api_event(
                    "/features/commerce/public-read",
                    {"operation": "offerList", "input": {"limit": 1}},
                ),
                None,
            )

        self.assertEqual(response["statusCode"], 200)
        self.assertNotIn(CURSOR_SIGNING_KEY, json.dumps(response))
        self.assertNotIn(CURSOR_SIGNING_KEY, output.getvalue())

    def test_public_and_protected_reads_use_distinct_policy_and_auth_boundaries(self):
        from src.handlers import catalog_public_read, catalog_read

        resolved = resolved_policies()
        store = HandlerStore()
        public_event = api_event(
            "/features/commerce/public-read",
            {"operation": "offerList", "input": {"limit": 50}},
        )
        with (
            patch.object(catalog_public_read, "resolve_commerce_policy", return_value=resolved) as public_resolver,
            patch.object(catalog_public_read, "_store", return_value=store),
        ):
            public_response = catalog_public_read.lambda_handler(public_event, None)
        self.assertEqual(public_response["statusCode"], 200)
        public_resolver.assert_called_once_with(DOMAIN)
        self.assertEqual(store.calls[-1][0], "list_public_offers")
        self.assertEqual(store.calls[-1][1].domain, DOMAIN)

        protected_event = api_event(
            "/features/commerce/read",
            {"operation": "offerList", "input": {"limit": 10}},
        )
        with (
            patch.object(catalog_read, "resolve_policies", return_value=resolved) as protected_resolver,
            patch.object(catalog_read, "authorize_request", return_value=Mock()) as authorize,
            patch.object(catalog_read, "_store", return_value=store),
        ):
            protected_response = catalog_read.lambda_handler(protected_event, None)
        self.assertEqual(protected_response["statusCode"], 200)
        protected_resolver.assert_called_once_with(DOMAIN)
        self.assertEqual(authorize.call_args.kwargs["capability"], "commerce:catalog:read")
        self.assertFalse(authorize.call_args.kwargs["mutation"])

    def test_catalog_action_is_closed_and_authorized_before_server_scoped_write(self):
        from src.handlers import catalog_action

        resolved = resolved_policies()
        store = HandlerStore()
        payload = {
            "operation": "createItem",
            "input": {
                "itemId": "landing",
                "sellableType": "service",
                "variants": [{"variantId": "base", "sku": "LANDING-BASE"}],
            },
        }
        with (
            patch.object(catalog_action, "resolve_policies", return_value=resolved),
            patch.object(catalog_action, "authorize_request", return_value=Mock(subject="operator-1")) as authorize,
            patch.object(catalog_action, "_store", return_value=store),
            patch.object(catalog_action.time, "time", return_value=1_800_000_000),
        ):
            response = catalog_action.lambda_handler(
                api_event("/features/commerce/catalog/action", payload), None
            )
        self.assertEqual(response["statusCode"], 200)
        authorize.assert_called_once()
        self.assertEqual(authorize.call_args.kwargs["capability"], "commerce:catalog:write")
        self.assertTrue(authorize.call_args.kwargs["mutation"])
        _, scope, item, metadata = store.calls[-1]
        self.assertEqual((scope.environment, scope.tenant_id, scope.draft_id, scope.domain), (
            "test", "tenant-example", "draft-example", DOMAIN
        ))
        self.assertEqual(item.item_id, "landing")
        self.assertEqual(metadata["actor_hash"], hashlib.sha256(b"operator-1").hexdigest())
        self.assertEqual(metadata["idempotency_key"], "operation-1")
        self.assertEqual(metadata["request_id"], "request-1")
        self.assertEqual(metadata["correlation_id"], "request-1")

        injected = dict(payload)
        injected["tenantId"] = "other"
        with patch.object(catalog_action, "resolve_policies", return_value=resolved) as resolver:
            rejected = catalog_action.lambda_handler(
                api_event("/features/commerce/catalog/action", injected), None
            )
        self.assertEqual(rejected["statusCode"], 400)
        resolver.assert_not_called()

    def test_inventory_action_derives_scope_location_and_clock_and_rejects_authority_fields(self):
        from src.handlers import inventory_action

        resolved = resolved_policies()
        store = HandlerStore()
        payload = {
            "operation": "adjustStock",
            "input": {"stockId": "landing", "delta": 3, "expectedRevision": 0},
        }
        with (
            patch.object(inventory_action, "resolve_policies", return_value=resolved),
            patch.object(inventory_action, "authorize_request", return_value=Mock(subject="operator-1")) as authorize,
            patch.object(inventory_action, "_store", return_value=store),
            patch.object(inventory_action.time, "time", return_value=1_800_000_000),
        ):
            response = inventory_action.lambda_handler(
                api_event("/features/commerce/inventory/action", payload), None
            )
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(authorize.call_args.kwargs["capability"], "commerce:inventory:write")
        _, args, kwargs = store.calls[-1]
        self.assertEqual(args[0].domain, DOMAIN)
        self.assertEqual(kwargs["location_id"], "primary")
        self.assertEqual(kwargs["now_epoch"], 1_800_000_000)
        self.assertEqual(kwargs["idempotency_key"], "operation-1")

        unsafe = {
            "operation": "adjustStock",
            "input": {
                "stockId": "landing", "delta": 3, "expectedRevision": 0,
                "locationId": "other", "nowEpoch": 1,
            },
        }
        with patch.object(inventory_action, "resolve_policies", return_value=resolved) as resolver:
            rejected = inventory_action.lambda_handler(
                api_event("/features/commerce/inventory/action", unsafe), None
            )
        self.assertEqual(rejected["statusCode"], 400)
        resolver.assert_not_called()

    def test_catalog_browser_action_cannot_activate_before_provider_command_exists(self):
        from src.handlers import catalog_action

        resolved = resolved_policies()
        with (
            patch.object(catalog_action, "resolve_policies", return_value=resolved),
            patch.object(catalog_action, "authorize_request", return_value=Mock(subject="operator-1")),
            patch.object(catalog_action, "_store", return_value=HandlerStore()),
        ):
            response = catalog_action.lambda_handler(
                api_event(
                    "/features/commerce/catalog/action",
                    {
                        "operation": "advanceOfferLifecycle",
                        "input": {"versionId": "offer-1", "targetState": "active", "expectedRevision": 2},
                    },
                ),
                None,
            )
        self.assertEqual(response["statusCode"], 503)
        self.assertEqual(response_body(response)["code"], "upstream_unavailable")

    def test_transport_rejects_wrong_route_duplicate_json_and_unsafe_error_details(self):
        from src.handlers import catalog_action, catalog_public_read

        wrong = api_event(
            "/features/commerce/read",
            {"operation": "offerList", "input": {}},
        )
        with patch.object(catalog_public_read, "resolve_commerce_policy") as resolver:
            response = catalog_public_read.lambda_handler(wrong, None)
        self.assertEqual(response["statusCode"], 404)
        resolver.assert_not_called()

        duplicate = api_event("/features/commerce/public-read", {})
        duplicate["body"] = '{"operation":"offerList","operation":"offerDetail","input":{}}'
        with patch.object(catalog_public_read, "resolve_commerce_policy") as resolver:
            response = catalog_public_read.lambda_handler(duplicate, None)
        self.assertEqual(response["statusCode"], 400)
        resolver.assert_not_called()

        malformed_reference = {
            "operation": "createItem",
            "input": {
                "itemId": "landing",
                "sellableType": "service",
                "dataSpaceReference": {
                    "spaceId": "catalog",
                    "collectionId": "services",
                    "recordId": "landing",
                    "revision": 1,
                    "fieldIds": [{}],
                },
            },
        }
        with patch.object(catalog_action, "resolve_policies") as resolver:
            response = catalog_action.lambda_handler(
                api_event("/features/commerce/catalog/action", malformed_reference), None
            )
        self.assertEqual(response["statusCode"], 400)
        resolver.assert_not_called()

        with patch.object(
            catalog_public_read, "resolve_commerce_policy", side_effect=RuntimeError("sensitive detail")
        ):
            response = catalog_public_read.lambda_handler(
                api_event("/features/commerce/public-read", {"operation": "offerList", "input": {}}), None
            )
        self.assertEqual(response["statusCode"], 500)
        self.assertEqual(response_body(response)["code"], "internal_error")
        self.assertNotIn("sensitive", response["body"])

        malformed_discount = {
            "operation": "createDiscountVersion",
            "input": {
                "versionId": "discount-1",
                "revision": 1,
                "duration": "once",
                "percentageBasisPoints": 1_000,
                "eligibleOfferVersionIds": [{}],
            },
        }
        with patch.object(catalog_action, "resolve_policies") as resolver:
            response = catalog_action.lambda_handler(
                api_event("/features/commerce/catalog/action", malformed_discount), None
            )
        self.assertEqual(response["statusCode"], 400)
        resolver.assert_not_called()


if __name__ == "__main__":
    unittest.main()
