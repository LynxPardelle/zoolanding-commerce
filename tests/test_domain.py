from dataclasses import FrozenInstanceError
import unittest

from src.domain import catalog, fiscal, inventory, offers, orders, shipping, subscriptions


SUPPORTED_CURRENCIES = frozenset({"MXN", "USD"})


def offer_version(**overrides):
    values = {
        "version_id": "offer-v1",
        "catalog_item_id": "landing-page",
        "variant_id": "standard",
        "revision": 1,
        "sellable_type": "service",
        "unit_price": offers.Money(90_000, "MXN", SUPPORTED_CURRENCIES),
        "tax_behavior": "exclusive",
        "display_name": "Landing page",
        "display_description": "Configuración y publicación",
    }
    values.update(overrides)
    return offers.OfferVersion(**values)


def discount_version(**overrides):
    values = {
        "version_id": "discount-v1",
        "revision": 1,
        "percentage_basis_points": 1_500,
        "duration": "once",
        "eligible_offer_version_ids": frozenset({"offer-v1", "offer-v2"}),
        "redemption_limit": 100,
        "redeem_by_epoch": 1_800_000_000,
        "customer_facing_code": "WELCOME15",
        "display_name": "Welcome discount",
    }
    values.update(overrides)
    return offers.DiscountVersion(**values)


class DomainTests(unittest.TestCase):
    def test_code_owned_registries_are_closed(self):
        self.assertEqual(
            catalog.SELLABLE_TYPES,
            frozenset({"physical", "service", "subscription", "add_on"}),
        )
        self.assertEqual(
            shipping.SHIPPING_METHODS,
            frozenset({"fixed", "free", "pickup"}),
        )
        self.assertEqual(fiscal.MANUAL_DISCLOSURE_ID, "manual-invoice-v1")
        self.assertEqual(
            fiscal.TAX_BEHAVIORS,
            frozenset({"exclusive", "inclusive", "provider-calculated"}),
        )

    def test_money_is_immutable_integer_minor_units_with_canonical_currency(self):
        value = offers.Money(90_000, "MXN", SUPPORTED_CURRENCIES)

        self.assertEqual((value.amount_minor, value.currency), (90_000, "MXN"))
        with self.assertRaises(FrozenInstanceError):
            value.amount_minor = 1
        for invalid_amount in (True, 1.5, -1, "90000"):
            with self.subTest(amount=invalid_amount), self.assertRaises(ValueError):
                offers.Money(invalid_amount, "MXN", SUPPORTED_CURRENCIES)
        for invalid_currency in ("mxn", "MX", "MXN1", "MÉX", 1):
            with self.subTest(currency=invalid_currency), self.assertRaises(ValueError):
                offers.Money(0, invalid_currency, SUPPORTED_CURRENCIES)
        with self.assertRaises(ValueError):
            offers.Money(0, "AAA", SUPPORTED_CURRENCIES)
        for invalid_allowlist in (set(SUPPORTED_CURRENCIES), frozenset(), frozenset({"mxn"})):
            with self.subTest(allowlist=invalid_allowlist), self.assertRaises(ValueError):
                offers.Money(0, "MXN", invalid_allowlist)

    def test_money_matches_the_integrations_minor_unit_boundary(self):
        boundary = offers.Money(99_999_999, "MXN", SUPPORTED_CURRENCIES)

        self.assertEqual(boundary.amount_minor, 99_999_999)
        with self.assertRaises(ValueError):
            offers.Money(100_000_000, "MXN", SUPPORTED_CURRENCIES)

    def test_commercial_revisions_match_the_integrations_integer_boundary(self):
        maximum = 9_999_999_999

        self.assertEqual(offer_version(revision=maximum).revision, maximum)
        self.assertEqual(discount_version(redeem_by_epoch=maximum).redeem_by_epoch, maximum)
        for call in (
            lambda: offer_version(revision=maximum + 1),
            lambda: offer_version(lifecycle_revision=maximum + 1),
            lambda: offer_version(presentation_revision=maximum + 1),
            lambda: discount_version(revision=maximum + 1),
            lambda: discount_version(lifecycle_revision=maximum + 1),
            lambda: discount_version(presentation_revision=maximum + 1),
            lambda: discount_version(redeem_by_epoch=maximum + 1),
        ):
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()

    def test_discount_limits_match_the_integrations_provider_contract(self):
        duration_boundary = discount_version(
            duration="repeating",
            duration_in_months=36,
        )
        redemption_boundary = discount_version(redemption_limit=1_000_000)

        self.assertEqual(duration_boundary.duration_in_months, 36)
        self.assertEqual(redemption_boundary.redemption_limit, 1_000_000)
        for call in (
            lambda: discount_version(duration="repeating", duration_in_months=37),
            lambda: discount_version(redemption_limit=1_000_001),
        ):
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()

    def test_provider_presentation_is_bounded_plain_text_without_domains_or_bidi(self):
        boundary = offer_version(display_name="N" * 160)

        self.assertEqual(len(boundary.display_name), 160)
        for unsafe in (
            "N" * 161,
            "example.com",
            "plans.example.com.mx",
            "support@example.com",
            "safe\u2066text",
            "safe\u202etext",
            "bad\r\nheader",
        ):
            with self.subTest(unsafe=unsafe), self.assertRaises(ValueError):
                offer_version(display_name=unsafe)

    def test_catalog_inventory_shipping_and_fiscal_values_fail_closed(self):
        self.assertEqual(catalog.validate_sellable_type("service"), "service")
        self.assertEqual(inventory.validate_quantity(0), 0)
        self.assertEqual(shipping.validate_shipping_method("pickup"), "pickup")
        self.assertEqual(
            fiscal.validate_fiscal_policy("manual-invoice-v1", "exclusive"),
            ("manual-invoice-v1", "exclusive"),
        )
        invalid_calls = (
            lambda: catalog.validate_sellable_type("digital"),
            lambda: inventory.validate_quantity(True),
            lambda: inventory.validate_quantity(-1),
            lambda: shipping.validate_shipping_method("carrier"),
            lambda: fiscal.validate_fiscal_policy("automatic", "exclusive"),
            lambda: fiscal.validate_fiscal_policy("manual-invoice-v1", "unknown"),
        )
        for call in invalid_calls:
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()

    def test_order_totals_stay_in_minor_units_and_physical_recurring_is_rejected(self):
        unit_price = offers.Money(3_000, "MXN", SUPPORTED_CURRENCIES)
        self.assertEqual(
            orders.line_total(unit_price, 3),
            offers.Money(9_000, "MXN", SUPPORTED_CURRENCIES),
        )
        self.assertEqual(
            subscriptions.validate_recurring_sellable_type("subscription"),
            "subscription",
        )
        with self.assertRaises(ValueError):
            orders.line_total(unit_price, 1.5)
        with self.assertRaises(ValueError):
            subscriptions.validate_recurring_sellable_type("physical")

    def test_checkout_line_quantity_is_positive_and_bounded(self):
        unit_price = offers.Money(1, "MXN", SUPPORTED_CURRENCIES)
        boundary = orders.CheckoutLine(
            "line-1",
            "offer-v1",
            orders.MAX_CHECKOUT_LINE_QUANTITY,
            unit_price,
        )
        self.assertEqual(boundary.quantity, orders.MAX_CHECKOUT_LINE_QUANTITY)
        for invalid_quantity in (0, orders.MAX_CHECKOUT_LINE_QUANTITY + 1, 10**100):
            with self.subTest(quantity=invalid_quantity), self.assertRaises(ValueError):
                orders.CheckoutLine("line-1", "offer-v1", invalid_quantity, unit_price)

    def test_catalog_items_keep_variants_and_data_space_references_deeply_immutable(self):
        reference = catalog.DataSpaceRecordReference(
            space_id="products",
            collection_id="catalog",
            record_id="landing-page",
            revision=3,
            field_ids=("title", "summary"),
        )
        variant = catalog.CatalogVariant(variant_id="standard", sku="ZLP-LP-STD")
        item = catalog.CatalogItem(
            item_id="landing-page",
            sellable_type="service",
            variants=(variant,),
            data_space_reference=reference,
        )

        self.assertEqual(item.variants, (variant,))
        self.assertEqual(item.data_space_reference.field_ids, ("title", "summary"))
        with self.assertRaises(FrozenInstanceError):
            item.item_id = "other"
        with self.assertRaises(FrozenInstanceError):
            reference.revision = 4
        with self.assertRaises(ValueError):
            catalog.CatalogItem("bad", "service", [variant])
        with self.assertRaises(ValueError):
            catalog.CatalogItem(
                "bad-reference",
                "service",
                data_space_reference={"record_id": "landing-page"},
            )
        for sellable_type in catalog.SELLABLE_TYPES:
            with self.subTest(sellable_type=sellable_type):
                minimal = catalog.CatalogItem(
                    item_id=f"{sellable_type}-item",
                    sellable_type=sellable_type,
                )
                self.assertEqual(minimal.variants, ())
                self.assertIsNone(minimal.data_space_reference)

    def test_data_space_references_match_the_internal_snapshot_request_contract(self):
        valid = {
            "space_id": "products",
            "collection_id": "catalog",
            "record_id": "landing-page",
            "revision": 1,
            "field_ids": ("title",),
        }
        for field in ("space_id", "collection_id", "record_id"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                catalog.DataSpaceRecordReference(**{**valid, field: "Unsafe/Id"})
        for revision in (True, 0, -1, 1.5):
            with self.subTest(revision=revision), self.assertRaises(ValueError):
                catalog.DataSpaceRecordReference(**{**valid, "revision": revision})
        for field_ids in (
            [],
            (),
            ("title", "title"),
            ("unsafe.field",),
            tuple(f"field{index}" for index in range(201)),
        ):
            with self.subTest(field_ids=field_ids), self.assertRaises(ValueError):
                catalog.DataSpaceRecordReference(**{**valid, "field_ids": field_ids})

    def test_catalog_rejects_unsafe_or_duplicate_variant_identity(self):
        variant = catalog.CatalogVariant("standard", "ZLP-LP-STD")
        for invalid_variant in (
            lambda: catalog.CatalogVariant("Unsafe/Id", "SKU"),
            lambda: catalog.CatalogVariant("standard", "unsafe sku"),
            lambda: catalog.CatalogItem("landing-page", "digital"),
            lambda: catalog.CatalogItem("Unsafe/Id", "service"),
            lambda: catalog.CatalogItem("landing-page", "service", (variant, catalog.CatalogVariant("standard", "OTHER"))),
            lambda: catalog.CatalogItem("landing-page", "service", (variant, catalog.CatalogVariant("other", "ZLP-LP-STD"))),
        ):
            with self.subTest(call=invalid_variant), self.assertRaises(ValueError):
                invalid_variant()
        case_distinct = catalog.CatalogItem(
            "landing-page",
            "service",
            (
                variant,
                catalog.CatalogVariant("other", "zlp-lp-std"),
            ),
        )
        self.assertEqual(len(case_distinct.variants), 2)

    def test_offer_fingerprint_is_canonical_and_ignores_lifecycle_and_presentation(self):
        original = offer_version()
        equivalent = offer_version(
            version_id="offer-v2",
            revision=9,
            sellable_type="add_on",
            lifecycle_state="active",
            lifecycle_revision=4,
            presentation_revision=7,
            display_name="A new safe label",
            display_description=None,
        )

        self.assertRegex(original.provider_fingerprint, r"^[0-9a-f]{64}$")
        self.assertEqual(original.provider_fingerprint, equivalent.provider_fingerprint)
        self.assertEqual(
            original.provider_snapshot(),
            {
                "schemaVersion": 1,
                "amountMinor": 90_000,
                "billingScheme": "per_unit",
                "currency": "MXN",
                "recurrence": None,
                "saleType": "one_time",
                "taxBehavior": "exclusive",
            },
        )
        self.assertEqual(
            original.provider_fingerprint,
            offer_version(
                unit_price=offers.Money(
                    90_000,
                    "MXN",
                    frozenset({"CAD", "MXN", "USD"}),
                ),
            ).provider_fingerprint,
        )
        changed_values = (
            offer_version(unit_price=offers.Money(90_001, "MXN", SUPPORTED_CURRENCIES)),
            offer_version(unit_price=offers.Money(90_000, "USD", SUPPORTED_CURRENCIES)),
            offer_version(recurrence=offers.OfferRecurrence("month")),
            offer_version(tax_behavior="inclusive"),
        )
        for changed in changed_values:
            with self.subTest(changed=changed):
                self.assertNotEqual(
                    original.provider_fingerprint,
                    changed.provider_fingerprint,
                )

    def test_recurring_offers_are_closed_to_supported_provider_semantics(self):
        monthly = offers.OfferRecurrence("month")
        yearly = offers.OfferRecurrence("year")

        self.assertEqual(offer_version(recurrence=monthly).sale_type, "recurring")
        self.assertEqual(
            offer_version(recurrence=monthly).provider_snapshot()["recurrence"],
            {
                "interval": "month",
                "intervalCount": 1,
                "usageType": "licensed",
            },
        )
        self.assertEqual(offer_version(sellable_type="subscription", recurrence=yearly).sale_type, "recurring")
        self.assertEqual(offer_version().sale_type, "one_time")
        self.assertIsNone(offer_version(variant_id=None).variant_id)
        with self.assertRaises(ValueError):
            offer_version(sellable_type="physical", recurrence=monthly)
        with self.assertRaises(ValueError):
            offer_version(sellable_type="subscription", recurrence=None)
        for recurrence in (
            lambda: offers.OfferRecurrence("week"),
            lambda: offers.OfferRecurrence("month", interval_count=2),
            lambda: offers.OfferRecurrence("month", billing_scheme="tiered"),
            lambda: offers.OfferRecurrence("month", usage_type="metered"),
        ):
            with self.subTest(call=recurrence), self.assertRaises(ValueError):
                recurrence()

    def test_offer_lifecycle_and_presentation_revisions_advance_without_economic_mutation(self):
        draft = offer_version()
        provisioning = draft.with_lifecycle("provisioning", 2)
        active = provisioning.with_lifecycle("active", 3)
        existing_only = active.with_lifecycle("existing_only", 4)
        retired = existing_only.with_lifecycle("retired", 5)
        presented = active.with_presentation(
            2,
            display_name="Landing page updated",
            display_description="Updated safe description",
        )

        self.assertEqual(retired.lifecycle_state, "retired")
        self.assertEqual(presented.provider_fingerprint, active.provider_fingerprint)
        self.assertEqual(
            (presented.lifecycle_state, presented.lifecycle_revision),
            (active.lifecycle_state, active.lifecycle_revision),
        )
        self.assertEqual(
            (active.presentation_revision, active.display_name, active.display_description),
            (draft.presentation_revision, draft.display_name, draft.display_description),
        )
        for call in (
            lambda: draft.with_lifecycle("active", 2),
            lambda: active.with_lifecycle("provisioning", 4),
            lambda: active.with_lifecycle("existing_only", 3),
            lambda: active.with_lifecycle("existing_only", True),
            lambda: active.with_presentation(1, display_name="Same", display_description=None),
            lambda: active.with_presentation(True, display_name="Same", display_description=None),
        ):
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()

    def test_offer_versions_reject_unsafe_identity_and_invalid_revisions(self):
        invalid_values = (
            lambda: offer_version(version_id="Unsafe/Id"),
            lambda: offer_version(catalog_item_id="Unsafe/Id"),
            lambda: offer_version(variant_id="Unsafe/Id"),
            lambda: offer_version(revision=True),
            lambda: offer_version(revision=0),
            lambda: offer_version(lifecycle_state="enabled"),
            lambda: offer_version(lifecycle_revision=False),
            lambda: offer_version(presentation_revision=0),
            lambda: offer_version(tax_behavior="unknown"),
            lambda: offer_version(unit_price=object()),
            lambda: offer_version(recurrence={"interval": "month"}),
            lambda: offer_version(display_name=""),
            lambda: offer_version(display_name="x" * 201),
            lambda: offer_version(display_description="bad\r\nheader"),
            lambda: offer_version(display_description="bad\u2028line"),
            lambda: offer_version(display_description="safe\u202etext"),
            lambda: offer_version(display_description="tel:+525500000000"),
            lambda: offer_version(display_description="data:text/plain,hello"),
            lambda: offer_version(display_description="urn:example:test"),
            lambda: offer_version(display_description="https:example.com"),
        )
        for call in invalid_values:
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()

    def test_discount_versions_support_percentage_or_single_currency_fixed_amount(self):
        percentage = discount_version()
        fixed = discount_version(
            version_id="discount-fixed-v1",
            percentage_basis_points=None,
            fixed_amount=offers.Money(10_000, "MXN", SUPPORTED_CURRENCIES),
            duration="repeating",
            duration_in_months=3,
        )

        self.assertEqual(percentage.discount_type, "percentage")
        self.assertEqual(fixed.discount_type, "fixed_amount")
        self.assertNotEqual(percentage.provider_fingerprint, fixed.provider_fingerprint)
        self.assertEqual(
            percentage.provider_snapshot(),
            {
                "schemaVersion": 1,
                "customerFacingCode": "WELCOME15",
                "duration": "once",
                "durationInMonths": None,
                "eligibleOfferVersionIds": ["offer-v1", "offer-v2"],
                "redeemByEpoch": 1_800_000_000,
                "redemptionLimit": 100,
                "value": {
                    "basisPoints": 1_500,
                    "type": "percentage",
                },
            },
        )
        self.assertEqual(
            fixed.provider_snapshot()["value"],
            {
                "amountMinor": 10_000,
                "currency": "MXN",
                "type": "fixed_amount",
            },
        )
        with self.assertRaises(FrozenInstanceError):
            percentage.duration = "forever"
        with self.assertRaises(FrozenInstanceError):
            offer_version().unit_price = offers.Money(
                1,
                "MXN",
                SUPPORTED_CURRENCIES,
            )
        equivalent = discount_version(
            version_id="discount-v2",
            revision=8,
            eligible_offer_version_ids=frozenset({"offer-v2", "offer-v1"}),
            lifecycle_state="existing_only",
            lifecycle_revision=6,
            presentation_revision=4,
            display_name="Updated label",
        )
        self.assertEqual(percentage.provider_fingerprint, equivalent.provider_fingerprint)

    def test_discount_fingerprint_covers_every_economic_and_restriction_field(self):
        baseline = discount_version()
        changed_values = (
            discount_version(percentage_basis_points=1_501),
            discount_version(
                percentage_basis_points=None,
                fixed_amount=offers.Money(10_000, "MXN", SUPPORTED_CURRENCIES),
            ),
            discount_version(
                percentage_basis_points=None,
                fixed_amount=offers.Money(10_001, "MXN", SUPPORTED_CURRENCIES),
            ),
            discount_version(
                percentage_basis_points=None,
                fixed_amount=offers.Money(10_000, "USD", SUPPORTED_CURRENCIES),
            ),
            discount_version(duration="forever", redemption_limit=100),
            discount_version(duration="repeating", duration_in_months=3),
            discount_version(duration="repeating", duration_in_months=4),
            discount_version(eligible_offer_version_ids=frozenset({"offer-v1"})),
            discount_version(redemption_limit=99),
            discount_version(redeem_by_epoch=1_800_000_001),
            discount_version(customer_facing_code="OTHER15"),
            discount_version(customer_facing_code="welcome15"),
        )
        for changed in changed_values:
            with self.subTest(changed=changed):
                self.assertNotEqual(
                    baseline.provider_fingerprint,
                    changed.provider_fingerprint,
                )

    def test_discount_versions_fail_closed_on_ambiguous_or_unsafe_restrictions(self):
        invalid_values = (
            lambda: discount_version(percentage_basis_points=None),
            lambda: discount_version(fixed_amount=offers.Money(1, "MXN", SUPPORTED_CURRENCIES)),
            lambda: discount_version(percentage_basis_points=True),
            lambda: discount_version(percentage_basis_points=0),
            lambda: discount_version(percentage_basis_points=10_001),
            lambda: discount_version(percentage_basis_points=None, fixed_amount=offers.Money(0, "MXN", SUPPORTED_CURRENCIES)),
            lambda: discount_version(duration="repeating", duration_in_months=None),
            lambda: discount_version(duration="repeating", duration_in_months=True),
            lambda: discount_version(duration="once", duration_in_months=1),
            lambda: discount_version(duration="unknown"),
            lambda: discount_version(redemption_limit=0),
            lambda: discount_version(redeem_by_epoch=True),
            lambda: discount_version(customer_facing_code="unsafe code"),
            lambda: discount_version(eligible_offer_version_ids=["offer-v1"]),
            lambda: discount_version(eligible_offer_version_ids={"offer-v1"}),
            lambda: discount_version(eligible_offer_version_ids=frozenset({"Unsafe/Id"})),
            lambda: discount_version(display_name="<script>alert(1)</script>"),
            lambda: discount_version(display_description="https://unapproved.example"),
        )
        for call in invalid_values:
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()

    def test_discount_revisions_follow_the_same_monotonic_lifecycle_contract(self):
        draft = discount_version()
        provisioning = draft.with_lifecycle("provisioning", 2)
        active = provisioning.with_lifecycle("active", 3)
        presented = active.with_presentation(
            2,
            display_name="Updated discount",
            display_description="Safe customer copy",
        )

        self.assertEqual(presented.provider_fingerprint, active.provider_fingerprint)
        self.assertEqual(
            (presented.lifecycle_state, presented.lifecycle_revision),
            (active.lifecycle_state, active.lifecycle_revision),
        )
        self.assertEqual(
            (active.presentation_revision, active.display_name),
            (draft.presentation_revision, draft.display_name),
        )
        with self.assertRaises(ValueError):
            active.with_lifecycle("retired", 4)
        with self.assertRaises(ValueError):
            active.with_presentation(1, display_name=None, display_description=None)
        for call in (
            lambda: discount_version(version_id="Unsafe/Id"),
            lambda: discount_version(revision=True),
            lambda: discount_version(lifecycle_state="enabled"),
            lambda: discount_version(lifecycle_revision=0),
            lambda: discount_version(presentation_revision=False),
            lambda: active.with_lifecycle("existing_only", True),
            lambda: active.with_presentation(True, display_name=None, display_description=None),
        ):
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()


if __name__ == "__main__":
    unittest.main()
