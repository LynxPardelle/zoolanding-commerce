from dataclasses import FrozenInstanceError
import unittest

from src.domain import catalog, fiscal, inventory, offers, orders, shipping, subscriptions


SUPPORTED_CURRENCIES = frozenset({"MXN", "USD"})


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


if __name__ == "__main__":
    unittest.main()
