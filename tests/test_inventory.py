from dataclasses import FrozenInstanceError
import unittest

from src.domain.inventory import (
    CHECKOUT_EXPIRY_SECONDS,
    RECONCILIATION_GRACE_SECONDS,
    RECONCILER_INTERVAL_SECONDS,
    StockState,
    checkout_outcome,
    reconciliation_outcome,
    reservation_timing,
)
from src.domain.offers import Money
from src.domain.orders import CheckoutLine, PendingOrder


SUPPORTED_CURRENCIES = frozenset({"MXN"})


class InventoryDomainTests(unittest.TestCase):
    def test_stock_transitions_keep_exact_integer_invariants(self):
        stock = StockState("landing", "primary", True, 10, 2, 4)

        self.assertEqual(stock.available, 8)
        adjusted = stock.adjust(-3)
        reserved = adjusted.reserve(4)
        committed = reserved.commit(4)

        self.assertEqual((adjusted.on_hand, adjusted.reserved, adjusted.available), (7, 2, 5))
        self.assertEqual((reserved.on_hand, reserved.reserved, reserved.available), (7, 6, 1))
        self.assertEqual((committed.on_hand, committed.reserved, committed.available), (3, 2, 1))
        self.assertEqual(reserved.release(4), StockState("landing", "primary", True, 7, 2, 7))
        with self.assertRaises(FrozenInstanceError):
            stock.on_hand = 11
        for call in (
            lambda: StockState("landing", "primary", True, True, 0, 1),
            lambda: StockState("landing", "primary", True, 1, 2, 1),
            lambda: StockState("landing", "primary", False, 1, 0, 1).adjust(1),
            lambda: stock.adjust(-9),
            lambda: stock.reserve(9),
            lambda: stock.commit(3),
            lambda: stock.release(3),
            lambda: stock.reserve(True),
        ):
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()

    def test_checkout_lines_are_bounded_internal_snapshots(self):
        line = CheckoutLine(
            "line-1",
            "offer-v1",
            2,
            Money(90_000, "MXN", SUPPORTED_CURRENCIES),
            "landing",
        )
        order = PendingOrder("order-1", "attempt-1", (line,))

        self.assertEqual(order.lines, (line,))
        self.assertEqual(order.total.amount_minor, 180_000)
        self.assertEqual(order.total.currency, "MXN")
        with self.assertRaises(ValueError):
            PendingOrder("order-1", "attempt-1", ())
        with self.assertRaises(ValueError):
            PendingOrder("order-1", "attempt-1", tuple(line for _ in range(21)))
        with self.assertRaises(ValueError):
            PendingOrder("order-1", "attempt-1", (line, line))
        with self.assertRaises(ValueError):
            PendingOrder(
                "order-1",
                "attempt-1",
                (
                    line,
                    CheckoutLine(
                        "line-2",
                        "offer-v1",
                        1,
                        line.unit_price,
                        "other-stock",
                    ),
                ),
            )
        with self.assertRaises(ValueError):
            CheckoutLine("line-2", "offer-v1", 0, line.unit_price, None)

    def test_reservation_deadlines_and_provider_neutral_decisions_are_exact(self):
        timing = reservation_timing(1_800_000_000)

        self.assertEqual(CHECKOUT_EXPIRY_SECONDS, 2_100)
        self.assertEqual(RECONCILIATION_GRACE_SECONDS, 300)
        self.assertEqual(RECONCILER_INTERVAL_SECONDS, 300)
        self.assertEqual(timing.checkout_expires_at, 1_800_002_100)
        self.assertEqual(timing.reconcile_after, 1_800_002_400)
        for evidence in ("confirmed_not_created", "confirmed_expiry_precondition_not_created"):
            with self.subTest(evidence=evidence):
                decision = checkout_outcome(evidence)
                self.assertEqual(decision.action, "release")
                self.assertFalse(decision.retry_same_attempt)
                self.assertEqual(decision.completion_reason, evidence)
        for evidence in ("timeout", "network_error", "provider_5xx", "provider_429", "ambiguous"):
            with self.subTest(evidence=evidence):
                decision = checkout_outcome(evidence)
                self.assertEqual(decision.action, "hold")
                self.assertTrue(decision.retry_same_attempt)
                self.assertTrue(decision.requires_reconciliation)
        self.assertEqual(checkout_outcome("confirmed_created").action, "hold")

        self.assertEqual(reconciliation_outcome("paid", 10).action, "commit")
        self.assertEqual(reconciliation_outcome("paid", 10).completion_reason, "canonical_paid")
        self.assertEqual(
            reconciliation_outcome("terminal_unpaid", 10).completion_reason,
            "canonical_terminal_unpaid",
        )
        self.assertEqual(
            reconciliation_outcome("not_created", 10).completion_reason,
            "canonical_not_created",
        )
        for status in ("pending", "unknown", "lookup_failure"):
            decision = reconciliation_outcome(status, 10)
            self.assertEqual((decision.action, decision.next_reconcile_at), ("hold", 310))
        for invalid in ("refund", "raw-stripe-status", "", None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                reconciliation_outcome(invalid, 10)


if __name__ == "__main__":
    unittest.main()
