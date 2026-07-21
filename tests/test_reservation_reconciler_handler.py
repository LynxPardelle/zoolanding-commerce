from contextlib import redirect_stdout
import io
import json
import os
from unittest import mock
import unittest

from src.handlers import reservation_reconciler


class Reconciler:
    def __init__(self):
        self.calls = []

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return {"processed": 1, "committed": 0, "released": 0, "deferred": 1, "failed": 0}


class Context:
    def get_remaining_time_in_millis(self):
        return 8_000


class ReservationReconcilerHandlerTests(unittest.TestCase):
    def setUp(self):
        self.original = reservation_reconciler._RECONCILER
        self.reconciler = Reconciler()
        reservation_reconciler._RECONCILER = self.reconciler

    def tearDown(self):
        reservation_reconciler._RECONCILER = self.original

    def test_handler_normalizes_prod_and_uses_server_time(self):
        output = io.StringIO()
        with mock.patch.dict(os.environ, {"ENVIRONMENT_NAME": "prod"}, clear=False), mock.patch.object(
            reservation_reconciler.time,
            "time",
            return_value=1_900_000_000.9,
        ), redirect_stdout(output):
            result = reservation_reconciler.lambda_handler(
                {"draftId": "must-not-be-logged", "email": "private@example.test"},
                Context(),
            )

        self.assertEqual(result["processed"], 1)
        self.assertEqual(len(self.reconciler.calls), 1)
        call = self.reconciler.calls[0]
        remaining_time_ms = call.pop("remaining_time_ms")
        self.assertEqual(remaining_time_ms(), 8_000)
        self.assertEqual(call, {"environment": "production", "now_epoch": 1_900_000_000})
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        log = json.loads(lines[0])
        self.assertEqual(set(log), {"environment", "counters", "budget"})
        self.assertEqual(log["environment"], "production")
        self.assertEqual(log["counters"], result)
        self.assertEqual(log["budget"], {"workLimit": 25, "minimumRemainingTimeMs": 1_500})
        self.assertNotIn("must-not-be-logged", lines[0])
        self.assertNotIn("private@example.test", lines[0])

    def test_handler_fails_closed_for_an_unconfigured_environment(self):
        with mock.patch.dict(os.environ, {"ENVIRONMENT_NAME": "dev"}, clear=False):
            with self.assertRaises(RuntimeError):
                reservation_reconciler.lambda_handler({}, Context())

        self.assertEqual(self.reconciler.calls, [])


if __name__ == "__main__":
    unittest.main()
