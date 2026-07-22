from contextlib import redirect_stdout
from io import StringIO
import json
import unittest


class MetricsTests(unittest.TestCase):
    def test_emf_metrics_contain_only_closed_operational_fields(self):
        from src.common.metrics import emit_metric

        for metric_name in (
            "StaleReservations",
            "MigrationBacklog",
            "MigrationFailures",
            "ProviderFailures",
            "TestLiveMismatch",
        ):
            with self.subTest(metric_name=metric_name):
                output = StringIO()
                with redirect_stdout(output):
                    emit_metric(metric_name, 2, environment="test")
                payload = json.loads(output.getvalue())
                self.assertEqual(payload["Environment"], "test")
                self.assertEqual(payload[metric_name], 2)
                self.assertEqual(
                    payload["_aws"]["CloudWatchMetrics"][0]["Namespace"],
                    "Zoolanding/Commerce",
                )
                rendered = output.getvalue().lower()
                for forbidden in (
                    "email",
                    "account",
                    "payload",
                    "secret",
                    "token",
                ):
                    self.assertNotIn(forbidden, rendered)

    def test_emf_metric_rejects_unknown_names_values_and_environments(self):
        from src.common.metrics import emit_metric

        for args in (
            ("UnknownMetric", 1, "test"),
            ("StaleReservations", -1, "test"),
            ("StaleReservations", 1.5, "test"),
            ("StaleReservations", 1, "dev"),
            ("StaleReservations", 1, "prod"),
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                emit_metric(args[0], args[1], environment=args[2])


if __name__ == "__main__":
    unittest.main()
