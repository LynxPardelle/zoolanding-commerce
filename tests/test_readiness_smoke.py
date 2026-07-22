import json
import os
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


def environment():
    return {
        "ZLP_COMMERCE_SMOKE_API_URL": (
            "https://abcdefghij.execute-api.us-east-1.amazonaws.com/test"
        ),
        "ZLP_COMMERCE_SMOKE_DOMAIN": "example.com",
        "AWS_REGION": "us-east-1",
    }


class ReadinessSmokeTests(unittest.TestCase):
    def smoke(self):
        from tools import commerce_readiness_smoke

        return commerce_readiness_smoke

    def test_classifies_success_auth_configuration_provider_and_propagation(self):
        smoke = self.smoke()
        cases = (
            (200, {}, "ready", True),
            (403, {}, "auth_failure", False),
            (400, {}, "configuration_failure", False),
            (502, {}, "provider_failure", False),
            (
                404,
                {"ZLP_COMMERCE_SMOKE_PROPAGATION_UNTIL_EPOCH": "1800000030"},
                "propagation_delay",
                False,
            ),
            (404, {}, "configuration_failure", False),
        )
        for status, extra, classification, ok in cases:
            with self.subTest(status=status, classification=classification):
                result = smoke.run(
                    {**environment(), **extra},
                    sender=lambda request, value=status: smoke.SmokeResponse(value),
                    clock=lambda: 1_800_000_000,
                )
                self.assertEqual(result["classification"], classification)
                self.assertEqual(result["ok"], ok)
                self.assertEqual(result.get("httpStatus"), status)
                self.assertEqual(result["environment"], "test")
                self.assertEqual(result["observedAtEpoch"], 1_800_000_000)
                self.assertTrue(
                    set(result).issubset(
                        {
                            "ok",
                            "classification",
                            "httpStatus",
                            "attempts",
                            "environment",
                            "observedAtEpoch",
                        }
                    )
                )

    def test_missing_or_malformed_input_fails_before_transport(self):
        smoke = self.smoke()
        for values in (
            {key: value for key, value in environment().items() if key != "ZLP_COMMERCE_SMOKE_DOMAIN"},
            {
                **environment(),
                "ZLP_COMMERCE_SMOKE_API_URL": (
                    "https://abcdefghij.execute-api.us-east-1.amazonaws.com:invalid/test"
                ),
            },
        ):
            called = []
            self.assertEqual(
                smoke.run(
                    values,
                    sender=lambda request: called.append(request),
                    clock=lambda: 1_800_000_000,
                ),
                {
                    "ok": False,
                    "classification": "missing_input",
                    "attempts": 0,
                    "environment": None,
                    "observedAtEpoch": 1_800_000_000,
                },
            )
            self.assertEqual(called, [])

    def test_request_is_exact_safe_and_never_contains_credentials(self):
        smoke = self.smoke()
        captured = []
        values = {
            **environment(),
            "AWS_ACCESS_KEY_ID": "DO-NOT-PRINT-ACCESS",
            "AWS_SECRET_ACCESS_KEY": "DO-NOT-PRINT-SECRET",
            "AWS_SESSION_TOKEN": "DO-NOT-PRINT-TOKEN",
        }
        result = smoke.run(
            values,
            sender=lambda request: captured.append(request) or smoke.SmokeResponse(200),
            clock=lambda: 1_800_000_000,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(len(captured), 1)
        request = captured[0]
        self.assertEqual(
            request.url,
            "https://abcdefghij.execute-api.us-east-1.amazonaws.com/test/"
            "features/commerce/public-read",
        )
        self.assertEqual(request.region, "us-east-1")
        self.assertEqual(request.headers, {"x-zlp-domain": "example.com"})
        self.assertEqual(
            request.payload,
            {"operation": "offerList", "input": {"limit": 1}},
        )
        rendered = json.dumps({"result": result, "request": request.payload})
        for secret in (
            values["AWS_ACCESS_KEY_ID"],
            values["AWS_SECRET_ACCESS_KEY"],
            values["AWS_SESSION_TOKEN"],
        ):
            self.assertNotIn(secret, rendered)

    def test_transport_and_invalid_deadline_fail_closed(self):
        smoke = self.smoke()
        self.assertEqual(
            smoke.run(
                environment(),
                sender=lambda request: object(),
                clock=lambda: 1_800_000_000,
            ),
            {
                "ok": False,
                "classification": "provider_failure",
                "attempts": 1,
                "environment": "test",
                "observedAtEpoch": 1_800_000_000,
            },
        )
        result = smoke.run(
            {
                **environment(),
                "ZLP_COMMERCE_SMOKE_PROPAGATION_UNTIL_EPOCH": "9" * 10_000,
            },
            sender=lambda request: smoke.SmokeResponse(404),
            clock=lambda: 1_800_000_000,
        )
        self.assertEqual(result["classification"], "configuration_failure")

    def test_environment_comes_only_from_validated_url_stage_and_clock_runs_once(self):
        smoke = self.smoke()
        values = {
            **environment(),
            "ZLP_COMMERCE_SMOKE_API_URL": (
                "https://abcdefghij.execute-api.us-east-1.amazonaws.com/production"
            ),
            "ENVIRONMENT_NAME": "test",
        }
        clock_calls = []

        def clock():
            clock_calls.append(True)
            if len(clock_calls) > 1:
                raise AssertionError("clock called more than once")
            return 1_800_000_000

        result = smoke.run(
            values,
            sender=lambda request: smoke.SmokeResponse(404),
            clock=clock,
        )

        self.assertEqual(result["environment"], "production")
        self.assertEqual(result["observedAtEpoch"], 1_800_000_000)
        self.assertEqual(clock_calls, [True])

    def test_invalid_clock_is_rejected_before_transport(self):
        smoke = self.smoke()
        called = []
        for invalid in (True, 1.5, "1800000000", -1):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                smoke.run(
                    environment(),
                    sender=lambda request: called.append(request),
                    clock=lambda value=invalid: value,
                )
        self.assertEqual(called, [])

    def test_cli_emits_only_redacted_missing_input_result(self):
        values = dict(os.environ)
        values.pop("ZLP_COMMERCE_SMOKE_API_URL", None)
        values["AWS_SECRET_ACCESS_KEY"] = "DO-NOT-PRINT-SECRET"
        result = subprocess.run(
            [sys.executable, str(ROOT / "tools" / "commerce_readiness_smoke.py")],
            cwd=ROOT,
            env=values,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["attempts"], 0)
        self.assertEqual(payload["classification"], "missing_input")
        self.assertIs(payload["ok"], False)
        self.assertIsNone(payload["environment"])
        self.assertIs(type(payload["observedAtEpoch"]), int)
        self.assertGreaterEqual(payload["observedAtEpoch"], 0)
        self.assertNotIn("DO-NOT-PRINT-SECRET", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
