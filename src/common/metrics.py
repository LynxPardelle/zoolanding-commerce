"""Closed, PII-free CloudWatch Embedded Metric Format helpers."""

from __future__ import annotations

import json
import time


_METRICS = {
    "StaleReservations": "Count",
    "MigrationBacklog": "Count",
    "MigrationFailures": "Count",
    "ProviderFailures": "Count",
    "TestLiveMismatch": "Count",
}
_ENVIRONMENTS = {"test", "production"}


def emit_metric(name: str, value: int, *, environment: str) -> None:
    if name not in _METRICS:
        raise ValueError("Unsupported metric")
    if type(value) is not int or value < 0:
        raise ValueError("Invalid metric value")
    if environment not in _ENVIRONMENTS:
        raise ValueError("Invalid metric environment")
    payload = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "Zoolanding/Commerce",
                    "Dimensions": [["Environment"]],
                    "Metrics": [{"Name": name, "Unit": _METRICS[name]}],
                }
            ],
        },
        "Environment": environment,
        name: value,
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
