"""Scheduled entrypoint for reservation reconciliation."""

from __future__ import annotations

import json
import os
import time
from typing import Any

try:
    from common.metrics import emit_metric
    from common.published_policy import resolve_commerce_policy
    from reconciliation import (
        MINIMUM_REMAINING_TIME_MS,
        RECONCILIATION_WORK_LIMIT,
        ReservationReconciler,
    )
    from integrations_gateway import InternalIntegrationsGateway
    from storage import CommerceStore
except ModuleNotFoundError:
    from src.common.metrics import emit_metric
    from src.common.published_policy import resolve_commerce_policy
    from src.reconciliation import (
        MINIMUM_REMAINING_TIME_MS,
        RECONCILIATION_WORK_LIMIT,
        ReservationReconciler,
    )
    from src.integrations_gateway import InternalIntegrationsGateway
    from src.storage import CommerceStore


_RECONCILER: ReservationReconciler | None = None


def lambda_handler(_event: object, context: Any) -> dict[str, int]:
    environment = os.environ.get("ENVIRONMENT_NAME", "").strip().lower()
    if environment not in {"test", "production"}:
        raise RuntimeError("Commerce environment is unavailable")
    remaining_time_ms = getattr(context, "get_remaining_time_in_millis", None)
    if not callable(remaining_time_ms):
        raise RuntimeError("Lambda time budget is unavailable")
    result = _reconciler_from_environment().run(
        environment=environment,
        now_epoch=int(time.time()),
        remaining_time_ms=remaining_time_ms,
    )
    print(json.dumps({
        "environment": environment,
        "counters": result,
        "budget": {
            "workLimit": RECONCILIATION_WORK_LIMIT,
            "minimumRemainingTimeMs": MINIMUM_REMAINING_TIME_MS,
        },
    }, sort_keys=True, separators=(",", ":"), allow_nan=False))
    try:
        emit_metric(
            "StaleReservations",
            result["deferred"] + result["failed"],
            environment=environment,
        )
    except Exception:
        pass
    return result


def _reconciler_from_environment() -> ReservationReconciler:
    global _RECONCILER
    if _RECONCILER is None:
        _RECONCILER = ReservationReconciler(
            CommerceStore.from_environment(),
            resolve_commerce_policy,
            InternalIntegrationsGateway.from_environment(),
        )
    return _RECONCILER
