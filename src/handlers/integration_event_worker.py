"""SQS partial-batch consumer for normalized Commerce integration events."""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Mapping

try:
    from events import IntegrationEventProcessor, parse_integration_event
    from common.metrics import emit_metric
    from storage import CommerceStore
    from subscription_storage import SubscriptionProjectionStore
    from migration_storage import MigrationRequestStore
except ModuleNotFoundError:
    from src.events import IntegrationEventProcessor, parse_integration_event
    from src.common.metrics import emit_metric
    from src.storage import CommerceStore
    from src.subscription_storage import SubscriptionProjectionStore
    from src.migration_storage import MigrationRequestStore


MAX_EVENT_BYTES = 32 * 1024
_PROCESSOR: IntegrationEventProcessor | None = None


class _DuplicateKey(ValueError):
    pass


def process_batch(
    event: object,
    processor: IntegrationEventProcessor,
    *,
    now_epoch: int,
    expected_environment: str | None = None,
    metric_emitter: Callable[..., None] | None = None,
) -> dict[str, list[dict[str, str]]]:
    if not isinstance(event, Mapping) or type(event.get("Records")) is not list:
        raise ValueError("SQS batch is invalid")
    records = event["Records"]
    if not 1 <= len(records) <= 10:
        raise ValueError("SQS batch must contain 1 to 10 records")
    if expected_environment is not None and expected_environment not in {"test", "production"}:
        raise ValueError("expected environment is invalid")
    identifiers = [_message_id(record) for record in records]
    failures = []
    for record, message_id in zip(records, identifiers):
        try:
            raw = record.get("body")
            if type(raw) is not str or not 1 <= len(raw.encode("utf-8")) <= MAX_EVENT_BYTES:
                raise ValueError
            value = json.loads(raw, object_pairs_hook=_unique_object)
            parsed = parse_integration_event(value)
            if expected_environment is not None and parsed.scope.environment != expected_environment:
                _safe_emit(
                    metric_emitter,
                    "TestLiveMismatch",
                    1,
                    environment=expected_environment,
                )
                raise ValueError("integration event environment mismatch")
            processor.process(parsed, now_epoch=now_epoch)
            _emit_migration_metrics(
                parsed,
                metric_emitter,
                environment=expected_environment,
            )
        except Exception:
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}


def lambda_handler(event: object, _context: Any) -> dict[str, list[dict[str, str]]]:
    return process_batch(
        event,
        _processor_from_environment(),
        now_epoch=int(time.time()),
        expected_environment=runtime_environment(os.getenv("ENVIRONMENT_NAME")),
        metric_emitter=emit_metric,
    )


def runtime_environment(value: object) -> str:
    if value == "test":
        return "test"
    if value == "production":
        return "production"
    raise RuntimeError("runtime environment is unavailable")


def _emit_migration_metrics(
    event: Any,
    metric_emitter: Callable[..., None] | None,
    *,
    environment: str | None,
) -> None:
    if environment is None or not event.event_type.startswith("migration."):
        return
    data = event.data
    counts = data.get("counts") if isinstance(data, Mapping) else None
    if isinstance(counts, Mapping):
        _safe_emit(
            metric_emitter,
            "MigrationBacklog",
            counts["pending"],
            environment=environment,
        )
        _safe_emit(
            metric_emitter,
            "MigrationFailures",
            counts["needsReview"] + counts["failed"],
            environment=environment,
        )
    elif event.event_type == "migration.item_needs_review.v1":
        _safe_emit(
            metric_emitter,
            "MigrationFailures",
            1,
            environment=environment,
        )


def _safe_emit(
    metric_emitter: Callable[..., None] | None,
    name: str,
    value: int,
    *,
    environment: str,
) -> None:
    if metric_emitter is None:
        return
    try:
        metric_emitter(name, value, environment=environment)
    except Exception:
        pass


def _processor_from_environment() -> IntegrationEventProcessor:
    global _PROCESSOR
    if _PROCESSOR is None:
        _PROCESSOR = IntegrationEventProcessor(
            CommerceStore.from_environment(),
            SubscriptionProjectionStore.from_environment(),
            MigrationRequestStore.from_environment(),
        )
    return _PROCESSOR


def _message_id(record: object) -> str:
    if not isinstance(record, Mapping):
        raise ValueError("SQS record is invalid")
    value = record.get("messageId")
    if type(value) is not str or not 1 <= len(value) <= 80 or any(ord(char) < 33 for char in value):
        raise ValueError("SQS message ID is invalid")
    return value


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey
        result[key] = value
    return result
