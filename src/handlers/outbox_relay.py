"""DynamoDB Streams partial-batch relay for Commerce outbox records."""

from __future__ import annotations

import os
import time
from typing import Any, Mapping

try:
    from outbox import OutboxRelay
    from storage import CommerceStore, decode_dynamodb_item
except ModuleNotFoundError:
    from src.outbox import OutboxRelay
    from src.storage import CommerceStore, decode_dynamodb_item


_RELAY: OutboxRelay | None = None


def process_batch(
    event: object,
    relay: OutboxRelay,
    *,
    now_epoch: int,
) -> dict[str, list[dict[str, str]]]:
    if not isinstance(event, Mapping) or type(event.get("Records")) is not list:
        raise ValueError("DynamoDB Streams batch is invalid")
    records = event["Records"]
    if not 1 <= len(records) <= 1_000:
        raise ValueError("DynamoDB Streams batch is invalid")
    identifiers = [_sequence_number(record) for record in records]
    failures = []
    for record, event_id in zip(records, identifiers):
        try:
            if record.get("eventName") not in {"INSERT", "MODIFY"}:
                continue
            dynamodb = record.get("dynamodb")
            if not isinstance(dynamodb, Mapping):
                raise ValueError
            relay.relay(decode_dynamodb_item(dynamodb.get("NewImage")), now_epoch=now_epoch)
        except Exception:
            failures.append({"itemIdentifier": event_id})
    return {"batchItemFailures": failures}


def lambda_handler(event: object, _context: Any) -> dict[str, list[dict[str, str]]]:
    return process_batch(event, _relay_from_environment(), now_epoch=int(time.time()))


def _relay_from_environment() -> OutboxRelay:
    global _RELAY
    if _RELAY is None:
        topic_arn = os.environ.get("NOTIFICATION_TOPIC_ARN", "").strip()
        if not topic_arn:
            raise RuntimeError("notification topic is unavailable")
        import boto3

        _RELAY = OutboxRelay(
            CommerceStore.from_environment(),
            boto3.client("sns"),
            topic_arn,
            runtime_environment(os.getenv("ENVIRONMENT_NAME")),
        )
    return _RELAY


def runtime_environment(value: object) -> str:
    if value == "test":
        return "test"
    if value == "production":
        return "production"
    raise RuntimeError("runtime environment is unavailable")


def _sequence_number(record: object) -> str:
    if not isinstance(record, Mapping):
        raise ValueError("DynamoDB Streams record is invalid")
    dynamodb = record.get("dynamodb")
    value = dynamodb.get("SequenceNumber") if isinstance(dynamodb, Mapping) else None
    if type(value) is not str or not 1 <= len(value) <= 128 or not value.isascii() or not value.isdigit():
        raise ValueError("DynamoDB Streams sequence number is invalid")
    return value
