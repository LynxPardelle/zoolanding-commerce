"""Relay the single approved Commerce outbox event to a fixed SNS topic."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping

try:
    from storage import CommerceScope, CommerceStore, StorageConflict
except ModuleNotFoundError:
    from src.storage import CommerceScope, CommerceStore, StorageConflict


_SNS_ARN = re.compile(r"arn:(?:aws|aws-us-gov|aws-cn):sns:[a-z0-9-]+:\d{12}:[A-Za-z0-9_.-]{1,256}")
_PENDING_KEYS = frozenset(
    {
        "pk",
        "sk",
        "itemType",
        "environment",
        "tenantId",
        "draftId",
        "domain",
        "schemaVersion",
        "eventId",
        "eventType",
        "sourceEventId",
        "payload",
        "deliveryStatus",
        "revision",
        "createdAt",
        "requestId",
        "correlationId",
    }
)
_DELIVERED_KEYS = _PENDING_KEYS | {"deliveredAt", "expiresAt"}


class OutboxRelay:
    def __init__(
        self,
        store: CommerceStore,
        publisher: Any,
        topic_arn: str,
        expected_environment: str,
    ) -> None:
        if type(store) is not CommerceStore:
            raise ValueError("store must be a CommerceStore")
        if not hasattr(publisher, "publish"):
            raise ValueError("publisher must expose publish")
        if type(topic_arn) is not str or _SNS_ARN.fullmatch(topic_arn) is None:
            raise ValueError("notification topic ARN is invalid")
        if expected_environment not in {"test", "production"}:
            raise ValueError("expected environment is invalid")
        self.store = store
        self.publisher = publisher
        self.topic_arn = topic_arn
        self.expected_environment = expected_environment

    def relay(self, record_image: object, *, now_epoch: int) -> dict[str, Any]:
        if (
            not isinstance(record_image, Mapping)
            or set(record_image) not in {_PENDING_KEYS, _DELIVERED_KEYS}
        ):
            raise StorageConflict("outbox stream image is invalid")
        try:
            scope = CommerceScope(
                record_image["environment"],
                record_image["tenantId"],
                record_image["draftId"],
                record_image["domain"],
            )
            if scope.environment != self.expected_environment:
                raise ValueError
            event_id = record_image["eventId"]
            current = self.store.get_outbox(scope, event_id)
        except (KeyError, TypeError, ValueError):
            raise StorageConflict("outbox stream image is invalid") from None
        if current["deliveryStatus"] == "delivered":
            return current
        if current != dict(record_image):
            raise StorageConflict("outbox stream image does not match current state")
        message = {
            "schemaVersion": 1,
            "eventId": current["eventId"],
            "eventType": "notification.requested.v1",
            "occurredAt": current["createdAt"],
            "environment": scope.environment,
            "tenantId": scope.tenant_id,
            "draftId": scope.draft_id,
            "domain": scope.domain,
            "data": current["payload"],
        }
        self.publisher.publish(
            TopicArn=self.topic_arn,
            Message=json.dumps(
                message,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ),
        )
        return self.store.mark_outbox_delivered(scope, event_id, now_epoch=now_epoch)
