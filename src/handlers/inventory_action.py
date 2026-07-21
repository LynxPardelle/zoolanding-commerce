"""Protected inventory adjustments."""

from __future__ import annotations

import hashlib
import time
from typing import Any

try:
    from common.auth_admin import authorize_request
    from common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        idempotency_header,
        nonnegative_int,
        resolved_scope,
        safe_id,
        validated_commerce,
        validation_error,
    )
    from common.published_policy import resolve_policies
    from storage import CommerceStore
except ModuleNotFoundError:
    from src.common.auth_admin import authorize_request
    from src.common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        idempotency_header,
        nonnegative_int,
        resolved_scope,
        safe_id,
        validated_commerce,
        validation_error,
    )
    from src.common.published_policy import resolve_policies
    from src.storage import CommerceStore


PATH = "/features/commerce/inventory/action"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    return dispatch(event, PATH, lambda payload, request_id: _handle(event, payload, request_id))


def _handle(event: dict[str, Any], payload: dict[str, Any], request_id: str) -> dict[str, Any]:
    request = closed_object(payload, {"operation", "input"})
    if request["operation"] != "adjustStock":
        raise validation_error()
    input_value = closed_object(request["input"], {"stockId", "delta", "expectedRevision"})
    stock_id = safe_id(input_value["stockId"])
    delta = input_value["delta"]
    if not isinstance(delta, int) or isinstance(delta, bool) or delta == 0 or abs(delta) > 1_000_000_000:
        raise validation_error()
    expected_revision = nonnegative_int(input_value["expectedRevision"])
    idempotency_key = idempotency_header(event)

    policies = resolve_policies(domain_header(event))
    commerce = validated_commerce(policies)
    inventory = commerce.get("inventory")
    if not isinstance(inventory, dict) or inventory.get("enabled") is not True or inventory.get("tracked") is not True:
        raise HttpError(404, "not_found", "Resource not found.")
    location_id = safe_id(inventory.get("locationId"))
    context = authorize_request(
        event=event,
        policies=policies,
        capability="commerce:inventory:write",
        mutation=True,
    )
    now = int(time.time())
    return _store().adjust_stock(
        resolved_scope(policies),
        stock_id,
        delta,
        expected_revision,
        location_id=location_id,
        idempotency_key=idempotency_key,
        request_id=request_id,
        correlation_id=request_id,
        actor_hash=hashlib.sha256(context.subject.encode("utf-8")).hexdigest(),
        now_epoch=now,
    )


def _store() -> CommerceStore:
    return CommerceStore.from_environment()
