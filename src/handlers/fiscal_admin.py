"""Accountant-only manual fiscal request administration."""

from __future__ import annotations

import hashlib
import os
import time
from typing import Any

try:  # Lambda CodeUri is src/.
    from common.auth_admin import authorize_request, require_session_cookie
    from common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        policy_unavailable,
        positive_int,
        safe_id,
        validated_commerce,
        validation_error,
    )
    from common.published_policy import resolve_policies
    from fiscal_storage import (
        CORRECTION_REASONS,
        FiscalCaptureDisabled,
        FiscalScope,
        FiscalStore,
        fiscal_request_window_seconds,
        validate_fiscal_details,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.common.auth_admin import authorize_request, require_session_cookie
    from src.common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        policy_unavailable,
        positive_int,
        safe_id,
        validated_commerce,
        validation_error,
    )
    from src.common.published_policy import resolve_policies
    from src.fiscal_storage import (
        CORRECTION_REASONS,
        FiscalCaptureDisabled,
        FiscalScope,
        FiscalStore,
        fiscal_request_window_seconds,
        validate_fiscal_details,
    )


PATH = "/features/commerce/fiscal/admin"
CAPABILITY = "commerce:fiscal:manage"
OPERATIONS = frozenset({
    "getRequest",
    "correctRequest",
    "markNeedsCorrection",
    "markReady",
    "markDelivered",
    "cancel",
})


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    return dispatch(event, PATH, lambda payload, _request_id: _handle(event, payload))


def _handle(event: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    request = closed_object(payload, {"operation", "input"})
    operation = request["operation"]
    if type(operation) is not str or operation not in OPERATIONS:
        raise validation_error()
    input_value = _validated_input(operation, request["input"])
    require_session_cookie(event)
    policies = resolve_policies(domain_header(event))
    commerce = validated_commerce(policies)
    context = authorize_request(
        event=event,
        policies=policies,
        capability=CAPABILITY,
        mutation=operation != "getRequest",
    )
    fiscal = commerce.get("fiscal")
    if not isinstance(fiscal, dict) or fiscal.get("enabled") is not True:
        raise HttpError(404, "not_found", "Resource not found.")
    try:
        fiscal_request_window_seconds(fiscal, policies.environment, os.environ)
    except FiscalCaptureDisabled:
        raise policy_unavailable() from None
    scope = FiscalScope(
        policies.environment,
        policies.tenant_id,
        policies.draft_id,
        policies.domain,
    )
    store = _store()
    request_id = input_value["requestId"]
    if operation == "getRequest":
        return store.get_request(scope, request_id)
    metadata = {
        "expected_revision": input_value["expectedRevision"],
        "actor_hash": hashlib.sha256(context.subject.encode("utf-8")).hexdigest(),
        "now_epoch": int(time.time()),
    }
    if operation == "correctRequest":
        return store.correct_request(
            scope,
            request_id,
            input_value["details"],
            **metadata,
        )
    kwargs: dict[str, Any] = {}
    if operation == "markNeedsCorrection":
        kwargs["reason_code"] = input_value["reasonCode"]
    if operation == "markDelivered":
        kwargs["delivery_reference_id"] = input_value["deliveryReferenceId"]
    return store.transition_request(
        scope,
        request_id,
        operation,
        **metadata,
        **kwargs,
    )


def _validated_input(operation: str, value: Any) -> dict[str, Any]:
    if operation == "getRequest":
        item = closed_object(value, {"requestId"})
    elif operation == "correctRequest":
        item = closed_object(value, {"requestId", "expectedRevision", "details"})
        validate_fiscal_details(item["details"])
    elif operation == "markNeedsCorrection":
        item = closed_object(value, {"requestId", "expectedRevision", "reasonCode"})
        if type(item["reasonCode"]) is not str or item["reasonCode"] not in CORRECTION_REASONS:
            raise validation_error()
    elif operation == "markDelivered":
        item = closed_object(value, {"requestId", "expectedRevision", "deliveryReferenceId"})
        safe_id(item["deliveryReferenceId"])
    else:
        item = closed_object(value, {"requestId", "expectedRevision"})
    safe_id(item["requestId"])
    if "expectedRevision" in item:
        positive_int(item["expectedRevision"])
    return item


def _store() -> FiscalStore:
    return FiscalStore.from_environment()
