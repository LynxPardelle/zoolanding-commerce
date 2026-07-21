"""Authenticated provider-neutral subscription command boundary."""

from __future__ import annotations

import hashlib
import time
from typing import Any

try:  # Lambda CodeUri is src/.
    from common.auth_admin import authorize_request
    from common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        idempotency_header,
        positive_int,
        resolved_scope,
        safe_id,
        validated_commerce,
        validation_error,
    )
    from common.published_policy import (
        resolve_policies,
        validated_pause_policy,
        validated_plan_change_policy,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.common.auth_admin import authorize_request
    from src.common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        idempotency_header,
        positive_int,
        resolved_scope,
        safe_id,
        validated_commerce,
        validation_error,
    )
    from src.common.published_policy import (
        resolve_policies,
        validated_pause_policy,
        validated_plan_change_policy,
    )


PATH = "/features/commerce/subscription/action"
CAPABILITY = "commerce:subscription:manage"
OPERATIONS = frozenset({"changePlan", "applyDiscount", "pause", "resume"})


class UnavailableSubscriptionGateway:
    def execute(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        raise HttpError(
            503,
            "upstream_unavailable",
            "Service temporarily unavailable.",
            retryable=True,
        )


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    return dispatch(event, PATH, lambda payload, request_id: _handle(event, payload, request_id))


def _handle(event: dict[str, Any], payload: dict[str, Any], request_id: str) -> dict[str, Any]:
    request = closed_object(payload, {"operation", "input"})
    operation = request["operation"]
    if type(operation) is not str or operation not in OPERATIONS:
        raise validation_error()
    input_value = _validated_input(operation, request["input"])
    idempotency_key = idempotency_header(event)
    policies = resolve_policies(domain_header(event))
    commerce = validated_commerce(policies)
    context = authorize_request(
        event=event,
        policies=policies,
        capability=CAPABILITY,
        mutation=True,
    )
    command_input = _command_input(operation, input_value, commerce)
    result = _gateway().execute(
        operation,
        resolved_scope(policies),
        command_input,
        idempotency_key=idempotency_key,
        request_id=request_id,
        actor_hash=hashlib.sha256(context.subject.encode("utf-8")).hexdigest(),
    )
    return _validated_gateway_result(result)


def _validated_input(operation: str, value: Any) -> dict[str, Any]:
    if operation == "changePlan":
        item = closed_object(
            value,
            {"subscriptionId", "targetOfferVersionId", "expectedRevision"},
        )
        safe_id(item["targetOfferVersionId"])
    elif operation == "applyDiscount":
        item = closed_object(
            value,
            {"subscriptionId", "discountVersionId", "expectedRevision"},
        )
        safe_id(item["discountVersionId"])
    else:
        item = closed_object(value, {"subscriptionId", "expectedRevision"})
    safe_id(item["subscriptionId"])
    positive_int(item["expectedRevision"])
    return item


def _command_input(
    operation: str,
    input_value: dict[str, Any],
    commerce: dict[str, Any],
) -> dict[str, Any]:
    payments = commerce.get("payments")
    if not isinstance(payments, dict) or payments.get("subscriptions") is not True:
        raise _forbidden()
    if operation == "applyDiscount" and payments.get("coupons") is not True:
        raise _forbidden()
    command_input = dict(input_value)
    if operation == "changePlan":
        policy = validated_plan_change_policy(payments.get("planChangePolicy"))
        if policy["mode"] == "disabled":
            raise _forbidden()
        command_input["planChangePolicy"] = policy
        if policy["mode"] == "immediate-prorated":
            preview_timestamp = int(time.time())
            if preview_timestamp < 1:
                raise HttpError(
                    503,
                    "upstream_unavailable",
                    "Service temporarily unavailable.",
                    retryable=True,
                )
            command_input["previewTimestamp"] = preview_timestamp
    elif operation in {"pause", "resume"}:
        policy = validated_pause_policy(payments.get("pausePolicy"))
        if policy["enabled"] is not True:
            raise _forbidden()
        command_input["pausePolicy"] = policy
    return command_input


def _forbidden() -> HttpError:
    return HttpError(403, "forbidden", "You do not have access to this resource.")


def _validated_gateway_result(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"commandId", "status"}:
        raise HttpError(503, "upstream_unavailable", "Service temporarily unavailable.", retryable=True)
    try:
        command_id = safe_id(value.get("commandId"))
    except HttpError:
        raise HttpError(503, "upstream_unavailable", "Service temporarily unavailable.", retryable=True) from None
    status = value.get("status")
    if status not in {"accepted", "pending"}:
        raise HttpError(503, "upstream_unavailable", "Service temporarily unavailable.", retryable=True)
    return {"commandId": command_id, "status": status}


def _gateway() -> UnavailableSubscriptionGateway:
    # TASK-040 replaces this fail-closed boundary with exact AWS_IAM commands.
    return UnavailableSubscriptionGateway()
