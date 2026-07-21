"""Authenticated provider-neutral subscription command boundary."""

from __future__ import annotations

import hashlib
import time
from typing import Any
from urllib.parse import urlsplit

try:  # Lambda CodeUri is src/.
    from subscription_storage import SubscriptionCommandStore
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
    from integrations_gateway import (
        GatewayConfigurationError,
        IntegrationsConflict,
        IntegrationsUnavailable,
        InternalIntegrationsGateway,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.subscription_storage import SubscriptionCommandStore
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
    from src.integrations_gateway import (
        GatewayConfigurationError,
        IntegrationsConflict,
        IntegrationsUnavailable,
        InternalIntegrationsGateway,
    )


PATH = "/features/commerce/subscription/action"
CAPABILITY = "commerce:subscription:manage"
OPERATIONS = frozenset({
    "changePlan",
    "applyDiscount",
    "removeDiscount",
    "pause",
    "resume",
    "openPortal",
})


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
    scope = resolved_scope(policies)
    command_input = _command_input(
        operation,
        input_value,
        commerce,
        scope=scope,
        idempotency_key=idempotency_key,
    )
    result = _execute_gateway(
        operation,
        scope,
        command_input,
        connection_id=safe_id(commerce.get("payments", {}).get("bindingId")),
        idempotency_key=idempotency_key,
        request_id=request_id,
        actor_hash=hashlib.sha256(context.subject.encode("utf-8")).hexdigest(),
    )
    validated_result = _validated_gateway_result(result, operation)
    if operation in {"pause", "resume"} and validated_result["status"] == "accepted":
        _subscription_command_store().apply_access_transition(
            scope,
            command_input,
            command_id=validated_result["commandId"],
            idempotency_key=idempotency_key,
            now_epoch=int(time.time()),
        )
    return validated_result


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
    elif operation == "removeDiscount":
        item = closed_object(value, {"subscriptionId", "expectedRevision"})
    elif operation == "openPortal":
        item = closed_object(value, {"subscriptionId"})
    else:
        item = closed_object(value, {"subscriptionId", "expectedRevision"})
    safe_id(item["subscriptionId"])
    if operation != "openPortal":
        positive_int(item["expectedRevision"])
    return item


def _command_input(
    operation: str,
    input_value: dict[str, Any],
    commerce: dict[str, Any],
    *,
    scope: Any,
    idempotency_key: str,
) -> dict[str, Any]:
    payments = commerce.get("payments")
    if not isinstance(payments, dict) or payments.get("subscriptions") is not True:
        raise _forbidden()
    if operation in {"applyDiscount", "removeDiscount"} and payments.get("coupons") is not True:
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
            command_input["previewTimestamp"] = _subscription_command_store().preview_timestamp(
                scope,
                operation,
                command_input,
                idempotency_key=idempotency_key,
                now_epoch=preview_timestamp,
            )
    elif operation == "applyDiscount":
        command_input["action"] = "apply"
    elif operation == "removeDiscount":
        command_input["action"] = "remove"
    elif operation in {"pause", "resume"}:
        policy = validated_pause_policy(payments.get("pausePolicy"))
        if policy["enabled"] is not True:
            raise _forbidden()
        command_input["action"] = "pause" if operation == "pause" else "resume"
        command_input["pausePolicy"] = policy
    return command_input


def _forbidden() -> HttpError:
    return HttpError(403, "forbidden", "You do not have access to this resource.")


def _validated_gateway_result(value: Any, operation: str) -> dict[str, Any]:
    if operation == "openPortal" and isinstance(value, dict) and set(value) == {
        "commandId", "status", "redirectUrl", "expiresAt"
    }:
        try:
            command_id = safe_id(value.get("commandId"))
        except HttpError:
            raise _unavailable() from None
        try:
            redirect = urlsplit(value.get("redirectUrl"))
            redirect_port = redirect.port
        except (TypeError, ValueError):
            raise _unavailable() from None
        if (
            value.get("status") != "accepted"
            or redirect.scheme != "https"
            or redirect.hostname != "billing.stripe.com"
            or redirect_port not in {None, 443}
            or redirect.username is not None
            or redirect.password is not None
            or type(value.get("expiresAt")) is not int
            or value["expiresAt"] <= int(time.time())
        ):
            raise _unavailable()
        return {
            "commandId": command_id,
            "status": "accepted",
            "redirectUrl": value["redirectUrl"],
            "expiresAt": value["expiresAt"],
        }
    if not isinstance(value, dict) or set(value) != {"commandId", "status"}:
        raise _unavailable()
    if operation == "openPortal" and value.get("status") == "accepted":
        raise _unavailable()
    try:
        command_id = safe_id(value.get("commandId"))
    except HttpError:
        raise _unavailable() from None
    status = value.get("status")
    if status not in {"accepted", "pending", "needs_review"}:
        raise _unavailable()
    return {"commandId": command_id, "status": status}


def _unavailable() -> HttpError:
    return HttpError(
        503,
        "upstream_unavailable",
        "Service temporarily unavailable.",
        retryable=True,
    )


def _gateway() -> Any:
    try:
        return InternalIntegrationsGateway.from_environment()
    except (GatewayConfigurationError, IntegrationsUnavailable):
        return UnavailableSubscriptionGateway()


def _execute_gateway(*args: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return _gateway().execute(*args, **kwargs)
    except IntegrationsConflict:
        raise HttpError(409, "conflict", "Request conflicts with current state.") from None
    except (GatewayConfigurationError, IntegrationsUnavailable):
        raise _unavailable() from None


def _subscription_command_store() -> SubscriptionCommandStore:
    return SubscriptionCommandStore.from_environment()
