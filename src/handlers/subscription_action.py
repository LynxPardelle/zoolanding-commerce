"""Authenticated provider-neutral subscription command boundary."""

from __future__ import annotations

import hashlib
import re
import time
from typing import Any
from urllib.parse import urlsplit

try:  # Lambda CodeUri is src/.
    from subscription_storage import SubscriptionCommandStore
    from common.auth_admin import authorize_request, require_session_cookie
    from common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        idempotency_header,
        positive_int,
        resolved_scope,
        safe_id,
        supported_currencies,
        validated_commerce,
        validation_error,
    )
    from common.published_policy import (
        resolve_policies,
        validated_migration_policy,
        validated_pause_policy,
        validated_plan_change_policy,
    )
    from integrations_gateway import (
        canonical_hash,
        GatewayConfigurationError,
        IntegrationsConflict,
        IntegrationsUnavailable,
        InternalIntegrationsGateway,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.subscription_storage import SubscriptionCommandStore
    from src.common.auth_admin import authorize_request, require_session_cookie
    from src.common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        idempotency_header,
        positive_int,
        resolved_scope,
        safe_id,
        supported_currencies,
        validated_commerce,
        validation_error,
    )
    from src.common.published_policy import (
        resolve_policies,
        validated_migration_policy,
        validated_pause_policy,
        validated_plan_change_policy,
    )
    from src.integrations_gateway import (
        canonical_hash,
        GatewayConfigurationError,
        IntegrationsConflict,
        IntegrationsUnavailable,
        InternalIntegrationsGateway,
    )


PATH = "/features/commerce/subscription/action"
CAPABILITY = "commerce:subscription:manage"
MIGRATION_CAPABILITY = "subscription:migration:execute"
OPERATIONS = frozenset({
    "changePlan",
    "applyDiscount",
    "removeDiscount",
    "pause",
    "resume",
    "openPortal",
    "migrationPreview",
    "migrationExecute",
    "migrationPause",
    "migrationResume",
    "migrationCancel",
    "migrationStatus",
})
MIGRATION_OPERATIONS = frozenset({
    "migrationPreview",
    "migrationExecute",
    "migrationPause",
    "migrationResume",
    "migrationCancel",
    "migrationStatus",
})
_HASH = re.compile(r"[a-f0-9]{64}", re.ASCII)
_MIGRATION_POLICY_MODES = {
    "next-renewal": "next_renewal",
    "immediate-prorated": "immediate_prorated",
}


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
    require_session_cookie(event)
    policies = resolve_policies(domain_header(event))
    commerce = validated_commerce(policies)
    idempotency_key = (
        None if operation == "migrationStatus" else idempotency_header(event)
    )
    bulk_migration = operation in MIGRATION_OPERATIONS
    context = authorize_request(
        event=event,
        policies=policies,
        capability=MIGRATION_CAPABILITY if bulk_migration else CAPABILITY,
        mutation=operation != "migrationStatus",
    )
    scope = resolved_scope(policies)
    if operation in MIGRATION_OPERATIONS:
        return _handle_migration(
            operation,
            input_value,
            commerce,
            scope=scope,
            idempotency_key=idempotency_key,
            request_id=request_id,
            actor_hash=_actor_hash(scope, context.subject),
        )
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
        actor_hash=_actor_hash(scope, context.subject),
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
    if operation == "migrationPreview":
        item = closed_object(
            value,
            {"sourceOfferVersionId", "targetOfferVersionId"},
        )
        safe_id(item["sourceOfferVersionId"])
        safe_id(item["targetOfferVersionId"])
        if item["sourceOfferVersionId"] == item["targetOfferVersionId"]:
            raise validation_error()
    elif operation == "migrationExecute":
        item = closed_object(
            value,
            {"commercialRequestId", "dryRunRevision", "dryRunHash", "confirmation"},
        )
        safe_id(item["commercialRequestId"])
        positive_int(item["dryRunRevision"])
        if (
            type(item["dryRunHash"]) is not str
            or _HASH.fullmatch(item["dryRunHash"]) is None
            or item["confirmation"] is not True
        ):
            raise validation_error()
    elif operation in {"migrationPause", "migrationResume", "migrationCancel"}:
        item = closed_object(value, {"commercialRequestId", "expectedRevision"})
        safe_id(item["commercialRequestId"])
        positive_int(item["expectedRevision"])
    elif operation == "migrationStatus":
        item = closed_object(value, {"commercialRequestId"}, {"limit", "cursor"})
        safe_id(item["commercialRequestId"])
        if "limit" in item and item["limit"] is not None:
            if type(item["limit"]) is not int or not 1 <= item["limit"] <= 100:
                raise validation_error()
        if "cursor" in item and item["cursor"] is not None:
            safe_id(item["cursor"])
    elif operation == "changePlan":
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
    if operation not in MIGRATION_OPERATIONS:
        safe_id(item["subscriptionId"])
        if operation != "openPortal":
            positive_int(item["expectedRevision"])
    return item


def _handle_migration(
    operation: str,
    input_value: dict[str, Any],
    commerce: dict[str, Any],
    *,
    scope: Any,
    idempotency_key: str | None,
    request_id: str,
    actor_hash: str,
) -> dict[str, Any]:
    payments = commerce.get("payments")
    if not isinstance(payments, dict) or payments.get("subscriptions") is not True:
        raise _forbidden()
    connection_id = safe_id(payments.get("bindingId"))
    now = int(time.time())
    store = _migration_store()
    command_request_hash = canonical_hash({
        "operation": operation,
        "input": input_value,
    })

    if operation in {
        "migrationExecute", "migrationPause", "migrationResume", "migrationCancel"
    }:
        replay = store.replay_command(
            scope,
            input_value["commercialRequestId"],
            operation=operation,
            idempotency_key=idempotency_key,
            request_hash=command_request_hash,
        )
        if replay is not None:
            return _migration_projection(replay)

    if operation == "migrationPreview":
        policy = validated_plan_change_policy(payments.get("planChangePolicy"))
        policy_mode = _MIGRATION_POLICY_MODES.get(policy["mode"])
        if policy_mode is None:
            raise _forbidden()
        currencies = supported_currencies(commerce)
        migration_policy = validated_migration_policy(payments.get("migrationPolicy"))
        catalog = _catalog_store()
        source = catalog.get_offer_version(
            scope, input_value["sourceOfferVersionId"], currencies
        )
        target = catalog.get_offer_version(
            scope, input_value["targetOfferVersionId"], currencies
        )
        source_binding = _offer_binding(source)
        target_binding = _offer_binding(target)
        stored = store.prepare_preview(
            scope,
            connection_id=connection_id,
            source_offer=source_binding,
            target_offer=target_binding,
            requested_policy={"mode": policy_mode},
            candidate_scope={"kind": "all_matching_source_price"},
            canary_size=migration_policy["canarySize"],
            account_concurrency=migration_policy["accountConcurrency"],
            actor_hash=actor_hash,
            idempotency_key=idempotency_key,
            request_id=request_id,
            now_epoch=now,
        )
        if stored.get("jobId") is not None:
            return _migration_projection(stored)
        command_input = {
            "commercialRequestId": stored["commercialRequestId"],
            "sourceOffer": source_binding,
            "targetOffer": target_binding,
            "requestedPolicy": {"mode": policy_mode},
            "candidateScope": {"kind": "all_matching_source_price"},
            "canarySize": migration_policy["canarySize"],
            "accountConcurrency": migration_policy["accountConcurrency"],
        }
    elif operation == "migrationExecute":
        stored = store.approve_execution(
            scope,
            input_value["commercialRequestId"],
            dry_run_revision=input_value["dryRunRevision"],
            dry_run_hash=input_value["dryRunHash"],
            actor_hash=actor_hash,
            idempotency_key=idempotency_key,
            now_epoch=now,
        )
        connection_id = _bound_connection(stored, connection_id)
        command_input = {
            "commercialRequestId": stored["commercialRequestId"],
            "jobId": _bound_job(stored),
            "dryRunRevision": input_value["dryRunRevision"],
            "dryRunHash": input_value["dryRunHash"],
            "confirmation": True,
        }
    elif operation in {"migrationPause", "migrationResume", "migrationCancel"}:
        action = {
            "migrationPause": "pause",
            "migrationResume": "resume",
            "migrationCancel": "cancel",
        }[operation]
        stored = store.prepare_control(
            scope,
            input_value["commercialRequestId"],
            action=action,
            expected_revision=input_value["expectedRevision"],
        )
        connection_id = _bound_connection(stored, connection_id)
        command_input = {
            "commercialRequestId": stored["commercialRequestId"],
            "jobId": _bound_job(stored),
            "expectedRevision": input_value["expectedRevision"],
            "action": action,
        }
    else:
        stored = store.get_request(scope, input_value["commercialRequestId"])
        connection_id = _bound_connection(stored, connection_id)
        command_input = {
            "commercialRequestId": stored["commercialRequestId"],
            "jobId": _bound_job(stored),
        }
        if input_value.get("limit") is not None:
            command_input["limit"] = input_value["limit"]
        if input_value.get("cursor") is not None:
            command_input["cursor"] = input_value["cursor"]

    result = _execute_gateway(
        operation,
        scope,
        command_input,
        connection_id=connection_id,
        idempotency_key=(
            idempotency_key
            if idempotency_key is not None
            else f"migration-status:{stored['commercialRequestId']}"
        ),
        request_id=request_id,
        actor_hash=actor_hash,
        expected_result_revision=(
            None if operation == "migrationStatus" else stored["revision"] + 1
        ),
    )
    if operation != "migrationStatus" and result.get("status") == "pending":
        # Integrations persisted the command but could not enqueue its work. Do not
        # create a local receipt: an exact retry lets Integrations replay the command
        # and retry its durable dispatch under the same technical idempotency key.
        raise _unavailable()
    if operation == "migrationStatus":
        try:
            from integrations_gateway import validate_migration_status_result
        except ModuleNotFoundError:
            from src.integrations_gateway import validate_migration_status_result
        status = validate_migration_status_result(
            result,
            commercial_request_id=stored["commercialRequestId"],
            job_id=_bound_job(stored),
            connection_id=connection_id,
        )
        return {
            **status,
            "commandStatus": _migration_command_status(stored),
        }
    recorded = store.record_command_result(
        scope,
        stored["commercialRequestId"],
        operation=operation,
        idempotency_key=idempotency_key,
        request_hash=command_request_hash,
        actor_hash=actor_hash,
        result=result,
        now_epoch=now,
    )
    return _migration_projection(recorded)


def _offer_binding(offer: Any) -> dict[str, Any]:
    if (
        getattr(offer, "sale_type", None) != "recurring"
        or getattr(offer, "sellable_type", None) != "subscription"
        or getattr(offer, "lifecycle_state", None) not in {"active", "existing_only"}
    ):
        raise validation_error()
    try:
        from integrations_gateway import canonical_hash
    except ModuleNotFoundError:
        from src.integrations_gateway import canonical_hash
    snapshot = offer.provider_snapshot()
    return {
        "offerVersionId": safe_id(offer.version_id),
        "revision": positive_int(offer.revision),
        "schemaVersion": 1,
        "snapshot": snapshot,
        "contentHash": canonical_hash({"schemaVersion": 1, "snapshot": snapshot}),
    }


def _bound_connection(stored: Any, expected_connection_id: str) -> str:
    if not isinstance(stored, dict) or stored.get("connectionId") != expected_connection_id:
        raise _unavailable()
    return expected_connection_id


def _bound_job(stored: Any) -> str:
    if not isinstance(stored, dict):
        raise _unavailable()
    try:
        return safe_id(stored.get("jobId"))
    except HttpError:
        raise _unavailable() from None


def _migration_command_status(stored: Any) -> str | None:
    if not isinstance(stored, dict):
        raise _unavailable()
    last_command = stored.get("lastCommand")
    if last_command is None:
        return None
    if not isinstance(last_command, dict) or set(last_command) != {
        "operation", "idempotencyDigest", "requestHash", "actorHash", "result"
    }:
        raise _unavailable()
    if (
        last_command.get("operation") not in MIGRATION_OPERATIONS - {"migrationStatus"}
        or type(last_command.get("idempotencyDigest")) is not str
        or _HASH.fullmatch(last_command["idempotencyDigest"]) is None
        or type(last_command.get("requestHash")) is not str
        or _HASH.fullmatch(last_command["requestHash"]) is None
        or type(last_command.get("actorHash")) is not str
        or _HASH.fullmatch(last_command["actorHash"]) is None
    ):
        raise _unavailable()
    result = last_command.get("result")
    if (
        not isinstance(result, dict)
        or set(result) != {"commandId", "status", "jobId", "revision"}
        or result.get("status") not in {"accepted", "needs_review"}
        or result.get("jobId") != stored.get("jobId")
    ):
        raise _unavailable()
    try:
        safe_id(result.get("commandId"))
        safe_id(result.get("jobId"))
        positive_int(result.get("revision"))
    except HttpError:
        raise _unavailable() from None
    return result["status"]


def _actor_hash(scope: Any, subject: str) -> str:
    value = "\0".join(
        (scope.environment, scope.tenant_id, scope.draft_id, scope.domain, subject)
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _migration_projection(value: Any) -> dict[str, Any]:
    try:
        from migration_storage import public_migration_request
    except ModuleNotFoundError:
        from src.migration_storage import public_migration_request
    try:
        return public_migration_request(value)
    except (KeyError, TypeError, ValueError):
        raise _unavailable() from None


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


def _catalog_store() -> Any:
    try:
        from catalog_storage import CatalogStore
    except ModuleNotFoundError:
        from src.catalog_storage import CatalogStore
    return CatalogStore.from_environment()


def _migration_store() -> Any:
    try:
        from migration_storage import MigrationRequestStore
    except ModuleNotFoundError:
        from src.migration_storage import MigrationRequestStore
    return MigrationRequestStore.from_environment()
