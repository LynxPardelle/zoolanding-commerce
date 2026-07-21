"""Public provider-neutral Checkout admission."""

from __future__ import annotations

import hashlib
import time
from typing import Any

try:
    from catalog_storage import CatalogStore
    from common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        header,
        positive_int,
        public_checkout_idempotency_header,
        resolved_scope,
        safe_id,
        supported_currencies,
        validated_commerce,
        policy_unavailable,
        validation_error,
    )
    from common.published_policy import resolve_checkout_policy
    from domain.orders import CheckoutLine, MAX_CHECKOUT_LINE_QUANTITY, PendingOrder
    from fiscal_storage import (
        FiscalCaptureDisabled,
        fiscal_request_window_seconds,
        new_fiscal_access_proof,
    )
    from storage import CommerceScope, CommerceStore
except ModuleNotFoundError:
    from src.catalog_storage import CatalogStore
    from src.common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        header,
        positive_int,
        public_checkout_idempotency_header,
        resolved_scope,
        safe_id,
        supported_currencies,
        validated_commerce,
        policy_unavailable,
        validation_error,
    )
    from src.common.published_policy import resolve_checkout_policy
    from src.domain.orders import CheckoutLine, MAX_CHECKOUT_LINE_QUANTITY, PendingOrder
    from src.fiscal_storage import (
        FiscalCaptureDisabled,
        fiscal_request_window_seconds,
        new_fiscal_access_proof,
    )
    from src.storage import CommerceScope, CommerceStore


PATH = "/features/commerce/public-action"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    return dispatch(event, PATH, lambda payload, request_id: _handle(event, payload, request_id))


def _handle(event: dict[str, Any], payload: dict[str, Any], request_id: str) -> dict[str, Any]:
    request = closed_object(payload, {"operation", "input"})
    if request["operation"] != "admitCheckout":
        raise validation_error()
    input_value = closed_object(request["input"], {"lines"})
    raw_lines = input_value["lines"]
    if not isinstance(raw_lines, list) or not 1 <= len(raw_lines) <= 20:
        raise validation_error()
    parsed_lines: list[dict[str, Any]] = []
    for raw_line in raw_lines:
        line = closed_object(raw_line, {"offerVersionId", "quantity"})
        quantity = positive_int(line["quantity"])
        if quantity > MAX_CHECKOUT_LINE_QUANTITY:
            raise validation_error()
        parsed_lines.append({
            "offerVersionId": safe_id(line["offerVersionId"]),
            "quantity": quantity,
        })
    if len({line["offerVersionId"] for line in parsed_lines}) != len(parsed_lines):
        raise validation_error()
    idempotency_key = public_checkout_idempotency_header(event)

    domain = domain_header(event)
    if header(event, "origin") != f"https://{domain}":
        raise HttpError(403, "forbidden", "You do not have access to this resource.")
    policies = resolve_checkout_policy(domain)
    commerce = validated_commerce(policies)
    scope = resolved_scope(policies)
    catalog = _catalog_store()
    currency_set = supported_currencies(commerce)
    payments = commerce.get("payments") if isinstance(commerce.get("payments"), dict) else {}
    inventory = commerce.get("inventory") if isinstance(commerce.get("inventory"), dict) else {}
    sellable_types = set(commerce.get("sellableTypes", []))

    resolved_lines: list[tuple[Any, int, str | None]] = []
    contains_physical = False
    contains_recurring = False
    for index, request_line in enumerate(parsed_lines):
        offer, item = catalog.get_checkout_offer(
            scope,
            request_line["offerVersionId"],
            currency_set,
        )
        if offer.unit_price.currency not in currency_set:
            raise validation_error()
        if offer.sellable_type not in sellable_types or item.sellable_type != offer.sellable_type:
            raise validation_error()
        if offer.recurrence is None:
            if payments.get("oneTime") is not True:
                raise validation_error()
        else:
            if payments.get("subscriptions") is not True:
                raise validation_error()
            contains_recurring = True
        stock_id = None
        if offer.sellable_type == "physical":
            contains_physical = True
            if inventory.get("enabled") is not True or inventory.get("tracked") is not True:
                raise validation_error()
            stock_id = _stock_id(item.item_id, offer.variant_id)
        resolved_lines.append((offer, request_line["quantity"], stock_id))
    if contains_physical and contains_recurring:
        raise validation_error()

    order_id = _new_id("order", scope, idempotency_key)
    payment_attempt_id = _new_id("attempt", scope, idempotency_key)
    reservation_id = _new_id("reservation", scope, idempotency_key)
    lines = [
        CheckoutLine(
            _new_id("line", scope, idempotency_key, index),
            offer.version_id,
            quantity,
            offer.unit_price,
            stock_id,
        )
        for index, (offer, quantity, stock_id) in enumerate(resolved_lines)
    ]
    order = PendingOrder(order_id, payment_attempt_id, tuple(lines))
    location_id = safe_id(inventory.get("locationId"))
    now = int(time.time())
    fiscal_access, fiscal_proof = _fiscal_access(commerce, policies.environment)
    result = _commerce_store().reserve_checkout(
        scope,
        order,
        reservation_id,
        location_id=location_id,
        created_at_epoch=now,
        idempotency_key=idempotency_key,
        request_id=request_id,
        correlation_id=request_id,
        actor_hash=None,
        now_epoch=now,
        notification_target=_notification_target(policies, commerce),
        fiscal_access=fiscal_access,
    )
    result = dict(result)
    stored_hash = result.pop("fiscalAccessHash", None)
    if fiscal_access is not None and stored_hash == fiscal_access["proofHash"]:
        result["fiscalAccessProof"] = fiscal_proof
        result["fiscalAccessState"] = "pending_payment"
    return result


def _new_id(prefix: str, scope: CommerceScope, idempotency_key: str, index: int | None = None) -> str:
    suffix = "" if index is None else f"\0{index}"
    digest = hashlib.sha256(
        f"{scope.partition_key}\0{scope.domain}\0{idempotency_key}\0{prefix}{suffix}".encode("utf-8")
    ).hexdigest()[:40]
    return f"{prefix}-{digest}"


def _stock_id(item_id: str, variant_id: str | None) -> str:
    value = item_id if variant_id is None else f"{item_id}.{variant_id}"
    return value if len(value) <= 64 else hashlib.sha256(value.encode("utf-8")).hexdigest()


def _notification_target(policies: Any, commerce: dict[str, Any]) -> dict[str, Any] | None:
    policy_ids = commerce.get("notificationPolicyIds", [])
    if not policy_ids:
        return None
    if not isinstance(policy_ids, list) or len(policy_ids) != 1:
        raise policy_unavailable()
    descriptor = policies.notification_policies
    if (
        not isinstance(descriptor, dict)
        or descriptor.get("version") != 1
        or descriptor.get("scope") != policies.scope
        or not isinstance(descriptor.get("policies"), list)
    ):
        raise policy_unavailable()
    matches = [
        policy for policy in descriptor["policies"]
        if isinstance(policy, dict) and policy.get("id") == policy_ids[0]
    ]
    if len(matches) != 1:
        raise policy_unavailable()
    policy = matches[0]
    if policy.get("status") == "disabled":
        return None
    recipient_sets = policy.get("recipientSets")
    if not isinstance(recipient_sets, list) or len(recipient_sets) != 1:
        raise policy_unavailable()
    recipient_set = recipient_sets[0]
    members = recipient_set.get("members") if isinstance(recipient_set, dict) else None
    if not isinstance(members, list) or len(members) != 1 or not isinstance(members[0], dict):
        raise policy_unavailable()
    try:
        return {
            "publishedVersionId": policies.version_id,
            "recipientSetId": safe_id(recipient_set.get("id")),
            "recipientSetVersion": positive_int(recipient_set.get("version")),
            "recipientMemberId": safe_id(members[0].get("id")),
        }
    except Exception:
        raise policy_unavailable() from None


def _fiscal_access(
    commerce: dict[str, Any],
    environment: str,
) -> tuple[dict[str, Any] | None, str | None]:
    fiscal = commerce.get("fiscal")
    if not isinstance(fiscal, dict) or fiscal.get("enabled") is not True:
        return None, None
    try:
        window_seconds = fiscal_request_window_seconds(fiscal, environment, {})
    except FiscalCaptureDisabled:
        raise policy_unavailable() from None
    token, proof_hash = new_fiscal_access_proof()
    return {"proofHash": proof_hash, "windowSeconds": window_seconds}, token


def _catalog_store() -> CatalogStore:
    return CatalogStore.from_environment()


def _commerce_store() -> CommerceStore:
    return CommerceStore.from_environment()
