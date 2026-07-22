"""Public provider-neutral Checkout admission."""

from __future__ import annotations

import hashlib
import os
import time
from typing import Any

try:
    from catalog_storage import CatalogStore
    from common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
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
    from integrations_gateway import (
        GatewayConfigurationError,
        IntegrationsConflict,
        IntegrationsUnavailable,
        InternalIntegrationsGateway,
        canonical_hash,
    )
    from storage import CommerceScope, CommerceStore
except ModuleNotFoundError:
    from src.catalog_storage import CatalogStore
    from src.common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
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
    from src.integrations_gateway import (
        GatewayConfigurationError,
        IntegrationsConflict,
        IntegrationsUnavailable,
        InternalIntegrationsGateway,
        canonical_hash,
    )
    from src.storage import CommerceScope, CommerceStore


PATH = "/features/commerce/public-action"
OWNED_TEST_PREVIEW_ORIGIN = "https://test.zoolandingpage.com.mx"
_NOTIFICATION_TYPE_TEMPLATES = {
    "payment-failed": "payment-failed-v1",
    "payment-succeeded": "payment-succeeded-v1",
}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    return dispatch(event, PATH, lambda payload, request_id: _handle(event, payload, request_id))


def _handle(event: dict[str, Any], payload: dict[str, Any], request_id: str) -> dict[str, Any]:
    request = closed_object(payload, {"operation", "input"})
    if request["operation"] != "admitCheckout":
        raise validation_error()
    input_value = closed_object(
        request["input"], {"lines"}, {"discountVersionId"}
    )
    discount_version_id = input_value.get("discountVersionId")
    if discount_version_id is not None:
        safe_id(discount_version_id)
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
    _validate_origin_binding(event, domain)
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
    recurring_count = 0
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
            recurring_count += 1
        stock_id = None
        if offer.sellable_type == "physical":
            contains_physical = True
            if inventory.get("enabled") is not True or inventory.get("tracked") is not True:
                raise validation_error()
            stock_id = _stock_id(item.item_id, offer.variant_id)
        resolved_lines.append((offer, request_line["quantity"], stock_id))
    if recurring_count > 1 or (contains_physical and recurring_count):
        raise validation_error()
    if recurring_count == 1:
        resolved_lines.sort(key=lambda entry: entry[0].recurrence is None)

    discount = None
    if discount_version_id is not None:
        if payments.get("coupons") is not True:
            raise validation_error()
        discount = catalog.get_checkout_discount(
            scope,
            discount_version_id,
            currency_set,
        )
        intended_offers = {offer.version_id for offer, _quantity, _stock in resolved_lines}
        if discount.eligible_offer_version_ids and not intended_offers.issubset(
            discount.eligible_offer_version_ids
        ):
            raise validation_error()

    tax_policy = _checkout_tax_policy(payments)
    shipping_policy = _checkout_shipping_policy(
        commerce.get("shipping"), contains_physical
    )

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
    command_input = {
        "orderId": result["orderId"],
        "paymentAttemptId": result["paymentAttemptId"],
        "revision": 1,
        "reservationIds": [result["reservationId"]],
        "checkoutExpiresAt": result["checkoutExpiresAt"],
        "offerBindings": [
            {
                "offerVersionId": offer.version_id,
                "revision": offer.revision,
                "quantity": quantity,
                "sellableType": offer.sellable_type,
                "snapshot": offer.provider_snapshot(),
                "contentHash": canonical_hash({
                    "schemaVersion": 1,
                    "snapshot": offer.provider_snapshot(),
                }),
            }
            for offer, quantity, _stock_id_value in resolved_lines
        ],
        "taxPolicy": tax_policy,
        "shippingPolicy": shipping_policy,
        "paymentCollection": "immediate_card_link",
    }
    if discount is not None:
        command_input["discountVersionId"] = discount.version_id
    gateway_result = _create_checkout(
        scope,
        safe_id(payments.get("bindingId")),
        command_input,
    )
    if (
        gateway_result.get("status") == "accepted"
        and gateway_result.get("expiresAt") != result["checkoutExpiresAt"]
    ):
        raise HttpError(
            503,
            "upstream_unavailable",
            "Service temporarily unavailable.",
            retryable=True,
        )
    response = dict(gateway_result)
    if (
        response.get("status") == "accepted"
        and fiscal_access is not None
        and stored_hash == fiscal_access["proofHash"]
    ):
        response["fiscalAccessProof"] = fiscal_proof
        response["fiscalAccessState"] = "pending_payment"
    return response


def _validate_origin_binding(event: dict[str, Any], domain: str) -> None:
    environment = os.getenv("ENVIRONMENT_NAME", "").strip().lower()
    origin = _single_origin(event)
    query = _single_preview_query(event)
    if environment == "production":
        if origin != f"https://{domain}" or query:
            raise _origin_forbidden()
        return
    if environment != "test":
        raise policy_unavailable()
    configured_origin = os.getenv("TEST_PREVIEW_ORIGIN", "").strip()
    if configured_origin != OWNED_TEST_PREVIEW_ORIGIN:
        raise policy_unavailable()
    if origin != configured_origin or query != {"draftDomain": domain}:
        raise _origin_forbidden()


def _single_origin(event: dict[str, Any]) -> str:
    headers = event.get("headers")
    if headers is None:
        headers = {}
    if not isinstance(headers, dict):
        raise _origin_forbidden()
    singular = [
        value
        for key, value in headers.items()
        if type(key) is str and key.lower() == "origin"
    ]
    if len(singular) > 1 or any(type(value) is not str for value in singular):
        raise _origin_forbidden()
    singular_value = singular[0].strip() if singular else None

    multi_headers = event.get("multiValueHeaders")
    if multi_headers is None:
        multi_headers = {}
    if not isinstance(multi_headers, dict):
        raise _origin_forbidden()
    multi = [
        value
        for key, value in multi_headers.items()
        if type(key) is str and key.lower() == "origin"
    ]
    if len(multi) > 1:
        raise _origin_forbidden()
    multi_value = None
    if multi:
        values = multi[0]
        if (
            not isinstance(values, list)
            or len(values) != 1
            or type(values[0]) is not str
        ):
            raise _origin_forbidden()
        multi_value = values[0].strip()
    if singular_value is not None and multi_value is not None and singular_value != multi_value:
        raise _origin_forbidden()
    return singular_value if singular_value is not None else multi_value or ""


def _single_preview_query(event: dict[str, Any]) -> dict[str, str]:
    query = event.get("queryStringParameters")
    if query is None:
        query = {}
    multi_query = event.get("multiValueQueryStringParameters")
    if multi_query is None:
        multi_query = {}
    if not isinstance(query, dict) or not isinstance(multi_query, dict):
        raise _origin_forbidden()
    if set(query) - {"draftDomain"} or set(multi_query) - {"draftDomain"}:
        raise _origin_forbidden()
    if any(type(key) is not str for key in query) or any(type(key) is not str for key in multi_query):
        raise _origin_forbidden()

    singular_value = query.get("draftDomain")
    if singular_value is not None and type(singular_value) is not str:
        raise _origin_forbidden()
    multi_value = None
    if "draftDomain" in multi_query:
        values = multi_query["draftDomain"]
        if (
            not isinstance(values, list)
            or len(values) != 1
            or type(values[0]) is not str
        ):
            raise _origin_forbidden()
        multi_value = values[0]
    if singular_value is not None and multi_value is not None and singular_value != multi_value:
        raise _origin_forbidden()
    selected = singular_value if singular_value is not None else multi_value
    return {} if selected is None else {"draftDomain": selected}


def _origin_forbidden() -> HttpError:
    return HttpError(403, "forbidden", "You do not have access to this resource.")


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
    notification_types = policy.get("notificationTypes")
    template_ids = policy.get("templateIds")
    if (
        not isinstance(notification_types, list)
        or not isinstance(template_ids, list)
        or not 1 <= len(notification_types) <= len(_NOTIFICATION_TYPE_TEMPLATES)
        or len(set(notification_types)) != len(notification_types)
        or any(value not in _NOTIFICATION_TYPE_TEMPLATES for value in notification_types)
    ):
        raise policy_unavailable()
    type_templates = {
        value: _NOTIFICATION_TYPE_TEMPLATES[value]
        for value in sorted(notification_types)
    }
    if len(template_ids) != len(type_templates) or set(template_ids) != set(type_templates.values()):
        raise policy_unavailable()
    recipient_sets = policy.get("recipientSets")
    if not isinstance(recipient_sets, list) or len(recipient_sets) != 1:
        raise policy_unavailable()
    recipient_set = recipient_sets[0]
    members = recipient_set.get("members") if isinstance(recipient_set, dict) else None
    if not isinstance(members, list) or len(members) != 1 or not isinstance(members[0], dict):
        raise policy_unavailable()
    try:
        return {
            "notificationPolicyId": safe_id(policy.get("id")),
            "publishedVersionId": policies.version_id,
            "recipientSetId": safe_id(recipient_set.get("id")),
            "recipientSetVersion": positive_int(recipient_set.get("version")),
            "recipientMemberId": safe_id(members[0].get("id")),
            "notificationTypeTemplates": type_templates,
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


def _gateway() -> InternalIntegrationsGateway:
    return InternalIntegrationsGateway.from_environment()


def _create_checkout(
    scope: CommerceScope,
    connection_id: str,
    command_input: dict[str, Any],
) -> dict[str, Any]:
    try:
        return _gateway().create_checkout(scope, connection_id, command_input)
    except IntegrationsConflict:
        raise HttpError(
            409,
            "conflict",
            "Request conflicts with current state.",
        ) from None
    except (IntegrationsUnavailable, GatewayConfigurationError):
        raise HttpError(
            503,
            "upstream_unavailable",
            "Service temporarily unavailable.",
            retryable=True,
        ) from None


def _checkout_tax_policy(payments: Any) -> dict[str, str]:
    if not isinstance(payments, dict):
        raise policy_unavailable()
    policy = payments.get("taxPolicy", {"mode": "disabled"})
    if (
        not isinstance(policy, dict)
        or set(policy) != {"mode"}
        or policy.get("mode") not in {"disabled", "automatic"}
    ):
        raise policy_unavailable()
    return {"mode": policy["mode"]}


def _checkout_shipping_policy(
    shipping: Any,
    contains_physical: bool,
) -> dict[str, Any]:
    if not contains_physical:
        return {"collection": "none"}
    if not isinstance(shipping, dict):
        raise policy_unavailable()
    countries = shipping.get("allowedCountries")
    if (
        shipping.get("enabled") is not True
        or not isinstance(countries, list)
        or not 1 <= len(countries) <= 50
        or len(set(countries)) != len(countries)
        or any(
            type(country) is not str
            or len(country) != 2
            or not country.isascii()
            or not country.isupper()
            for country in countries
        )
    ):
        raise policy_unavailable()
    return {"collection": "required", "allowedCountries": list(countries)}
