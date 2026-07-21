"""Protected, CSRF-checked catalog mutations."""

from __future__ import annotations

import hashlib
import time
from typing import Any

try:
    from catalog_storage import CatalogStore
    from common.auth_admin import authorize_request
    from common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        idempotency_header,
        nonnegative_int,
        positive_int,
        resolved_scope,
        safe_id,
        supported_currencies,
        validated_commerce,
        validation_error,
    )
    from common.published_policy import resolve_policies
    from domain.catalog import CatalogItem, CatalogVariant, DataSpaceRecordReference
    from domain.offers import DiscountVersion, Money, OfferRecurrence, OfferVersion
    from integrations_gateway import (
        GatewayConfigurationError,
        IntegrationsConflict,
        IntegrationsUnavailable,
        InternalIntegrationsGateway,
    )
except ModuleNotFoundError:
    from src.catalog_storage import CatalogStore
    from src.common.auth_admin import authorize_request
    from src.common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        idempotency_header,
        nonnegative_int,
        positive_int,
        resolved_scope,
        safe_id,
        supported_currencies,
        validated_commerce,
        validation_error,
    )
    from src.common.published_policy import resolve_policies
    from src.domain.catalog import CatalogItem, CatalogVariant, DataSpaceRecordReference
    from src.domain.offers import DiscountVersion, Money, OfferRecurrence, OfferVersion
    from src.integrations_gateway import (
        GatewayConfigurationError,
        IntegrationsConflict,
        IntegrationsUnavailable,
        InternalIntegrationsGateway,
    )


PATH = "/features/commerce/catalog/action"
OPERATIONS = frozenset({
    "createItem",
    "createOfferVersion",
    "createDiscountVersion",
    "advanceOfferLifecycle",
    "updateOfferPresentation",
    "advanceDiscountLifecycle",
    "updateDiscountPresentation",
})


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    return dispatch(event, PATH, lambda payload, request_id: _handle(event, payload, request_id))


def _handle(event: dict[str, Any], payload: dict[str, Any], request_id: str) -> dict[str, Any]:
    request = closed_object(payload, {"operation", "input"})
    operation = request["operation"]
    if not isinstance(operation, str) or operation not in OPERATIONS:
        raise validation_error()
    input_value = _validated_input(operation, request["input"])
    idempotency_key = idempotency_header(event)

    policies = resolve_policies(domain_header(event))
    commerce = validated_commerce(policies)
    currencies = supported_currencies(commerce)
    context = authorize_request(
        event=event,
        policies=policies,
        capability="commerce:catalog:write",
        mutation=True,
    )
    metadata = {
        "idempotency_key": idempotency_key,
        "request_id": request_id,
        "correlation_id": request_id,
        "actor_hash": hashlib.sha256(context.subject.encode("utf-8")).hexdigest(),
        "now_epoch": int(time.time()),
    }
    scope = resolved_scope(policies)
    store = _store()
    connection_id = safe_id(commerce.get("payments", {}).get("bindingId"))

    if operation == "createItem":
        if input_value["sellableType"] not in commerce.get("sellableTypes", []):
            raise validation_error()
        return store.create_item(scope, _catalog_item(input_value), **metadata)
    if operation == "createOfferVersion":
        if input_value["sellableType"] not in commerce.get("sellableTypes", []):
            raise validation_error()
        payments = commerce.get("payments", {})
        recurring = input_value.get("recurrence") is not None
        if (recurring and payments.get("subscriptions") is not True) or (
            not recurring and payments.get("oneTime") is not True
        ):
            raise validation_error()
        return store.create_offer(
            scope,
            _offer(input_value, currencies),
            supported_currencies=currencies,
            **metadata,
        )
    if operation == "createDiscountVersion":
        if commerce.get("payments", {}).get("coupons") is not True:
            raise validation_error()
        return store.create_discount(
            scope,
            _discount(input_value, currencies),
            supported_currencies=currencies,
            **metadata,
        )
    if operation == "advanceOfferLifecycle":
        current = store.get_offer_version(
            scope, input_value["versionId"], currencies
        )
        if current.lifecycle_revision != input_value["expectedRevision"]:
            raise _conflict()
        try:
            updated = current.with_lifecycle(
                input_value["targetState"], input_value["expectedRevision"] + 1
            )
        except ValueError:
            raise _conflict() from None
        if updated.lifecycle_state == "active":
            gateway = _configured_gateway()
            result = _gateway_call(
                gateway.provision_offer,
                scope,
                connection_id,
                updated,
            )
            if result["status"] != "accepted":
                return result
            if updated.display_name is not None:
                result = _gateway_call(
                    gateway.update_offer_presentation,
                    scope,
                    connection_id,
                    updated,
                )
                if result["status"] != "accepted":
                    return result
        elif updated.lifecycle_state == "retired":
            gateway = _configured_gateway()
            result = _gateway_call(
                gateway.deactivate_offer,
                scope,
                connection_id,
                updated.version_id,
                updated.lifecycle_revision,
            )
            if result["status"] != "accepted":
                return result
        return store.advance_offer_lifecycle(
            scope,
            input_value["versionId"],
            input_value["targetState"],
            input_value["expectedRevision"],
            currencies,
            **metadata,
        )
    if operation == "updateOfferPresentation":
        current = store.get_offer_version(
            scope, input_value["versionId"], currencies
        )
        if current.presentation_revision != input_value["expectedRevision"]:
            raise _conflict()
        try:
            updated = current.with_presentation(
                input_value["expectedRevision"] + 1,
                display_name=input_value.get("displayName"),
                display_description=input_value.get("displayDescription"),
            )
        except ValueError:
            raise _conflict() from None
        if current.lifecycle_state in {"active", "existing_only"}:
            gateway = _configured_gateway()
            result = _gateway_call(
                gateway.update_offer_presentation,
                scope,
                connection_id,
                updated,
            )
            if result["status"] != "accepted":
                return result
        elif current.lifecycle_state == "retired":
            raise _conflict()
        return store.update_offer_presentation(
            scope,
            input_value["versionId"],
            input_value["expectedRevision"],
            currencies,
            display_name=input_value.get("displayName"),
            display_description=input_value.get("displayDescription"),
            **metadata,
        )
    if operation == "advanceDiscountLifecycle":
        current = store.get_discount_version(
            scope, input_value["versionId"], currencies
        )
        if current.lifecycle_revision != input_value["expectedRevision"]:
            raise _conflict()
        try:
            updated = current.with_lifecycle(
                input_value["targetState"], input_value["expectedRevision"] + 1
            )
        except ValueError:
            raise _conflict() from None
        if updated.lifecycle_state == "active":
            gateway = _configured_gateway()
            result = _gateway_call(
                gateway.provision_discount,
                scope,
                connection_id,
                updated,
            )
            if result["status"] != "accepted":
                return result
        elif updated.lifecycle_state in {"existing_only", "retired"}:
            gateway = _configured_gateway()
            result = _gateway_call(
                gateway.update_discount_lifecycle,
                scope,
                connection_id,
                updated,
            )
            if result["status"] != "accepted":
                return result
        return store.advance_discount_lifecycle(
            scope,
            input_value["versionId"],
            input_value["targetState"],
            input_value["expectedRevision"],
            currencies,
            **metadata,
        )
    return store.update_discount_presentation(
        scope,
        input_value["versionId"],
        input_value["expectedRevision"],
        currencies,
        display_name=input_value.get("displayName"),
        display_description=input_value.get("displayDescription"),
        **metadata,
    )


def _validated_input(operation: str, value: Any) -> dict[str, Any]:
    if operation == "createItem":
        item = closed_object(
            value,
            {"itemId", "sellableType"},
            {"variants", "dataSpaceReference"},
        )
        safe_id(item["itemId"])
        if not isinstance(item["sellableType"], str):
            raise validation_error()
        variants = item.get("variants", [])
        if not isinstance(variants, list) or len(variants) > 100:
            raise validation_error()
        for variant in variants:
            parsed = closed_object(variant, {"variantId", "sku"})
            safe_id(parsed["variantId"])
            if not isinstance(parsed["sku"], str) or not 1 <= len(parsed["sku"]) <= 128:
                raise validation_error()
        if "dataSpaceReference" in item:
            _data_space_reference(item["dataSpaceReference"])
        return item
    if operation == "createOfferVersion":
        item = closed_object(
            value,
            {"versionId", "catalogItemId", "revision", "sellableType", "unitPrice", "taxBehavior"},
            {"variantId", "recurrence", "displayName", "displayDescription"},
        )
        safe_id(item["versionId"])
        safe_id(item["catalogItemId"])
        if item.get("variantId") is not None:
            safe_id(item["variantId"])
        positive_int(item["revision"])
        _money(item["unitPrice"], allow_zero=True)
        if item.get("recurrence") is not None:
            recurrence = closed_object(item["recurrence"], {"interval"}, {"intervalCount"})
            if recurrence["interval"] not in {"month", "year"}:
                raise validation_error()
            if recurrence.get("intervalCount", 1) != 1:
                raise validation_error()
        _display_fields(item)
        return item
    if operation == "createDiscountVersion":
        item = closed_object(
            value,
            {"versionId", "revision", "duration"},
            {
                "percentageBasisPoints", "fixedAmount", "durationInMonths",
                "eligibleOfferVersionIds", "redemptionLimit", "redeemByEpoch",
                "customerFacingCode", "displayName", "displayDescription",
            },
        )
        safe_id(item["versionId"])
        positive_int(item["revision"])
        has_percentage = "percentageBasisPoints" in item
        has_fixed = "fixedAmount" in item
        if has_percentage == has_fixed:
            raise validation_error()
        if has_percentage:
            basis_points = item["percentageBasisPoints"]
            if not isinstance(basis_points, int) or isinstance(basis_points, bool) or not 1 <= basis_points <= 10_000:
                raise validation_error()
        if has_fixed:
            _money(item["fixedAmount"], allow_zero=False)
        for key in ("durationInMonths", "redemptionLimit", "redeemByEpoch"):
            if key in item:
                positive_int(item[key])
        eligible = item.get("eligibleOfferVersionIds", [])
        if not isinstance(eligible, list) or len(eligible) > 200:
            raise validation_error()
        for offer_id in eligible:
            safe_id(offer_id)
        if len(set(eligible)) != len(eligible):
            raise validation_error()
        if item.get("customerFacingCode") is not None and not isinstance(item["customerFacingCode"], str):
            raise validation_error()
        _display_fields(item)
        return item
    if operation in {"advanceOfferLifecycle", "advanceDiscountLifecycle"}:
        item = closed_object(value, {"versionId", "targetState", "expectedRevision"})
        safe_id(item["versionId"])
        if item["targetState"] not in {"provisioning", "active", "existing_only", "retired"}:
            raise validation_error()
        positive_int(item["expectedRevision"])
        return item
    item = closed_object(
        value,
        {"versionId", "expectedRevision"},
        {"displayName", "displayDescription"},
    )
    safe_id(item["versionId"])
    positive_int(item["expectedRevision"])
    _display_fields(item)
    return item


def _catalog_item(value: dict[str, Any]) -> CatalogItem:
    return CatalogItem(
        value["itemId"],
        value["sellableType"],
        tuple(CatalogVariant(item["variantId"], item["sku"]) for item in value.get("variants", [])),
        _data_space_reference(value.get("dataSpaceReference")) if value.get("dataSpaceReference") is not None else None,
    )


def _offer(
    value: dict[str, Any],
    supported: frozenset[str],
) -> OfferVersion:
    recurrence = value.get("recurrence")
    return OfferVersion(
        version_id=value["versionId"],
        catalog_item_id=value["catalogItemId"],
        variant_id=value.get("variantId"),
        revision=value["revision"],
        sellable_type=value["sellableType"],
        unit_price=_money(value["unitPrice"], allow_zero=True, supported=supported),
        tax_behavior=value["taxBehavior"],
        recurrence=None if recurrence is None else OfferRecurrence(
            recurrence["interval"], recurrence.get("intervalCount", 1)
        ),
        display_name=value.get("displayName"),
        display_description=value.get("displayDescription"),
    )


def _discount(
    value: dict[str, Any],
    supported: frozenset[str],
) -> DiscountVersion:
    fixed = value.get("fixedAmount")
    return DiscountVersion(
        version_id=value["versionId"],
        revision=value["revision"],
        duration=value["duration"],
        percentage_basis_points=value.get("percentageBasisPoints"),
        fixed_amount=None if fixed is None else _money(
            fixed, allow_zero=False, supported=supported
        ),
        duration_in_months=value.get("durationInMonths"),
        eligible_offer_version_ids=frozenset(value.get("eligibleOfferVersionIds", [])),
        redemption_limit=value.get("redemptionLimit"),
        redeem_by_epoch=value.get("redeemByEpoch"),
        customer_facing_code=value.get("customerFacingCode"),
        display_name=value.get("displayName"),
        display_description=value.get("displayDescription"),
    )


def _money(
    value: Any,
    *,
    allow_zero: bool,
    supported: frozenset[str] | None = None,
) -> Money:
    item = closed_object(value, {"amountMinor", "currency"})
    amount = nonnegative_int(item["amountMinor"])
    if not allow_zero and amount == 0:
        raise validation_error()
    currency = item["currency"]
    if not isinstance(currency, str):
        raise validation_error()
    return Money(amount, currency, supported or frozenset({currency}))


def _data_space_reference(value: Any) -> DataSpaceRecordReference:
    item = closed_object(
        value,
        {"spaceId", "collectionId", "recordId", "revision", "fieldIds"},
    )
    for key in ("spaceId", "collectionId", "recordId"):
        safe_id(item[key])
    positive_int(item["revision"])
    field_ids = item["fieldIds"]
    if not isinstance(field_ids, list) or not 1 <= len(field_ids) <= 200:
        raise validation_error()
    for field_id in field_ids:
        safe_id(field_id)
    if len(set(field_ids)) != len(field_ids):
        raise validation_error()
    return DataSpaceRecordReference(
        item["spaceId"], item["collectionId"], item["recordId"], item["revision"], tuple(field_ids)
    )


def _display_fields(value: dict[str, Any]) -> None:
    for key in ("displayName", "displayDescription"):
        if key in value and value[key] is not None and not isinstance(value[key], str):
            raise validation_error()


def _store() -> CatalogStore:
    return CatalogStore.from_environment(mutations=True)


def _gateway() -> InternalIntegrationsGateway:
    return InternalIntegrationsGateway.from_environment()


def _configured_gateway() -> InternalIntegrationsGateway:
    try:
        return _gateway()
    except GatewayConfigurationError:
        raise HttpError(
            503,
            "upstream_unavailable",
            "Service temporarily unavailable.",
            retryable=True,
        ) from None


def _gateway_call(callback: Any, *args: Any) -> dict[str, Any]:
    try:
        return callback(*args)
    except IntegrationsConflict:
        raise _conflict() from None
    except (IntegrationsUnavailable, GatewayConfigurationError):
        raise HttpError(
            503,
            "upstream_unavailable",
            "Service temporarily unavailable.",
            retryable=True,
        ) from None


def _conflict() -> HttpError:
    return HttpError(409, "conflict", "Request conflicts with current state.")
