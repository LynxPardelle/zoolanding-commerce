"""Protected catalog reads."""

from __future__ import annotations

import os
from typing import Any

try:
    from catalog_storage import CatalogStore
    from common.auth_admin import authorize_request, require_session_cookie
    from common.http import (
        bounded_page_size,
        catalog_cursor_signing_key,
        closed_object,
        decode_catalog_cursor,
        dispatch,
        domain_header,
        encode_catalog_cursor,
        resolved_scope,
        safe_id,
        supported_currencies,
        validated_commerce,
        validation_error,
    )
    from common.published_policy import resolve_policies
except ModuleNotFoundError:
    from src.catalog_storage import CatalogStore
    from src.common.auth_admin import authorize_request, require_session_cookie
    from src.common.http import (
        bounded_page_size,
        catalog_cursor_signing_key,
        closed_object,
        decode_catalog_cursor,
        dispatch,
        domain_header,
        encode_catalog_cursor,
        resolved_scope,
        safe_id,
        supported_currencies,
        validated_commerce,
        validation_error,
    )
    from src.common.published_policy import resolve_policies


PATH = "/features/commerce/read"
KINDS = frozenset({"items", "offers", "discounts"})
OPERATIONS = frozenset({"itemList", "itemDetail", "offerList", "offerDetail", "discountList", "discountDetail"})


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    return dispatch(event, PATH, lambda payload, _request_id: _handle(event, payload))


def _handle(event: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    request = closed_object(payload, {"operation", "input"})
    operation = request["operation"]
    if not isinstance(operation, str) or operation not in OPERATIONS:
        raise validation_error()
    kind = {
        "itemList": "items", "itemDetail": "items",
        "offerList": "offers", "offerDetail": "offers",
        "discountList": "discounts", "discountDetail": "discounts",
    }[operation]
    if operation.endswith("List"):
        input_value = closed_object(request["input"], set(), {"limit", "cursor"})
    else:
        input_value = closed_object(request["input"], {"resourceId"})
        safe_id(input_value["resourceId"])

    require_session_cookie(event)
    policies = resolve_policies(domain_header(event))
    commerce = validated_commerce(policies)
    currencies = supported_currencies(commerce)
    authorize_request(
        event=event,
        policies=policies,
        capability="commerce:catalog:read",
        mutation=False,
    )
    scope = resolved_scope(policies)
    store = _store()
    if operation.endswith("Detail"):
        return {
            "item": store.get_catalog(
                scope, kind, input_value["resourceId"], currencies
            )
        }
    signing_key = catalog_cursor_signing_key(
        os.environ.get("COMMERCE_CURSOR_SIGNING_KEY")
    )
    cursor = decode_catalog_cursor(
        policies, scope, kind, input_value.get("cursor"), signing_key
    )
    items, next_cursor = store.list_catalog(
        scope,
        kind,
        bounded_page_size(input_value.get("limit")),
        cursor,
        currencies,
    )
    return {
        "items": items,
        "cursor": encode_catalog_cursor(
            policies, scope, kind, next_cursor, signing_key
        ),
    }


def _store() -> CatalogStore:
    return CatalogStore.from_environment()
