"""Same-origin manual fiscal opt-in using a paid-order single-use claim."""

from __future__ import annotations

import os
import time
from typing import Any

try:  # Lambda CodeUri is src/.
    from common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        header,
        idempotency_header,
        policy_unavailable,
        safe_id,
        validated_commerce,
        validation_error,
    )
    from common.published_policy import resolve_commerce_policy
    from fiscal_storage import (
        FiscalCaptureDisabled,
        FiscalScope,
        FiscalStore,
        fiscal_request_window_seconds,
        validate_claim_token,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.common.http import (
        HttpError,
        closed_object,
        dispatch,
        domain_header,
        header,
        idempotency_header,
        policy_unavailable,
        safe_id,
        validated_commerce,
        validation_error,
    )
    from src.common.published_policy import resolve_commerce_policy
    from src.fiscal_storage import (
        FiscalCaptureDisabled,
        FiscalScope,
        FiscalStore,
        fiscal_request_window_seconds,
        validate_claim_token,
    )


PATH = "/features/commerce/fiscal/request"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    return dispatch(event, PATH, lambda payload, _request_id: _handle(event, payload))


def _handle(event: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    request = closed_object(
        payload,
        {"operation", "orderId", "fiscalAccessProof", "input"},
    )
    if request["operation"] != "submitRequest" or type(request["operation"]) is not str:
        raise validation_error()
    order_id = safe_id(request["orderId"])
    claim_token = validate_claim_token(request["fiscalAccessProof"])
    domain = domain_header(event)
    if header(event, "origin") != f"https://{domain}":
        raise HttpError(403, "forbidden", "You do not have access to this resource.")
    policies = resolve_commerce_policy(domain)
    commerce = validated_commerce(policies)
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
    return _store().redeem_claim(
        scope,
        order_id,
        claim_token,
        request["input"],
        idempotency_key=idempotency_header(event),
        now_epoch=int(time.time()),
    )


def _store() -> FiscalStore:
    return FiscalStore.from_environment()
