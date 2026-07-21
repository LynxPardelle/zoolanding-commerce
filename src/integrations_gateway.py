"""Exact, SigV4-signed Commerce to Integrations command gateway."""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import time
from types import MappingProxyType
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

try:
    from domain.limits import MAX_COMMAND_INTEGER
    from domain.offers import DiscountVersion, OfferVersion
    from storage import CommerceScope
except ModuleNotFoundError:
    from src.domain.limits import MAX_COMMAND_INTEGER
    from src.domain.offers import DiscountVersion, OfferVersion
    from src.storage import CommerceScope


MAX_COMMAND_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
REQUEST_TIMEOUT_SECONDS = 2.5
MAX_ATTEMPTS = 2

_API_ID_RE = re.compile(r"^[a-z0-9]{10}$", re.ASCII)
_REGION_RE = re.compile(r"^[a-z]{2}(?:-gov)?-[a-z]+-[0-9]$", re.ASCII)
_SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$", re.ASCII)
_HASH_RE = re.compile(r"^[a-f0-9]{64}$", re.ASCII)
_ROUTES = MappingProxyType({
    "/internal/v1/stripe/offer": "POST",
    "/internal/v1/stripe/product-presentation": "POST",
    "/internal/v1/stripe/discount": "POST",
    "/internal/v1/stripe/discount-lifecycle": "POST",
    "/internal/v1/stripe/checkout": "POST",
    "/internal/v1/stripe/checkout-status": "GET",
    "/internal/v1/stripe/subscription/change": "POST",
    "/internal/v1/stripe/subscription/discount": "POST",
    "/internal/v1/stripe/subscription/pause": "POST",
    "/internal/v1/stripe/customer-portal": "POST",
})
_RETRYABLE_HTTP = frozenset({429, 500, 502, 503, 504})
_ORDINARY_STATUSES = frozenset({"accepted", "pending", "needs_review"})
_CHECKOUT_STATUSES = frozenset({
    "not_created", "pending", "paid", "terminal_unpaid", "unknown"
})


class GatewayConfigurationError(RuntimeError):
    pass


class IntegrationsUnavailable(RuntimeError):
    pass


class IntegrationsConflict(RuntimeError):
    pass


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del req, fp, code, msg, headers, newurl
        return None


class SigV4ExecuteApiTransport:
    """Bounded HTTPS transport for the exact approved Integrations routes."""

    def __init__(
        self,
        *,
        api_id: str,
        stage: str,
        region: str,
        credentials: Any = None,
        opener: Any = None,
        signer: Any = None,
    ) -> None:
        if type(api_id) is not str or _API_ID_RE.fullmatch(api_id) is None:
            raise GatewayConfigurationError("Integrations gateway configuration is invalid")
        if stage not in {"test", "prod"}:
            raise GatewayConfigurationError("Integrations gateway configuration is invalid")
        if type(region) is not str or _REGION_RE.fullmatch(region) is None:
            raise GatewayConfigurationError("Integrations gateway configuration is invalid")
        suffix = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
        self._origin = f"https://{api_id}.execute-api.{region}.{suffix}/{stage}"
        self._region = region
        self._credentials = credentials
        self._opener = opener or build_opener(_NoRedirect())
        if signer is not None and not hasattr(signer, "sign"):
            raise GatewayConfigurationError("Integrations gateway configuration is invalid")
        self._signer = signer

    @classmethod
    def from_environment(cls) -> "SigV4ExecuteApiTransport":
        environment = os.getenv("ENVIRONMENT_NAME", "").strip().lower()
        if environment not in {"test", "prod"}:
            raise GatewayConfigurationError("Integrations gateway configuration is invalid")
        api_id = os.getenv("INTEGRATIONS_API_ID", "").strip().lower()
        region = (
            os.getenv("AWS_REGION", "").strip().lower()
            or os.getenv("AWS_DEFAULT_REGION", "").strip().lower()
        )
        return cls(api_id=api_id, stage=environment, region=region)

    def request(self, method: str, path: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if type(method) is not str or _ROUTES.get(path) != method:
            raise GatewayConfigurationError("Integrations gateway route is invalid")
        body = _encoded_json(payload)
        url = f"{self._origin}{path}"
        for attempt in range(MAX_ATTEMPTS):
            try:
                headers = self._signed_headers(method, url, body)
                request = Request(url, data=body, headers=headers, method=method)
                with self._opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    if response.status != 200 or response.geturl() != url:
                        raise IntegrationsUnavailable("Integrations service is unavailable")
                    raw = response.read(MAX_RESPONSE_BYTES + 1)
                return _decoded_json(raw)
            except HTTPError as exc:
                if exc.code == 409:
                    raise IntegrationsConflict("Integrations command conflicts with current state") from None
                if exc.code not in _RETRYABLE_HTTP or attempt + 1 == MAX_ATTEMPTS:
                    raise IntegrationsUnavailable("Integrations service is unavailable") from None
            except IntegrationsConflict:
                raise
            except (IntegrationsUnavailable, URLError, socket.timeout, TimeoutError, OSError):
                if attempt + 1 == MAX_ATTEMPTS:
                    raise IntegrationsUnavailable("Integrations service is unavailable") from None
            except Exception:
                raise IntegrationsUnavailable("Integrations service is unavailable") from None
        raise IntegrationsUnavailable("Integrations service is unavailable")

    def _signed_headers(self, method: str, url: str, body: bytes) -> dict[str, str]:
        headers = {"Content-Type": "application/json; charset=utf-8"}
        if self._signer is not None:
            signed = self._signer.sign(method, url, body, headers)
            if not isinstance(signed, Mapping):
                raise GatewayConfigurationError("Integrations request signing is unavailable")
            return {str(key): str(value) for key, value in signed.items()}
        try:
            import boto3
            from botocore.auth import SigV4Auth
            from botocore.awsrequest import AWSRequest

            credentials = self._credentials
            if credentials is None:
                credentials = boto3.Session(region_name=self._region).get_credentials()
                if credentials is None:
                    raise RuntimeError("missing credentials")
                credentials = credentials.get_frozen_credentials()
            aws_request = AWSRequest(method=method, url=url, data=body, headers=headers)
            SigV4Auth(credentials, "execute-api", self._region).add_auth(aws_request)
            return {str(key): str(value) for key, value in aws_request.headers.items()}
        except Exception:
            raise GatewayConfigurationError("Integrations request signing is unavailable") from None


class InternalIntegrationsGateway:
    """Builds the closed internal command envelopes accepted by Integrations."""

    def __init__(self, transport: Any) -> None:
        if not hasattr(transport, "request"):
            raise GatewayConfigurationError("Integrations transport is unavailable")
        self._transport = transport

    @classmethod
    def from_environment(cls) -> "InternalIntegrationsGateway":
        return cls(SigV4ExecuteApiTransport.from_environment())

    def provision_offer(
        self, scope: CommerceScope, connection_id: str, offer: OfferVersion
    ) -> dict[str, str]:
        if type(offer) is not OfferVersion:
            raise ValueError("offer must be an immutable OfferVersion")
        return self._snapshot_command(
            "/internal/v1/stripe/offer",
            scope,
            connection_id,
            resource_id=offer.version_id,
            revision=offer.revision,
            operation="provision",
            snapshot=offer.provider_snapshot(),
            include_operation=True,
        )

    def deactivate_offer(
        self,
        scope: CommerceScope,
        connection_id: str,
        resource_id: str,
        lifecycle_revision: int,
    ) -> dict[str, str]:
        return self._snapshot_command(
            "/internal/v1/stripe/offer",
            scope,
            connection_id,
            resource_id=resource_id,
            revision=lifecycle_revision,
            operation="deactivate",
            snapshot={"targetState": "retired"},
            include_operation=True,
        )

    def update_offer_presentation(
        self, scope: CommerceScope, connection_id: str, offer: OfferVersion
    ) -> dict[str, str]:
        if type(offer) is not OfferVersion or offer.display_name is None:
            raise IntegrationsConflict("Offer presentation is not provider-ready")
        snapshot: dict[str, Any] = {"displayName": offer.display_name}
        if offer.display_description is not None:
            snapshot["displayDescription"] = offer.display_description
        return self._snapshot_command(
            "/internal/v1/stripe/product-presentation",
            scope,
            connection_id,
            resource_id=offer.version_id,
            revision=offer.presentation_revision,
            operation="product-presentation",
            snapshot=snapshot,
        )

    def provision_discount(
        self, scope: CommerceScope, connection_id: str, discount: DiscountVersion
    ) -> dict[str, str]:
        if type(discount) is not DiscountVersion:
            raise ValueError("discount must be an immutable DiscountVersion")
        return self._snapshot_command(
            "/internal/v1/stripe/discount",
            scope,
            connection_id,
            resource_id=discount.version_id,
            revision=discount.revision,
            operation="discount",
            snapshot=discount.provider_snapshot(),
        )

    def update_discount_presentation(
        self, scope: CommerceScope, connection_id: str, discount: DiscountVersion
    ) -> dict[str, str]:
        if type(discount) is not DiscountVersion or discount.display_name is None:
            raise IntegrationsConflict("Discount presentation is not provider-ready")
        snapshot: dict[str, Any] = {"displayName": discount.display_name}
        if discount.display_description is not None:
            snapshot["displayDescription"] = discount.display_description
        return self._snapshot_command(
            "/internal/v1/stripe/discount",
            scope,
            connection_id,
            resource_id=discount.version_id,
            revision=discount.presentation_revision,
            operation="discount-presentation",
            snapshot=snapshot,
            input_operation="presentation",
        )

    def update_discount_lifecycle(
        self, scope: CommerceScope, connection_id: str, discount: DiscountVersion
    ) -> dict[str, str]:
        if type(discount) is not DiscountVersion:
            raise ValueError("discount must be an immutable DiscountVersion")
        return self._snapshot_command(
            "/internal/v1/stripe/discount-lifecycle",
            scope,
            connection_id,
            resource_id=discount.version_id,
            revision=discount.lifecycle_revision,
            operation=discount.lifecycle_state,
            snapshot={"targetState": discount.lifecycle_state},
        )

    def create_checkout(
        self,
        scope: CommerceScope,
        connection_id: str,
        command_input: Mapping[str, Any],
    ) -> dict[str, Any]:
        payload = self._operation_command(
            "/internal/v1/stripe/checkout",
            scope,
            connection_id,
            operation="checkout",
            resource_id=command_input.get("paymentAttemptId"),
            revision=command_input.get("revision"),
            command_input=command_input,
        )
        result = self._transport.request("POST", "/internal/v1/stripe/checkout", payload)
        return _validated_redirect_result(result, payload["commandId"], "checkout")

    def lookup_status(
        self,
        scope: CommerceScope,
        connection_id: str,
        order_id: str,
        payment_attempt_id: str,
        revision: int,
    ) -> str:
        command_input = {
            "orderId": _safe_id(order_id),
            "paymentAttemptId": _safe_id(payment_attempt_id),
            "revision": _positive_int(revision),
        }
        payload = _command_envelope(
            scope,
            connection_id,
            command_input,
            idempotency_key=(
                "checkout-status-v1:"
                + canonical_hash({"scope": _scope_fields(scope), "input": command_input})
            ),
        )
        result = self._transport.request(
            "GET", "/internal/v1/stripe/checkout-status", payload
        )
        if (
            not isinstance(result, dict)
            or set(result) != {"orderId", "paymentAttemptId", "revision", "status"}
            or result.get("orderId") != order_id
            or result.get("paymentAttemptId") != payment_attempt_id
            or result.get("revision") != revision
            or result.get("status") not in _CHECKOUT_STATUSES
        ):
            raise IntegrationsUnavailable("Integrations service is unavailable")
        return result["status"]

    def execute_subscription(
        self,
        operation: str,
        scope: CommerceScope,
        connection_id: str,
        command_input: Mapping[str, Any],
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        specifications = {
            "changePlan": ("/internal/v1/stripe/subscription/change", "subscription-change"),
            "applyDiscount": ("/internal/v1/stripe/subscription/discount", "apply"),
            "removeDiscount": ("/internal/v1/stripe/subscription/discount", "remove"),
            "pause": ("/internal/v1/stripe/subscription/pause", "pause"),
            "resume": ("/internal/v1/stripe/subscription/pause", "resume"),
            "openPortal": ("/internal/v1/stripe/customer-portal", "customer-portal"),
        }
        selected = specifications.get(operation)
        if selected is None:
            raise GatewayConfigurationError("Integrations gateway route is invalid")
        path, identity_operation = selected
        input_copy = _plain_mapping(command_input)
        if operation == "openPortal":
            if set(input_copy) != {"subscriptionId"}:
                raise ValueError("customer portal command is invalid")
            portal_input = {
                "subscriptionId": _safe_id(input_copy["subscriptionId"]),
                "portalAttemptId": _portal_attempt_id(scope, idempotency_key),
            }
            technical_key = _derived_idempotency(
                scope,
                connection_id,
                identity_operation,
                portal_input["subscriptionId"],
                1,
                canonical_hash(portal_input),
            )
            payload = _command_envelope(
                scope, connection_id, portal_input, idempotency_key=technical_key
            )
        else:
            payload = self._operation_command(
                path,
                scope,
                connection_id,
                operation=identity_operation,
                resource_id=input_copy.get("subscriptionId"),
                revision=input_copy.get("expectedRevision"),
                command_input=input_copy,
            )
        result = self._transport.request("POST", path, payload)
        if operation == "openPortal":
            return _validated_redirect_result(result, payload["commandId"], "portal")
        return _validated_ordinary_result(result, payload["commandId"])

    def execute(
        self,
        operation: str,
        scope: CommerceScope,
        command_input: Mapping[str, Any],
        *,
        connection_id: str,
        idempotency_key: str,
        **_metadata: Any,
    ) -> dict[str, Any]:
        return self.execute_subscription(
            operation,
            scope,
            connection_id,
            command_input,
            idempotency_key=idempotency_key,
        )

    def _snapshot_command(
        self,
        path: str,
        scope: CommerceScope,
        connection_id: str,
        *,
        resource_id: str,
        revision: int,
        operation: str,
        snapshot: Mapping[str, Any],
        include_operation: bool = False,
        input_operation: str | None = None,
    ) -> dict[str, str]:
        snapshot_value = _plain_mapping(snapshot)
        content_hash = canonical_hash({"schemaVersion": 1, "snapshot": snapshot_value})
        command_input: dict[str, Any] = {
            "resourceId": _safe_id(resource_id),
            "revision": _positive_int(revision),
            "schemaVersion": 1,
            "snapshot": snapshot_value,
            "contentHash": content_hash,
        }
        if include_operation:
            command_input["operation"] = operation
        elif input_operation is not None:
            command_input["operation"] = input_operation
        payload = _command_envelope(
            scope,
            connection_id,
            command_input,
            idempotency_key=_derived_idempotency(
                scope,
                connection_id,
                operation,
                resource_id,
                revision,
                content_hash,
            ),
        )
        result = self._transport.request("POST", path, payload)
        return _validated_ordinary_result(result, payload["commandId"])

    def _operation_command(
        self,
        path: str,
        scope: CommerceScope,
        connection_id: str,
        *,
        operation: str,
        resource_id: Any,
        revision: Any,
        command_input: Mapping[str, Any],
    ) -> dict[str, Any]:
        if _ROUTES.get(path) is None:
            raise GatewayConfigurationError("Integrations gateway route is invalid")
        input_copy = _plain_mapping(command_input)
        content_hash = canonical_hash(input_copy)
        return _command_envelope(
            scope,
            connection_id,
            input_copy,
            idempotency_key=_derived_idempotency(
                scope,
                connection_id,
                operation,
                _safe_id(resource_id),
                _positive_int(revision),
                content_hash,
            ),
        )


def canonical_hash(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError("command payload is invalid") from None
    return hashlib.sha256(encoded).hexdigest()


def _derived_idempotency(
    scope: CommerceScope,
    connection_id: str,
    operation: str,
    resource_id: str,
    revision: int,
    content_hash: str,
) -> str:
    if type(operation) is not str or not operation:
        raise ValueError("command operation is invalid")
    if type(content_hash) is not str or _HASH_RE.fullmatch(content_hash) is None:
        raise ValueError("command content hash is invalid")
    return "integrations-command-v1:" + canonical_hash({
        "scope": _scope_fields(scope),
        "connectionId": _safe_id(connection_id),
        "operation": operation,
        "resourceId": _safe_id(resource_id),
        "revision": _positive_int(revision),
        "contentHash": content_hash,
    })


def _command_envelope(
    scope: CommerceScope,
    connection_id: str,
    command_input: Mapping[str, Any],
    *,
    idempotency_key: str,
) -> dict[str, Any]:
    input_copy = _plain_mapping(command_input)
    connection_id = _safe_id(connection_id)
    if type(idempotency_key) is not str or not 1 <= len(idempotency_key) <= 256:
        raise ValueError("command idempotency is invalid")
    identity = canonical_hash({
        "scope": _scope_fields(scope),
        "connectionId": connection_id,
        "idempotencyKey": idempotency_key,
        "input": input_copy,
    })
    return {
        "version": 1,
        "scope": _scope_fields(scope),
        "connectionId": connection_id,
        "commandId": f"command-{identity[:40]}",
        "idempotencyKey": idempotency_key,
        "input": input_copy,
    }


def _validated_ordinary_result(value: Any, command_id: str) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or set(value) != {"commandId", "status"}
        or value.get("commandId") != command_id
        or value.get("status") not in _ORDINARY_STATUSES
    ):
        raise IntegrationsUnavailable("Integrations service is unavailable")
    return {"commandId": command_id, "status": value["status"]}


def _validated_redirect_result(
    value: Any, command_id: str, redirect_kind: str
) -> dict[str, Any]:
    if isinstance(value, dict) and set(value) == {"commandId", "status"}:
        return _validated_ordinary_result(value, command_id)
    if (
        not isinstance(value, dict)
        or set(value) != {"commandId", "status", "redirectUrl", "expiresAt"}
        or value.get("commandId") != command_id
        or value.get("status") != "accepted"
        or type(value.get("expiresAt")) is not int
        or value["expiresAt"] <= 0
        or (redirect_kind == "portal" and value["expiresAt"] <= int(time.time()))
    ):
        raise IntegrationsUnavailable("Integrations service is unavailable")
    try:
        parsed = urlsplit(value["redirectUrl"])
    except (TypeError, ValueError):
        raise IntegrationsUnavailable("Integrations service is unavailable") from None
    allowed_host = "checkout.stripe.com" if redirect_kind == "checkout" else "billing.stripe.com"
    if (
        parsed.scheme != "https"
        or parsed.hostname != allowed_host
        or parsed.port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise IntegrationsUnavailable("Integrations service is unavailable")
    return {
        "commandId": command_id,
        "status": "accepted",
        "redirectUrl": value["redirectUrl"],
        "expiresAt": value["expiresAt"],
    }


def _scope_fields(scope: CommerceScope) -> dict[str, str]:
    if type(scope) is not CommerceScope:
        raise ValueError("scope must be an immutable CommerceScope")
    return {
        "environment": scope.environment,
        "tenantId": scope.tenant_id,
        "draftId": scope.draft_id,
        "domain": scope.domain,
    }


def _safe_id(value: Any) -> str:
    if type(value) is not str or _SAFE_ID_RE.fullmatch(value) is None:
        raise ValueError("command identifier is invalid")
    return value


def _positive_int(value: Any) -> int:
    if type(value) is not int or not 1 <= value <= MAX_COMMAND_INTEGER:
        raise ValueError("command revision is invalid")
    return value


def _portal_attempt_id(scope: CommerceScope, browser_idempotency_key: Any) -> str:
    if (
        type(browser_idempotency_key) is not str
        or not 1 <= len(browser_idempotency_key) <= 256
        or any(ord(character) < 32 for character in browser_idempotency_key)
    ):
        raise ValueError("command idempotency is invalid")
    digest = canonical_hash({
        "scope": _scope_fields(scope),
        "idempotencyKey": browser_idempotency_key,
    })
    return f"portal-{digest[:56]}"


def _plain_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("command input is invalid")
    try:
        return json.loads(
            json.dumps(
                dict(value),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError("command input is invalid") from None


def _encoded_json(value: Mapping[str, Any]) -> bytes:
    body = json.dumps(
        _plain_mapping(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if not body or len(body) > MAX_COMMAND_BYTES:
        raise GatewayConfigurationError("Integrations command is invalid")
    return body


def _decoded_json(raw: Any) -> dict[str, Any]:
    if type(raw) is not bytes or not raw or len(raw) > MAX_RESPONSE_BYTES:
        raise IntegrationsUnavailable("Integrations service is unavailable")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
        raise IntegrationsUnavailable("Integrations service is unavailable") from None
    if not isinstance(value, dict):
        raise IntegrationsUnavailable("Integrations service is unavailable")
    return value


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value
