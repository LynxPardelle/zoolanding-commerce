"""Strict HTTP transport and safe Commerce response helpers."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import uuid
from typing import Any, Callable

from .auth_admin import AuthenticationError, AuthorizationError
from .published_policy import PolicyResolutionError, ResolvedPolicies

try:  # Lambda packages src/ at the import root.
    from storage import (
        CommerceScope,
        StorageConflict,
        StorageLimitExceeded,
        StorageNotFound,
        StorageOutcomeUnknown,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.storage import (
        CommerceScope,
        StorageConflict,
        StorageLimitExceeded,
        StorageNotFound,
        StorageOutcomeUnknown,
    )


MAX_BODY_BYTES = 256 * 1024
MAX_ENCODED_BODY_CHARS = ((MAX_BODY_BYTES + 2) // 3) * 4
MAX_JSON_DEPTH = 32
SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
DOMAIN_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$"
)
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
CURRENCY_RE = re.compile(r"^[A-Z]{3}$", re.ASCII)
CURSOR_RE = re.compile(r"^[A-Za-z0-9_-]{1,1024}$", re.ASCII)
CURSOR_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{32,128}$", re.ASCII)
CURSOR_MAC_RE = re.compile(r"^[A-Za-z0-9_-]{43}$", re.ASCII)
PUBLIC_CHECKOUT_RECOVERY_KEY_RE = re.compile(
    r"^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
    re.ASCII,
)
PUBLIC_CHECKOUT_RECOVERY_NAMESPACE = "public-checkout-recovery-v1"


class HttpError(Exception):
    def __init__(self, status_code: int, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable


def dispatch(event: Any, exact_path: str, callback: Callable[[dict[str, Any], str], Any]) -> dict[str, Any]:
    request_id = request_id_from(event)
    try:
        if not isinstance(event, dict) or _method(event) != "POST" or _path(event) != exact_path:
            raise HttpError(404, "not_found", "Resource not found.")
        payload = strict_json_body(event)
        return success_response(callback(payload, request_id), request_id)
    except Exception as exc:
        return error_response(exc, request_id)


def strict_json_body(event: dict[str, Any]) -> dict[str, Any]:
    body = event.get("body")
    encoded = event.get("isBase64Encoded", False)
    if not isinstance(encoded, bool) or not isinstance(body, str):
        raise validation_error()
    if (encoded and len(body) > MAX_ENCODED_BODY_CHARS) or (not encoded and len(body) > MAX_BODY_BYTES):
        raise validation_error()
    try:
        raw = base64.b64decode(body.encode("ascii"), validate=True) if encoded else body.encode("utf-8")
    except (UnicodeEncodeError, ValueError):
        raise validation_error() from None
    if not raw or len(raw) > MAX_BODY_BYTES:
        raise validation_error()
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
        raise validation_error() from None
    if not isinstance(value, dict) or _json_depth(value) > MAX_JSON_DEPTH:
        raise validation_error()
    return value


def closed_object(value: Any, required: set[str], optional: set[str] | None = None) -> dict[str, Any]:
    optional = optional or set()
    if (
        not isinstance(value, dict)
        or not required.issubset(value)
        or not set(value).issubset(required | optional)
    ):
        raise validation_error()
    return value


def header(event: dict[str, Any], name: str) -> str:
    headers = event.get("headers") if isinstance(event.get("headers"), dict) else {}
    for key, value in headers.items():
        if str(key).lower() == name.lower() and isinstance(value, str):
            return value.strip()
    return ""


def domain_header(event: dict[str, Any]) -> str:
    domain = header(event, "x-zlp-domain").lower()
    if not DOMAIN_RE.fullmatch(domain):
        raise validation_error()
    return domain


def idempotency_header(event: dict[str, Any]) -> str:
    value = header(event, "idempotency-key")
    if not 1 <= len(value) <= 256 or any(ord(character) < 32 for character in value):
        raise validation_error()
    return value


def public_checkout_idempotency_header(event: dict[str, Any]) -> str:
    """Return a namespaced 256-bit browser recovery capability.

    The browser must create the raw header from 32 cryptographically random bytes.
    Only its SHA-256 digest is persisted by Commerce's idempotency receipt.
    """
    value = header(event, "idempotency-key")
    if PUBLIC_CHECKOUT_RECOVERY_KEY_RE.fullmatch(value) is None:
        raise validation_error()
    try:
        decoded = base64.b64decode(value + "=", altchars=b"-_", validate=True)
    except ValueError:
        raise validation_error() from None
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if len(decoded) != 32 or not hmac.compare_digest(value, canonical):
        raise validation_error()
    return f"{PUBLIC_CHECKOUT_RECOVERY_NAMESPACE}:{value}"


def safe_id(value: Any) -> str:
    if not isinstance(value, str) or not SAFE_ID_RE.fullmatch(value):
        raise validation_error()
    return value


def positive_int(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise validation_error()
    return value


def nonnegative_int(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise validation_error()
    return value


def bounded_page_size(value: Any, maximum: int = 100) -> int:
    if value is None:
        return min(25, maximum)
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
        raise validation_error()
    return value


def resolved_scope(policies: ResolvedPolicies) -> CommerceScope:
    return CommerceScope(
        policies.environment,
        policies.tenant_id,
        policies.draft_id,
        policies.domain,
    )


def validated_commerce(policies: ResolvedPolicies) -> dict[str, Any]:
    descriptor = policies.commerce
    if (
        not isinstance(descriptor, dict)
        or descriptor.get("version") != 1
        or descriptor.get("scope") != policies.scope
        or not isinstance(descriptor.get("commerce"), dict)
    ):
        raise policy_unavailable()
    commerce = descriptor["commerce"]
    if commerce.get("status") != "active":
        raise HttpError(404, "not_found", "Resource not found.")
    return commerce


def supported_currencies(commerce: Any) -> frozenset[str]:
    payments = commerce.get("payments") if isinstance(commerce, dict) else None
    values = payments.get("supportedCurrencies") if isinstance(payments, dict) else None
    if (
        not isinstance(values, list)
        or not 1 <= len(values) <= 16
        or any(not isinstance(value, str) or CURRENCY_RE.fullmatch(value) is None for value in values)
        or len(set(values)) != len(values)
    ):
        raise policy_unavailable()
    return frozenset(values)


def encode_catalog_cursor(
    policies: ResolvedPolicies,
    scope: CommerceScope,
    kind: str,
    raw_cursor: str | None,
    signing_key: bytes,
) -> str | None:
    if raw_cursor is None:
        return None
    signing_key = _validated_cursor_key(signing_key)
    prefix = _catalog_cursor_prefix(kind)
    if not isinstance(raw_cursor, str) or not raw_cursor.startswith(prefix):
        raise validation_error()
    last_id = safe_id(raw_cursor[len(prefix):])
    payload = {
        "v": 1,
        "k": kind,
        "l": last_id,
        "s": _catalog_cursor_scope(policies, scope, kind),
    }
    payload["m"] = _catalog_cursor_mac(payload, signing_key)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    token = base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")
    if CURSOR_RE.fullmatch(token) is None:
        raise validation_error()
    return token


def decode_catalog_cursor(
    policies: ResolvedPolicies,
    scope: CommerceScope,
    kind: str,
    token: Any,
    signing_key: bytes,
) -> str | None:
    if token is None:
        return None
    signing_key = _validated_cursor_key(signing_key)
    if not isinstance(token, str) or CURSOR_RE.fullmatch(token) is None:
        raise validation_error()
    try:
        raw = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, ValueError, TypeError):
        raise validation_error() from None
    expected_scope = _catalog_cursor_scope(policies, scope, kind)
    if (
        not isinstance(payload, dict)
        or set(payload) != {"v", "k", "l", "s", "m"}
        or payload.get("v") != 1
        or payload.get("k") != kind
        or not isinstance(payload.get("s"), str)
        or not hmac.compare_digest(payload["s"], expected_scope)
        or not isinstance(payload.get("m"), str)
        or CURSOR_MAC_RE.fullmatch(payload["m"]) is None
    ):
        raise validation_error()
    last_id = safe_id(payload.get("l"))
    unsigned = {"v": 1, "k": kind, "l": last_id, "s": expected_scope}
    if not hmac.compare_digest(payload["m"], _catalog_cursor_mac(unsigned, signing_key)):
        raise validation_error()
    return f"{_catalog_cursor_prefix(kind)}{last_id}"


def catalog_cursor_signing_key(value: Any) -> bytes:
    if not isinstance(value, str) or CURSOR_KEY_RE.fullmatch(value) is None:
        raise policy_unavailable()
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except ValueError:
        raise policy_unavailable() from None
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if not hmac.compare_digest(value, canonical):
        raise policy_unavailable()
    return _validated_cursor_key(decoded)


def validation_error() -> HttpError:
    return HttpError(400, "validation_error", "Request validation failed.")


def policy_unavailable() -> HttpError:
    return HttpError(503, "upstream_unavailable", "Service temporarily unavailable.", retryable=True)


def success_response(data: Any, request_id: str) -> dict[str, Any]:
    return _json_response(200, {"ok": True, "data": data, "requestId": request_id})


def error_response(exc: Exception, request_id: str) -> dict[str, Any]:
    if isinstance(exc, HttpError):
        status, code, message, retryable = exc.status_code, exc.code, exc.message, exc.retryable
    elif isinstance(exc, AuthenticationError):
        status, code, message, retryable = 401, "auth_required", "Authentication required.", False
    elif isinstance(exc, AuthorizationError):
        status, code, message, retryable = 403, "forbidden", "You do not have access to this resource.", False
    elif isinstance(exc, PolicyResolutionError):
        status, code, message, retryable = 503, "upstream_unavailable", "Service temporarily unavailable.", True
    elif isinstance(exc, StorageNotFound):
        status, code, message, retryable = 404, "not_found", "Resource not found.", False
    elif isinstance(exc, StorageConflict):
        status, code, message, retryable = 409, "conflict", "Request conflicts with current state.", False
    elif isinstance(exc, StorageOutcomeUnknown):
        status, code, message, retryable = 503, "upstream_unavailable", "Service temporarily unavailable.", True
    elif isinstance(exc, (StorageLimitExceeded, ValueError)):
        status, code, message, retryable = 400, "validation_error", "Request validation failed.", False
    else:
        status, code, message, retryable = 500, "internal_error", "Request failed.", True
    return _json_response(status, {
        "ok": False,
        "code": code,
        "error": message,
        "message": message,
        "requestId": request_id,
        "retryable": retryable,
    })


def request_id_from(event: Any) -> str:
    context = event.get("requestContext") if isinstance(event, dict) and isinstance(event.get("requestContext"), dict) else {}
    candidate = context.get("requestId")
    if isinstance(candidate, str) and REQUEST_ID_RE.fullmatch(candidate):
        return candidate
    return f"request-{uuid.uuid4().hex}"


def _method(event: dict[str, Any]) -> str:
    context = event.get("requestContext") if isinstance(event.get("requestContext"), dict) else {}
    http = context.get("http") if isinstance(context.get("http"), dict) else {}
    value = http.get("method") or event.get("httpMethod")
    return value.upper() if isinstance(value, str) else ""


def _path(event: dict[str, Any]) -> str:
    value = event.get("rawPath") or event.get("path")
    return value if isinstance(value, str) else ""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("duplicate key")
        output[key] = value
    return output


def _reject_constant(_value: str) -> None:
    raise ValueError("non-finite number")


def _json_depth(value: Any) -> int:
    deepest = 0
    stack = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        deepest = max(deepest, depth)
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)
    return deepest


def _json_response(status_code: int, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "no-store",
        },
        "body": json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
    }


def _catalog_cursor_prefix(kind: str) -> str:
    prefixes = {
        "items": "CATALOG_ITEM#",
        "offers": "OFFER#",
        "discounts": "DISCOUNT#",
        "public-offers": "OFFER#",
    }
    prefix = prefixes.get(kind) if isinstance(kind, str) else None
    if prefix is None:
        raise validation_error()
    return prefix


def _catalog_cursor_scope(
    policies: ResolvedPolicies,
    scope: CommerceScope,
    kind: str,
) -> str:
    _catalog_cursor_prefix(kind)
    value = "\0".join((
        policies.environment,
        policies.tenant_id,
        policies.draft_id,
        policies.domain,
        policies.version_id,
        scope.environment,
        scope.tenant_id,
        scope.draft_id,
        scope.domain,
        kind,
    ))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _catalog_cursor_mac(payload: dict[str, Any], signing_key: bytes) -> str:
    message = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hmac.new(signing_key, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _validated_cursor_key(value: Any) -> bytes:
    if type(value) is not bytes or not 24 <= len(value) <= 96:
        raise policy_unavailable()
    return value
