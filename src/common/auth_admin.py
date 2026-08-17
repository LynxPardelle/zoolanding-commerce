"""Auth Admin session, current-state, capability, and CSRF checks for Commerce."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .published_policy import ResolvedPolicies


SESSION_COOKIE_NAME = "__Host-zlp_session"
DOMAIN_HEADER = "x-zlp-domain"
AUTH_PROFILE_HEADER = "x-zlp-auth-profile-id"
DEFAULT_CSRF_COOKIE_NAME = "zlp_csrf"
DEFAULT_CSRF_HEADER_NAME = "x-zlp-csrf"
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
COOKIE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
HEADER_NAME_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")
CAPABILITIES = frozenset({
    "commerce:catalog:read",
    "commerce:catalog:write",
    "commerce:inventory:write",
    "commerce:subscription:manage",
    "subscription:migration:execute",
    "commerce:fiscal:manage",
})


class AuthError(Exception):
    status_code = 403
    error_code = "forbidden"


class AuthenticationError(AuthError):
    status_code = 401
    error_code = "auth_required"


class AuthorizationError(AuthError):
    status_code = 403
    error_code = "forbidden"


@dataclass(frozen=True)
class AuthorizedContext:
    environment: str
    tenant_id: str
    draft_id: str
    domain: str
    subject: str
    roles: tuple[str, ...]
    profile: dict[str, Any]
    commerce: dict[str, Any]
    session: dict[str, Any]


def require_session_cookie(event: dict[str, Any]) -> None:
    """Reject anonymous requests before published-policy or storage I/O."""

    if not _cookie_value(event, SESSION_COOKIE_NAME):
        raise AuthenticationError("Authentication required")


class DynamoAuthStore:
    def __init__(self, session_table_name: str, user_table_name: str, dynamodb: Any = None):
        if not session_table_name or not user_table_name:
            raise AuthenticationError("Authentication is unavailable")
        if dynamodb is None:
            try:
                import boto3  # type: ignore

                dynamodb = boto3.resource("dynamodb")
            except Exception:
                raise AuthenticationError("Authentication is unavailable") from None
        self._session_table = dynamodb.Table(session_table_name)
        self._user_table = dynamodb.Table(user_table_name)

    def get_session(self, session_hash: str) -> dict[str, Any] | None:
        try:
            item = self._session_table.get_item(
                Key={"sessionIdHash": session_hash},
                ConsistentRead=True,
            ).get("Item")
        except Exception:
            raise AuthenticationError("Authentication is unavailable") from None
        return item if isinstance(item, dict) else None

    def get_user(self, tenant_profile_key: str, user_key: str) -> dict[str, Any] | None:
        try:
            item = self._user_table.get_item(
                Key={"tenantProfileKey": tenant_profile_key, "userKey": user_key},
                ConsistentRead=True,
            ).get("Item")
        except Exception:
            raise AuthenticationError("Authentication is unavailable") from None
        return item if isinstance(item, dict) else None


def authorize_request(
    *,
    event: dict[str, Any],
    policies: ResolvedPolicies,
    capability: str,
    mutation: bool = False,
    store: Any = None,
    now_epoch: int | None = None,
) -> AuthorizedContext:
    """Authorize one fresh Auth Admin session against published Commerce policy."""

    if capability not in CAPABILITIES:
        raise AuthorizationError("Commerce access denied")
    domain = _header(event, DOMAIN_HEADER).lower()
    auth_profile_id = _header(event, AUTH_PROFILE_HEADER)
    if domain != policies.domain or not SAFE_ID_RE.fullmatch(auth_profile_id):
        raise AuthenticationError("Authentication required")

    commerce = _commerce(policies)
    access = commerce.get("adminAccess")
    if not isinstance(access, dict) or access.get("mode") != "auth-profile":
        raise AuthorizationError("Commerce access denied")
    if (
        access.get("authProfileId") != auth_profile_id
        or capability not in _string_list(access.get("capabilities"))
    ):
        raise AuthorizationError("Commerce access denied")

    profile = _unique_by_id(
        policies.auth_registry.get("profiles"), auth_profile_id, key="authProfileId"
    )
    if (
        profile.get("status") != "active"
        or profile.get("tenantId") != policies.tenant_id
        or profile.get("domain", policies.domain) != policies.domain
    ):
        raise AuthorizationError("Commerce access denied")
    if (
        profile.get("environment") is not None
        and _auth_environment(profile.get("environment"))
        != _auth_environment(policies.environment)
    ):
        raise AuthorizationError("Commerce access denied")
    admin_groups = set(_string_list(profile.get("adminGroups")))
    if not admin_groups:
        raise AuthorizationError("Commerce access denied")

    session_value = _cookie_value(event, SESSION_COOKIE_NAME)
    if not session_value:
        raise AuthenticationError("Authentication required")
    if store is None:
        store = DynamoAuthStore(
            os.getenv("AUTH_SESSION_TABLE_NAME", "").strip(),
            os.getenv("AUTH_USER_STATE_TABLE_NAME", "").strip(),
        )
    session = store.get_session(_sha256(session_value))
    now = int(time.time()) if now_epoch is None else int(now_epoch)
    auth_environment = _auth_environment(policies.environment)
    tenant_profile_key = f"{policies.domain}#{auth_profile_id}#{auth_environment}"
    if not isinstance(session, dict) or not _valid_session(
        session,
        now=now,
        tenant_profile_key=tenant_profile_key,
        policies=policies,
        auth_profile_id=auth_profile_id,
        auth_environment=auth_environment,
    ):
        raise AuthenticationError("Authentication required")

    subject_value = session.get("subject")
    subject = subject_value.strip() if isinstance(subject_value, str) else ""
    if not SAFE_ID_RE.fullmatch(subject):
        raise AuthenticationError("Authentication required")
    user = store.get_user(tenant_profile_key, f"USER#{subject}")
    if not isinstance(user, dict):
        raise AuthenticationError("Authentication required")
    session_version = _positive_int(session.get("sessionVersion"))
    user_version = _positive_int(user.get("sessionVersion"))
    if session_version == 0 or user_version == 0 or user_version != session_version:
        raise AuthenticationError("Authentication required")
    if user.get("enabled") is not True or user.get("approvalStatus") != "approved":
        raise AuthorizationError("Commerce access denied")

    roles = set(_string_list(user.get("roles")))
    allowed_groups = set(_string_list(profile["allowedGroups"])) if "allowedGroups" in profile else set()
    if allowed_groups:
        roles.intersection_update(allowed_groups)
    if not roles.intersection(admin_groups):
        raise AuthorizationError("Commerce access denied")
    if mutation:
        _require_csrf(event, session, profile)

    refreshed = dict(session)
    refreshed.update({
        "roles": sorted(roles),
        "approvalStatus": "approved",
        "enabled": True,
        "sessionVersion": user_version,
    })
    return AuthorizedContext(
        environment=policies.environment,
        tenant_id=policies.tenant_id,
        draft_id=policies.draft_id,
        domain=policies.domain,
        subject=subject,
        roles=tuple(sorted(roles)),
        profile=profile,
        commerce=commerce,
        session=refreshed,
    )


def _commerce(policies: ResolvedPolicies) -> dict[str, Any]:
    descriptor = policies.commerce
    if (
        not isinstance(descriptor, dict)
        or descriptor.get("version") != 1
        or descriptor.get("scope") != policies.scope
        or not isinstance(descriptor.get("commerce"), dict)
        or descriptor["commerce"].get("status") != "active"
    ):
        raise AuthorizationError("Commerce access denied")
    return descriptor["commerce"]


def _valid_session(
    session: dict[str, Any],
    *,
    now: int,
    tenant_profile_key: str,
    policies: ResolvedPolicies,
    auth_profile_id: str,
    auth_environment: str,
) -> bool:
    if session.get("revokedAt") or _positive_int(session.get("expiresAt")) <= now:
        return False
    if session.get("recordType") not in {None, "authSession"}:
        return False
    return (
        session.get("tenantProfileKey") == tenant_profile_key
        and session.get("domain") == policies.domain
        and session.get("authProfileId") == auth_profile_id
        and session.get("environment") == auth_environment
        and session.get("tenantId") == policies.tenant_id
    )


def _require_csrf(event: dict[str, Any], session: dict[str, Any], profile: dict[str, Any]) -> None:
    session_policy = profile.get("session") if isinstance(profile.get("session"), dict) else {}
    cookie_name = str(session_policy.get("csrfCookieName") or "").strip()
    header_name = str(session_policy.get("csrfHeaderName") or "").strip()
    if not COOKIE_NAME_RE.fullmatch(cookie_name):
        cookie_name = DEFAULT_CSRF_COOKIE_NAME
    if not HEADER_NAME_RE.fullmatch(header_name):
        header_name = DEFAULT_CSRF_HEADER_NAME
    header_value = _header(event, header_name)
    cookie_value = _cookie_value(event, cookie_name)
    expected_hash = str(session.get("csrfHash") or "")
    if (
        not header_value
        or not cookie_value
        or not expected_hash
        or not hmac.compare_digest(header_value, cookie_value)
        or not hmac.compare_digest(_sha256(header_value), expected_hash)
    ):
        raise AuthorizationError("CSRF validation failed")


def _unique_by_id(values: Any, expected: Any, *, key: str) -> dict[str, Any]:
    expected_id = str(expected or "").strip()
    if not SAFE_ID_RE.fullmatch(expected_id) or not isinstance(values, list):
        raise AuthorizationError("Commerce access denied")
    matches = [item for item in values if isinstance(item, dict) and item.get(key) == expected_id]
    if len(matches) != 1:
        raise AuthorizationError("Commerce access denied")
    return matches[0]


def _header(event: dict[str, Any], name: str) -> str:
    headers = event.get("headers") if isinstance(event, dict) and isinstance(event.get("headers"), dict) else {}
    for key, value in headers.items():
        if str(key).lower() == name.lower():
            return str(value or "").strip()
    return ""


def _cookie_value(event: dict[str, Any], name: str) -> str:
    cookies = event.get("cookies") if isinstance(event, dict) and isinstance(event.get("cookies"), list) else []
    for raw_cookie in cookies:
        parts = str(raw_cookie).split(";", 1)[0].split("=", 1)
        if len(parts) == 2 and parts[0].strip() == name:
            return parts[1].strip()
    for raw_cookie in _header(event, "cookie").split(";"):
        parts = raw_cookie.strip().split("=", 1)
        if len(parts) == 2 and parts[0] == name:
            return parts[1].strip()
    return ""


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise AuthorizationError("Commerce access denied")
    return [item.strip() for item in value]


def _positive_int(value: Any) -> int:
    if type(value) is int:
        return value if value > 0 else 0
    if type(value) is Decimal and value.is_finite() and value == value.to_integral_value():
        parsed = int(value)
        return parsed if parsed > 0 else 0
    return 0


def _auth_environment(value: Any) -> str:
    environment = str(value or "").strip().lower()
    return "prod" if environment == "production" else environment


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
