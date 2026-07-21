"""Resolve immutable Commerce policy from the current published pointer."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any


MAX_DESCRIPTOR_BYTES = 256 * 1024
MAX_JSON_DEPTH = 32
ENVIRONMENTS = {"test", "production"}
COMMERCE_CAPABILITIES = {
    "commerce:catalog:read",
    "commerce:catalog:write",
    "commerce:inventory:write",
    "commerce:subscription:manage",
    "subscription:migration:execute",
    "commerce:fiscal:manage",
}
SELLABLE_TYPES = {"physical", "service", "subscription", "add_on"}
SHIPPING_METHODS = {"fixed", "free", "pickup"}
DOMAIN_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$"
)
SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
VERSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
CURRENCY_RE = re.compile(r"^[A-Z]{3}$", re.ASCII)


class PolicyResolutionError(Exception):
    """A safe, fail-closed policy resolution failure."""


@dataclass(frozen=True)
class ResolvedPolicies:
    environment: str
    tenant_id: str
    draft_id: str
    domain: str
    version_id: str
    prefix: str
    commerce: dict[str, Any]
    auth_registry: dict[str, Any]
    notification_policies: dict[str, Any] = field(default_factory=dict)

    @property
    def scope(self) -> dict[str, str]:
        return {
            "environment": self.environment,
            "tenantId": self.tenant_id,
            "draftId": self.draft_id,
            "domain": self.domain,
        }


class _DuplicateKey(ValueError):
    pass


class PublishedPolicyResolver:
    """Read a fresh pointer and cache only version-addressed descriptors."""

    def __init__(self, registry_table: Any, s3_client: Any, bucket_name: str):
        if not bucket_name:
            raise PolicyResolutionError("Policy storage is unavailable")
        self._registry_table = registry_table
        self._s3 = s3_client
        self._bucket_name = bucket_name
        self._cache: dict[tuple[str, str, str, str, str, bool, bool], ResolvedPolicies] = {}

    def resolve(
        self,
        *,
        environment: str,
        domain: str,
        tenant_id: str | None = None,
        draft_id: str | None = None,
    ) -> ResolvedPolicies:
        """Resolve Commerce and Auth Admin policy for a protected operation."""

        return self._resolve(
            environment=environment,
            domain=domain,
            tenant_id=tenant_id,
            draft_id=draft_id,
            include_auth=True,
            include_notifications=False,
        )

    def resolve_commerce(
        self,
        *,
        environment: str,
        domain: str,
        tenant_id: str | None = None,
        draft_id: str | None = None,
    ) -> ResolvedPolicies:
        """Resolve only Commerce policy for a genuinely public operation."""

        return self._resolve(
            environment=environment,
            domain=domain,
            tenant_id=tenant_id,
            draft_id=draft_id,
            include_auth=False,
            include_notifications=False,
        )

    def resolve_checkout(
        self,
        *,
        environment: str,
        domain: str,
        tenant_id: str | None = None,
        draft_id: str | None = None,
    ) -> ResolvedPolicies:
        """Resolve Commerce plus its optional same-version notification policy."""

        return self._resolve(
            environment=environment,
            domain=domain,
            tenant_id=tenant_id,
            draft_id=draft_id,
            include_auth=False,
            include_notifications=True,
        )

    def _resolve(
        self,
        *,
        environment: str,
        domain: str,
        tenant_id: str | None,
        draft_id: str | None,
        include_auth: bool,
        include_notifications: bool,
    ) -> ResolvedPolicies:
        environment = _environment(environment)
        domain = _domain(domain)
        metadata = self._metadata(domain)
        metadata_scope = metadata.get("serverScope")
        if (
            not isinstance(metadata_scope, dict)
            or set(metadata_scope) != {"tenantId", "draftId"}
            or metadata.get("domain") != domain
        ):
            raise PolicyResolutionError("Published policy scope is invalid")

        resolved_tenant = _safe_id(metadata_scope.get("tenantId"))
        resolved_draft = _safe_id(metadata_scope.get("draftId"))
        if tenant_id is not None and _safe_id(tenant_id) != resolved_tenant:
            raise PolicyResolutionError("Published policy scope does not match")
        if draft_id is not None and _safe_id(draft_id) != resolved_draft:
            raise PolicyResolutionError("Published policy scope does not match")

        pointer = _published_pointer(metadata, environment)
        version_id = _version_id(pointer.get("versionId") if pointer else None)
        expected_prefix = f"sites/{domain}/versions/{version_id}/"
        if not pointer or pointer.get("prefix") != expected_prefix:
            raise PolicyResolutionError("Published policy pointer is invalid")

        cache_key = (
            environment,
            resolved_tenant,
            resolved_draft,
            domain,
            version_id,
            include_auth,
            include_notifications,
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        base_key = f"{expected_prefix}{domain}/server"
        commerce = self._load_json(f"{base_key}/commerce.json")
        _validate_commerce(commerce, environment, resolved_tenant, resolved_draft, domain)
        auth_registry = (
            self._load_json(f"{base_key}/auth-profile-registry.json")
            if include_auth
            else {}
        )
        if include_auth:
            _validate_auth_registry(auth_registry)
        notification_policy_ids = commerce["commerce"].get("notificationPolicyIds", [])
        notification_policies: dict[str, Any] = {}
        if include_notifications and notification_policy_ids:
            if len(notification_policy_ids) != 1:
                raise PolicyResolutionError("Published notification policy is ambiguous")
            notification_policies = self._load_json(f"{base_key}/notification-policies.json")
            _validate_notification_policies(
                notification_policies,
                environment,
                resolved_tenant,
                resolved_draft,
                domain,
                notification_policy_ids[0],
            )

        resolved = ResolvedPolicies(
            environment=environment,
            tenant_id=resolved_tenant,
            draft_id=resolved_draft,
            domain=domain,
            version_id=version_id,
            prefix=expected_prefix,
            commerce=commerce,
            auth_registry=auth_registry,
            notification_policies=notification_policies,
        )
        self._cache[cache_key] = resolved
        return resolved

    def _metadata(self, domain: str) -> dict[str, Any]:
        try:
            response = self._registry_table.get_item(
                Key={"pk": f"SITE#{domain}", "sk": "METADATA"},
                ConsistentRead=True,
            )
        except Exception:
            raise PolicyResolutionError("Published policy is unavailable") from None
        item = response.get("Item") if isinstance(response, dict) else None
        if not isinstance(item, dict):
            raise PolicyResolutionError("Published policy is unavailable")
        return item

    def _load_json(self, key: str) -> dict[str, Any]:
        try:
            response = self._s3.get_object(Bucket=self._bucket_name, Key=key)
            length = response.get("ContentLength")
            if not isinstance(length, int) or length < 0 or length > MAX_DESCRIPTOR_BYTES:
                raise PolicyResolutionError("Published policy descriptor is invalid")
            body = response.get("Body")
            raw = body.read(MAX_DESCRIPTOR_BYTES + 1)
        except PolicyResolutionError:
            raise
        except Exception:
            raise PolicyResolutionError("Published policy descriptor is unavailable") from None
        if not isinstance(raw, bytes) or len(raw) != length or len(raw) > MAX_DESCRIPTOR_BYTES:
            raise PolicyResolutionError("Published policy descriptor is invalid")
        try:
            value = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
            )
        except (UnicodeDecodeError, ValueError, TypeError):
            raise PolicyResolutionError("Published policy descriptor is invalid") from None
        if not isinstance(value, dict) or _json_depth(value) > MAX_JSON_DEPTH:
            raise PolicyResolutionError("Published policy descriptor is invalid")
        return value


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise _DuplicateKey()
        output[key] = value
    return output


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


def _published_pointer(metadata: dict[str, Any], environment: str) -> dict[str, Any] | None:
    published = metadata.get("publishedEnvironments")
    environments = published if isinstance(published, dict) else {}
    pointer = environments.get("test") if environment == "test" else metadata.get("published") or environments.get("production")
    return pointer if isinstance(pointer, dict) else None


def _validate_commerce(
    policy: dict[str, Any],
    environment: str,
    tenant_id: str,
    draft_id: str,
    domain: str,
) -> None:
    expected_scope = {
        "environment": environment,
        "tenantId": tenant_id,
        "draftId": draft_id,
        "domain": domain,
    }
    if (
        set(policy) != {"version", "scope", "commerce"}
        or policy.get("version") != 1
        or policy.get("scope") != expected_scope
        or not isinstance(policy.get("commerce"), dict)
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")

    commerce = policy["commerce"]
    required = {
        "status",
        "adminAccess",
        "sellableTypes",
        "payments",
        "inventory",
        "shipping",
        "fiscal",
        "checkout",
    }
    if set(commerce) not in (required, required | {"notificationPolicyIds"}):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if commerce.get("status") not in {"disabled", "active"}:
        raise PolicyResolutionError("Published Commerce policy is invalid")

    _validate_admin_access(commerce.get("adminAccess"))
    sellable_types = _unique_enum_list(commerce.get("sellableTypes"), SELLABLE_TYPES, 1, 4)
    payments = _validate_payments(commerce.get("payments"))
    inventory = _validate_inventory(commerce.get("inventory"))
    shipping = _validate_shipping(commerce.get("shipping"))
    _validate_fiscal(commerce.get("fiscal"), environment)
    _validate_checkout(commerce.get("checkout"))
    if "notificationPolicyIds" in commerce:
        _safe_id_list(commerce.get("notificationPolicyIds"), maximum=1, allow_empty=True)

    if "subscription" in sellable_types and not payments["subscriptions"]:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if "physical" in sellable_types and (not inventory["enabled"] or not shipping["enabled"]):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if inventory["tracked"] and not inventory["enabled"]:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if commerce["fiscal"]["enabled"] and (
        commerce["adminAccess"].get("mode") != "auth-profile"
        or "commerce:fiscal:manage" not in commerce["adminAccess"].get("capabilities", [])
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")


def _validate_admin_access(value: Any) -> None:
    if not isinstance(value, dict):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if value.get("mode") == "none":
        if set(value) != {"mode"}:
            raise PolicyResolutionError("Published Commerce policy is invalid")
        return
    if value.get("mode") != "auth-profile" or set(value) != {"mode", "authProfileId", "capabilities"}:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    _safe_id(value.get("authProfileId"))
    _unique_enum_list(value.get("capabilities"), COMMERCE_CAPABILITIES, 1, 32)


def _validate_payments(value: Any) -> dict[str, Any]:
    required_keys = {
        "bindingId",
        "supportedCurrencies",
        "oneTime",
        "subscriptions",
        "editablePrices",
        "coupons",
        "planChangePolicy",
        "pausePolicy",
    }
    if (
        not isinstance(value, dict)
        or not required_keys.issubset(value)
        or not set(value).issubset(required_keys | {"taxPolicy", "migrationPolicy"})
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    _safe_id(value.get("bindingId"))
    currencies = value.get("supportedCurrencies")
    if (
        not isinstance(currencies, list)
        or not 1 <= len(currencies) <= 16
        or any(type(currency) is not str or CURRENCY_RE.fullmatch(currency) is None for currency in currencies)
        or len(set(currencies)) != len(currencies)
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if any(
        type(value.get(key)) is not bool
        for key in required_keys
        - {
            "bindingId",
            "supportedCurrencies",
            "planChangePolicy",
            "pausePolicy",
        }
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    validated_plan_change_policy(value.get("planChangePolicy"))
    validated_pause_policy(value.get("pausePolicy"))
    validated_migration_policy(value.get("migrationPolicy"))
    if "taxPolicy" in value:
        tax_policy = value["taxPolicy"]
        if (
            not isinstance(tax_policy, dict)
            or set(tax_policy) != {"mode"}
            or tax_policy.get("mode") not in {"disabled", "automatic"}
        ):
            raise PolicyResolutionError("Published Commerce policy is invalid")
    return value


def validated_migration_policy(value: Any) -> dict[str, int]:
    if value is None:
        return {"canarySize": 5, "accountConcurrency": 2}
    if not isinstance(value, dict) or set(value) != {"canarySize", "accountConcurrency"}:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    canary_size = value.get("canarySize")
    account_concurrency = value.get("accountConcurrency")
    if (
        type(canary_size) is not int
        or not 1 <= canary_size <= 25
        or type(account_concurrency) is not int
        or not 1 <= account_concurrency <= 5
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    return {
        "canarySize": canary_size,
        "accountConcurrency": account_concurrency,
    }


def validated_plan_change_policy(value: Any) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or set(value) != {"mode"}
        or value.get("mode")
        not in {"disabled", "next-renewal", "immediate-prorated"}
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    return {"mode": value["mode"]}


def validated_pause_policy(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or type(value.get("enabled")) is not bool:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if value["enabled"] is False:
        if set(value) != {"enabled"}:
            raise PolicyResolutionError("Published Commerce policy is invalid")
        return {"enabled": False}
    if set(value) != {
        "enabled",
        "newInvoiceBehavior",
        "existingInvoiceBehavior",
        "accessBehavior",
        "resume",
        "onResume",
    }:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if value.get("newInvoiceBehavior") not in {
        "void",
        "keep-as-draft",
        "mark-uncollectible",
    }:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if value.get("existingInvoiceBehavior") != "unchanged":
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if value.get("accessBehavior") not in {"retain", "suspend"}:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    resume = value.get("resume")
    if (
        not isinstance(resume, dict)
        or set(resume) != {"mode"}
        or resume.get("mode") != "manual"
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    on_resume = value.get("onResume")
    if on_resume != {
        "collection": "restore",
        "access": "restore-if-suspended",
    }:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    return {
        "enabled": True,
        "newInvoiceBehavior": value["newInvoiceBehavior"],
        "existingInvoiceBehavior": "unchanged",
        "accessBehavior": value["accessBehavior"],
        "resume": {"mode": "manual"},
        "onResume": {
            "collection": "restore",
            "access": "restore-if-suspended",
        },
    }


def _validate_inventory(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"enabled", "tracked", "backorders", "locationId"}:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if type(value.get("enabled")) is not bool or type(value.get("tracked")) is not bool or value.get("backorders") is not False:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    _safe_id(value.get("locationId"))
    return value


def _validate_shipping(value: Any) -> dict[str, Any]:
    required_keys = {"enabled", "methods"}
    if (
        not isinstance(value, dict)
        or set(value) not in (required_keys, required_keys | {"allowedCountries"})
        or type(value.get("enabled")) is not bool
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    _unique_enum_list(value.get("methods"), SHIPPING_METHODS, 1, 3)
    if "allowedCountries" in value:
        countries = value["allowedCountries"]
        if (
            not isinstance(countries, list)
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
            raise PolicyResolutionError("Published Commerce policy is invalid")
    return value


def _validate_fiscal(value: Any, environment: str) -> None:
    if not isinstance(value, dict) or type(value.get("enabled")) is not bool:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if value["enabled"] is False:
        if set(value) != {"enabled"}:
            raise PolicyResolutionError("Published Commerce policy is invalid")
        return
    required = {
        "enabled",
        "manual",
        "disclosureId",
        "taxBehavior",
        "retentionDays",
        "requestWindowHours",
    }
    if set(value) not in (required, required | {"accountantApprovalId"}):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if value.get("manual") is not True or value.get("disclosureId") != "manual-invoice-v1":
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if value.get("taxBehavior") not in {"exclusive", "inclusive", "provider-calculated"}:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if not _bounded_integer(value.get("retentionDays"), 1, 3650) or not _bounded_integer(value.get("requestWindowHours"), 1, 720):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    if "accountantApprovalId" in value:
        _safe_id(value.get("accountantApprovalId"))
    if environment == "production" and "accountantApprovalId" not in value:
        raise PolicyResolutionError("Published Commerce policy is invalid")


def _validate_checkout(value: Any) -> None:
    keys = {"successPath", "cancelPath", "termsPath", "privacyPath", "refundPolicyPath", "supportPath"}
    if not isinstance(value, dict) or set(value) != keys:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    for path in value.values():
        if (
            not isinstance(path, str)
            or not 1 <= len(path) <= 256
            or not path.startswith("/")
            or path.startswith("//")
            or "\\" in path
            or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in path)
        ):
            raise PolicyResolutionError("Published Commerce policy is invalid")


def _unique_enum_list(value: Any, allowed: set[str], minimum: int, maximum: int) -> set[str]:
    if (
        not isinstance(value, list)
        or not minimum <= len(value) <= maximum
        or any(not isinstance(item, str) or item not in allowed for item in value)
        or len(set(value)) != len(value)
    ):
        raise PolicyResolutionError("Published Commerce policy is invalid")
    return set(value)


def _safe_id_list(value: Any, *, maximum: int, allow_empty: bool) -> None:
    minimum = 0 if allow_empty else 1
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise PolicyResolutionError("Published Commerce policy is invalid")
    parsed = [_safe_id(item) for item in value]
    if len(set(parsed)) != len(parsed):
        raise PolicyResolutionError("Published Commerce policy is invalid")


def _validate_auth_registry(registry: dict[str, Any]) -> None:
    profiles = registry.get("profiles")
    if registry.get("version") != 1 or not isinstance(profiles, list):
        raise PolicyResolutionError("Published auth policy is invalid")


def _validate_notification_policies(
    descriptor: dict[str, Any],
    environment: str,
    tenant_id: str,
    draft_id: str,
    domain: str,
    referenced_policy_id: str,
) -> None:
    expected_scope = {
        "environment": environment,
        "tenantId": tenant_id,
        "draftId": draft_id,
        "domain": domain,
    }
    policies = descriptor.get("policies")
    if (
        set(descriptor) != {"version", "scope", "policies"}
        or descriptor.get("version") != 1
        or descriptor.get("scope") != expected_scope
        or not isinstance(policies, list)
        or len(policies) > 32
    ):
        raise PolicyResolutionError("Published notification policy is invalid")

    seen: set[str] = set()
    referenced = []
    for policy in policies:
        required = {
            "id", "status", "provider", "connectionId", "notificationTypes",
            "templateIds", "recipientSets", "retryPolicy", "acceptanceStatus",
        }
        if not isinstance(policy, dict) or set(policy) not in (required, required | {"transportApprovalId"}):
            raise PolicyResolutionError("Published notification policy is invalid")
        policy_id = _safe_id(policy.get("id"))
        if policy_id in seen:
            raise PolicyResolutionError("Published notification policy is invalid")
        seen.add(policy_id)
        if policy.get("status") not in {"disabled", "active"}:
            raise PolicyResolutionError("Published notification policy is invalid")
        if policy.get("provider") != "email.smtp":
            raise PolicyResolutionError("Published notification policy is invalid")
        _safe_id(policy.get("connectionId"))
        types = _notification_list(
            policy.get("notificationTypes"),
            {"payment-succeeded", "payment-failed"},
            32,
        )
        templates = _notification_list(
            policy.get("templateIds"),
            {"payment-succeeded-v1", "payment-failed-v1"},
            32,
        )
        if {f"{value}-v1" for value in types} != templates:
            raise PolicyResolutionError("Published notification policy is invalid")
        recipient_sets = policy.get("recipientSets")
        if not isinstance(recipient_sets, list) or not 1 <= len(recipient_sets) <= 16:
            raise PolicyResolutionError("Published notification policy is invalid")
        recipient_ids: set[str] = set()
        for recipient_set in recipient_sets:
            if not isinstance(recipient_set, dict) or set(recipient_set) != {"id", "version", "members"}:
                raise PolicyResolutionError("Published notification policy is invalid")
            recipient_id = _safe_id(recipient_set.get("id"))
            if recipient_id in recipient_ids:
                raise PolicyResolutionError("Published notification policy is invalid")
            recipient_ids.add(recipient_id)
            if not _bounded_integer(recipient_set.get("version"), 1, 2_147_483_647):
                raise PolicyResolutionError("Published notification policy is invalid")
            members = recipient_set.get("members")
            if (
                not isinstance(members, list)
                or len(members) != 1
                or not isinstance(members[0], dict)
                or set(members[0]) != {"id"}
            ):
                raise PolicyResolutionError("Published notification policy is invalid")
            _safe_id(members[0].get("id"))
        retry = policy.get("retryPolicy")
        if (
            not isinstance(retry, dict)
            or set(retry) != {"maxAttempts"}
            or not _bounded_integer(retry.get("maxAttempts"), 1, 5)
            or policy.get("acceptanceStatus") != "accepted_by_smtp"
        ):
            raise PolicyResolutionError("Published notification policy is invalid")
        if "transportApprovalId" in policy:
            _safe_id(policy.get("transportApprovalId"))
        if environment == "production" and policy.get("status") == "active" and "transportApprovalId" not in policy:
            raise PolicyResolutionError("Published notification policy is invalid")
        if policy_id == referenced_policy_id:
            referenced.append(policy)
    if len(referenced) != 1:
        raise PolicyResolutionError("Published notification policy reference is invalid")


def _notification_list(value: Any, allowed: set[str], maximum: int) -> set[str]:
    if (
        not isinstance(value, list)
        or not 1 <= len(value) <= maximum
        or any(not isinstance(item, str) or item not in allowed for item in value)
        or len(set(value)) != len(value)
    ):
        raise PolicyResolutionError("Published notification policy is invalid")
    return set(value)


def _bounded_integer(value: Any, lower: int, upper: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and lower <= value <= upper


def _environment(value: Any) -> str:
    if not isinstance(value, str):
        raise PolicyResolutionError("Policy environment is invalid")
    environment = value.strip().lower()
    if environment == "prod":
        environment = "production"
    if environment not in ENVIRONMENTS:
        raise PolicyResolutionError("Policy environment is invalid")
    return environment


def _domain(value: Any) -> str:
    if not isinstance(value, str):
        raise PolicyResolutionError("Policy domain is invalid")
    domain = value.strip().lower()
    if not 4 <= len(domain) <= 253 or not DOMAIN_RE.fullmatch(domain):
        raise PolicyResolutionError("Policy domain is invalid")
    return domain


def _safe_id(value: Any) -> str:
    if not isinstance(value, str):
        raise PolicyResolutionError("Policy scope is invalid")
    if not SAFE_ID_RE.fullmatch(value):
        raise PolicyResolutionError("Policy scope is invalid")
    return value


def _version_id(value: Any) -> str:
    if not isinstance(value, str):
        raise PolicyResolutionError("Published policy pointer is invalid")
    version_id = value.strip()
    if not VERSION_ID_RE.fullmatch(version_id):
        raise PolicyResolutionError("Published policy pointer is invalid")
    return version_id


_DEFAULT_RESOLVER: PublishedPolicyResolver | None = None


def resolve_policies(
    domain: str,
    environment: str | None = None,
    *,
    tenant_id: str | None = None,
    draft_id: str | None = None,
) -> ResolvedPolicies:
    resolver = _resolver_from_environment()
    return resolver.resolve(
        environment=environment or os.getenv("ENVIRONMENT_NAME", ""),
        domain=domain,
        tenant_id=tenant_id,
        draft_id=draft_id,
    )


def resolve_commerce_policy(
    domain: str,
    environment: str | None = None,
    *,
    tenant_id: str | None = None,
    draft_id: str | None = None,
) -> ResolvedPolicies:
    resolver = _resolver_from_environment()
    return resolver.resolve_commerce(
        environment=environment or os.getenv("ENVIRONMENT_NAME", ""),
        domain=domain,
        tenant_id=tenant_id,
        draft_id=draft_id,
    )


def resolve_checkout_policy(
    domain: str,
    environment: str | None = None,
    *,
    tenant_id: str | None = None,
    draft_id: str | None = None,
) -> ResolvedPolicies:
    resolver = _resolver_from_environment()
    return resolver.resolve_checkout(
        environment=environment or os.getenv("ENVIRONMENT_NAME", ""),
        domain=domain,
        tenant_id=tenant_id,
        draft_id=draft_id,
    )


def _resolver_from_environment() -> PublishedPolicyResolver:
    global _DEFAULT_RESOLVER
    if _DEFAULT_RESOLVER is None:
        table_name = os.getenv("CONFIG_REGISTRY_TABLE_NAME", "").strip()
        bucket_name = os.getenv("CONFIG_PAYLOADS_BUCKET_NAME", "").strip()
        if not table_name or not bucket_name:
            raise PolicyResolutionError("Published policy storage is unavailable")
        try:
            import boto3  # type: ignore

            table = boto3.resource("dynamodb").Table(table_name)
            s3_client = boto3.client("s3")
        except Exception:
            raise PolicyResolutionError("Published policy storage is unavailable") from None
        _DEFAULT_RESOLVER = PublishedPolicyResolver(table, s3_client, bucket_name)
    return _DEFAULT_RESOLVER
