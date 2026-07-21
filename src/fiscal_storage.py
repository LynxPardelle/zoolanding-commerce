"""Isolated manual fiscal intake and single-use paid-order claims."""

from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import json
import os
import re
import secrets
import unicodedata
from typing import Any, Mapping

try:  # Lambda CodeUri is src/.
    from storage import (
        ConditionalWriteFailed,
        StorageConflict,
        StorageNotFound,
        StorageOutcomeUnknown,
        _DynamoBackend,
        _stored_fiscal_access,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.storage import (
        ConditionalWriteFailed,
        StorageConflict,
        StorageNotFound,
        StorageOutcomeUnknown,
        _DynamoBackend,
        _stored_fiscal_access,
    )


MANUAL_DISCLOSURE_ID = "manual-invoice-v1"
MAX_CLAIM_ATTEMPTS = 5
MAX_WINDOW_HOURS = 720
FISCAL_FIELDS = frozenset({
    "rfc",
    "legalName",
    "postalCode",
    "fiscalRegime",
    "cfdiUse",
    "contactEmail",
})
CORRECTION_REASONS = frozenset({
    "invalid_rfc",
    "legal_name_mismatch",
    "invalid_tax_profile",
    "invalid_contact",
    "other",
})
SUBMISSION_TRANSITIONS = {
    "markNeedsCorrection": {"requested"},
    "markReady": {"requested"},
    "markDelivered": {"ready"},
    "cancel": {"requested", "needs_correction", "ready"},
}
TARGET_STATUS = {
    "markNeedsCorrection": "needs_correction",
    "markReady": "ready",
    "markDelivered": "delivered",
    "cancel": "canceled",
}
_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)
_DOMAIN = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+",
    re.ASCII,
)
_RFC = re.compile(r"[A-Z&Ñ]{3,4}[0-9]{6}[A-Z0-9]{3}")
_POSTAL_CODE = re.compile(r"[0-9]{5}", re.ASCII)
_FISCAL_REGIME = re.compile(r"[0-9]{3}", re.ASCII)
_CFDI_USE = re.compile(r"[A-Z0-9]{3,4}", re.ASCII)
_EMAIL = re.compile(
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+",
    re.ASCII,
)
_CLAIM_TOKEN = re.compile(r"[A-Za-z0-9_-]{43}", re.ASCII)
_ACTOR_HASH = re.compile(r"[a-f0-9]{64}", re.ASCII)

# Production remains code-blocked until deletion/retention and accountant access
# controls exist in this service. Deploy parameters and draft data cannot open it.
_PRODUCTION_FISCAL_CAPTURE_READY = False


class FiscalCaptureDisabled(Exception):
    """The server-controlled fiscal live gate is closed."""


@dataclass(frozen=True, slots=True)
class FiscalScope:
    environment: str
    tenant_id: str
    draft_id: str
    domain: str

    def __post_init__(self) -> None:
        if self.environment not in {"test", "production"}:
            raise ValueError("environment is invalid")
        _safe_id(self.tenant_id, "tenant_id")
        _safe_id(self.draft_id, "draft_id")
        if type(self.domain) is not str or _DOMAIN.fullmatch(self.domain) is None:
            raise ValueError("domain is invalid")

    @property
    def partition_key(self) -> str:
        return f"ENV#{self.environment}#TENANT#{self.tenant_id}#DRAFT#{self.draft_id}"


def fiscal_request_window_seconds(
    policy: object,
    environment: object,
    server_gates: Mapping[str, str],
) -> int:
    """Return the approved claim window; draft approval strings never open production."""

    if not isinstance(policy, Mapping) or policy.get("enabled") is not True:
        raise FiscalCaptureDisabled("fiscal capture is disabled")
    if policy.get("manual") is not True or policy.get("disclosureId") != MANUAL_DISCLOSURE_ID:
        raise FiscalCaptureDisabled("manual fiscal policy is unavailable")
    hours = policy.get("requestWindowHours")
    if type(hours) is not int or not 1 <= hours <= MAX_WINDOW_HOURS:
        raise FiscalCaptureDisabled("fiscal request window is unavailable")
    if environment == "test":
        if hours != 24:
            raise FiscalCaptureDisabled("test fiscal request window is not approved")
        return hours * 60 * 60
    if environment != "production":
        raise FiscalCaptureDisabled("fiscal capture is unavailable")
    if not _PRODUCTION_FISCAL_CAPTURE_READY:
        raise FiscalCaptureDisabled("production fiscal capture is disabled")
    if not _SAFE_ID.fullmatch(str(policy.get("accountantApprovalId") or "")):
        raise FiscalCaptureDisabled("accountant approval is unavailable")
    if server_gates.get("FISCAL_PRODUCTION_APPROVED") != "true":
        raise FiscalCaptureDisabled("production fiscal capture is disabled")
    for name in ("FISCAL_RETENTION_APPROVAL_ID", "FISCAL_ACCESS_APPROVAL_ID"):
        if not _SAFE_ID.fullmatch(str(server_gates.get(name) or "")):
            raise FiscalCaptureDisabled("production fiscal approval is unavailable")
    return hours * 60 * 60


class FiscalStore:
    def __init__(self, backend: Any, table_name: str, operations_table_name: str) -> None:
        self._backend = backend
        self._table_name = _table_name(table_name)
        self._operations_table_name = _table_name(operations_table_name)
        if self._table_name == self._operations_table_name:
            raise ValueError("fiscal and operations tables must be distinct")

    @classmethod
    def from_environment(cls) -> "FiscalStore":
        table_name = os.getenv("FISCAL_TABLE_NAME", "").strip()
        operations_table_name = os.getenv("COMMERCE_OPERATIONS_TABLE_NAME", "").strip()
        if not table_name or not operations_table_name:
            raise RuntimeError("fiscal storage is unavailable")
        try:
            import boto3  # type: ignore

            backend = _DynamoBackend(boto3.client("dynamodb"))
        except Exception:
            raise RuntimeError("fiscal storage is unavailable") from None
        return cls(backend, table_name, operations_table_name)

    def redeem_claim(
        self,
        scope: FiscalScope,
        order_id: object,
        claim_token: object,
        details: object,
        *,
        idempotency_key: object,
        now_epoch: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        selected_order = _safe_id(order_id, "order_id")
        now = _epoch(now_epoch, "now_epoch")
        digest = _claim_digest(claim_token)
        idempotency_digest = _idempotency_digest(idempotency_key)
        access_sk = f"FISCAL_ACCESS#{selected_order}"
        raw_access = self._backend.get(
            self._operations_table_name,
            selected_scope.partition_key,
            access_sk,
        )
        if raw_access is None:
            raise StorageNotFound("fiscal order access was not found")
        access = _stored_fiscal_access(raw_access, selected_scope, selected_order)

        try:
            parsed_details = _fiscal_details(details)
        except ValueError:
            self._record_failed_attempt(selected_scope, access, digest, now)
            raise

        request_hash = _hash_json({
            "scope": selected_scope.partition_key,
            "orderId": selected_order,
            "proofHash": digest,
            "details": parsed_details,
        })
        receipt_sk = f"FISCAL_IDEMPOTENCY#{idempotency_digest}"
        existing = self._backend.get(
            self._table_name,
            selected_scope.partition_key,
            receipt_sk,
        )
        if existing is not None:
            return _idempotency_result(existing, selected_scope, idempotency_digest, request_hash)
        _eligible_access(access, digest, now)

        request_id = "request-" + hashlib.sha256(
            f"{selected_scope.partition_key}\0{selected_order}\0{idempotency_digest}".encode("utf-8")
        ).hexdigest()[:32]
        request = {
            "pk": selected_scope.partition_key,
            "sk": f"FISCAL_REQUEST#{request_id}",
            "itemType": "FiscalRequest",
            **_scope_fields(selected_scope),
            "requestId": request_id,
            "orderId": selected_order,
            "status": "requested",
            "revision": 1,
            "details": parsed_details,
            "disclosureId": MANUAL_DISCLOSURE_ID,
            "manualDelivery": True,
            "createdAt": now,
            "updatedAt": now,
        }
        consumed_access = copy.deepcopy(dict(access))
        consumed_access.update({
            "state": "consumed",
            "attempts": access["attempts"] + 1,
            "revision": access["revision"] + 1,
            "consumedAt": now,
            "requestId": request_id,
        })
        result = _request_projection(request)
        receipt = {
            "pk": selected_scope.partition_key,
            "sk": receipt_sk,
            "itemType": "FiscalRequestIdempotency",
            **_scope_fields(selected_scope),
            "idempotencyDigest": idempotency_digest,
            "requestHash": request_hash,
            "orderId": selected_order,
            "proofHash": digest,
            "result": copy.deepcopy(result),
            "createdAt": now,
        }
        operations = [
            _put(
                self._operations_table_name,
                consumed_access,
                {
                    "state": "eligible",
                    "proofHash": digest,
                    "attempts": access["attempts"],
                    "revision": access["revision"],
                    "expiresAt": access["expiresAt"],
                },
            ),
            _put(self._table_name, request, "absent"),
            _put(self._table_name, receipt, "absent"),
        ]
        try:
            self._backend.transact(operations, _client_token(selected_scope, operations))
        except Exception as exc:
            replay = self._backend.get(
                self._table_name,
                selected_scope.partition_key,
                receipt_sk,
            )
            if replay is not None:
                return _idempotency_result(
                    replay,
                    selected_scope,
                    idempotency_digest,
                    request_hash,
                )
            if isinstance(exc, ConditionalWriteFailed):
                raise StorageConflict("fiscal claim is unavailable") from None
            raise StorageOutcomeUnknown("fiscal request outcome is unknown") from None
        return result

    def _record_failed_attempt(
        self,
        scope: FiscalScope,
        access: Mapping[str, Any],
        proof_hash: str,
        now_epoch: int,
    ) -> None:
        _eligible_access(access, proof_hash, now_epoch)
        attempted = copy.deepcopy(dict(access))
        attempted["attempts"] = access["attempts"] + 1
        attempted["revision"] = access["revision"] + 1
        operations = [
            _put(
                self._operations_table_name,
                attempted,
                {
                    "state": "eligible",
                    "proofHash": proof_hash,
                    "attempts": access["attempts"],
                    "revision": access["revision"],
                    "expiresAt": access["expiresAt"],
                },
            )
        ]
        try:
            self._backend.transact(operations, _client_token(scope, operations))
        except ConditionalWriteFailed:
            raise StorageConflict("fiscal claim is unavailable") from None

    def get_request(self, scope: FiscalScope, request_id: object) -> dict[str, Any]:
        selected_scope = _scope(scope)
        selected_id = _safe_id(request_id, "request_id")
        item = self._backend.get(
            self._table_name,
            selected_scope.partition_key,
            f"FISCAL_REQUEST#{selected_id}",
        )
        request = _stored_request(item, selected_scope, selected_id)
        return _request_projection(request, include_details=True)

    def correct_request(
        self,
        scope: FiscalScope,
        request_id: object,
        details: object,
        *,
        expected_revision: object,
        actor_hash: object,
        now_epoch: object,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        selected_id = _safe_id(request_id, "request_id")
        revision = _positive_int(expected_revision, "expected_revision")
        actor = _actor_hash(actor_hash)
        now = _epoch(now_epoch, "now_epoch")
        current = _stored_request(
            self._backend.get(self._table_name, selected_scope.partition_key, f"FISCAL_REQUEST#{selected_id}"),
            selected_scope,
            selected_id,
        )
        if current["status"] != "needs_correction" or current["revision"] != revision:
            raise StorageConflict("fiscal request state changed")
        updated = copy.deepcopy(dict(current))
        updated.update({
            "details": _fiscal_details(details),
            "status": "requested",
            "revision": revision + 1,
            "updatedAt": now,
            "lastActorHash": actor,
        })
        updated.pop("correctionReasonCode", None)
        self._replace_request(selected_scope, current, updated)
        return _request_projection(updated)

    def transition_request(
        self,
        scope: FiscalScope,
        request_id: object,
        action: object,
        *,
        expected_revision: object,
        actor_hash: object,
        now_epoch: object,
        reason_code: object = None,
        delivery_reference_id: object = None,
    ) -> dict[str, Any]:
        selected_scope = _scope(scope)
        selected_id = _safe_id(request_id, "request_id")
        if type(action) is not str or action not in SUBMISSION_TRANSITIONS:
            raise ValueError("fiscal action is invalid")
        revision = _positive_int(expected_revision, "expected_revision")
        actor = _actor_hash(actor_hash)
        now = _epoch(now_epoch, "now_epoch")
        current = _stored_request(
            self._backend.get(self._table_name, selected_scope.partition_key, f"FISCAL_REQUEST#{selected_id}"),
            selected_scope,
            selected_id,
        )
        if current["revision"] != revision or current["status"] not in SUBMISSION_TRANSITIONS[action]:
            raise StorageConflict("fiscal request state changed")
        updated = copy.deepcopy(dict(current))
        updated.update({
            "status": TARGET_STATUS[action],
            "revision": revision + 1,
            "updatedAt": now,
            "lastActorHash": actor,
        })
        updated.pop("correctionReasonCode", None)
        if action == "markNeedsCorrection":
            if type(reason_code) is not str or reason_code not in CORRECTION_REASONS:
                raise ValueError("correction reason is invalid")
            updated["correctionReasonCode"] = reason_code
        elif reason_code is not None:
            raise ValueError("correction reason is not accepted")
        if action == "markDelivered":
            updated["deliveryReferenceId"] = _safe_id(delivery_reference_id, "delivery_reference_id")
        elif delivery_reference_id is not None:
            raise ValueError("delivery reference is not accepted")
        self._replace_request(selected_scope, current, updated)
        return _request_projection(updated)

    def _replace_request(
        self,
        scope: FiscalScope,
        current: Mapping[str, Any],
        updated: Mapping[str, Any],
    ) -> None:
        operations = [
            _put(
                self._table_name,
                updated,
                {"revision": current["revision"], "status": current["status"]},
            )
        ]
        try:
            self._backend.transact(operations, _client_token(scope, operations))
        except ConditionalWriteFailed:
            raise StorageConflict("fiscal request state changed") from None


def validate_fiscal_details(value: object) -> dict[str, str]:
    return _fiscal_details(value)


def validate_claim_token(value: object) -> str:
    _claim_digest(value)
    return value


def new_fiscal_access_proof() -> tuple[str, str]:
    token = secrets.token_urlsafe(32)
    return token, _claim_digest(token)


def _fiscal_details(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != FISCAL_FIELDS:
        raise ValueError("fiscal details are invalid")
    rfc = value.get("rfc")
    postal_code = value.get("postalCode")
    fiscal_regime = value.get("fiscalRegime")
    cfdi_use = value.get("cfdiUse")
    contact_email = value.get("contactEmail")
    if type(rfc) is not str or _RFC.fullmatch(rfc) is None:
        raise ValueError("RFC format is invalid")
    if type(postal_code) is not str or _POSTAL_CODE.fullmatch(postal_code) is None:
        raise ValueError("postal code is invalid")
    if type(fiscal_regime) is not str or _FISCAL_REGIME.fullmatch(fiscal_regime) is None:
        raise ValueError("fiscal regime is invalid")
    if type(cfdi_use) is not str or _CFDI_USE.fullmatch(cfdi_use) is None:
        raise ValueError("CFDI use is invalid")
    if type(contact_email) is not str or len(contact_email) > 254 or _EMAIL.fullmatch(contact_email) is None:
        raise ValueError("contact email is invalid")
    return {
        "rfc": rfc,
        "legalName": _plain_text(value.get("legalName"), "legal_name", 200),
        "postalCode": postal_code,
        "fiscalRegime": fiscal_regime,
        "cfdiUse": cfdi_use,
        "contactEmail": contact_email,
    }


def _eligible_access(access: Mapping[str, Any], proof_hash: str, now_epoch: int) -> None:
    if (
        access.get("proofHash") != proof_hash
        or access.get("state") != "eligible"
        or access.get("attempts", MAX_CLAIM_ATTEMPTS) >= MAX_CLAIM_ATTEMPTS
        or now_epoch >= access.get("expiresAt", 0)
    ):
        raise StorageConflict("fiscal claim is unavailable")


def _idempotency_result(
    item: object,
    scope: FiscalScope,
    digest: str,
    request_hash: str,
) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise StorageConflict("fiscal idempotency record is invalid")
    result = item.get("result")
    if (
        item.get("pk") != scope.partition_key
        or item.get("sk") != f"FISCAL_IDEMPOTENCY#{digest}"
        or item.get("itemType") != "FiscalRequestIdempotency"
        or not _scope_matches(item, scope)
        or item.get("idempotencyDigest") != digest
        or item.get("requestHash") != request_hash
        or not isinstance(result, Mapping)
    ):
        raise StorageConflict("fiscal idempotency key was already used")
    sanitized = copy.deepcopy(dict(result))
    if set(sanitized) != {
        "requestId",
        "orderId",
        "status",
        "revision",
        "disclosureId",
        "manualDelivery",
    }:
        raise StorageConflict("fiscal idempotency record is invalid")
    return sanitized


def _stored_request(item: Any, scope: FiscalScope, request_id: str) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise StorageNotFound("fiscal request was not found")
    request = copy.deepcopy(dict(item))
    if (
        request.get("pk") != scope.partition_key
        or request.get("sk") != f"FISCAL_REQUEST#{request_id}"
        or request.get("itemType") != "FiscalRequest"
        or request.get("requestId") != request_id
        or not _scope_matches(request, scope)
        or request.get("disclosureId") != MANUAL_DISCLOSURE_ID
        or request.get("manualDelivery") is not True
    ):
        raise StorageConflict("stored fiscal request is invalid")
    if request.get("status") not in {"requested", "needs_correction", "ready", "delivered", "canceled"}:
        raise StorageConflict("stored fiscal request is invalid")
    _positive_int(request.get("revision"), "revision")
    _safe_id(request.get("orderId"), "order_id")
    try:
        request["details"] = _fiscal_details(request.get("details"))
    except ValueError:
        raise StorageConflict("stored fiscal request is invalid") from None
    return request


def _request_projection(request: Mapping[str, Any], *, include_details: bool = False) -> dict[str, Any]:
    result = {
        "requestId": request["requestId"],
        "orderId": request["orderId"],
        "status": request["status"],
        "revision": request["revision"],
        "disclosureId": MANUAL_DISCLOSURE_ID,
        "manualDelivery": True,
    }
    for key in ("correctionReasonCode", "deliveryReferenceId"):
        if key in request:
            result[key] = request[key]
    if include_details:
        result["details"] = copy.deepcopy(request["details"])
    return result


def _scope(value: object) -> FiscalScope:
    if type(value) is not FiscalScope:
        raise ValueError("scope must be an immutable FiscalScope")
    return value


def _scope_fields(scope: FiscalScope) -> dict[str, str]:
    return {
        "environment": scope.environment,
        "tenantId": scope.tenant_id,
        "draftId": scope.draft_id,
        "domain": scope.domain,
    }


def _scope_matches(item: Mapping[str, Any], scope: FiscalScope) -> bool:
    return all(item.get(key) == expected for key, expected in _scope_fields(scope).items())


def _safe_id(value: object, field_name: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field_name} is invalid")
    return value


def _plain_text(value: object, field_name: str, maximum: int) -> str:
    if type(value) is not str or value != value.strip() or not 1 <= len(value) <= maximum:
        raise ValueError(f"{field_name} is invalid")
    if "<" in value or ">" in value or any(unicodedata.category(char) in {"Cc", "Cf"} for char in value):
        raise ValueError(f"{field_name} is invalid")
    return value


def _actor_hash(value: object) -> str:
    if type(value) is not str or _ACTOR_HASH.fullmatch(value) is None:
        raise ValueError("actor_hash is invalid")
    return value


def _epoch(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _positive_int(value: object, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _table_name(value: object) -> str:
    if type(value) is not str or not value.strip() or any(ord(character) < 33 for character in value):
        raise ValueError("table_name is invalid")
    return value


def _claim_digest(value: object) -> str:
    if type(value) is not str or _CLAIM_TOKEN.fullmatch(value) is None:
        raise ValueError("fiscal claim token is invalid")
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _idempotency_digest(value: object) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 256
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError("idempotency_key is invalid")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_json(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _put(table_name: str, item: Mapping[str, Any], condition: object) -> dict[str, Any]:
    return {"kind": "put", "table_name": table_name, "item": copy.deepcopy(dict(item)), "condition": condition}


def _client_token(scope: FiscalScope, operations: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        {"scope": scope.partition_key, "operations": operations},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:36]
