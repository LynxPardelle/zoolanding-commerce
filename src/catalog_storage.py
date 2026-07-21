"""Conditional catalog persistence and sanitized public projection."""

from __future__ import annotations

import copy
import os
import re
from typing import Any, Mapping

try:  # Lambda packages src/ at the import root.
    from domain.catalog import CatalogItem, CatalogVariant, DataSpaceRecordReference
    from domain.offers import DiscountVersion, Money, OfferRecurrence, OfferVersion
    from storage import (
        CommerceScope,
        ConditionalWriteFailed,
        StorageConflict,
        StorageNotFound,
        StorageOutcomeUnknown,
        _DynamoBackend,
        _client_request_token,
        _from_item,
        _hash_json,
        _idempotency_digest,
        _metadata,
        _receipt_item,
        _validated_replay,
    )
except ModuleNotFoundError:  # Repository-root tests import src.*.
    from src.domain.catalog import CatalogItem, CatalogVariant, DataSpaceRecordReference
    from src.domain.offers import DiscountVersion, Money, OfferRecurrence, OfferVersion
    from src.storage import (
        CommerceScope,
        ConditionalWriteFailed,
        StorageConflict,
        StorageNotFound,
        StorageOutcomeUnknown,
        _DynamoBackend,
        _client_request_token,
        _from_item,
        _hash_json,
        _idempotency_digest,
        _metadata,
        _receipt_item,
        _validated_replay,
    )


MAX_LIST_SIZE = 100
MAX_PUBLIC_SCAN_PAGES = 2
_ACTOR_HASH = re.compile(r"[a-f0-9]{64}", re.ASCII)
_KINDS = {
    "items": ("CatalogItem", "CATALOG_ITEM#"),
    "offers": ("OfferVersion", "OFFER#"),
    "discounts": ("DiscountVersion", "DISCOUNT#"),
}


class CatalogStore:
    def __init__(
        self,
        backend: Any,
        catalog_table_name: str,
        operations_table_name: str | None = None,
    ):
        if not isinstance(catalog_table_name, str) or not catalog_table_name.strip():
            raise ValueError("catalog_table_name is required")
        if operations_table_name is not None and (
            not isinstance(operations_table_name, str) or not operations_table_name.strip()
        ):
            raise ValueError("operations_table_name must be a non-empty string when present")
        if operations_table_name == catalog_table_name:
            raise ValueError("catalog and operations tables must be distinct")
        self.backend = backend
        self.catalog_table_name = catalog_table_name
        self.operations_table_name = operations_table_name

    @classmethod
    def from_environment(cls, *, mutations: bool = False) -> "CatalogStore":
        table_name = os.getenv("COMMERCE_CATALOG_TABLE_NAME", "").strip()
        if not table_name:
            raise RuntimeError("Commerce catalog table name is required")
        operations_table_name = os.getenv("COMMERCE_OPERATIONS_TABLE_NAME", "").strip()
        if mutations and not operations_table_name:
            raise RuntimeError("Commerce operations table name is required for catalog mutations")
        import boto3  # type: ignore

        return cls(
            _DynamoCatalogBackend(boto3.client("dynamodb")),
            table_name,
            operations_table_name or None,
        )

    def create_item(
        self,
        scope: CommerceScope,
        item: CatalogItem,
        *,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str,
        now_epoch: int,
    ) -> dict[str, Any]:
        _scope(scope)
        if type(item) is not CatalogItem:
            raise ValueError("item must be an immutable CatalogItem")
        fields = _catalog_item_fields(item)
        replay, receipt, metadata = self._begin_mutation(
            scope,
            idempotency_key,
            {"action": "createItem", "definition": fields},
            request_id,
            correlation_id,
            actor_hash,
            now_epoch,
        )
        if replay is not None:
            return replay
        stored = {
            "pk": scope.partition_key,
            "sk": f"CATALOG_ITEM#{item.item_id}",
            "itemType": "CatalogItem",
            **_scope_fields(scope),
            **fields,
            "revision": 1,
            **_audit_fields(metadata),
        }
        result = _admin_projection(stored)
        return self._write_mutation(scope, stored, "absent", receipt, result, metadata)

    def create_offer(
        self,
        scope: CommerceScope,
        offer: OfferVersion,
        *,
        supported_currencies: frozenset[str],
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str,
        now_epoch: int,
    ) -> dict[str, Any]:
        _scope(scope)
        if type(offer) is not OfferVersion:
            raise ValueError("offer must be an immutable OfferVersion")
        currencies = _currencies(supported_currencies)
        if offer.unit_price.currency not in currencies:
            raise ValueError("offer currency is not enabled by the owning policy")
        fields = _offer_fields(offer)
        replay, receipt, metadata = self._begin_mutation(
            scope,
            idempotency_key,
            {"action": "createOfferVersion", "definition": fields},
            request_id,
            correlation_id,
            actor_hash,
            now_epoch,
        )
        if replay is not None:
            return replay
        item = self._catalog_item(scope, offer.catalog_item_id)
        if item.sellable_type != offer.sellable_type:
            raise ValueError("offer sellable type does not match its catalog item")
        variant_ids = {variant.variant_id for variant in item.variants}
        if offer.variant_id is not None and offer.variant_id not in variant_ids:
            raise ValueError("offer variant does not belong to its catalog item")
        stored = {
            "pk": scope.partition_key,
            "sk": f"OFFER#{offer.version_id}",
            "itemType": "OfferVersion",
            **_scope_fields(scope),
            **fields,
            **_audit_fields(metadata),
        }
        result = _admin_projection(stored)
        return self._write_mutation(scope, stored, "absent", receipt, result, metadata)

    def create_discount(
        self,
        scope: CommerceScope,
        discount: DiscountVersion,
        *,
        supported_currencies: frozenset[str],
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str,
        now_epoch: int,
    ) -> dict[str, Any]:
        _scope(scope)
        if type(discount) is not DiscountVersion:
            raise ValueError("discount must be an immutable DiscountVersion")
        currencies = _currencies(supported_currencies)
        if discount.fixed_amount is not None and discount.fixed_amount.currency not in currencies:
            raise ValueError("discount currency is not enabled by the owning policy")
        fields = _discount_fields(discount)
        replay, receipt, metadata = self._begin_mutation(
            scope,
            idempotency_key,
            {"action": "createDiscountVersion", "definition": fields},
            request_id,
            correlation_id,
            actor_hash,
            now_epoch,
        )
        if replay is not None:
            return replay
        for offer_id in discount.eligible_offer_version_ids:
            self._offer(scope, offer_id, currencies)
        stored = {
            "pk": scope.partition_key,
            "sk": f"DISCOUNT#{discount.version_id}",
            "itemType": "DiscountVersion",
            **_scope_fields(scope),
            **fields,
            **_audit_fields(metadata),
        }
        result = _admin_projection(stored)
        return self._write_mutation(scope, stored, "absent", receipt, result, metadata)

    def advance_offer_lifecycle(
        self,
        scope: CommerceScope,
        version_id: str,
        target_state: str,
        expected_revision: int,
        supported_currencies: frozenset[str],
        *,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str,
        now_epoch: int,
    ) -> dict[str, Any]:
        version_id = _safe_id(version_id)
        expected_revision = _positive_revision(expected_revision)
        currencies = _currencies(supported_currencies)
        replay, receipt, metadata = self._begin_mutation(
            scope,
            idempotency_key,
            {
                "action": "advanceOfferLifecycle",
                "versionId": version_id,
                "targetState": target_state,
                "expectedRevision": expected_revision,
            },
            request_id,
            correlation_id,
            actor_hash,
            now_epoch,
        )
        if replay is not None:
            return replay
        current = self._offer(scope, version_id, currencies)
        if current.lifecycle_revision != expected_revision:
            raise StorageConflict("offer lifecycle revision changed")
        updated = current.with_lifecycle(target_state, expected_revision + 1)
        return self._replace_mutation(
            scope,
            f"OFFER#{updated.version_id}",
            {**_offer_fields(updated), **_audit_fields(metadata)},
            {
                "lifecycleRevision": expected_revision,
                "presentationRevision": current.presentation_revision,
            },
            receipt,
            metadata,
        )

    def update_offer_presentation(
        self,
        scope: CommerceScope,
        version_id: str,
        expected_revision: int,
        supported_currencies: frozenset[str],
        *,
        display_name: str | None,
        display_description: str | None,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str,
        now_epoch: int,
    ) -> dict[str, Any]:
        version_id = _safe_id(version_id)
        expected_revision = _positive_revision(expected_revision)
        currencies = _currencies(supported_currencies)
        replay, receipt, metadata = self._begin_mutation(
            scope,
            idempotency_key,
            {
                "action": "updateOfferPresentation",
                "versionId": version_id,
                "expectedRevision": expected_revision,
                "displayName": display_name,
                "displayDescription": display_description,
            },
            request_id,
            correlation_id,
            actor_hash,
            now_epoch,
        )
        if replay is not None:
            return replay
        current = self._offer(scope, version_id, currencies)
        if current.presentation_revision != expected_revision:
            raise StorageConflict("offer presentation revision changed")
        updated = current.with_presentation(
            expected_revision + 1,
            display_name=display_name,
            display_description=display_description,
        )
        return self._replace_mutation(
            scope,
            f"OFFER#{updated.version_id}",
            {**_offer_fields(updated), **_audit_fields(metadata)},
            {
                "lifecycleRevision": current.lifecycle_revision,
                "presentationRevision": expected_revision,
            },
            receipt,
            metadata,
        )

    def advance_discount_lifecycle(
        self,
        scope: CommerceScope,
        version_id: str,
        target_state: str,
        expected_revision: int,
        supported_currencies: frozenset[str],
        *,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str,
        now_epoch: int,
    ) -> dict[str, Any]:
        version_id = _safe_id(version_id)
        expected_revision = _positive_revision(expected_revision)
        currencies = _currencies(supported_currencies)
        replay, receipt, metadata = self._begin_mutation(
            scope,
            idempotency_key,
            {
                "action": "advanceDiscountLifecycle",
                "versionId": version_id,
                "targetState": target_state,
                "expectedRevision": expected_revision,
            },
            request_id,
            correlation_id,
            actor_hash,
            now_epoch,
        )
        if replay is not None:
            return replay
        current = self._discount(scope, version_id, currencies)
        if current.lifecycle_revision != expected_revision:
            raise StorageConflict("discount lifecycle revision changed")
        updated = current.with_lifecycle(target_state, expected_revision + 1)
        return self._replace_mutation(
            scope,
            f"DISCOUNT#{updated.version_id}",
            {**_discount_fields(updated), **_audit_fields(metadata)},
            {
                "lifecycleRevision": expected_revision,
                "presentationRevision": current.presentation_revision,
            },
            receipt,
            metadata,
        )

    def update_discount_presentation(
        self,
        scope: CommerceScope,
        version_id: str,
        expected_revision: int,
        supported_currencies: frozenset[str],
        *,
        display_name: str | None,
        display_description: str | None,
        idempotency_key: str,
        request_id: str,
        correlation_id: str,
        actor_hash: str,
        now_epoch: int,
    ) -> dict[str, Any]:
        version_id = _safe_id(version_id)
        expected_revision = _positive_revision(expected_revision)
        currencies = _currencies(supported_currencies)
        replay, receipt, metadata = self._begin_mutation(
            scope,
            idempotency_key,
            {
                "action": "updateDiscountPresentation",
                "versionId": version_id,
                "expectedRevision": expected_revision,
                "displayName": display_name,
                "displayDescription": display_description,
            },
            request_id,
            correlation_id,
            actor_hash,
            now_epoch,
        )
        if replay is not None:
            return replay
        current = self._discount(scope, version_id, currencies)
        if current.presentation_revision != expected_revision:
            raise StorageConflict("discount presentation revision changed")
        updated = current.with_presentation(
            expected_revision + 1,
            display_name=display_name,
            display_description=display_description,
        )
        return self._replace_mutation(
            scope,
            f"DISCOUNT#{updated.version_id}",
            {**_discount_fields(updated), **_audit_fields(metadata)},
            {
                "lifecycleRevision": current.lifecycle_revision,
                "presentationRevision": expected_revision,
            },
            receipt,
            metadata,
        )

    def get_catalog(
        self,
        scope: CommerceScope,
        kind: str,
        resource_id: str,
        supported_currencies: frozenset[str],
    ) -> dict[str, Any]:
        item_type, prefix = _kind(kind)
        stored = self.backend.get(
            self.catalog_table_name, scope.partition_key, f"{prefix}{_safe_id(resource_id)}"
        )
        selected = _stored(stored, scope, item_type)
        _validate_economics(selected, item_type, _currencies(supported_currencies))
        return _admin_projection(selected)

    def list_catalog(
        self,
        scope: CommerceScope,
        kind: str,
        limit: int,
        cursor: str | None,
        supported_currencies: frozenset[str],
    ) -> tuple[list[dict[str, Any]], str | None]:
        item_type, prefix = _kind(kind)
        currencies = _currencies(supported_currencies)
        items, next_cursor = self.backend.query_prefix(
            self.catalog_table_name,
            scope.partition_key,
            prefix,
            _limit(limit),
            _storage_cursor(prefix, cursor),
        )
        output = []
        for item in items:
            selected = _stored(item, scope, item_type)
            _validate_economics(selected, item_type, currencies)
            output.append(_admin_projection(selected))
        return output, _storage_cursor(prefix, next_cursor)

    def get_checkout_offer(
        self,
        scope: CommerceScope,
        version_id: str,
        supported_currencies: frozenset[str],
    ) -> tuple[OfferVersion, CatalogItem]:
        offer = self._offer(scope, version_id, _currencies(supported_currencies))
        if offer.lifecycle_state != "active":
            raise StorageNotFound("offer is not available")
        return offer, self._catalog_item(scope, offer.catalog_item_id)

    def get_offer_version(
        self,
        scope: CommerceScope,
        version_id: str,
        supported_currencies: frozenset[str],
    ) -> OfferVersion:
        return self._offer(scope, version_id, _currencies(supported_currencies))

    def get_discount_version(
        self,
        scope: CommerceScope,
        version_id: str,
        supported_currencies: frozenset[str],
    ) -> DiscountVersion:
        return self._discount(scope, version_id, _currencies(supported_currencies))

    def get_checkout_discount(
        self,
        scope: CommerceScope,
        version_id: str,
        supported_currencies: frozenset[str],
    ) -> DiscountVersion:
        discount = self.get_discount_version(scope, version_id, supported_currencies)
        if discount.lifecycle_state != "active":
            raise StorageNotFound("discount is not available")
        return discount

    def get_public_offer(
        self,
        scope: CommerceScope,
        version_id: str,
        supported_currencies: frozenset[str],
    ) -> dict[str, Any]:
        offer, _item = self.get_checkout_offer(scope, version_id, supported_currencies)
        return _public_offer(offer)

    def list_public_offers(
        self,
        scope: CommerceScope,
        limit: int,
        cursor: str | None,
        supported_currencies: frozenset[str],
    ) -> tuple[list[dict[str, Any]], str | None]:
        requested = _limit(limit)
        currencies = _currencies(supported_currencies)
        page_cursor = _storage_cursor("OFFER#", cursor)
        result: list[dict[str, Any]] = []
        last_inspected = page_cursor
        for page_number in range(MAX_PUBLIC_SCAN_PAGES):
            items, next_cursor = self.backend.query_prefix(
                self.catalog_table_name,
                scope.partition_key,
                "OFFER#",
                MAX_LIST_SIZE,
                page_cursor,
            )
            validated_next = _storage_cursor("OFFER#", next_cursor)
            for index, stored in enumerate(items):
                selected = _stored(stored, scope, "OfferVersion")
                inspected_cursor = _storage_cursor("OFFER#", selected.get("sk"))
                last_inspected = inspected_cursor
                offer = _rehydrated_offer(selected, currencies)
                if offer.lifecycle_state != "active":
                    continue
                result.append(_public_offer(offer))
                if len(result) == requested:
                    has_more = index < len(items) - 1 or validated_next is not None
                    return result, inspected_cursor if has_more else None
            if validated_next is None:
                return result, None
            if validated_next == page_cursor:
                raise StorageConflict("catalog pagination did not advance")
            if page_number == MAX_PUBLIC_SCAN_PAGES - 1:
                if last_inspected is None:
                    raise StorageConflict("catalog pagination did not inspect an item")
                return result, last_inspected
            page_cursor = validated_next
        raise StorageConflict("catalog pagination exceeded its work budget")

    def _catalog_item(self, scope: CommerceScope, item_id: str) -> CatalogItem:
        stored = self.backend.get(
            self.catalog_table_name, scope.partition_key, f"CATALOG_ITEM#{_safe_id(item_id)}"
        )
        return _catalog_item(_stored(stored, scope, "CatalogItem"))

    def _offer(
        self,
        scope: CommerceScope,
        version_id: str,
        supported_currencies: frozenset[str],
    ) -> OfferVersion:
        stored = self.backend.get(
            self.catalog_table_name, scope.partition_key, f"OFFER#{_safe_id(version_id)}"
        )
        return _rehydrated_offer(
            _stored(stored, scope, "OfferVersion"),
            _currencies(supported_currencies),
        )

    def _discount(
        self,
        scope: CommerceScope,
        version_id: str,
        supported_currencies: frozenset[str],
    ) -> DiscountVersion:
        stored = self.backend.get(
            self.catalog_table_name, scope.partition_key, f"DISCOUNT#{_safe_id(version_id)}"
        )
        return _rehydrated_discount(
            _stored(stored, scope, "DiscountVersion"),
            _currencies(supported_currencies),
        )

    def _begin_mutation(
        self,
        scope: CommerceScope,
        idempotency_key: str,
        request: Mapping[str, Any],
        request_id: str,
        correlation_id: str,
        actor_hash: str,
        now_epoch: int,
    ) -> tuple[dict[str, Any] | None, dict[str, str], dict[str, Any]]:
        _scope(scope)
        operations_table = self._operations_table()
        metadata = _metadata(request_id, correlation_id, actor_hash, now_epoch)
        digest = _idempotency_digest(idempotency_key)
        request_hash = _hash_json(request)
        receipt = {
            "sk": f"IDEMPOTENCY#{digest}",
            "digest": digest,
            "requestHash": request_hash,
        }
        existing = self.backend.get(operations_table, scope.partition_key, receipt["sk"])
        replay = None if existing is None else _validated_replay(existing, request_hash, scope)
        return replay, receipt, metadata

    def _replace_mutation(
        self,
        scope: CommerceScope,
        sk: str,
        fields: Mapping[str, Any],
        condition: Mapping[str, Any],
        receipt: Mapping[str, str],
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        current = self.backend.get(self.catalog_table_name, scope.partition_key, sk)
        if not isinstance(current, Mapping):
            raise StorageNotFound("catalog resource was not found")
        updated = {**dict(current), **copy.deepcopy(dict(fields))}
        result = _admin_projection(updated)
        return self._write_mutation(scope, updated, dict(condition), receipt, result, metadata)

    def _write_mutation(
        self,
        scope: CommerceScope,
        item: Mapping[str, Any],
        condition: Any,
        receipt: Mapping[str, str],
        result: Mapping[str, Any],
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        operations_table = self._operations_table()
        operations = [
            {
                "kind": "put",
                "table_name": self.catalog_table_name,
                "item": copy.deepcopy(dict(item)),
                "condition": condition,
            },
            {
                "kind": "put",
                "table_name": operations_table,
                "item": _receipt_item(scope, receipt, result, metadata),
                "condition": "absent",
            },
        ]
        try:
            self.backend.transact(operations, _client_request_token(scope, operations))
        except ConditionalWriteFailed:
            return self._replay_after_failure(scope, receipt, conditional=True)
        except Exception:
            return self._replay_after_failure(scope, receipt, conditional=False)
        return copy.deepcopy(dict(result))

    def _replay_after_failure(
        self,
        scope: CommerceScope,
        receipt: Mapping[str, str],
        *,
        conditional: bool,
    ) -> dict[str, Any]:
        try:
            existing = self.backend.get(
                self._operations_table(), scope.partition_key, receipt["sk"]
            )
        except Exception:
            raise StorageOutcomeUnknown("catalog mutation outcome is unknown") from None
        if existing is not None:
            return _validated_replay(existing, receipt["requestHash"], scope)
        if conditional:
            raise StorageConflict("catalog revision changed") from None
        raise StorageOutcomeUnknown("catalog mutation outcome is unknown") from None

    def _operations_table(self) -> str:
        if self.operations_table_name is None:
            raise RuntimeError("Commerce operations table is required for catalog mutations")
        return self.operations_table_name


class _DynamoCatalogBackend:
    def __init__(self, client: Any):
        self.client = client
        self.transactions = _DynamoBackend(client)

    def get(self, table_name: str, pk: str, sk: str):
        response = self.client.get_item(
            TableName=table_name,
            Key={"pk": {"S": pk}, "sk": {"S": sk}},
            ConsistentRead=True,
        )
        item = response.get("Item")
        return _from_item(item) if item else None

    def transact(self, operations: list[dict[str, Any]], client_token: str) -> None:
        self.transactions.transact(operations, client_token)

    def query_prefix(
        self,
        table_name: str,
        pk: str,
        prefix: str,
        limit: int,
        cursor: str | None = None,
    ):
        params = {
            "TableName": table_name,
            "KeyConditionExpression": "#pk = :pk AND begins_with(#sk, :prefix)",
            "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
            "ExpressionAttributeValues": {
                ":pk": {"S": pk},
                ":prefix": {"S": prefix},
            },
            "Limit": limit,
            "ConsistentRead": True,
        }
        if cursor is not None:
            params["ExclusiveStartKey"] = {"pk": {"S": pk}, "sk": {"S": cursor}}
        response = self.client.query(**params)
        items = [_from_item(item) for item in response.get("Items", [])]
        last_key = response.get("LastEvaluatedKey")
        if last_key is None:
            return items, None
        if (
            not isinstance(last_key, Mapping)
            or last_key.get("pk") != {"S": pk}
            or not isinstance(last_key.get("sk"), Mapping)
            or not isinstance(last_key["sk"].get("S"), str)
        ):
            raise StorageConflict("catalog pagination state is invalid")
        return items, last_key["sk"]["S"]


def _scope(value: object) -> CommerceScope:
    if type(value) is not CommerceScope:
        raise ValueError("scope must be an immutable CommerceScope")
    return value


def _scope_fields(scope: CommerceScope) -> dict[str, str]:
    return {
        "environment": scope.environment,
        "tenantId": scope.tenant_id,
        "draftId": scope.draft_id,
        "domain": scope.domain,
    }


def _stored(item: Any, scope: CommerceScope, item_type: str) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise StorageNotFound("catalog resource was not found")
    if (
        item.get("pk") != scope.partition_key
        or item.get("environment") != scope.environment
        or item.get("tenantId") != scope.tenant_id
        or item.get("draftId") != scope.draft_id
        or item.get("domain") != scope.domain
        or item.get("itemType") != item_type
    ):
        raise StorageConflict("stored catalog scope is invalid")
    return copy.deepcopy(dict(item))


def _catalog_item_fields(item: CatalogItem) -> dict[str, Any]:
    reference = item.data_space_reference
    return {
        "itemId": item.item_id,
        "sellableType": item.sellable_type,
        "variants": [{"variantId": value.variant_id, "sku": value.sku} for value in item.variants],
        "dataSpaceReference": None if reference is None else {
            "spaceId": reference.space_id,
            "collectionId": reference.collection_id,
            "recordId": reference.record_id,
            "revision": reference.revision,
            "fieldIds": list(reference.field_ids),
        },
    }


def _offer_fields(offer: OfferVersion) -> dict[str, Any]:
    recurrence = offer.recurrence
    return {
        "versionId": offer.version_id,
        "catalogItemId": offer.catalog_item_id,
        "variantId": offer.variant_id,
        "revision": offer.revision,
        "sellableType": offer.sellable_type,
        "amountMinor": offer.unit_price.amount_minor,
        "currency": offer.unit_price.currency,
        "taxBehavior": offer.tax_behavior,
        "recurrence": None if recurrence is None else {
            "interval": recurrence.interval,
            "intervalCount": recurrence.interval_count,
            "billingScheme": recurrence.billing_scheme,
            "usageType": recurrence.usage_type,
        },
        "lifecycleState": offer.lifecycle_state,
        "lifecycleRevision": offer.lifecycle_revision,
        "presentationRevision": offer.presentation_revision,
        "displayName": offer.display_name,
        "displayDescription": offer.display_description,
        "providerFingerprint": offer.provider_fingerprint,
    }


def _discount_fields(discount: DiscountVersion) -> dict[str, Any]:
    fixed = discount.fixed_amount
    return {
        "versionId": discount.version_id,
        "revision": discount.revision,
        "duration": discount.duration,
        "percentageBasisPoints": discount.percentage_basis_points,
        "fixedAmount": None if fixed is None else {
            "amountMinor": fixed.amount_minor, "currency": fixed.currency
        },
        "durationInMonths": discount.duration_in_months,
        "eligibleOfferVersionIds": sorted(discount.eligible_offer_version_ids),
        "redemptionLimit": discount.redemption_limit,
        "redeemByEpoch": discount.redeem_by_epoch,
        "customerFacingCode": discount.customer_facing_code,
        "lifecycleState": discount.lifecycle_state,
        "lifecycleRevision": discount.lifecycle_revision,
        "presentationRevision": discount.presentation_revision,
        "displayName": discount.display_name,
        "displayDescription": discount.display_description,
        "providerFingerprint": discount.provider_fingerprint,
    }


def _catalog_item(item: Mapping[str, Any]) -> CatalogItem:
    reference = item.get("dataSpaceReference")
    return CatalogItem(
        item_id=item.get("itemId"),
        sellable_type=item.get("sellableType"),
        variants=tuple(
            CatalogVariant(value.get("variantId"), value.get("sku"))
            for value in item.get("variants", [])
            if isinstance(value, Mapping)
        ),
        data_space_reference=None if reference is None else DataSpaceRecordReference(
            reference.get("spaceId"), reference.get("collectionId"), reference.get("recordId"),
            reference.get("revision"), tuple(reference.get("fieldIds", [])),
        ),
    )


def _offer(
    item: Mapping[str, Any],
    supported_currencies: frozenset[str],
) -> OfferVersion:
    recurrence = item.get("recurrence")
    currency = item.get("currency")
    return OfferVersion(
        version_id=item.get("versionId"),
        catalog_item_id=item.get("catalogItemId"),
        variant_id=item.get("variantId"),
        revision=item.get("revision"),
        sellable_type=item.get("sellableType"),
        unit_price=Money(item.get("amountMinor"), currency, supported_currencies),
        tax_behavior=item.get("taxBehavior"),
        recurrence=None if recurrence is None else OfferRecurrence(
            recurrence.get("interval"), recurrence.get("intervalCount"),
            recurrence.get("billingScheme"), recurrence.get("usageType"),
        ),
        lifecycle_state=item.get("lifecycleState"),
        lifecycle_revision=item.get("lifecycleRevision"),
        presentation_revision=item.get("presentationRevision"),
        display_name=item.get("displayName"),
        display_description=item.get("displayDescription"),
    )


def _discount(
    item: Mapping[str, Any],
    supported_currencies: frozenset[str],
) -> DiscountVersion:
    fixed = item.get("fixedAmount")
    currency = fixed.get("currency") if isinstance(fixed, Mapping) else None
    return DiscountVersion(
        version_id=item.get("versionId"),
        revision=item.get("revision"),
        duration=item.get("duration"),
        percentage_basis_points=item.get("percentageBasisPoints"),
        fixed_amount=None if fixed is None else Money(
            fixed.get("amountMinor"), currency, supported_currencies
        ),
        duration_in_months=item.get("durationInMonths"),
        eligible_offer_version_ids=frozenset(item.get("eligibleOfferVersionIds", [])),
        redemption_limit=item.get("redemptionLimit"),
        redeem_by_epoch=item.get("redeemByEpoch"),
        customer_facing_code=item.get("customerFacingCode"),
        lifecycle_state=item.get("lifecycleState"),
        lifecycle_revision=item.get("lifecycleRevision"),
        presentation_revision=item.get("presentationRevision"),
        display_name=item.get("displayName"),
        display_description=item.get("displayDescription"),
    )


def _public_offer(offer: OfferVersion) -> dict[str, Any]:
    recurrence = offer.recurrence
    return {
        "offerVersionId": offer.version_id,
        "catalogItemId": offer.catalog_item_id,
        "variantId": offer.variant_id,
        "sellableType": offer.sellable_type,
        "saleType": offer.sale_type,
        "amountMinor": offer.unit_price.amount_minor,
        "currency": offer.unit_price.currency,
        "taxBehavior": offer.tax_behavior,
        "recurrence": None if recurrence is None else {
            "interval": recurrence.interval,
            "intervalCount": recurrence.interval_count,
        },
        "displayName": offer.display_name,
        "displayDescription": offer.display_description,
    }


def _admin_projection(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: copy.deepcopy(value)
        for key, value in item.items()
        if key not in {"pk", "sk", "environment", "tenantId", "draftId", "domain", "actorHash"}
    }


def _audit_fields(metadata: Mapping[str, Any]) -> dict[str, Any]:
    actor_hash = metadata.get("actor_hash")
    now_epoch = metadata.get("now_epoch")
    request_id = metadata.get("request_id")
    correlation_id = metadata.get("correlation_id")
    if not isinstance(actor_hash, str) or not _ACTOR_HASH.fullmatch(actor_hash):
        raise ValueError("actor_hash must be a lowercase SHA-256 digest")
    if not isinstance(now_epoch, int) or isinstance(now_epoch, bool) or now_epoch < 0:
        raise ValueError("now_epoch must be a non-negative integer")
    if not isinstance(request_id, str) or not isinstance(correlation_id, str):
        raise ValueError("request metadata is invalid")
    return {
        "actorHash": actor_hash,
        "requestId": request_id,
        "correlationId": correlation_id,
        "updatedAt": now_epoch,
    }


def _kind(value: object) -> tuple[str, str]:
    if not isinstance(value, str) or value not in _KINDS:
        raise ValueError("unsupported catalog kind")
    return _KINDS[value]


def _limit(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= MAX_LIST_SIZE:
        raise ValueError("limit must be between 1 and 100")
    return value


def _currencies(value: object) -> frozenset[str]:
    if (
        type(value) is not frozenset
        or not 1 <= len(value) <= 16
        or any(
            not isinstance(currency, str)
            or re.fullmatch(r"[A-Z]{3}", currency, re.ASCII) is None
            for currency in value
        )
    ):
        raise ValueError("supported_currencies must contain 1 to 16 canonical codes")
    return value


def _storage_cursor(prefix: str, value: object) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(prefix, str)
        or prefix not in {"CATALOG_ITEM#", "OFFER#", "DISCOUNT#"}
        or not isinstance(value, str)
        or not value.startswith(prefix)
    ):
        raise ValueError("catalog cursor is invalid")
    _safe_id(value[len(prefix):])
    return value


def _validate_economics(
    item: Mapping[str, Any],
    item_type: str,
    supported_currencies: frozenset[str],
) -> None:
    if item_type == "OfferVersion":
        _rehydrated_offer(item, supported_currencies)
    elif item_type == "DiscountVersion":
        _rehydrated_discount(item, supported_currencies)


def _rehydrated_offer(
    item: Mapping[str, Any],
    supported_currencies: frozenset[str],
) -> OfferVersion:
    try:
        return _offer(item, supported_currencies)
    except ValueError:
        raise StorageConflict("stored offer economics are invalid") from None


def _rehydrated_discount(
    item: Mapping[str, Any],
    supported_currencies: frozenset[str],
) -> DiscountVersion:
    try:
        return _discount(item, supported_currencies)
    except ValueError:
        raise StorageConflict("stored discount economics are invalid") from None


def _positive_revision(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError("expected_revision must be a positive integer")
    return value


def _safe_id(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", value):
        raise ValueError("resource_id must be a safe canonical identifier")
    return value
