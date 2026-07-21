"""Provider-neutral catalog identity and immutable Data Spaces references."""

from dataclasses import dataclass
import re

SELLABLE_TYPES = frozenset({"physical", "service", "subscription", "add_on"})
_SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", re.ASCII)
_SAFE_FIELD_ID = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,63}", re.ASCII)
_SAFE_SKU = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", re.ASCII)


def validate_sellable_type(value: object) -> str:
    if not isinstance(value, str) or value not in SELLABLE_TYPES:
        raise ValueError("unsupported sellable type")
    return value


def _validate_safe_id(value: object, field_name: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe canonical identifier")
    return value


def _validate_positive_revision(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError("revision must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class DataSpaceRecordReference:
    """Pointer to one exact, allowlisted Data Spaces record revision.

    The reference deliberately carries no scope, snapshot values, or customer
    data. The authorized activation handler will derive scope and resolve the
    pinned snapshot in TASK-030.
    """

    space_id: str
    collection_id: str
    record_id: str
    revision: int
    field_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_safe_id(self.space_id, "space_id")
        _validate_safe_id(self.collection_id, "collection_id")
        _validate_safe_id(self.record_id, "record_id")
        _validate_positive_revision(self.revision)
        if type(self.field_ids) is not tuple or not 1 <= len(self.field_ids) <= 200:
            raise ValueError("field_ids must be a tuple containing 1 to 200 identifiers")
        if any(
            type(field_id) is not str or _SAFE_FIELD_ID.fullmatch(field_id) is None
            for field_id in self.field_ids
        ):
            raise ValueError("field_ids contains an unsafe identifier")
        if len(set(self.field_ids)) != len(self.field_ids):
            raise ValueError("field_ids must be unique")


@dataclass(frozen=True, slots=True)
class CatalogVariant:
    variant_id: str
    sku: str

    def __post_init__(self) -> None:
        _validate_safe_id(self.variant_id, "variant_id")
        if type(self.sku) is not str or _SAFE_SKU.fullmatch(self.sku) is None:
            raise ValueError("sku must be a safe canonical identifier")


@dataclass(frozen=True, slots=True)
class CatalogItem:
    item_id: str
    sellable_type: str
    variants: tuple[CatalogVariant, ...] = ()
    data_space_reference: DataSpaceRecordReference | None = None

    def __post_init__(self) -> None:
        _validate_safe_id(self.item_id, "item_id")
        validate_sellable_type(self.sellable_type)
        if type(self.variants) is not tuple or any(
            type(variant) is not CatalogVariant for variant in self.variants
        ):
            raise ValueError("variants must be a tuple of CatalogVariant values")
        variant_ids = [variant.variant_id for variant in self.variants]
        sku_keys = [variant.sku for variant in self.variants]
        if len(set(variant_ids)) != len(variant_ids):
            raise ValueError("variant_id values must be unique within an item")
        if len(set(sku_keys)) != len(sku_keys):
            raise ValueError("sku values must be unique within an item")
        if (
            self.data_space_reference is not None
            and type(self.data_space_reference) is not DataSpaceRecordReference
        ):
            raise ValueError("data_space_reference must be immutable")
