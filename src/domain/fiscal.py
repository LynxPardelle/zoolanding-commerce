"""Non-PII fiscal policy identifiers."""

MANUAL_DISCLOSURE_ID = "manual-invoice-v1"
TAX_BEHAVIORS = frozenset({"exclusive", "inclusive", "provider-calculated"})


def validate_fiscal_policy(disclosure_id: object, tax_behavior: object) -> tuple[str, str]:
    if disclosure_id != MANUAL_DISCLOSURE_ID or not isinstance(disclosure_id, str):
        raise ValueError("unsupported fiscal disclosure")
    if not isinstance(tax_behavior, str) or tax_behavior not in TAX_BEHAVIORS:
        raise ValueError("unsupported tax behavior")
    return disclosure_id, tax_behavior
