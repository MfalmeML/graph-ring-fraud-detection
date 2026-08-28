from typing import Any, Dict

from .schema_validator import validate_transaction


class EntityResolver:
    """Canonicalize identifiers before they become graph node identities."""

    def resolve_transaction(self, event: Dict[str, Any]) -> Dict[str, Any]:
        resolved = validate_transaction(event)
        for field in ("account_id", "device_id", "ip_address", "merchant_id", "card_id"):
            value = resolved.get(field)
            if isinstance(value, str):
                resolved[field] = value.strip()
        return resolved
