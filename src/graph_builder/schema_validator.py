from datetime import datetime
from numbers import Real
from typing import Any, Dict


REQUIRED_FIELDS = ("transaction_id", "account_id", "timestamp")
IDENTIFIER_FIELDS = ("transaction_id", "account_id", "device_id", "ip_address", "merchant_id", "card_id")


def validate_transaction(event: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        raise ValueError("transaction must be an object")

    missing = [field for field in REQUIRED_FIELDS if not event.get(field)]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    validated = dict(event)
    for field in IDENTIFIER_FIELDS:
        value = validated.get(field)
        if value is not None:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be a non-empty string")
            validated[field] = value.strip()

    try:
        datetime.fromisoformat(str(validated["timestamp"]))
    except (TypeError, ValueError) as exc:
        raise ValueError("timestamp must be an ISO-8601 datetime") from exc

    amount = validated.get("amount")
    if amount is not None and (not isinstance(amount, Real) or isinstance(amount, bool)):
        raise ValueError("amount must be numeric")

    return validated
