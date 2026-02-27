import re
from typing import Dict, Any, Tuple, List, Optional

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

REQUIRED_FIELDS = ("date", "orders", "avg_order_value", "revenue")
ALLOWED_FIELDS = set(REQUIRED_FIELDS)

def is_valid_date(date_str: Any) -> bool:
    return isinstance(date_str, str) and bool(_DATE_RE.match(date_str))

def validate_record(rec: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate a normalized record.
    Must contain required fields and only allowed fields, and date must be valid format.
    """
    for f in REQUIRED_FIELDS:
        if f not in rec:
            return False, f"missing_field:{f}"
    if not is_valid_date(rec.get("date")):
        return False, "invalid_date"
    # only strict schema
    extra = set(rec.keys()) - ALLOWED_FIELDS
    if extra:
        return False, "extra_fields"
    return True, "ok"
