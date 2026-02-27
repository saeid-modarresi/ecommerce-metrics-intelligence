from typing import Any, Dict, List, Tuple
from .coerce import coerce_int, coerce_float, clamp_non_negative_int, clamp_non_negative_float

def normalize_row(raw_row: Dict[str, Any], clamp_negatives: bool = True) -> Dict[str, Any]:
    """Transform a raw row into strict schema (may still be invalid due to date)."""
    date_val = raw_row.get("date")
    orders = coerce_int(raw_row.get("orders"), default=0)
    aov = coerce_float(raw_row.get("avg_order_value"), default=0.0)
    revenue = coerce_float(raw_row.get("revenue"), default=0.0)

    if clamp_negatives:
        orders = clamp_non_negative_int(orders)
        aov = clamp_non_negative_float(aov)
        revenue = clamp_non_negative_float(revenue)

    return {
        "date": date_val,
        "orders": orders,
        "avg_order_value": aov,
        "revenue": revenue,
    }


def extract_rows(payload: Any) -> Tuple[List[Dict[str, Any]], bool]:
    """Extract rows from payload. Returns (rows, discarded_missing_data_flag)."""
    if not isinstance(payload, dict):
        return [], True
    data = payload.get("data")
    if not isinstance(data, list):
        return [], True
    rows = [r for r in data if isinstance(r, dict)]
    return rows, False
