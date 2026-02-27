import re
from typing import Any, Optional, Tuple

_CURRENCY_RE = re.compile(r"[^0-9.\-]+")

def coerce_int(value: Any, default: int = 0) -> int:
    """Coerce messy values into an int.
    - None/null/missing -> default
    - Strings like "1,234" -> 1234
    - On failure -> default
    """
    if value is None:
        return default
    try:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int,)):
            return int(value)
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            v = value.strip()
            if v == "":
                return default
            v = v.replace(",", "")
            # Remove currency symbols and other noise
            v = _CURRENCY_RE.sub("", v)
            if v in ("", "-", ".", "-."):
                return default
            return int(float(v))
    except Exception:
        return default
    return default


def coerce_float(value: Any, default: float = 0.0) -> float:
    """Coerce messy values into a float.
    - None/null/missing -> default
    - Strings like "12,222.50" -> 12222.5
    - Currency like "$1,200.00" -> 1200.0
    - On failure -> default
    """
    if value is None:
        return default
    try:
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            v = value.strip()
            if v == "":
                return default
            v = v.replace(",", "")
            v = _CURRENCY_RE.sub("", v)
            if v in ("", "-", ".", "-."):
                return default
            return float(v)
    except Exception:
        return default
    return default


def clamp_non_negative_int(value: int) -> int:
    return value if value >= 0 else 0


def clamp_non_negative_float(value: float) -> float:
    return value if value >= 0 else 0.0
