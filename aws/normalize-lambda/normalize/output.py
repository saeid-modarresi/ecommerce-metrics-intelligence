import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

_FILENAME_DATE_RE = re.compile(r"(\d{8})T")  # e.g. metrics_20260227T215207Z.json

def build_output_key(output_prefix: str, input_key: str, now_utc: datetime) -> str:
    """Build deterministic-ish output key.
    Prefer a date embedded in filename (YYYYMMDDT...) if present; else use processing date.
    Pattern: normalized/YYYY/MM/DD/<basename>.ndjson
    """
    basename = input_key.split("/")[-1]
    if basename.lower().endswith(".json"):
        basename = basename[:-5]

    yyyy, mm, dd = now_utc.strftime("%Y"), now_utc.strftime("%m"), now_utc.strftime("%d")

    m = _FILENAME_DATE_RE.search(basename)
    if m:
        ymd = m.group(1)
        yyyy, mm, dd = ymd[0:4], ymd[4:6], ymd[6:8]

    return f"{output_prefix}{yyyy}/{mm}/{dd}/{basename}.ndjson"


def rows_to_ndjson(rows: List[Dict[str, Any]]) -> str:
    """Serialize rows to NDJSON deterministically."""
    lines = [json.dumps(r, separators=(',', ':'), ensure_ascii=False) for r in rows]
    return "\n".join(lines) + ("\n" if lines else "")
