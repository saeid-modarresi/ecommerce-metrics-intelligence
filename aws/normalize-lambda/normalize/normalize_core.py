from typing import Any, Dict, Tuple
from .transform import extract_rows, normalize_row
from .validate import validate_record
from .dedupe import dedupe_by_date_keep_highest_revenue
from .output import rows_to_ndjson

def normalize_payload_to_ndjson(payload: Any, clamp_negatives: bool = True) -> Tuple[str, Dict[str, int]]:
    """Backward-compatible helper used by older handler code.
    Returns (ndjson_text, stats).
    """
    stats = {
        "discarded_missing_data": 0,
        "valid_rows": 0,
        "dropped_invalid_rows": 0,
        "duplicates_removed": 0,
        "written_rows": 0,
    }

    raw_rows, missing_data = extract_rows(payload)
    if missing_data:
        stats["discarded_missing_data"] = 1
        return "", stats

    normalized_rows = []
    dropped = 0
    for r in raw_rows:
        nr = normalize_row(r, clamp_negatives=clamp_negatives)
        ok, _reason = validate_record(nr)
        if not ok:
            dropped += 1
            continue
        normalized_rows.append(nr)

    stats["valid_rows"] = len(normalized_rows)
    stats["dropped_invalid_rows"] = dropped

    if not normalized_rows:
        return "", stats

    deduped, dup_removed = dedupe_by_date_keep_highest_revenue(normalized_rows)
    stats["duplicates_removed"] = dup_removed
    stats["written_rows"] = len(deduped)

    return rows_to_ndjson(deduped), stats
