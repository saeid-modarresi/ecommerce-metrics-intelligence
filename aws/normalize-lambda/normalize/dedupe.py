from typing import Dict, Any, List, Tuple

def dedupe_by_date_keep_highest_revenue(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """Deduplicate by date.
    Rule: keep record with highest revenue per date.
    Returns (deduped_rows, duplicates_removed_count).
    """
    best_by_date: Dict[str, Dict[str, Any]] = {}
    duplicates_removed = 0

    for r in rows:
        d = r.get("date")
        if not isinstance(d, str):
            # leave it; validator will drop
            continue

        if d not in best_by_date:
            best_by_date[d] = r
            continue

        prev = best_by_date[d]
        prev_rev = prev.get("revenue", 0.0) or 0.0
        cur_rev = r.get("revenue", 0.0) or 0.0
        if cur_rev > prev_rev:
            best_by_date[d] = r
        duplicates_removed += 1

    # deterministic order by date string
    deduped = [best_by_date[d] for d in sorted(best_by_date.keys())]
    return deduped, duplicates_removed
