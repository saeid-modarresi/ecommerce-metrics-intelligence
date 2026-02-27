import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

from .s3_io import read_object_bytes, write_text_object, object_exists
from .transform import extract_rows, normalize_row
from .validate import validate_record
from .dedupe import dedupe_by_date_keep_highest_revenue
from .output import build_output_key, rows_to_ndjson
from .metrics import emit_emf, default_namespace

RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "normalized/")
CLAMP_NEGATIVES = os.getenv("CLAMP_NEGATIVES", "true").lower() == "true"
FAIL_ON_IO_ERRORS = os.getenv("FAIL_ON_IO_ERRORS", "true").lower() == "true"

def process_one(bucket: str, key: str, now_utc: datetime) -> Dict[str, Any]:
    """Process a single S3 object key. Returns stats dict. May raise on fatal I/O errors."""
    stats = {
        "bucket": bucket,
        "key": key,
        "output_key": None,
        "skipped_existing": 0,
        "invalid_json": 0,
        "discarded_missing_data": 0,
        "valid_rows": 0,
        "written_rows": 0,
        "dropped_invalid_rows": 0,
        "duplicates_removed": 0,
        "written_object": 0,
        "missing_data_file": 0,
        "zero_valid_rows_file": 0,
    }

    output_key = build_output_key(OUTPUT_PREFIX, key, now_utc)
    stats["output_key"] = output_key

    if object_exists(bucket, output_key):
        stats["skipped_existing"] = 1
        return stats

    # read
    raw_bytes = read_object_bytes(bucket, key)

    # parse json
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        stats["invalid_json"] = 1
        return stats

    raw_rows, missing_data = extract_rows(payload)
    if missing_data:
        stats["discarded_missing_data"] = 1
        stats["missing_data_file"] = 1
        return stats

    # transform + validate
    normalized_rows = []
    dropped = 0
    for r in raw_rows:
        nr = normalize_row(r, clamp_negatives=CLAMP_NEGATIVES)
        ok, _reason = validate_record(nr)
        if not ok:
            dropped += 1
            continue
        normalized_rows.append(nr)

    stats["valid_rows"] = len(normalized_rows)
    stats["dropped_invalid_rows"] = dropped

    if not normalized_rows:
        stats["zero_valid_rows_file"] = 1
        return stats

    # dedupe
    deduped_rows, dup_removed = dedupe_by_date_keep_highest_revenue(normalized_rows)
    stats["duplicates_removed"] = dup_removed

    ndjson_text = rows_to_ndjson(deduped_rows)
    stats["written_rows"] = len(deduped_rows)

    # write
    write_text_object(bucket, output_key, ndjson_text, content_type="application/x-ndjson")
    stats["written_object"] = 1
    return stats


def process_event(records, logger_print=print) -> Dict[str, Any]:
    """Process S3 event records. Returns aggregate stats. Raises on fatal I/O errors if enabled."""
    start = time.time()
    now_utc = datetime.now(timezone.utc)

    agg = {
        "records_received": 0,
        "processed_objects": 0,
        "written_objects": 0,
        "skipped_existing_outputs": 0,
        "invalid_json_files": 0,
        "missing_data_files": 0,
        "zero_valid_rows_files": 0,
        "rows_received": 0,
        "rows_written": 0,
        "dropped_invalid_rows": 0,
        "duplicates_removed": 0,
        "fatal_errors": 0,
    }

    agg["records_received"] = len(records or [])

    for rec in records or []:
        s3_info = rec.get("s3", {})
        bucket = (s3_info.get("bucket", {}) or {}).get("name")
        key = (s3_info.get("object", {}) or {}).get("key")
        if not bucket or not key:
            continue

        # caller should already unquote; still be defensive
        try:
            from urllib.parse import unquote_plus
            key = unquote_plus(key)
        except Exception:
            pass

        if not key.startswith(RAW_PREFIX):
            continue

        try:
            st = process_one(bucket=bucket, key=key, now_utc=now_utc)
        except Exception as e:
            agg["fatal_errors"] += 1
            logger_print(f"FATAL: processing failed for s3://{bucket}/{key} err={str(e)}")
            continue

        # aggregate
        agg["processed_objects"] += 1
        agg["written_objects"] += st.get("written_object", 0)
        agg["skipped_existing_outputs"] += st.get("skipped_existing", 0)
        agg["invalid_json_files"] += st.get("invalid_json", 0)
        agg["missing_data_files"] += st.get("missing_data_file", 0)
        agg["zero_valid_rows_files"] += st.get("zero_valid_rows_file", 0)
        agg["rows_received"] += st.get("valid_rows", 0) + st.get("dropped_invalid_rows", 0)
        agg["rows_written"] += st.get("written_rows", 0)
        agg["dropped_invalid_rows"] += st.get("dropped_invalid_rows", 0)
        agg["duplicates_removed"] += st.get("duplicates_removed", 0)

    processing_ms = int((time.time() - start) * 1000)

    # Emit EMF metrics
    dims = {"Function": os.getenv("AWS_LAMBDA_FUNCTION_NAME", "saeid-emi-normalize")}
    metrics = {
        "RecordsReceived": float(agg["records_received"]),
        "FilesProcessed": float(agg["processed_objects"]),
        "FilesWritten": float(agg["written_objects"]),
        "SkippedExistingOutputs": float(agg["skipped_existing_outputs"]),
        "InvalidJsonFiles": float(agg["invalid_json_files"]),
        "MissingDataFiles": float(agg["missing_data_files"]),
        "ZeroValidRowsFiles": float(agg["zero_valid_rows_files"]),
        "RowsReceived": float(agg["rows_received"]),
        "RowsWritten": float(agg["rows_written"]),
        "DroppedInvalidRows": float(agg["dropped_invalid_rows"]),
        "DuplicatesRemoved": float(agg["duplicates_removed"]),
        "ProcessingTimeMs": float(processing_ms),
    }
    emit_emf(default_namespace(), dims, metrics)

    if FAIL_ON_IO_ERRORS and agg["fatal_errors"] > 0:
        raise RuntimeError(f"Normalize fatal_errors={agg['fatal_errors']}")

    agg["processing_time_ms"] = processing_ms
    return agg
