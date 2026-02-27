import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from normalize.normalize_core import normalize_payload_to_ndjson

s3 = boto3.client("s3")

RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "normalized/")
CLAMP_NEGATIVES = os.getenv("CLAMP_NEGATIVES", "true").lower() == "true"


def _build_output_key(input_key: str, now_utc: datetime) -> str:
    """
    Output path pattern:
    normalized/YYYY/MM/DD/<original_basename>.ndjson
    Basename is preserved from the raw object key.
    """
    basename = input_key.split("/")[-1]
    if basename.lower().endswith(".json"):
        basename = basename[:-5]

    yyyy = now_utc.strftime("%Y")
    mm = now_utc.strftime("%m")
    dd = now_utc.strftime("%d")

    return f"{OUTPUT_PREFIX}{yyyy}/{mm}/{dd}/{basename}.ndjson"


def _s3_object_exists(bucket: str, key: str) -> bool:
    """
    Returns True if object exists, False if not found.
    Raises for other errors.
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def handler(event, context):
    """
    Triggered by S3 ObjectCreated events on prefix raw/.
    Reads the raw JSON, normalizes records, writes NDJSON into normalized/.
    Idempotency: if output object already exists, skip (no overwrite).
    """
    now_utc = datetime.now(timezone.utc)

    print("Normalize Lambda started")
    print("Event:", json.dumps(event))

    records = event.get("Records", [])
    if not records:
        print("No Records found in event. Exiting.")
        return {"statusCode": 200, "body": "No records"}

    processed = 0
    skipped_existing = 0
    written = 0
    invalid_json = 0
    missing_data = 0
    zero_rows = 0

    for rec in records:
        s3_info = rec.get("s3", {})
        bucket = (s3_info.get("bucket", {}) or {}).get("name")
        key = (s3_info.get("object", {}) or {}).get("key")

        if not bucket or not key:
            print("Skipping record: missing bucket or key")
            continue

        key = urllib.parse.unquote_plus(key)

        # Extra safety: ignore anything outside raw/
        if not key.startswith(RAW_PREFIX):
            print(f"Skipping key outside RAW_PREFIX: {key}")
            continue

        output_key = _build_output_key(key, now_utc)

        # Idempotency: if output exists, skip
        if _s3_object_exists(bucket, output_key):
            skipped_existing += 1
            print(f"Already processed. Skipping. output=s3://{bucket}/{output_key}")
            continue

        print(f"Processing s3://{bucket}/{key}")

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            raw_bytes = obj["Body"].read()
        except Exception as e:
            print(f"Failed to read raw object. Key={key}. Error={str(e)}")
            continue

        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:
            invalid_json += 1
            print(f"Invalid JSON. Key={key}. Error={str(e)}")
            continue

        ndjson_text, stats = normalize_payload_to_ndjson(
            payload=payload,
            clamp_negatives=CLAMP_NEGATIVES,
        )

        processed += 1

        if stats.get("discarded_missing_data") == 1:
            missing_data += 1
            print(f"Invalid payload shape (missing `data`). Key={key}. Skipping output.")
            continue

        if stats.get("valid_rows", 0) == 0:
            zero_rows += 1
            print(f"No valid normalized rows. Key={key}. Skipping output.")
            continue

        try:
            s3.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=ndjson_text.encode("utf-8"),
                ContentType="application/x-ndjson",
            )
            written += 1
            print(
                f"Wrote normalized output: s3://{bucket}/{output_key} | "
                f"stats={json.dumps(stats)}"
            )
        except Exception as e:
            print(f"Failed to write normalized output. Key={output_key}. Error={str(e)}")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "processed_objects": processed,
                "written_objects": written,
                "skipped_existing_outputs": skipped_existing,
                "invalid_json": invalid_json,
                "missing_data": missing_data,
                "zero_valid_rows": zero_rows,
            }
        ),
    }