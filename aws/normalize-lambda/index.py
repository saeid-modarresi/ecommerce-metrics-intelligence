import json
import os
import urllib.parse

from normalize.orchestrator import process_event

def handler(event, context):
    print("Normalize Lambda started")
    print("Event:", json.dumps(event))

    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": "No records"}

    # Ensure URL decoding of object keys (S3 event keys are URL-encoded)
    for rec in records:
        try:
            key = (rec.get("s3", {}).get("object", {}) or {}).get("key")
            if key:
                rec["s3"]["object"]["key"] = urllib.parse.unquote_plus(key)
        except Exception:
            pass

    agg = process_event(records)

    return {
        "statusCode": 200,
        "body": json.dumps(agg),
    }
