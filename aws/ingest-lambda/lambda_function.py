import os
import json
import boto3
import urllib.request
from datetime import datetime, timezone

s3 = boto3.client("s3")

def fetch_json(url: str, timeout: int = 15) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "emi-ingest-raw/1.0",
            "Accept": "application/json"
        }
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")

    # Validate JSON
    payload = json.loads(body)

    # Basic shape validation (lightweight)
    if not isinstance(payload, dict) or "data" not in payload:
        raise ValueError("Invalid payload: expected a JSON object with a top-level 'data' field.")

    return payload

def lambda_handler(event, context):
    source_url = os.environ["SOURCE_URL"]
    bucket = os.environ["BUCKET_NAME"]
    prefix = os.environ.get("RAW_PREFIX", "raw/")

    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix.rstrip('/')}/metrics_{ts}.json"

    payload = fetch_json(source_url)

    # Attach ingest metadata (safe + useful)
    payload.setdefault("ingest", {})
    payload["ingest"].update({
        "ingested_at_utc": now.isoformat().replace("+00:00", "Z"),
        "source_url": source_url,
        "request_id": getattr(context, "aws_request_id", None),
    })

    data_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data_bytes,
        ContentType="application/json"
    )

    return {
        "ok": True,
        "bucket": bucket,
        "key": key,
        "rows": len(payload.get("data", [])),
    }