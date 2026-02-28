import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

NORMALIZED_PREFIX = os.getenv("NORMALIZED_PREFIX", "normalized/")
PROCESSED_PREFIX = os.getenv("PROCESSED_PREFIX", "processed/")
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "30"))
THRESHOLD = float(os.getenv("THRESHOLD", "3.5"))


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def read_text(bucket: str, key: str) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def write_json(bucket: str, key: str, payload: dict) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )


def build_processed_key(normalized_key: str) -> str:
    rel = normalized_key[len(NORMALIZED_PREFIX):] if normalized_key.startswith(NORMALIZED_PREFIX) else normalized_key
    if rel.lower().endswith(".ndjson"):
        rel = rel[:-6] + ".json"
    else:
        rel = rel + ".json"
    return PROCESSED_PREFIX + rel


def parse_ndjson(text: str) -> list:
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def build_presentation_output(
    *,
    source_bucket: str,
    source_key: str,
    processed_key: str,
    window_size: int,
    threshold: float,
    anomaly_count: int,
    anomalies: list,
    note: str | None = None,
) -> dict:
    # ----------------------------------------
    # Presentation-friendly output (for slides/demo)
    # ----------------------------------------
    date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if note:
        message = f"No anomalies detected ({note})."
        status = "OK"
    elif anomaly_count == 0:
        message = "No anomalies detected."
        status = "OK"
    else:
        message = f"{anomaly_count} anomalies detected."
        status = "ALERT"

    return {
        "summary": {
            "status": status,
            "message": message,
            "anomaly_count": anomaly_count,
        },
        "context": {
            "date_utc": date_utc,
            "rolling_window": window_size,
            "threshold": threshold,
        },
        "artifacts": {
            "normalized": f"s3://{source_bucket}/{source_key}",
            "processed": f"s3://{source_bucket}/{processed_key}",
        },
        "anomalies": anomalies,
        # Keep technical details (if someone asks)
        "debug": {
            "source_bucket": source_bucket,
            "source_key": source_key,
            "processed_key": processed_key,
            "note": note,
        },
    }


# -------- Mahalanobis (3 features) --------
def _mean_vec(points: list) -> list:
    n = len(points)
    return [
        sum(p[0] for p in points) / n,
        sum(p[1] for p in points) / n,
        sum(p[2] for p in points) / n,
    ]


def _cov_3x3(points: list, mean: list, eps: float = 1e-6) -> list:
    n = len(points)
    if n < 2:
        return [
            [1.0 + eps, 0.0, 0.0],
            [0.0, 1.0 + eps, 0.0],
            [0.0, 0.0, 1.0 + eps],
        ]

    c00 = c01 = c02 = c11 = c12 = c22 = 0.0
    for x, y, z in points:
        dx = x - mean[0]
        dy = y - mean[1]
        dz = z - mean[2]
        c00 += dx * dx
        c01 += dx * dy
        c02 += dx * dz
        c11 += dy * dy
        c12 += dy * dz
        c22 += dz * dz

    denom = (n - 1)
    c00 /= denom
    c01 /= denom
    c02 /= denom
    c11 /= denom
    c12 /= denom
    c22 /= denom

    return [
        [c00 + eps, c01,      c02],
        [c01,      c11 + eps, c12],
        [c02,      c12,      c22 + eps],
    ]


def _inv_3x3(m: list) -> list:
    a, b, c = m[0]
    d, e, f = m[1]
    g, h, i = m[2]

    det = (
        a * (e * i - f * h)
        - b * (d * i - f * g)
        + c * (d * h - e * g)
    )

    if abs(det) < 1e-12:
        bump = 1e-3
        m2 = [
            [m[0][0] + bump, m[0][1],        m[0][2]],
            [m[1][0],        m[1][1] + bump, m[1][2]],
            [m[2][0],        m[2][1],        m[2][2] + bump],
        ]
        return _inv_3x3(m2)

    inv_det = 1.0 / det

    A = (e * i - f * h) * inv_det
    B = (c * h - b * i) * inv_det
    C = (b * f - c * e) * inv_det
    D = (f * g - d * i) * inv_det
    E = (a * i - c * g) * inv_det
    F = (c * d - a * f) * inv_det
    G = (d * h - e * g) * inv_det
    H = (b * g - a * h) * inv_det
    I = (a * e - b * d) * inv_det

    return [
        [A, B, C],
        [D, E, F],
        [G, H, I],
    ]


def mahalanobis_squared(x: list, mean: list, inv_cov: list) -> float:
    dx0 = x[0] - mean[0]
    dx1 = x[1] - mean[1]
    dx2 = x[2] - mean[2]

    v0 = inv_cov[0][0] * dx0 + inv_cov[0][1] * dx1 + inv_cov[0][2] * dx2
    v1 = inv_cov[1][0] * dx0 + inv_cov[1][1] * dx1 + inv_cov[1][2] * dx2
    v2 = inv_cov[2][0] * dx0 + inv_cov[2][1] * dx1 + inv_cov[2][2] * dx2

    return dx0 * v0 + dx1 * v1 + dx2 * v2


def detect(rows: list) -> dict:
    valid = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        date = r.get("date")
        if not isinstance(date, str):
            continue
        try:
            orders = float(r.get("orders", 0))
            aov = float(r.get("avg_order_value", 0))
            revenue = float(r.get("revenue", 0))
        except Exception:
            continue
        valid.append({"date": date, "orders": orders, "avg_order_value": aov, "revenue": revenue})

    if len(valid) <= WINDOW_SIZE:
        return {"anomaly_count": 0, "anomalies": [], "note": "Not enough rows for rolling window"}

    anomalies = []
    for idx in range(WINDOW_SIZE, len(valid)):
        window = valid[idx - WINDOW_SIZE: idx]
        points = [[w["orders"], w["avg_order_value"], w["revenue"]] for w in window]
        mean = _mean_vec(points)
        inv_cov = _inv_3x3(_cov_3x3(points, mean))

        cur = valid[idx]
        d2 = mahalanobis_squared([cur["orders"], cur["avg_order_value"], cur["revenue"]], mean, inv_cov)

        if d2 >= THRESHOLD:
            anomalies.append({"date": cur["date"], "d2": d2, "record": cur})

    return {"anomaly_count": len(anomalies), "anomalies": anomalies, "note": None}


def _extract_bucket_key_from_event(event: dict) -> tuple[str | None, str | None]:
    """
    Supports:
    1) S3 Event input: event["Records"][0]["s3"]["bucket"]["name"] + key
    2) Step Functions input: { "bucket": "...", "key": "normalized/...ndjson" }
       or { "source_bucket": "...", "source_key": "normalized/...ndjson" }
       or output from previous step { "bucket": "...", "key": "...", ... }
    """
    records = event.get("Records", [])
    if records:
        rec = records[0]
        s3_info = rec.get("s3", {})
        bucket = (s3_info.get("bucket", {}) or {}).get("name")
        key = (s3_info.get("object", {}) or {}).get("key")
        if key:
            key = urllib.parse.unquote_plus(key)
        return bucket, key

    bucket = event.get("bucket") or event.get("source_bucket")
    key = event.get("key") or event.get("source_key")
    if isinstance(key, str):
        key = urllib.parse.unquote_plus(key)
    return bucket, key


def handler(event, context):
    print("Detect Lambda started")
    print("Event:", json.dumps(event))

    bucket, key = _extract_bucket_key_from_event(event)

    if not bucket or not key:
        # For Step Functions, return a clean output (not statusCode)
        return {
            "summary": {"status": "SKIPPED", "message": "No bucket/key provided.", "anomaly_count": 0},
            "context": {"date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "rolling_window": WINDOW_SIZE, "threshold": THRESHOLD},
            "artifacts": {},
            "anomalies": [],
            "debug": {"note": "No bucket/key in event"},
        }

    # Only handle normalized NDJSON
    if not key.startswith(NORMALIZED_PREFIX) or not key.lower().endswith(".ndjson"):
        return {
            "summary": {"status": "SKIPPED", "message": "Input is not a normalized .ndjson file.", "anomaly_count": 0},
            "context": {"date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "rolling_window": WINDOW_SIZE, "threshold": THRESHOLD},
            "artifacts": {"normalized": f"s3://{bucket}/{key}"},
            "anomalies": [],
            "debug": {"note": "Key not matching normalized/*.ndjson"},
        }

    processed_key = build_processed_key(key)

    # Idempotency
    if object_exists(bucket, processed_key):
        print(f"Already processed. Skipping. output=s3://{bucket}/{processed_key}")
        return build_presentation_output(
            source_bucket=bucket,
            source_key=key,
            processed_key=processed_key,
            window_size=WINDOW_SIZE,
            threshold=THRESHOLD,
            anomaly_count=0,
            anomalies=[],
            note="Already processed (idempotent skip)",
        )

    ndjson_text = read_text(bucket, key)
    rows = parse_ndjson(ndjson_text)
    result = detect(rows)

    output = build_presentation_output(
        source_bucket=bucket,
        source_key=key,
        processed_key=processed_key,
        window_size=WINDOW_SIZE,
        threshold=THRESHOLD,
        anomaly_count=int(result.get("anomaly_count", 0) or 0),
        anomalies=result.get("anomalies", []) or [],
        note=result.get("note"),
    )

    write_json(bucket, processed_key, output)
    print(f"Wrote processed output: s3://{bucket}/{processed_key} anomalies={output['summary']['anomaly_count']}")

    # For Step Functions this becomes the final execution output (nice for demo)
    return output