from typing import Optional, Dict, Any
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def read_object_bytes(bucket: str, key: str) -> bytes:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def write_text_object(bucket: str, key: str, text: str, content_type: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
    )
