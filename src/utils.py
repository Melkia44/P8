import json
import re
from typing import Any, Iterable, Tuple
import boto3

NULL_LIKE = {"", "none", "null", "nan", "na", "n/a", "-", "—"}

def clean_nb(v: Any) -> str:
    s = "" if v is None else str(v)
    s = s.replace("\u00a0", " ").strip().lower()
    if s in NULL_LIKE:
        return ""
    return re.sub(r"[^\d\.\-]", "", s)

def to_int(v: Any):
    s = clean_nb(v)
    if not s: return None
    try: return int(float(s))
    except ValueError: return None

def to_float(v: Any):
    s = clean_nb(v)
    if not s: return None
    try: return float(s)
    except ValueError: return None

def iter_s3_jsonl(bucket: str, prefix: str) -> Iterable[Tuple[str, str]]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].iter_lines()
            for line in body:
                if line:
                    yield key, line.decode("utf-8", errors="replace")

def s3_put_jsonl(bucket: str, key: str, lines: Iterable[str]) -> None:
    s3 = boto3.client("s3")
    payload = "\n".join(lines) + "\n"
    s3.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"))

def s3_put_json(bucket: str, key: str, obj: Any) -> None:
    s3 = boto3.client("s3")
    payload = json.dumps(obj, ensure_ascii=False, indent=2)
    s3.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"))
