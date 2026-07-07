"""REST API -> MinIO connector for the API bronze ingestion (Phase F).

Fetches records from a paginated JSON endpoint and lands them as NDJSON on MinIO, where
the shared Scala BronzeExtract job (--input_path) validates them and writes Iceberg — the
same validation/quarantine/audit path as the JDBC sources.

Envelope contract: each page is either a bare JSON array, or {"data": [...], "next": <url|null>}.
`next` is followed until null, so real paginated/cursor APIs work unchanged; the local mock
returns a single page.
"""
import json

import boto3
import requests


def _fetch_all(url: str, timeout: int = 30) -> list:
    """Follow the `next` cursor, accumulating records across pages."""
    records: list = []
    while url:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, list):
            records.extend(payload)
            url = None
        else:
            records.extend(payload.get("data", []))
            url = payload.get("next")
    return records


def fetch_and_land(*, api_url: str, bucket: str, key: str,
                   minio_endpoint: str, access_key: str, secret_key: str) -> int:
    """Fetch api_url and write the records as NDJSON to s3://<bucket>/<key>. Returns the count."""
    records = _fetch_all(api_url)
    body = "\n".join(json.dumps(rec) for rec in records).encode("utf-8")

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    print(f"[api_connector] landed {len(records)} records -> s3://{bucket}/{key}")
    return len(records)
