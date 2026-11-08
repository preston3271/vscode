"""Lesson 4: react to a file landing in S3.

Upload a CSV with a header row ``title,content,tags`` to
``s3://<ImportBucket>/imports/anything.csv`` and every row becomes a note.
``tags`` is a ``;``-separated list, e.g. ``work;urgent``.

Patterns demonstrated:
    * looping over ``event["Records"]`` (S3 may batch several objects)
    * URL-decoding S3 object keys (spaces arrive as '+')
    * guarding against huge files before reading them into memory
    * IDEMPOTENCY: S3 events are delivered *at least once*, so the same file
      may be processed twice. We derive each note id deterministically from
      (bucket, key, version, line number) so a retry overwrites instead of
      creating duplicates.
    * DynamoDB batch_writer for efficient bulk inserts
"""

import csv
import io
import logging
import os
import urllib.parse
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()

MAX_BYTES = int(os.environ.get("MAX_IMPORT_BYTES", 5 * 1024 * 1024))
ID_NAMESPACE = uuid.UUID("6f1c2a52-7d1e-4c43-9a59-2f4f0f8b9a10")

_s3 = None
_table = None


def get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    return _table


def import_object(bucket, key, version):
    body = get_s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    # utf-8-sig silently strips the BOM that Excel likes to add.
    reader = csv.DictReader(io.StringIO(body.decode("utf-8-sig")))

    if not reader.fieldnames or "title" not in reader.fieldnames:
        raise ValueError(f"{key}: CSV must have a header row containing 'title'")

    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    imported, rejected = 0, []

    with get_table().batch_writer() as batch:
        for line_no, row in enumerate(reader, start=2):  # line 1 is the header
            title = (row.get("title") or "").strip()
            if not title or len(title) > 200:
                rejected.append(line_no)
                continue
            tags = sorted({t.strip().lower() for t in (row.get("tags") or "").split(";") if t.strip()})
            note_id = uuid.uuid5(ID_NAMESPACE, f"{bucket}/{key}/{version}/{line_no}")
            batch.put_item(
                Item={
                    "id": str(note_id),
                    "title": title,
                    "content": row.get("content") or "",
                    "tags": tags[:10],
                    "source": f"s3://{bucket}/{key}",
                    "createdAt": timestamp,
                    "updatedAt": timestamp,
                }
            )
            imported += 1

    return {"key": key, "imported": imported, "rejectedLines": rejected}


def lambda_handler(event, context):
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size = record["s3"]["object"].get("size", 0)
        version = record["s3"]["object"].get("versionId") or record["s3"]["object"].get("eTag", "")

        if size > MAX_BYTES:
            logger.warning("Skipping oversized file", extra={"key": key, "size": size})
            results.append({"key": key, "skipped": "too large"})
            continue

        result = import_object(bucket, key, version)
        logger.info("Imported file", extra=result)
        results.append(result)

    # S3 invokes asynchronously, so nobody receives this return value - but it
    # shows up in test output and in `sam local invoke`, which helps debugging.
    # Raising instead would make Lambda retry (up to 2 more times by default).
    return {"files": results}
