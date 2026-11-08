"""Lesson 5: an asynchronous worker fed by an SQS queue.

The notes API drops ``{"noteId": "..."}`` messages on a queue. Lambda polls
the queue for us and calls this function with up to ``BatchSize`` messages.
We compute a word count and store it back on the note.

Patterns demonstrated:
    * PARTIAL BATCH FAILURES: return the ids of the messages that failed so
      only *those* are retried (requires ``ReportBatchItemFailures`` in the
      template). Without this, one bad message makes the whole batch retry.
    * Transient vs. permanent errors: retrying a malformed message will never
      succeed, so we log and drop it rather than retry it forever. Transient
      problems (throttling, network) are retried; after ``maxReceiveCount``
      attempts SQS moves the message to the dead-letter queue (DLQ).
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()

_table = None


def get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    return _table


class PermanentError(Exception):
    """The message can never succeed; don't retry it."""


def count_words(text):
    return len(text.split())


def process_message(body):
    try:
        note_id = json.loads(body)["noteId"]
    except (json.JSONDecodeError, KeyError, TypeError):
        raise PermanentError(f"Malformed message: {body[:200]!r}")

    note = get_table().get_item(Key={"id": note_id}).get("Item")
    if note is None:
        raise PermanentError(f"Note {note_id} no longer exists")

    words = count_words(note.get("content", ""))
    get_table().update_item(
        Key={"id": note_id},
        UpdateExpression="SET wordCount = :w, wordCountAt = :t",
        ExpressionAttributeValues={
            ":w": words,
            ":t": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
    )
    return note_id, words


def lambda_handler(event, context):
    failures = []
    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            note_id, words = process_message(record["body"])
            logger.info("Counted words", extra={"noteId": note_id, "words": words})
        except PermanentError as err:
            logger.error("Dropping message", extra={"messageId": message_id, "error": str(err)})
        except Exception:
            logger.exception("Will retry message", extra={"messageId": message_id})
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}
