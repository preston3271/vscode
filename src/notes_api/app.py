"""Lesson 3: a production-style REST API backed by DynamoDB.

Routes (wired up by API Gateway HTTP API in template.yaml):

    POST   /notes          create a note
    GET    /notes          list notes (paginated with ?limit=&nextToken=)
    GET    /notes/{id}     fetch one note
    PUT    /notes/{id}     partially update a note
    DELETE /notes/{id}     delete a note

Patterns demonstrated:
    * one function, many routes (a tiny router keyed on ``routeKey``)
    * AWS clients created once and reused across warm invocations
    * configuration via environment variables
    * strict input validation with clear 4xx errors
    * a single place that turns exceptions into HTTP responses (no stack
      traces leaked to clients, full stack traces in CloudWatch)
    * DynamoDB conditional writes (no accidental overwrites, real 404s)
    * optional async hand-off to SQS (see src/word_count)
"""

import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()

MAX_TITLE = 200
MAX_CONTENT = 10_000
MAX_TAGS = 10
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100

# --------------------------------------------------------------------------
# AWS clients: created lazily, then cached for the life of the execution
# environment. (Lazy creation also makes unit testing with moto easy.)
# --------------------------------------------------------------------------
_table = None
_sqs = None


def get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    return _table


def get_sqs():
    global _sqs
    if _sqs is None:
        _sqs = boto3.client("sqs")
    return _sqs


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------
class HttpError(Exception):
    """Raise anywhere in a route to return a clean 4xx response."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def _json_default(value):
    # DynamoDB returns numbers as Decimal, which json.dumps can't handle.
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"Not JSON serializable: {type(value).__name__}")


def respond(status, body=None):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": "" if body is None else json.dumps(body, default=_json_default),
    }


def parse_json_body(event):
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    if not raw:
        raise HttpError(400, "Request body is required")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise HttpError(400, "Request body must be valid JSON")
    if not isinstance(data, dict):
        raise HttpError(400, "Request body must be a JSON object")
    return data


def path_id(event):
    note_id = (event.get("pathParameters") or {}).get("id", "")
    try:
        return str(uuid.UUID(note_id))
    except ValueError:
        raise HttpError(400, "Note id must be a UUID")


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------
def validate_note(data, partial=False):
    """Return a cleaned dict, or raise HttpError(400).

    ``partial=True`` is used for updates: every field becomes optional, but
    at least one must be present.
    """
    unknown = set(data) - {"title", "content", "tags"}
    if unknown:
        raise HttpError(400, f"Unknown field(s): {', '.join(sorted(unknown))}")

    clean = {}

    if "title" in data or not partial:
        title = data.get("title")
        if not isinstance(title, str) or not title.strip():
            raise HttpError(400, "'title' is required and must be a non-empty string")
        if len(title) > MAX_TITLE:
            raise HttpError(400, f"'title' must be at most {MAX_TITLE} characters")
        clean["title"] = title.strip()

    if "content" in data or not partial:
        content = data.get("content", "")
        if not isinstance(content, str):
            raise HttpError(400, "'content' must be a string")
        if len(content) > MAX_CONTENT:
            raise HttpError(400, f"'content' must be at most {MAX_CONTENT} characters")
        clean["content"] = content

    if "tags" in data or not partial:
        tags = data.get("tags", [])
        if not isinstance(tags, list) or not all(isinstance(t, str) and t.strip() for t in tags):
            raise HttpError(400, "'tags' must be a list of non-empty strings")
        if len(tags) > MAX_TAGS:
            raise HttpError(400, f"At most {MAX_TAGS} tags are allowed")
        clean["tags"] = sorted({t.strip().lower() for t in tags})

    if partial and not clean:
        raise HttpError(400, "Provide at least one of: title, content, tags")
    return clean


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
def create_note(event):
    note = validate_note(parse_json_body(event))
    timestamp = now_iso()
    note.update(id=str(uuid.uuid4()), createdAt=timestamp, updatedAt=timestamp)

    # The condition guarantees we never silently overwrite an existing item.
    get_table().put_item(Item=note, ConditionExpression="attribute_not_exists(id)")
    enqueue_word_count(note["id"])
    return respond(201, note)


def list_notes(event):
    params = event.get("queryStringParameters") or {}
    try:
        limit = int(params.get("limit", DEFAULT_PAGE_SIZE))
    except ValueError:
        raise HttpError(400, "'limit' must be an integer")
    if not 1 <= limit <= MAX_PAGE_SIZE:
        raise HttpError(400, f"'limit' must be between 1 and {MAX_PAGE_SIZE}")

    scan_args = {"Limit": limit}
    if params.get("nextToken"):
        try:
            scan_args["ExclusiveStartKey"] = json.loads(base64.urlsafe_b64decode(params["nextToken"]))
        except (ValueError, TypeError):
            raise HttpError(400, "'nextToken' is invalid")

    # NOTE: Scan reads the whole table. Fine for learning; in production you
    # would design a key schema/GSI so this becomes a Query.
    result = get_table().scan(**scan_args)
    body = {"items": result.get("Items", [])}
    if "LastEvaluatedKey" in result:
        body["nextToken"] = base64.urlsafe_b64encode(
            json.dumps(result["LastEvaluatedKey"]).encode()
        ).decode()
    return respond(200, body)


def get_note(event):
    item = get_table().get_item(Key={"id": path_id(event)}).get("Item")
    if not item:
        raise HttpError(404, "Note not found")
    return respond(200, item)


def update_note(event):
    note_id = path_id(event)
    changes = validate_note(parse_json_body(event), partial=True)
    changes["updatedAt"] = now_iso()

    # Build "SET #title = :title, #tags = :tags, ..." dynamically. Using
    # #name placeholders avoids clashes with DynamoDB reserved words.
    names = {f"#{k}": k for k in changes}
    values = {f":{k}": v for k, v in changes.items()}
    expression = "SET " + ", ".join(f"#{k} = :{k}" for k in changes)

    try:
        result = get_table().update_item(
            Key={"id": note_id},
            UpdateExpression=expression,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ConditionExpression="attribute_exists(id)",  # don't create on update
            ReturnValues="ALL_NEW",
        )
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise HttpError(404, "Note not found")
        raise

    if "content" in changes:
        enqueue_word_count(note_id)
    return respond(200, result["Attributes"])


def delete_note(event):
    try:
        get_table().delete_item(
            Key={"id": path_id(event)}, ConditionExpression="attribute_exists(id)"
        )
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise HttpError(404, "Note not found")
        raise
    return respond(204)


def enqueue_word_count(note_id):
    """Hand slow/optional work to a queue instead of making the caller wait."""
    queue_url = os.environ.get("QUEUE_URL")
    if not queue_url:
        return  # feature disabled (e.g. in unit tests)
    get_sqs().send_message(QueueUrl=queue_url, MessageBody=json.dumps({"noteId": note_id}))


ROUTES = {
    "POST /notes": create_note,
    "GET /notes": list_notes,
    "GET /notes/{id}": get_note,
    "PUT /notes/{id}": update_note,
    "DELETE /notes/{id}": delete_note,
}


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
def lambda_handler(event, context):
    route = event.get("routeKey", "")
    logger.info("Request received", extra={"route": route})

    handler = ROUTES.get(route)
    if handler is None:
        return respond(404, {"message": f"No route for '{route}'"})

    try:
        return handler(event)
    except HttpError as err:
        logger.info("Client error", extra={"status": err.status, "error": err.message})
        return respond(err.status, {"message": err.message})
    except Exception:
        # Full stack trace goes to CloudWatch; the client gets a generic message.
        logger.exception("Unhandled error")
        return respond(500, {"message": "Internal server error", "requestId": context.aws_request_id})
