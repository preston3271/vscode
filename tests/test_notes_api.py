import json

import boto3
import pytest

from conftest import load

api = load("notes_api")


@pytest.fixture(autouse=True)
def fresh_clients(monkeypatch):
    # Clients are cached at module level; reset so each test gets moto's fakes.
    api._table = None
    api._sqs = None
    monkeypatch.delenv("QUEUE_URL", raising=False)


def call(context, route, body=None, note_id=None, query=None):
    event = {"routeKey": route, "isBase64Encoded": False}
    if body is not None:
        event["body"] = body if isinstance(body, str) else json.dumps(body)
    if note_id is not None:
        event["pathParameters"] = {"id": note_id}
    if query is not None:
        event["queryStringParameters"] = query
    result = api.lambda_handler(event, context)
    return result["statusCode"], (json.loads(result["body"]) if result["body"] else None)


def create(context, **fields):
    fields.setdefault("title", "A note")
    status, note = call(context, "POST /notes", fields)
    assert status == 201, note
    return note


def test_full_crud_lifecycle(table, context):
    note = create(context, title="  Groceries ", content="milk eggs", tags=["Home", "home", "todo"])
    assert note["title"] == "Groceries"
    assert note["tags"] == ["home", "todo"]  # trimmed, lower-cased, de-duplicated

    status, fetched = call(context, "GET /notes/{id}", note_id=note["id"])
    assert status == 200 and fetched["content"] == "milk eggs"

    status, updated = call(context, "PUT /notes/{id}", {"content": "milk eggs bread"}, note_id=note["id"])
    assert status == 200
    assert updated["content"] == "milk eggs bread"
    assert updated["title"] == "Groceries"  # untouched fields are kept

    status, _ = call(context, "DELETE /notes/{id}", note_id=note["id"])
    assert status == 204
    status, _ = call(context, "GET /notes/{id}", note_id=note["id"])
    assert status == 404


@pytest.mark.parametrize(
    "body, message",
    [
        ("not json", "valid JSON"),
        ([1, 2], "JSON object"),
        ({}, "'title' is required"),
        ({"title": "x" * 201}, "at most 200"),
        ({"title": "ok", "tags": "nope"}, "'tags' must be a list"),
        ({"title": "ok", "colour": "red"}, "Unknown field(s): colour"),
    ],
)
def test_create_rejects_bad_input(table, context, body, message):
    status, error = call(context, "POST /notes", body)
    assert status == 400
    assert message in error["message"]


def test_missing_and_malformed_ids(table, context):
    assert call(context, "GET /notes/{id}", note_id="not-a-uuid")[0] == 400
    missing = "00000000-0000-0000-0000-000000000000"
    assert call(context, "GET /notes/{id}", note_id=missing)[0] == 404
    assert call(context, "PUT /notes/{id}", {"title": "x"}, note_id=missing)[0] == 404
    assert call(context, "DELETE /notes/{id}", note_id=missing)[0] == 404


def test_list_paginates(table, context):
    for i in range(5):
        create(context, title=f"note {i}")

    seen, token = [], None
    while True:
        query = {"limit": "2", **({"nextToken": token} if token else {})}
        status, page = call(context, "GET /notes", query=query)
        assert status == 200 and len(page["items"]) <= 2
        seen += [n["id"] for n in page["items"]]
        token = page.get("nextToken")
        if not token:
            break
    assert len(set(seen)) == 5


def test_unknown_route(context):
    assert call(context, "PATCH /notes")[0] == 404


def test_unexpected_errors_become_500(context, monkeypatch):
    def boom():
        raise RuntimeError("database exploded")

    monkeypatch.setattr(api, "get_table", boom)
    status, body = call(context, "GET /notes/{id}", note_id="00000000-0000-0000-0000-000000000000")
    assert status == 500
    assert "exploded" not in body["message"]  # internals are never leaked
    assert body["requestId"] == "test-request-id"


def test_create_enqueues_word_count_job(table, context, monkeypatch):
    queue_url = boto3.client("sqs").create_queue(QueueName="wc")["QueueUrl"]
    monkeypatch.setenv("QUEUE_URL", queue_url)

    note = create(context, content="one two three")

    messages = boto3.client("sqs").receive_message(QueueUrl=queue_url)["Messages"]
    assert json.loads(messages[0]["Body"]) == {"noteId": note["id"]}
