import json

import pytest

from conftest import load

worker = load("word_count")


@pytest.fixture(autouse=True)
def fresh_clients():
    worker._table = None


def sqs_event(*bodies):
    return {"Records": [{"messageId": f"m{i}", "body": b} for i, b in enumerate(bodies)]}


def test_counts_words(table, context):
    table.put_item(Item={"id": "n1", "title": "t", "content": "the quick  brown\nfox"})
    result = worker.lambda_handler(sqs_event(json.dumps({"noteId": "n1"})), context)
    assert result == {"batchItemFailures": []}
    assert table.get_item(Key={"id": "n1"})["Item"]["wordCount"] == 4


def test_permanent_errors_are_dropped_not_retried(table, context):
    result = worker.lambda_handler(sqs_event("not json", json.dumps({"noteId": "missing"})), context)
    assert result == {"batchItemFailures": []}


def test_transient_errors_report_only_the_failed_message(table, context, monkeypatch):
    table.put_item(Item={"id": "ok", "title": "t", "content": "a b"})
    real = worker.process_message

    def flaky(body):
        if "flaky" in body:
            raise ConnectionError("network blip")
        return real(body)

    monkeypatch.setattr(worker, "process_message", flaky)
    result = worker.lambda_handler(
        sqs_event(json.dumps({"noteId": "ok"}), json.dumps({"noteId": "flaky"})), context
    )
    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
