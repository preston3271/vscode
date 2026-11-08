import json

from conftest import load

hello = load("hello")


def test_greets_from_query_string(context):
    event = {"queryStringParameters": {"name": "Ada"}}
    result = hello.lambda_handler(event, context)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["message"] == "Hello, Ada!"
    assert body["requestId"] == "test-request-id"


def test_defaults_to_world(context):
    body = json.loads(hello.lambda_handler({}, context)["body"])
    assert body["message"] == "Hello, world!"
