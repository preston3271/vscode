"""Shared test fixtures.

`moto` intercepts boto3 calls and fakes DynamoDB/S3/SQS in memory, so these
tests run offline, for free, in about a second.
"""

import importlib.util
import os
import pathlib
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Fake credentials so nothing can ever touch a real AWS account.
os.environ.update(
    AWS_ACCESS_KEY_ID="testing",
    AWS_SECRET_ACCESS_KEY="testing",
    AWS_SESSION_TOKEN="testing",
    AWS_DEFAULT_REGION="us-east-1",
    TABLE_NAME="notes-test",
)


def load(function_dir):
    """Import src/<function_dir>/app.py under a unique module name.

    Every function has a file called app.py, so a plain `import app` would
    collide; loading by path keeps them separate.
    """
    path = ROOT / "src" / function_dir / "app.py"
    spec = importlib.util.spec_from_file_location(f"{function_dir}_app", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def context():
    """A stand-in for the Lambda context object."""
    return SimpleNamespace(
        aws_request_id="test-request-id",
        function_name="test-function",
        memory_limit_in_mb="256",
        get_remaining_time_in_millis=lambda: 9_000,
    )


@pytest.fixture
def aws():
    with mock_aws():
        yield


@pytest.fixture
def table(aws):
    return boto3.resource("dynamodb").create_table(
        TableName=os.environ["TABLE_NAME"],
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
    )
