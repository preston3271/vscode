import boto3
import pytest

from conftest import ROOT, load

importer = load("csv_importer")
BUCKET = "imports-test"


@pytest.fixture(autouse=True)
def fresh_clients():
    importer._s3 = None
    importer._table = None


@pytest.fixture
def bucket(aws):
    boto3.client("s3").create_bucket(Bucket=BUCKET)


def s3_event(key, size=100):
    return {"Records": [{"s3": {"bucket": {"name": BUCKET}, "object": {"key": key, "size": size, "eTag": "e1"}}}]}


def test_imports_rows_and_rejects_bad_ones(table, bucket, context):
    csv_bytes = (ROOT / "events" / "sample.csv").read_bytes()
    boto3.client("s3").put_object(Bucket=BUCKET, Key="imports/my file.csv", Body=csv_bytes)

    # S3 URL-encodes keys in events: the space arrives as '+'.
    result = importer.lambda_handler(s3_event("imports/my+file.csv"), context)

    assert result["files"][0] == {"key": "imports/my file.csv", "imported": 2, "rejectedLines": [4]}
    items = table.scan()["Items"]
    assert {i["title"] for i in items} == {"Buy groceries", "Read the Lambda docs"}
    groceries = next(i for i in items if i["title"] == "Buy groceries")
    assert groceries["tags"] == ["errands", "personal"]


def test_reprocessing_the_same_event_is_idempotent(table, bucket, context):
    boto3.client("s3").put_object(Bucket=BUCKET, Key="imports/a.csv", Body=b"title\nOne\nTwo\n")
    importer.lambda_handler(s3_event("imports/a.csv"), context)
    importer.lambda_handler(s3_event("imports/a.csv"), context)  # duplicate delivery
    assert table.scan()["Count"] == 2


def test_skips_oversized_files(table, bucket, context):
    result = importer.lambda_handler(s3_event("imports/huge.csv", size=10**9), context)
    assert result["files"][0]["skipped"] == "too large"


def test_rejects_csv_without_title_column(table, bucket, context):
    boto3.client("s3").put_object(Bucket=BUCKET, Key="imports/bad.csv", Body=b"name\nx\n")
    with pytest.raises(ValueError, match="title"):
        importer.lambda_handler(s3_event("imports/bad.csv"), context)
