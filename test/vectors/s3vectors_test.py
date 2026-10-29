"""Run the S3 Vectors API against Go/Rust volumes and single/distributed queries."""

import json
import time
import uuid

import boto3
from botocore.exceptions import ClientError
import pytest

BUCKET = "vectors-bucket"
INDEX = "idx"


def test_vector_lifecycle(vectors):
    vectors.create_vector_bucket(vectorBucketName=BUCKET)
    buckets = [b["vectorBucketName"] for b in vectors.list_vector_buckets()["vectorBuckets"]]
    assert BUCKET in buckets

    vectors.create_index(
        vectorBucketName=BUCKET,
        indexName=INDEX,
        dataType="float32",
        dimension=4,
        distanceMetric="euclidean",
    )
    idx = vectors.get_index(vectorBucketName=BUCKET, indexName=INDEX)["index"]
    assert idx["dimension"] == 4

    vectors.put_vectors(
        vectorBucketName=BUCKET,
        indexName=INDEX,
        vectors=[
            {"key": "a", "data": {"float32": [1, 0, 0, 0]}, "metadata": {"color": "red"}},
            {"key": "b", "data": {"float32": [0.95, 0.05, 0, 0]}, "metadata": {"color": "blue"}},
            {"key": "c", "data": {"float32": [0.9, 0.1, 0, 0]}, "metadata": {"color": "red"}},
        ],
    )

    got = vectors.get_vectors(
        vectorBucketName=BUCKET, indexName=INDEX, keys=["a"], returnData=True, returnMetadata=True
    )["vectors"]
    assert got[0]["key"] == "a" and got[0]["metadata"]["color"] == "red"

    listed = vectors.list_vectors(vectorBucketName=BUCKET, indexName=INDEX)["vectors"]
    assert sorted(v["key"] for v in listed) == ["a", "b", "c"]

    hits = vectors.query_vectors(
        vectorBucketName=BUCKET,
        indexName=INDEX,
        queryVector={"float32": [1, 0, 0, 0]},
        topK=2,
        returnDistance=True,
        returnMetadata=True,
        filter={"color": {"$eq": "red"}},
    )["vectors"]
    # b is closer than c but excluded by the filter.
    assert [h["key"] for h in hits] == ["a", "c"]
    distances = {h["key"]: h["distance"] for h in hits}
    assert distances["a"] == pytest.approx(0.0, abs=1e-6)
    assert distances["c"] == pytest.approx(0.02, abs=1e-4)
    assert all(h["metadata"]["color"] == "red" for h in hits)

    vectors.delete_vectors(vectorBucketName=BUCKET, indexName=INDEX, keys=["b"])
    listed = vectors.list_vectors(vectorBucketName=BUCKET, indexName=INDEX)["vectors"]
    assert sorted(v["key"] for v in listed) == ["a", "c"]

    vectors.delete_index(vectorBucketName=BUCKET, indexName=INDEX)
    vectors.delete_vector_bucket(vectorBucketName=BUCKET)


def test_vector_bucket_policy(vectors):
    vectors.create_vector_bucket(vectorBucketName=BUCKET)
    try:
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "s3tables:*",
                        "Resource": "*",
                    }
                ],
            }
        )
        vectors.put_vector_bucket_policy(vectorBucketName=BUCKET, policy=policy)
        got = vectors.get_vector_bucket_policy(vectorBucketName=BUCKET)["policy"]
        assert json.loads(got) == json.loads(policy)

        vectors.delete_vector_bucket_policy(vectorBucketName=BUCKET)
        with pytest.raises(ClientError) as exc:
            vectors.get_vector_bucket_policy(vectorBucketName=BUCKET)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"
    finally:
        vectors.delete_vector_bucket(vectorBucketName=BUCKET)


@pytest.fixture
def index(vectors):
    bucket = "vectors-" + uuid.uuid4().hex[:16]
    vectors.create_vector_bucket(vectorBucketName=bucket)
    vectors.create_index(vectorBucketName=bucket, indexName=INDEX, dataType="float32",
                         dimension=4, distanceMetric="euclidean",
                         metadataConfiguration={"nonFilterableMetadataKeys": ["private"]})
    args = {"vectorBucketName": bucket, "indexName": INDEX}
    yield args
    vectors.delete_index(**args)
    vectors.delete_vector_bucket(vectorBucketName=bucket)


def expect_error(code, operation, **kwargs):
    with pytest.raises(ClientError) as exc:
        operation(**kwargs)
    assert exc.value.response["Error"]["Code"] == code


def test_empty_catalog(vectors):
    bucket = "empty-" + uuid.uuid4().hex[:16]
    assert vectors.list_vector_buckets(prefix=bucket)["vectorBuckets"] == []
    vectors.create_vector_bucket(vectorBucketName=bucket)
    try:
        assert vectors.list_indexes(vectorBucketName=bucket)["indexes"] == []
    finally:
        vectors.delete_vector_bucket(vectorBucketName=bucket)


def test_empty_index(vectors, index):
    assert vectors.get_vectors(**index, keys=["absent"])["vectors"] == []
    assert vectors.list_vectors(**index)["vectors"] == []
    assert vectors.query_vectors(**index, queryVector={"float32": [1, 0, 0, 0]}, topK=3)["vectors"] == []
    expect_error("ValidationException", vectors.query_vectors, **index,
                 queryVector={"float32": [1, 0, 0]}, topK=3)


def test_upsert_and_invalid_batch(vectors, index):
    vectors.put_vectors(**index, vectors=[{"key": "a", "data": {"float32": [1, 0, 0, 0]}, "metadata": {"revision": 1}}])
    vectors.put_vectors(**index, vectors=[{"key": "a", "data": {"float32": [0, 1, 0, 0]}, "metadata": {"revision": 2}}])
    expected = [{"key": "a", "data": {"float32": [0, 1, 0, 0]}, "metadata": {"revision": 2}}]
    for invalid in (
        [{"key": "a", "data": {"float32": [1, 1, 1, 1]}}, {"key": "bad", "data": {"float32": [1, 2]}}],
        [{"key": "a", "data": {"float32": [1, 1, 1, 1]}}, {"key": "a", "data": {"float32": [2, 2, 2, 2]}}],
    ):
        expect_error("ValidationException", vectors.put_vectors, **index, vectors=invalid)
        assert vectors.list_vectors(**index, returnData=True, returnMetadata=True)["vectors"] == expected


def test_metadata_and_arn(vectors, index):
    description = vectors.get_index(**index)["index"]
    assert description["metadataConfiguration"] == {"nonFilterableMetadataKeys": ["private"]}
    arn = {"indexArn": description["indexArn"]}
    metadata = {"color": "red", "private": "secret", "nested": {"x": [1, True, "text"]}}
    vectors.put_vectors(**arn, vectors=[{"key": "quote'key", "data": {"float32": [1, 0, 0, 0]}, "metadata": metadata}])
    assert vectors.get_vectors(**arn, keys=["quote'key"], returnMetadata=True)["vectors"][0]["metadata"] == metadata
    hits = vectors.query_vectors(**arn, queryVector={"float32": [1, 0, 0, 0]}, topK=1, returnMetadata=True)["vectors"]
    assert hits[0]["metadata"] == metadata
    assert "distance" not in hits[0]
    expect_error("ValidationException", vectors.query_vectors, **arn, queryVector={"float32": [1, 0, 0, 0]},
                 topK=1, filter={"$and": [{"color": "red"}, {"private": "secret"}]})
    vectors.delete_vectors(**arn, keys=["quote'key"])
    assert vectors.list_vectors(**arn)["vectors"] == []


def test_list_pagination_and_segments(vectors, index):
    rows = [{"key": f"key-{i:02}", "data": {"float32": [i, 1, 0, 0]}, "metadata": {"row": i}} for i in range(17)]
    vectors.put_vectors(**index, vectors=rows)

    def collect(**extra):
        result, tokens = [], set()
        while True:
            page = vectors.list_vectors(**index, maxResults=2, returnData=True, returnMetadata=True, **extra)
            result.extend(page["vectors"])
            token = page.get("nextToken")
            if not token:
                return result
            assert token not in tokens
            tokens.add(token)
            extra["nextToken"] = token

    assert sorted(collect(), key=lambda r: r["key"]) == rows
    segmented = [row for segment in range(4) for row in collect(segmentCount=4, segmentIndex=segment)]
    assert sorted(segmented, key=lambda r: r["key"]) == rows
    expect_error("ValidationException", vectors.list_vectors, **index, nextToken="invalid")


def test_filter_types(vectors, index):
    values = [1, "1", True, "true"]
    vectors.put_vectors(**index, vectors=[{"key": str(i), "data": {"float32": [1, 0, 0, 0]}, "metadata": {"value": value}}
                                         for i, value in enumerate(values)])
    for i, value in enumerate(values):
        hits = vectors.query_vectors(**index, queryVector={"float32": [1, 0, 0, 0]}, topK=4,
                                     filter={"value": {"$eq": value}})["vectors"]
        assert [hit["key"] for hit in hits] == [str(i)]


def test_cosine_metric(vectors):
    bucket = "cosine-" + uuid.uuid4().hex[:16]
    vectors.create_vector_bucket(vectorBucketName=bucket)
    args = {"vectorBucketName": bucket, "indexName": INDEX}
    vectors.create_index(**args, dataType="float32", dimension=2, distanceMetric="cosine")
    try:
        vectors.put_vectors(**args, vectors=[{"key": "far", "data": {"float32": [10, 0]}},
                                            {"key": "near", "data": {"float32": [1, 1]}}])
        result = vectors.query_vectors(**args, queryVector={"float32": [1, 0]}, topK=2, returnDistance=True)
        assert result["distanceMetric"] == "cosine"
        assert [v["key"] for v in result["vectors"]] == ["far", "near"]
        assert result["vectors"][0]["distance"] == pytest.approx(0, abs=1e-6)
        expect_error("ValidationException", vectors.query_vectors, **args, queryVector={"float32": [1, 0, 0]}, topK=1)
    finally:
        vectors.delete_index(**args)
        vectors.delete_vector_bucket(vectorBucketName=bucket)


def test_delete_bucket_conflict(vectors, index):
    expect_error("ConflictException", vectors.delete_vector_bucket, vectorBucketName=index["vectorBucketName"])
    assert vectors.get_index(**index)["index"]["dimension"] == 4


def test_invalid_credentials(cluster):
    client = boto3.client("s3vectors", endpoint_url=cluster["endpoint"], aws_access_key_id="unknown",
                          aws_secret_access_key="unknown", region_name="us-east-1")
    with pytest.raises(ClientError) as exc:
        client.list_vector_buckets()
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_query_without_writer(vectors, index, cluster):
    from conftest import stop

    vectors.put_vectors(**index, vectors=[{"key": "survivor", "data": {"float32": [1, 0, 0, 0]}}])
    stop(cluster["processes"].pop("vector"))
    args = dict(index, queryVector={"float32": [1, 0, 0, 0]}, topK=1)
    if cluster["mode"] == "single":
        expect_error("ServiceUnavailableException", vectors.query_vectors, **args)
        return
    deadline = time.monotonic() + 15
    while True:
        try:
            result = vectors.query_vectors(**args)
            break
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ServiceUnavailableException" or time.monotonic() >= deadline:
                raise
            time.sleep(0.2)
    assert [v["key"] for v in result["vectors"]] == ["survivor"]
