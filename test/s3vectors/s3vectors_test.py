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
    assert distances["c"] == pytest.approx(0.02 ** 0.5, abs=1e-4)
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
                        "Action": "s3vectors:*",
                        "Resource": "*",
                    }
                ],
            }
        )
        invalid = json.loads(policy)
        invalid["Statement"][0]["Action"] = "s3tables:*"
        expect_error("ValidationException", vectors.put_vector_bucket_policy,
                     vectorBucketName=BUCKET, policy=json.dumps(invalid))
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
    for bad in ({"maxResults": 1001}, {"segmentCount": 17, "segmentIndex": 0}, {"segmentIndex": 0}):
        expect_error("ValidationException", vectors.list_vectors, **index, **bad)


def test_filter_types(vectors, index):
    values = [1, "1", True, "true"]
    vectors.put_vectors(**index, vectors=[{"key": str(i), "data": {"float32": [1, 0, 0, 0]}, "metadata": {"value": value}}
                                         for i, value in enumerate(values)])
    for i, value in enumerate(values):
        hits = vectors.query_vectors(**index, queryVector={"float32": [1, 0, 0, 0]}, topK=4,
                                     filter={"value": {"$eq": value}})["vectors"]
        assert [hit["key"] for hit in hits] == [str(i)]
    for op, members in (("$in", [1, "true"]), ("$nin", ["1", True])):
        hits = vectors.query_vectors(**index, queryVector={"float32": [1, 0, 0, 0]}, topK=4,
                                     filter={"value": {op: members}})["vectors"]
        assert sorted(hit["key"] for hit in hits) == ["0", "3"]


def test_euclidean_distance(vectors, index):
    vectors.put_vectors(**index, vectors=[{"key": "p", "data": {"float32": [3, 4, 0, 0]}}])
    result = vectors.query_vectors(**index, queryVector={"float32": [0, 0, 0, 0]}, topK=1, returnDistance=True)
    assert result["distanceMetric"] == "euclidean"
    assert result["vectors"][0]["distance"] == pytest.approx(5.0)


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


def test_vector_policy_permissions(vectors, vector_clients, index):
    reader, writer = vector_clients["reader"], vector_clients["writer"]
    arn = vectors.get_index(**index)["index"]["indexArn"]
    row = {"key": "policy-row", "data": {"float32": [1, 0, 0, 0]}, "metadata": {"owner": "writer"}}
    expect_error("AccessDeniedException", writer.put_vectors, **index, vectors=[row])
    expect_error("AccessDeniedException", reader.get_vectors, **index, keys=[row["key"]])
    statements = [
        {"Effect": "Allow", "Principal": {"AWS": "writer"},
         "Action": "s3vectors:PutVectors", "Resource": arn},
        {"Effect": "Allow", "Principal": {"AWS": "reader"},
         "Action": ["s3vectors:GetVectors", "s3vectors:QueryVectors"], "Resource": arn},
    ]

    def put_policy():
        vectors.put_vector_bucket_policy(vectorBucketName=index["vectorBucketName"],
                                         policy=json.dumps({"Version": "2012-10-17", "Statement": statements}))

    put_policy()
    writer.put_vectors(**index, vectors=[row])
    assert reader.get_vectors(**index, keys=[row["key"]], returnData=True, returnMetadata=True)["vectors"] == [row]
    assert reader.query_vectors(**index, queryVector=row["data"], topK=1)["vectors"][0]["key"] == row["key"]
    expect_error("AccessDeniedException", reader.put_vectors, **index, vectors=[row])
    expect_error("AccessDeniedException", writer.get_vectors, **index, keys=[row["key"]])
    expect_error("AccessDeniedException", reader.delete_index, **index)
    expect_error("AccessDeniedException", reader.list_vectors, **index)
    other = dict(index, indexName="other-index")
    vectors.create_index(**other, dataType="float32", dimension=4, distanceMetric="euclidean")
    try:
        expect_error("AccessDeniedException", reader.get_vectors, **other, keys=[row["key"]])
        expect_error("AccessDeniedException", writer.put_vectors, **other, vectors=[row])
        statements.append({"Effect": "Deny", "Principal": {"AWS": "writer"},
                           "Action": "s3vectors:PutVectors", "Resource": arn})
        put_policy()
        expect_error("AccessDeniedException", writer.put_vectors, **index, vectors=[row])
        assert reader.get_vectors(**index, keys=[row["key"]])["vectors"][0]["key"] == row["key"]
        vectors.delete_vector_bucket_policy(vectorBucketName=index["vectorBucketName"])
        expect_error("AccessDeniedException", reader.get_vectors, **index, keys=[row["key"]])
        expect_error("AccessDeniedException", writer.put_vectors, **index, vectors=[row])
        vectors.put_vectors(**index, vectors=[row])
    finally:
        vectors.delete_index(**other)


def test_vector_bucket_encryption(vectors):
    bucket = "encryption-" + uuid.uuid4().hex[:16]
    config = {"sseType": "AES256"}
    vectors.create_vector_bucket(vectorBucketName=bucket, encryptionConfiguration=config)
    try:
        assert vectors.get_vector_bucket(vectorBucketName=bucket)["vectorBucket"]["encryptionConfiguration"] == config
        expect_error("ValidationException", vectors.create_vector_bucket, vectorBucketName=bucket + "-kms",
                     encryptionConfiguration={"sseType": "aws:kms", "kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/test"})
        assert vectors.list_vector_buckets(prefix=bucket + "-kms")["vectorBuckets"] == []
    finally:
        vectors.delete_vector_bucket(vectorBucketName=bucket)


def test_catalog_pagination(vectors, index):
    prefix = "pages-" + uuid.uuid4().hex[:16]
    buckets, indexes = [], []
    try:
        for i in range(3):
            bucket = f"{prefix}-{i}"
            vectors.create_vector_bucket(vectorBucketName=bucket)
            buckets.append(bucket)
        for name in ("idx-a", "idx-b"):
            args = dict(index, indexName=name)
            vectors.create_index(**args, dataType="float32", dimension=4, distanceMetric="euclidean")
            indexes.append(args)
        pages = list(vectors.get_paginator("list_vector_buckets").paginate(
            prefix=prefix, PaginationConfig={"PageSize": 1}))
        assert all(len(page["vectorBuckets"]) <= 1 for page in pages)
        assert sorted(b["vectorBucketName"] for page in pages for b in page["vectorBuckets"]) == buckets
        pages = list(vectors.get_paginator("list_indexes").paginate(
            vectorBucketName=index["vectorBucketName"], prefix="idx", PaginationConfig={"PageSize": 1}))
        assert all(len(page["indexes"]) <= 1 for page in pages)
        assert sorted(i["indexName"] for page in pages for i in page["indexes"]) == [INDEX, "idx-a", "idx-b"]
        expect_error("ValidationException", vectors.list_vector_buckets, maxResults=501)
        expect_error("ValidationException", vectors.list_indexes, vectorBucketName=index["vectorBucketName"], maxResults=501)
    finally:
        for args in indexes:
            vectors.delete_index(**args)
        for bucket in buckets:
            vectors.delete_vector_bucket(vectorBucketName=bucket)


def test_delete_and_recreate_index(vectors, index):
    vectors.put_vectors(**index, vectors=[{"key": "old", "data": {"float32": [1, 0, 0, 0]}}])
    vectors.delete_index(**index)
    expect_error("NotFoundException", vectors.get_index, **index)
    vectors.create_index(**index, dataType="float32", dimension=2, distanceMetric="cosine")
    assert vectors.list_vectors(**index)["vectors"] == []
    row = {"key": "new", "data": {"float32": [1, 0]}}
    vectors.put_vectors(**index, vectors=[row])
    assert vectors.get_vectors(**index, keys=["old", "new"], returnData=True)["vectors"] == [row]
    result = vectors.query_vectors(**index, queryVector=row["data"], topK=1, returnDistance=True)
    assert result["distanceMetric"] == "cosine"
    assert result["vectors"][0]["key"] == "new"
    assert result["vectors"][0]["distance"] == pytest.approx(0, abs=1e-6)


def test_index_arn_validation(vectors, index):
    arn = vectors.get_index(**index)["index"]["indexArn"]
    foreign = []
    for field, value in ((1, "aws-cn"), (3, "eu-west-1"), (4, "999999999999")):
        parts = arn.split(":", 5)
        assert parts[field] != value
        parts[field] = value
        foreign.append(":".join(parts))
    for invalid in (
        *foreign,
        arn.replace(":s3vectors:", ":s3tables:"),
        arn.replace(":s3vectors:", ":s3:"),
        arn + "/index/extra",
        arn + "/",
    ):
        expect_error("ValidationException", vectors.get_index, indexArn=invalid)
        expect_error("ValidationException", vectors.delete_index, indexArn=invalid)
    expect_error("ValidationException", vectors.get_vector_bucket, vectorBucketArn=arn)
    expect_error("ValidationException", vectors.list_indexes, vectorBucketArn=arn)
    bucket_arn = arn.rsplit("/index/", 1)[0]
    for request in (
        {"vectorBucketName": "other-" + index["vectorBucketName"], "indexArn": arn},
        {"indexName": "other-" + index["indexName"], "indexArn": arn},
    ):
        expect_error("ValidationException", vectors.get_index, **request)
        expect_error("ValidationException", vectors.delete_index, **request)
    for operation in (vectors.get_vector_bucket, vectors.list_indexes, vectors.delete_vector_bucket):
        expect_error("ValidationException", operation, vectorBucketName="other-" + index["vectorBucketName"],
                     vectorBucketArn=bucket_arn)
    assert vectors.get_index(**index, indexArn=arn)["index"]["indexArn"] == arn
    assert vectors.get_vector_bucket(vectorBucketName=index["vectorBucketName"],
                                     vectorBucketArn=bucket_arn)["vectorBucket"]["vectorBucketArn"] == bucket_arn
    assert vectors.get_index(**index)["index"]["indexArn"] == arn


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
