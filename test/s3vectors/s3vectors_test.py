import urllib.request
"""Run the S3 Vectors API against Go/Rust volumes and single/distributed queries."""

import contextlib
import json
import time
import uuid

import boto3
from botocore.config import Config
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


def assert_engine_wrote_like_the_gateway(cluster, index):
    """The engine writes the table's files itself: the filer holds entries the
    gateway could have written (an ETag, chunks, no inline content), and every
    chunk carries a cipher key exactly when VECTOR_ENCRYPT_VOLUMES encrypts at
    the filer."""
    import os
    import urllib.request

    table = f"/buckets/{index['vectorBucketName']}/vector/{index['indexName']}"
    listing = urllib.request.Request(f"http://127.0.0.1:{cluster['filer']}{table}/data/", headers={"Accept": "application/json"})
    with urllib.request.urlopen(listing, timeout=5) as response:
        entries = json.load(response)["Entries"] or []
    assert entries, "the engine wrote no data file"
    for entry in entries:
        with urllib.request.urlopen(f"http://127.0.0.1:{cluster['filer']}{table}/data/{entry['FullPath'].rsplit('/', 1)[1]}?metadata=true", timeout=5) as response:
            meta = json.load(response)
        assert meta["chunks"], meta
        assert "Seaweed-X-Amz-ETag" in (meta.get("Extended") or {}), meta.get("Extended")
        if os.environ.get("VECTOR_ENCRYPT_VOLUMES"):
            assert all(chunk.get("cipher_key") for chunk in meta["chunks"]), meta["chunks"]
        else:
            assert not any(chunk.get("cipher_key") for chunk in meta["chunks"]), meta["chunks"]


def run_cli(cluster, *args):
    """weed-vector-cli's JSON answer. It reaches the cluster through the
    namespace that names the tables, the masters whose filers hold them and the
    cluster token's key, and finds security.toml and the read key where the
    weed processes do."""
    import os
    import subprocess
    from pathlib import Path
    cli = Path(os.environ.get("VECTOR_CLI_BINARY", Path(__file__).resolve().parent / "weed-vector-cli")).resolve()
    command = [str(cli), "--namespace", cluster["catalog"], "--masters", cluster["masters"],
               "--filer-signing-key", cluster["filer_signing_key"], "--filer-group", os.environ.get("VECTOR_FILER_GROUP", ""), *args]
    return json.loads(subprocess.run(command, check=True, capture_output=True, text=True,
                                     cwd=cluster["directory"], env=cluster["env"]).stdout)


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


def test_upsert_and_invalid_batch(vectors, index, cluster):
    vectors.put_vectors(**index, vectors=[{"key": "a", "data": {"float32": [1, 0, 0, 0]}, "metadata": {"revision": 1}}])
    vectors.put_vectors(**index, vectors=[{"key": "a", "data": {"float32": [0, 1, 0, 0]}, "metadata": {"revision": 2}}])
    expected = [{"key": "a", "data": {"float32": [0, 1, 0, 0]}, "metadata": {"revision": 2}}]
    assert_engine_wrote_like_the_gateway(cluster, index)
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


def test_filter_arrays(vectors, index):
    rows = {"space": ["space", "classic"], "nums": [1, 2.5], "strs": ["1", "2.5"], "flag": [True], "scalar": "space"}
    vectors.put_vectors(**index, vectors=[{"key": key, "data": {"float32": [1, 0, 0, 0]}, "metadata": {"tags": tags}}
                                         for key, tags in rows.items()])

    def matches(condition):
        hits = vectors.query_vectors(**index, queryVector={"float32": [1, 0, 0, 0]}, topK=10,
                                     filter={"tags": condition})["vectors"]
        return sorted(hit["key"] for hit in hits)

    assert matches("space") == ["scalar", "space"]
    assert matches({"$eq": 1}) == ["nums"]
    assert matches({"$eq": "1"}) == ["strs"]
    assert matches({"$eq": True}) == ["flag"]
    assert matches({"$in": ["classic", 2.5]}) == ["nums", "space"]
    assert matches({"$ne": "space"}) == ["flag", "nums", "strs"]
    assert matches({"$nin": [1, "2.5"]}) == ["flag", "scalar", "space"]


def test_euclidean_distance(vectors, index):
    vectors.put_vectors(**index, vectors=[{"key": "p", "data": {"float32": [3, 4, 0, 0]}}])
    result = vectors.query_vectors(**index, queryVector={"float32": [0, 0, 0, 0]}, topK=1, returnDistance=True)
    assert result["distanceMetric"] == "euclidean"
    assert result["vectors"][0]["distance"] == pytest.approx(5.0)


def test_sse_s3_bucket(vectors, cluster):
    """An index in a bucket that encrypts with SSE-S3 is written encrypted by
    the engine, under keys the gateway can unwrap: the entries carry the
    SSE-S3 attributes, the chunks hold ciphertext, and the gateway's own S3 API
    serves the files decrypted."""
    import urllib.request

    bucket = "sealed-" + uuid.uuid4().hex[:16]
    vectors.create_vector_bucket(vectorBucketName=bucket, encryptionConfiguration={"sseType": "AES256"})
    args = {"vectorBucketName": bucket, "indexName": INDEX}
    # A small index, whose data file is one chunk, and a wide one whose data
    # file spans several: the gateway reads the two differently.
    vectors.create_index(**args, dataType="float32", dimension=2, distanceMetric="euclidean")
    vectors.put_vectors(**args, vectors=[{"key": "a", "data": {"float32": [1, 0]}},
                                         {"key": "b", "data": {"float32": [0, 1]}}])
    hits = vectors.query_vectors(**args, queryVector={"float32": [0, 1]}, topK=1)["vectors"]
    assert [h["key"] for h in hits] == ["b"]
    wide = {"vectorBucketName": bucket, "indexName": "wide"}
    vectors.create_index(**wide, dataType="float32", dimension=4096, distanceMetric="euclidean")
    vectors.put_vectors(**wide, vectors=[{"key": f"w{i}", "data": {"float32": [float(i)] * 4096}} for i in range(300)])
    hits = vectors.query_vectors(**wide, queryVector={"float32": [7.0] * 4096}, topK=1)["vectors"]
    assert [h["key"] for h in hits] == ["w7"]

    s3 = boto3.client("s3", endpoint_url=cluster["endpoint"], aws_access_key_id="test", aws_secret_access_key="test",
                      region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
    chunk_counts = set()
    for index in (INDEX, "wide"):
        table = f"/buckets/{bucket}/vector/{index}"
        listing = urllib.request.Request(f"http://127.0.0.1:{cluster['filer']}{table}/data/", headers={"Accept": "application/json"})
        with urllib.request.urlopen(listing, timeout=5) as response:
            names = [e["FullPath"].rsplit("/", 1)[1] for e in json.load(response)["Entries"] or []]
        assert names, f"the engine wrote no data file for {index}"
        for name in names:
            with urllib.request.urlopen(f"http://127.0.0.1:{cluster['filer']}{table}/data/{name}?metadata=true", timeout=5) as response:
                meta = json.load(response)
            assert "x-seaweedfs-sse-s3-key" in meta["Extended"], meta["Extended"].keys()
            assert all(chunk.get("sse_type") == 3 for chunk in meta["chunks"]), meta["chunks"]
            chunk_counts.add(len(meta["chunks"]))
            # The gateway's own S3 API serves the file decrypted: a Lance
            # file, its magic in the footer, of the size the filer records.
            through_gateway = s3.get_object(Bucket=bucket, Key=f"vector/{index}/data/{name}")["Body"].read()
            assert through_gateway[-4:] == b"LANC", (index, name, through_gateway[-16:])
            assert len(through_gateway) == meta["FileSize"], (len(through_gateway), meta["FileSize"])
    assert 1 in chunk_counts and max(chunk_counts) > 1, chunk_counts

    vectors.delete_index(**wide)
    vectors.delete_index(**args)
    vectors.delete_vector_bucket(vectorBucketName=bucket)


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


def index_object_encryption(vectors, bucket):
    s3 = boto3.client("s3", endpoint_url=vectors.meta.endpoint_url, aws_access_key_id="test",
                      aws_secret_access_key="test", region_name="us-east-1")
    keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"vector/{INDEX}/")
            for o in page.get("Contents", []) if o["Size"] > 0]
    assert any("/data/" in key for key in keys) and any("/_versions/" in key for key in keys)
    encryption = {}
    for key in keys:
        try:
            encryption[key] = s3.head_object(Bucket=bucket, Key=key).get("ServerSideEncryption")
        except ClientError as exc:
            # a query's lease tag is released in the background after it answers
            if "/_refs/tags/weed-vector-query-" in key and exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                continue
            raise
    return encryption


def test_vector_bucket_encryption(vectors):
    bucket = "encryption-" + uuid.uuid4().hex[:16]
    plain = bucket + "-plain"
    config = {"sseType": "AES256"}
    vectors.create_vector_bucket(vectorBucketName=bucket, encryptionConfiguration=config)
    vectors.create_vector_bucket(vectorBucketName=plain)
    try:
        assert vectors.get_vector_bucket(vectorBucketName=bucket)["vectorBucket"]["encryptionConfiguration"] == config
        expect_error("ValidationException", vectors.create_vector_bucket, vectorBucketName=bucket + "-kms",
                     encryptionConfiguration={"sseType": "aws:kms", "kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/test"})
        assert vectors.list_vector_buckets(prefix=bucket + "-kms")["vectorBuckets"] == []

        for name in (bucket, plain):
            vectors.create_index(vectorBucketName=name, indexName=INDEX, dataType="float32", dimension=4,
                                 distanceMetric="euclidean")
            vectors.put_vectors(vectorBucketName=name, indexName=INDEX, vectors=[
                {"key": "a", "data": {"float32": [1, 0, 0, 0]}, "metadata": {"color": "red"}},
                {"key": "b", "data": {"float32": [0, 1, 0, 0]}, "metadata": {"color": "blue"}},
            ])
            hits = vectors.query_vectors(vectorBucketName=name, indexName=INDEX, queryVector={"float32": [1, 0, 0, 0]},
                                         topK=1, returnMetadata=True)["vectors"]
            assert [(h["key"], h["metadata"]) for h in hits] == [("a", {"color": "red"})]
        assert set(index_object_encryption(vectors, bucket).values()) == {"AES256"}
        assert set(index_object_encryption(vectors, plain).values()) == {None}
    finally:
        for name in (bucket, plain):
            with contextlib.suppress(ClientError):
                vectors.delete_index(vectorBucketName=name, indexName=INDEX)
            vectors.delete_vector_bucket(vectorBucketName=name)


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


def test_resource_tags(vectors, vector_clients, index):
    reader = vector_clients["reader"]
    bucket_arn = vectors.get_vector_bucket(vectorBucketName=index["vectorBucketName"])["vectorBucket"]["vectorBucketArn"]
    index_arn = vectors.get_index(**index)["index"]["indexArn"]
    assert vectors.list_tags_for_resource(resourceArn=bucket_arn)["tags"] == {}
    vectors.tag_resource(resourceArn=bucket_arn, tags={"team": "search", "env": "dev"})
    vectors.tag_resource(resourceArn=index_arn, tags={"team": "ranking"})
    assert vectors.list_tags_for_resource(resourceArn=bucket_arn)["tags"] == {"team": "search", "env": "dev"}
    assert vectors.list_tags_for_resource(resourceArn=index_arn)["tags"] == {"team": "ranking"}
    vectors.untag_resource(resourceArn=bucket_arn, tagKeys=["env", "absent"])
    assert vectors.list_tags_for_resource(resourceArn=bucket_arn)["tags"] == {"team": "search"}
    expect_error("ValidationException", vectors.tag_resource, resourceArn=bucket_arn,
                 tags={f"k{i}": "v" for i in range(50)})
    expect_error("NotFoundException", vectors.list_tags_for_resource, resourceArn=bucket_arn + "/index/missing")
    expect_error("AccessDeniedException", reader.list_tags_for_resource, resourceArn=bucket_arn)
    expect_error("AccessDeniedException", reader.tag_resource, resourceArn=index_arn, tags={"team": "other"})
    assert vectors.list_tags_for_resource(resourceArn=index_arn)["tags"] == {"team": "ranking"}


def test_create_with_tags(vectors):
    bucket = "tagged-" + uuid.uuid4().hex[:16]
    bucket_arn = vectors.create_vector_bucket(vectorBucketName=bucket, tags={"team": "search"})["vectorBucketArn"]
    try:
        assert vectors.list_tags_for_resource(resourceArn=bucket_arn)["tags"] == {"team": "search"}
        index_arn = vectors.create_index(vectorBucketName=bucket, indexName=INDEX, dataType="float32", dimension=4,
                                         distanceMetric="cosine", tags={"tier": "hot"})["indexArn"]
        try:
            assert vectors.list_tags_for_resource(resourceArn=index_arn)["tags"] == {"tier": "hot"}
        finally:
            vectors.delete_index(vectorBucketName=bucket, indexName=INDEX)
    finally:
        vectors.delete_vector_bucket(vectorBucketName=bucket)


def test_tag_conditions(vectors, vector_clients, index):
    reader, writer = vector_clients["reader"], vector_clients["writer"]
    bucket = index["vectorBucketName"]
    bucket_arn = vectors.get_vector_bucket(vectorBucketName=bucket)["vectorBucket"]["vectorBucketArn"]
    index_arn = vectors.get_index(**index)["index"]["indexArn"]
    other = dict(index, indexName="other-index")
    vectors.create_index(**other, dataType="float32", dimension=4, distanceMetric="euclidean")
    vectors.tag_resource(resourceArn=bucket_arn, tags={"env": "prod"})
    vectors.tag_resource(resourceArn=index_arn, tags={"stage": "live"})
    indexes = bucket_arn + "/index/*"
    statements = [
        {"Effect": "Allow", "Principal": {"AWS": "reader"}, "Action": "s3vectors:GetIndex", "Resource": indexes,
         "Condition": {"StringEquals": {"aws:ResourceTag/stage": "live"}}},
        {"Effect": "Allow", "Principal": {"AWS": "reader"}, "Action": "s3vectors:QueryVectors", "Resource": indexes,
         "Condition": {"StringEquals": {"s3vectors:VectorBucketTag/env": "prod"}}},
        {"Effect": "Allow", "Principal": {"AWS": "writer"}, "Action": "s3vectors:TagResource", "Resource": indexes,
         "Condition": {"StringEquals": {"aws:RequestTag/team": "search"},
                       "ForAllValues:StringEquals": {"aws:TagKeys": ["team"]}}},
    ]
    try:
        vectors.put_vector_bucket_policy(vectorBucketName=bucket,
                                         policy=json.dumps({"Version": "2012-10-17", "Statement": statements}))
        assert reader.get_index(**index)["index"]["indexArn"] == index_arn
        expect_error("AccessDeniedException", reader.get_index, **other)
        assert reader.query_vectors(**other, queryVector={"float32": [1, 0, 0, 0]}, topK=1)["vectors"] == []
        writer.tag_resource(resourceArn=index_arn, tags={"team": "search"})
        expect_error("AccessDeniedException", writer.tag_resource, resourceArn=index_arn, tags={"team": "other"})
        expect_error("AccessDeniedException", writer.tag_resource, resourceArn=index_arn,
                     tags={"team": "search", "env": "dev"})
        assert vectors.list_tags_for_resource(resourceArn=index_arn)["tags"] == {"stage": "live", "team": "search"}
        vectors.tag_resource(resourceArn=bucket_arn, tags={"env": "dev"})
        expect_error("AccessDeniedException", reader.query_vectors, **other,
                     queryVector={"float32": [1, 0, 0, 0]}, topK=1)
        unsupported = dict(statements[0], Condition={"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}})
        expect_error("ValidationException", vectors.put_vector_bucket_policy, vectorBucketName=bucket,
                     policy=json.dumps({"Version": "2012-10-17", "Statement": [unsupported]}))
    finally:
        vectors.delete_vector_bucket_policy(vectorBucketName=bucket)
        vectors.delete_index(**other)


def exact_neighbours(rows, query, k):
    import math
    qn = math.sqrt(sum(x * x for x in query))
    scored = sorted(((sum(a * b for a, b in zip(v, query)) / (math.sqrt(sum(a * a for a in v)) * qn), key)
                     for key, v in rows.items()), reverse=True)
    return {key for _, key in scored[:k]}


@pytest.mark.worker
def test_vector_index_job(vectors, cluster):
    """The maintenance worker indexes a table past the row threshold through the
    admin server, queries then run through the index, and rows written after
    the build are still found."""
    import random
    import urllib.request

    bucket, index, dim = "vectors-indexed", "idx", 16
    vectors.create_vector_bucket(vectorBucketName=bucket)
    vectors.create_index(vectorBucketName=bucket, indexName=index, dataType="float32",
                         dimension=dim, distanceMetric="cosine")
    # Embedding-like data: points around shared centres. Independent Gaussian
    # vectors have no locality for an IVF index to use, so recall through one
    # would only track the fraction of partitions probed.
    rng = random.Random(7)
    centres = [[rng.gauss(0, 1) for _ in range(dim)] for _ in range(30)]

    def point():
        centre = rng.choice(centres)
        return [c + 0.3 * rng.gauss(0, 1) for c in centre]

    rows = {f"v{i}": point() for i in range(1200)}
    keys = list(rows)
    for offset in range(0, len(keys), 400):
        vectors.put_vectors(vectorBucketName=bucket, indexName=index,
                            vectors=[{"key": key, "data": {"float32": rows[key]}} for key in keys[offset:offset + 400]])

    admin = cluster["admin"]
    job = "lance_vector_index"

    def call(path, method="GET", body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(f"{admin}/api/plugin/job-types/{job}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"} if data else {})
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)

    call("/config", "PUT", {"admin_runtime": {"enabled": True},
                            "worker_config_values": {"min_rows": {"int64_value": "1000"}}})
    run = call("/run", "POST", {"timeout_seconds": 110})
    assert run["success_count"] == 1 and run["error_count"] == 0, run

    coverage = run_cli(cluster, "coverage", f"{bucket}$vector${index}")
    assert len(coverage) == 1 and coverage[0]["segments"] == 1 and coverage[0]["unindexed_rows"] == 0, coverage
    # A table named by its s3:// location is opened on the cluster as well.
    uri = run_cli(cluster, "describe", f"{bucket}$vector${index}")["uri"]
    assert uri.startswith("s3://"), uri
    direct = run_cli(cluster, "coverage", uri)
    assert direct == coverage, (direct, coverage)

    found = 0
    for _ in range(20):
        query = point()
        hits = vectors.query_vectors(vectorBucketName=bucket, indexName=index,
                                     queryVector={"float32": query}, topK=10)["vectors"]
        assert len(hits) == 10
        found += len({h["key"] for h in hits} & exact_neighbours(rows, query, 10))
    assert found >= 180, f"recall {found}/200"

    late = point()
    vectors.put_vectors(vectorBucketName=bucket, indexName=index, vectors=[{"key": "late", "data": {"float32": late}}])
    hits = vectors.query_vectors(vectorBucketName=bucket, indexName=index, queryVector={"float32": late}, topK=1)
    assert [h["key"] for h in hits["vectors"]] == ["late"]

    again = call("/run", "POST", {"timeout_seconds": 60})
    assert again["detected_count"] == 0, again

    def coverage_now():
        return run_cli(cluster, "coverage", f"{bucket}$vector${index}")[0]

    def optimize(expect_detected):
        request = urllib.request.Request(
            f"{admin}/api/plugin/job-types/lance_optimize_indices/run", method="POST",
            data=json.dumps({"timeout_seconds": 110}).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=120) as response:
            result = json.load(response)
        assert result["detected_count"] == expect_detected and result["error_count"] == 0, result

    # A tenth of the index written after the build brings the optimize job
    # in, even though it is far below its absolute budget.
    optimize(0)
    more = {f"m{i}": point() for i in range(200)}
    vectors.put_vectors(vectorBucketName=bucket, indexName=index,
                        vectors=[{"key": key, "data": {"float32": v}} for key, v in more.items()])
    before = coverage_now()
    assert before["unindexed_rows"] == 201, before
    optimize(1)
    after = coverage_now()
    assert after["unindexed_rows"] == 0 and after["segments"] == 2, after

    # Five times the rows the index was sized for: the job trains it again
    # instead of extending it, and the partitions follow the new row count.
    rows.update(more)
    for offset in range(0, 4600, 400):
        batch = {f"g{offset + i}": point() for i in range(400)}
        rows.update(batch)
        vectors.put_vectors(vectorBucketName=bucket, indexName=index,
                            vectors=[{"key": key, "data": {"float32": v}} for key, v in batch.items()])
    optimize(1)
    retrained = coverage_now()
    assert retrained["unindexed_rows"] == 0 and retrained["segments"] == 1, retrained
    assert retrained["partitions"] > after["partitions"], (before, after, retrained)
    found = 0
    for _ in range(20):
        query = point()
        hits = vectors.query_vectors(vectorBucketName=bucket, indexName=index,
                                     queryVector={"float32": query}, topK=10)["vectors"]
        found += len({h["key"] for h in hits} & exact_neighbours(rows, query, 10))
    assert found >= 180, f"recall after retrain {found}/200"


def test_attached_iam_vector_policies(vectors, cluster):
    client = boto3.client("s3vectors", endpoint_url=cluster["endpoint"], region_name="us-east-1",
                          aws_access_key_id="policy-reader", aws_secret_access_key="policy-reader-secret",
                          config=vectors.meta.config)
    bucket = "vector-iam"
    vectors.create_vector_bucket(vectorBucketName=bucket)
    target = {"vectorBucketName": bucket, "indexName": "docs"}
    vectors.create_index(**target, dataType="float32", dimension=3, distanceMetric="cosine")
    index_arn = vectors.get_index(**target)["index"]["indexArn"]
    bucket_arn = vectors.get_vector_bucket(vectorBucketName=bucket)["vectorBucket"]["vectorBucketArn"]
    query = {**target, "queryVector": {"float32": [1, 0, 0]}, "topK": 1}
    try:
        assert client.query_vectors(**query)["vectors"] == []
        assert client.get_vectors(**target, keys=["missing"])["vectors"] == []
        expect_error("AccessDeniedException", client.put_vectors, **target,
                     vectors=[{"key": "blocked", "data": {"float32": [1, 0, 0]}}])
        vectors.tag_resource(resourceArn=index_arn, tags={"classification": "private"})
        expect_error("AccessDeniedException", client.query_vectors, **query)
        vectors.untag_resource(resourceArn=index_arn, tagKeys=["classification"])
        assert client.query_vectors(**query)["vectors"] == []
        vectors.tag_resource(resourceArn=bucket_arn, tags={"environment": "production"})
        expect_error("AccessDeniedException", client.query_vectors, **query)
        vectors.untag_resource(resourceArn=bucket_arn, tagKeys=["environment"])
        assert client.query_vectors(**query)["vectors"] == []
        vectors.put_vector_bucket_policy(vectorBucketName=bucket, policy=json.dumps({"Version": "2012-10-17", "Statement": [
            {"Effect": "Deny", "Principal": "*", "Action": "s3vectors:QueryVectors", "Resource": index_arn}]}))
        expect_error("AccessDeniedException", client.query_vectors, **query)
        expect_error("AccessDeniedException", client.create_vector_bucket,
                     vectorBucketName="vector-iam-rejected", tags={"classification": "forbidden"})
        expect_error("AccessDeniedException", client.create_vector_bucket,
                     vectorBucketName="vector-iam-key-denied", tags={"forbidden-key": "value"})
    finally:
        vectors.delete_vector_bucket_policy(vectorBucketName=bucket)
        vectors.delete_index(**target)
        vectors.delete_vector_bucket(vectorBucketName=bucket)


def test_invalid_credentials(cluster):
    client = boto3.client("s3vectors", endpoint_url=cluster["endpoint"], aws_access_key_id="unknown",
                          aws_secret_access_key="unknown", region_name="us-east-1")
    with pytest.raises(ClientError) as exc:
        client.list_vector_buckets()
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_vector_engine_observability(vectors, index, cluster):
    vectors.query_vectors(**index, queryVector={"float32": [1, 0, 0, 0]}, topK=1)
    with urllib.request.urlopen(cluster["vector_metrics"] + "/ready", timeout=5) as response:
        assert response.status == 200
    with urllib.request.urlopen(cluster["vector_metrics"] + "/metrics", timeout=5) as response:
        metrics = response.read().decode()
    assert "SeaweedFS_vector_connected 1" in metrics
    assert any(line.startswith("SeaweedFS_vector_request_total{") and 'type="QueryVectors"' in line
               and 'code="OK"' in line for line in metrics.splitlines())
    assert "SeaweedFS_vector_request_seconds_bucket" in metrics


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


def test_engine_without_credentials(open_cluster):
    """A gateway with no S3 identities serves an index like one with."""
    client = boto3.client("s3vectors", endpoint_url=open_cluster["endpoint"], aws_access_key_id="any",
                          aws_secret_access_key="any", region_name="us-east-1",
                          config=Config(connect_timeout=3, read_timeout=20, retries={"max_attempts": 0}))
    index = {"vectorBucketName": BUCKET, "indexName": INDEX}
    client.create_vector_bucket(vectorBucketName=BUCKET)
    client.create_index(**index, dataType="float32", dimension=2, distanceMetric="cosine")
    client.put_vectors(**index, vectors=[{"key": "a", "data": {"float32": [1, 0]}},
                                         {"key": "b", "data": {"float32": [0, 1]}}])
    hits = client.query_vectors(**index, queryVector={"float32": [1, 0]}, topK=1)["vectors"]
    assert [h["key"] for h in hits] == ["a"]



