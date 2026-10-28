"""Boto3 conformance subset for the S3 Vectors API gateway.

Run via `make test` — a mini cluster, an s3 gateway with -port.vectors, and a
weed-vector-server are up by then.
"""

import boto3
import pytest

ENDPOINT = "http://127.0.0.1:8334"
BUCKET = "vectors-bucket"
INDEX = "idx"


@pytest.fixture(scope="module")
def vectors():
    return boto3.client(
        "s3vectors",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


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
    # b is the second-nearest vector but blue, so an ignored filter would
    # return ["a", "b"] here.
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
