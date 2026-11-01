"""Measure the S3 Vectors data plane end to end against the cluster fixture.

Opt-in through run_perf.sh or `make perf`; run_test.sh does not collect it.
Sizes come from the environment so the same module runs a quick smoke pass or
a larger profile:

    PERF_VECTORS      vectors to ingest (10000)
    PERF_DIM          dimension (128)
    PERF_BATCH        vectors per PutVectors call, at most 500 (500)
    PERF_QUERIES      timed queries per scenario (100)
    PERF_TOPK         topK (10)
    PERF_CONCURRENCY  client threads for the throughput scenario ("1,4,8")
    PERF_RECALL       queries checked against an exact search (20)
    PERF_REPORT       JSON report path (perf-results.json in the cluster dir)

Each timed scenario starts with one untimed call. Timings only report; the
assertions check correctness (recall against an exact client-side search,
complete listings), never speed.
"""

import concurrent.futures
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import statistics
import threading
import time
import uuid

import boto3
from botocore.config import Config
import pytest

VECTORS = int(os.environ.get("PERF_VECTORS", "10000"))
DIM = int(os.environ.get("PERF_DIM", "128"))
BATCH = int(os.environ.get("PERF_BATCH", "500"))
QUERIES = int(os.environ.get("PERF_QUERIES", "100"))
TOPK = int(os.environ.get("PERF_TOPK", "10"))
CONCURRENCY = [int(c) for c in os.environ.get("PERF_CONCURRENCY", "1,4,8").split(",")]
CATEGORIES = 10
RECALL_SAMPLE = int(os.environ.get("PERF_RECALL", "20"))
INDEX = "perf"

REPORT = {}


def summarize(seconds):
    ms = sorted(s * 1000 for s in seconds)

    def pct(p):
        return ms[min(len(ms) - 1, max(0, math.ceil(p * len(ms)) - 1))]

    return {"n": len(ms), "mean_ms": statistics.fmean(ms), "p50_ms": pct(0.50),
            "p95_ms": pct(0.95), "p99_ms": pct(0.99), "max_ms": ms[-1]}


def timed(call):
    start = time.perf_counter()
    result = call()
    return time.perf_counter() - start, result


def client(endpoint):
    return boto3.client("s3vectors", endpoint_url=endpoint, aws_access_key_id="test",
                        aws_secret_access_key="test", region_name="us-east-1",
                        config=Config(connect_timeout=3, read_timeout=300, retries={"max_attempts": 0},
                                      max_pool_connections=max(CONCURRENCY) + 2))


def exact_top_k(data, query, k, keep=None):
    """Cosine top-k over the ingested vectors, the reference for recall."""
    qn = math.sqrt(sum(x * x for x in query))
    scored = []
    for i, (vector, norm) in enumerate(data):
        if keep is not None and not keep(i):
            continue
        scored.append((sum(a * b for a, b in zip(vector, query)) / (norm * qn), i))
    scored.sort(reverse=True)
    return {f"v{i}" for _, i in scored[:k]}


def list_all(s3v, args, **extra):
    keys, token = [], None
    while True:
        page = s3v.list_vectors(**args, maxResults=500, **extra, **({"nextToken": token} if token else {}))
        keys.extend(vector["key"] for vector in page["vectors"])
        token = page.get("nextToken")
        if not token:
            return keys


def assert_complete(keys):
    assert len(keys) == VECTORS and set(keys) == {f"v{i}" for i in range(VECTORS)}


def recall(expected, hits):
    return len(expected & {h["key"] for h in hits}) / len(expected)


@pytest.fixture(scope="module")
def perf(cluster):
    assert 1 <= BATCH <= 500 and VECTORS >= BATCH
    rng = random.Random(1)
    data = []
    for _ in range(VECTORS):
        vector = [round(rng.gauss(0, 1), 5) for _ in range(DIM)]
        data.append((vector, math.sqrt(sum(x * x for x in vector))))
    queries = [[round(rng.gauss(0, 1), 5) for _ in range(DIM)] for _ in range(QUERIES)]

    s3v = client(cluster["endpoint"])
    bucket = "perf-" + uuid.uuid4().hex[:12]
    s3v.create_vector_bucket(vectorBucketName=bucket)
    s3v.create_index(vectorBucketName=bucket, indexName=INDEX, dataType="float32",
                     dimension=DIM, distanceMetric="cosine")
    args = {"vectorBucketName": bucket, "indexName": INDEX}
    combo = re.sub(r"^vectors-|\d+$", "", cluster["directory"].name)
    REPORT[combo] = {"config": {"vectors": VECTORS, "dim": DIM, "batch": BATCH, "queries": QUERIES,
                                "top_k": TOPK, "concurrency": CONCURRENCY, "cpus": os.cpu_count(),
                                "load_1m": os.getloadavg()[0], "machine": platform.machine()}}
    yield {"client": s3v, "endpoint": cluster["endpoint"], "args": args, "data": data,
           "queries": queries, "report": REPORT[combo]}
    s3v.delete_index(**args)
    s3v.delete_vector_bucket(vectorBucketName=bucket)
    path = Path(os.environ.get("PERF_REPORT", cluster["directory"] / "perf-results.json"))
    path.write_text(json.dumps(REPORT, indent=2))
    print_report(combo, REPORT[combo], path)


def test_ingest(perf):
    s3v, args, data = perf["client"], perf["args"], perf["data"]
    latencies = []
    start = time.perf_counter()
    for offset in range(0, VECTORS, BATCH):
        batch = [{"key": f"v{i}", "data": {"float32": data[i][0]},
                  "metadata": {"category": f"c{i % CATEGORIES}", "n": i}}
                 for i in range(offset, min(offset + BATCH, VECTORS))]
        seconds, _ = timed(lambda: s3v.put_vectors(**args, vectors=batch))
        latencies.append(seconds)
    total = time.perf_counter() - start
    tenth = max(1, len(latencies) // 10)
    perf["report"]["ingest"] = {
        "vectors_per_s": VECTORS / total, "seconds": total, "calls": summarize(latencies),
        "first_tenth_mean_ms": statistics.fmean(latencies[:tenth]) * 1000,
        "last_tenth_mean_ms": statistics.fmean(latencies[-tenth:]) * 1000,
    }


def test_upsert_existing(perf):
    s3v, args, data = perf["client"], perf["args"], perf["data"]
    keys = random.Random(2).sample(range(VECTORS), BATCH)
    batch = [{"key": f"v{i}", "data": {"float32": data[i][0]},
              "metadata": {"category": f"c{i % CATEGORIES}", "n": i}} for i in keys]
    latencies = [timed(lambda: s3v.put_vectors(**args, vectors=batch))[0] for _ in range(5)]
    perf["report"]["upsert_existing"] = summarize(latencies)
    assert_complete(list_all(s3v, args))


def run_queries(perf, name, **extra):
    s3v, args, data, queries = perf["client"], perf["args"], perf["data"], perf["queries"]
    latencies, recalls = [], []
    keep = None
    if "filter" in extra:
        keep = lambda i: i % CATEGORIES == 3  # noqa: E731
    s3v.query_vectors(**args, queryVector={"float32": queries[0]}, topK=TOPK, **extra)
    for n, query in enumerate(queries):
        seconds, response = timed(lambda: s3v.query_vectors(
            **args, queryVector={"float32": query}, topK=TOPK, returnDistance=True, **extra))
        latencies.append(seconds)
        if n < RECALL_SAMPLE:
            recalls.append(recall(exact_top_k(data, query, TOPK, keep), response["vectors"]))
    perf["report"][name] = {**summarize(latencies), "recall_at_k": statistics.fmean(recalls) if recalls else None}
    assert not recalls or statistics.fmean(recalls) >= 0.9


def test_query(perf):
    run_queries(perf, "query")


def test_query_with_metadata(perf):
    run_queries(perf, "query_return_metadata", returnMetadata=True)


def test_query_filtered(perf):
    run_queries(perf, "query_filtered_10pct", filter={"category": {"$eq": "c3"}})


def test_query_throughput(perf):
    args, queries = perf["args"], perf["queries"]
    local = threading.local()
    results = {}
    for threads in CONCURRENCY:
        def one(query):
            if not hasattr(local, "client"):
                local.client = client(perf["endpoint"])
            return timed(lambda: local.client.query_vectors(
                **args, queryVector={"float32": query}, topK=TOPK))[0]

        work = (queries * math.ceil(QUERIES * threads / len(queries)))[: QUERIES * threads]
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(threads) as pool:
            latencies = list(pool.map(one, work))
        elapsed = time.perf_counter() - start
        results[str(threads)] = {"qps": len(work) / elapsed, **summarize(latencies)}
    perf["report"]["query_throughput"] = results


def test_get_vectors(perf):
    s3v, args = perf["client"], perf["args"]
    rng = random.Random(3)
    s3v.get_vectors(**args, keys=["v0"])
    latencies = []
    for _ in range(max(1, QUERIES // 2)):
        keys = [f"v{i}" for i in rng.sample(range(VECTORS), 100)]
        seconds, response = timed(lambda: s3v.get_vectors(
            **args, keys=keys, returnData=True, returnMetadata=True))
        latencies.append(seconds)
        assert len(response["vectors"]) == 100
    perf["report"]["get_vectors_100_keys"] = summarize(latencies)


def test_list_vectors(perf):
    args = perf["args"]
    local = threading.local()
    results = {}
    for segments in (1, 4, 16):
        def scan(segment):
            if not hasattr(local, "client"):
                local.client = client(perf["endpoint"])
            extra = {"segmentCount": segments, "segmentIndex": segment} if segments > 1 else {}
            return list_all(local.client, args, **extra)

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(segments) as pool:
            listed = [key for keys in pool.map(scan, range(segments)) for key in keys]
        elapsed = time.perf_counter() - start
        assert_complete(listed)
        results[str(segments)] = {"seconds": elapsed, "vectors_per_s": VECTORS / elapsed}
    perf["report"]["list_vectors_segments"] = results


def test_delete_vectors(perf):
    s3v, args = perf["client"], perf["args"]
    keys = [f"v{i}" for i in random.Random(4).sample(range(VECTORS), 500)]
    latencies = [timed(lambda: s3v.delete_vectors(**args, keys=keys[n:n + 100]))[0]
                 for n in range(0, len(keys), 100)]
    perf["report"]["delete_vectors_100_keys"] = summarize(latencies)


def print_report(combo, report, path):
    cfg = report["config"]
    lines = [f"\n== S3 Vectors perf [{combo}]: {cfg['vectors']} x {cfg['dim']}d cosine, "
             f"topK {cfg['top_k']}, {cfg['cpus']} CPUs, load {cfg['load_1m']:.1f}"]
    ingest = report.get("ingest")
    if ingest:
        lines.append(f"ingest            {ingest['vectors_per_s']:9.0f} vectors/s  "
                     f"batch p50 {ingest['calls']['p50_ms']:.0f} ms, first tenth "
                     f"{ingest['first_tenth_mean_ms']:.0f} ms -> last tenth {ingest['last_tenth_mean_ms']:.0f} ms")
    for name in ("upsert_existing", "query", "query_return_metadata", "query_filtered_10pct",
                 "get_vectors_100_keys", "delete_vectors_100_keys"):
        if name in report:
            r = report[name]
            extra = f"  recall@k {r['recall_at_k']:.3f}" if r.get("recall_at_k") is not None else ""
            lines.append(f"{name:<24} p50 {r['p50_ms']:7.1f} ms  p95 {r['p95_ms']:7.1f} ms  "
                         f"p99 {r['p99_ms']:7.1f} ms{extra}")
    for threads, r in report.get("query_throughput", {}).items():
        lines.append(f"query x{threads:<3} threads    {r['qps']:7.1f} qps   p50 {r['p50_ms']:7.1f} ms  "
                     f"p95 {r['p95_ms']:7.1f} ms")
    for segments, r in report.get("list_vectors_segments", {}).items():
        lines.append(f"list {segments:>2} segments     {r['vectors_per_s']:9.0f} vectors/s  "
                     f"({r['seconds']:.2f} s)")
    lines.append(f"report: {path}")
    print("\n".join(lines))
