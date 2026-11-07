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
    PERF_INDEX        1 builds the vector index after ingest with weed-vector-cli,
                      so the queries run through it (PERF_PARTITIONS overrides
                      the partition count)
    PERF_CLUSTERS     draw vectors around this many shared centres instead of
                      independently (0); PERF_NOISE is the spread around a
                      centre (0.3). Independent Gaussian vectors have no
                      locality for an IVF index to use, so recall through one
                      only tracks the fraction of partitions probed.

Each timed scenario starts with one untimed call. Timings only report; the
assertions check correctness (recall against an exact client-side search,
complete listings), never speed. Recall through an index is approximate, so
PERF_INDEX runs report it without asserting it.
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
import subprocess
import time
import uuid

import boto3
from botocore.config import Config
import pytest

HERE = Path(__file__).resolve().parent
VECTORS = int(os.environ.get("PERF_VECTORS", "10000"))
DIM = int(os.environ.get("PERF_DIM", "128"))
BATCH = int(os.environ.get("PERF_BATCH", "500"))
QUERIES = int(os.environ.get("PERF_QUERIES", "100"))
TOPK = int(os.environ.get("PERF_TOPK", "10"))
CONCURRENCY = [int(c) for c in os.environ.get("PERF_CONCURRENCY", "1,4,8").split(",")]
CATEGORIES = 10
RECALL_SAMPLE = int(os.environ.get("PERF_RECALL", "20"))
INDEXED = bool(os.environ.get("PERF_INDEX"))
CLUSTERS = int(os.environ.get("PERF_CLUSTERS", "0"))
NOISE = float(os.environ.get("PERF_NOISE", "0.3"))
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


def recall(data, query, k, hits, keep=None):
    """Hits in the exact cosine top-k over the ingested vectors. Hits within
    1e-6 of the k-th best score fill the slots the top-k leaves for that tie:
    tied vectors are interchangeable, and the server ranks in float32."""
    qn = math.sqrt(sum(x * x for x in query))

    def score(i):
        vector, norm = data[i]
        return sum(a * b for a, b in zip(vector, query)) / (norm * qn)

    def eligible(i):
        return keep is None or keep(i)

    best = sorted((score(i) for i in range(len(data)) if eligible(i)), reverse=True)[:k]
    found = {int(h["key"][1:]) for h in hits}
    cutoff = best[-1]
    scores = [score(i) for i in found if eligible(i)]
    above = sum(1 for s in scores if s > cutoff + 1e-6)
    tied = sum(1 for s in scores if abs(s - cutoff) <= 1e-6)
    slots = len(best) - sum(1 for s in best if s > cutoff + 1e-6)
    return (above + min(tied, slots)) / len(best)


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


@pytest.fixture(scope="module")
def perf(cluster):
    assert 1 <= BATCH <= 500 and VECTORS >= BATCH
    rng = random.Random(1)
    centres = [[rng.gauss(0, 1) for _ in range(DIM)] for _ in range(CLUSTERS)]

    def vector():
        if not centres:
            return [round(rng.gauss(0, 1), 5) for _ in range(DIM)]
        centre = rng.choice(centres)
        return [round(c + NOISE * rng.gauss(0, 1), 5) for c in centre]

    data = []
    for _ in range(VECTORS):
        v = vector()
        data.append((v, math.sqrt(sum(x * x for x in v))))
    queries = [vector() for _ in range(QUERIES)]

    s3v = client(cluster["endpoint"])
    bucket = "perf-" + uuid.uuid4().hex[:12]
    s3v.create_vector_bucket(vectorBucketName=bucket)
    s3v.create_index(vectorBucketName=bucket, indexName=INDEX, dataType="float32",
                     dimension=DIM, distanceMetric="cosine")
    args = {"vectorBucketName": bucket, "indexName": INDEX}
    combo = re.sub(r"^vectors-|\d+$", "", cluster["directory"].name)
    REPORT[combo] = {"config": {"vectors": VECTORS, "dim": DIM, "batch": BATCH, "queries": QUERIES,
                                "clusters": CLUSTERS, "noise": NOISE,
                                "top_k": TOPK, "concurrency": CONCURRENCY, "cpus": os.cpu_count(),
                                "load_1m": os.getloadavg()[0], "machine": platform.machine()}}
    yield {"client": s3v, "endpoint": cluster["endpoint"], "args": args, "data": data,
           "queries": queries, "report": REPORT[combo], "cluster": cluster, "bucket": bucket}
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


def test_build_index(perf):
    if not INDEXED:
        pytest.skip("PERF_INDEX not set")
    cli = Path(os.environ.get("VECTOR_CLI_BINARY", HERE / "weed-vector-cli")).resolve()
    assert cli.is_file(), f"missing {cli}; run make ./weed-vector-cli"
    command = [str(cli), "--namespace", perf["cluster"]["catalog"], "--masters", perf["cluster"]["masters"],
               "--filer-signing-key", perf["cluster"]["filer_signing_key"],
               "index", "build", f"{perf['bucket']}$vector${INDEX}"]
    if os.environ.get("PERF_PARTITIONS"):
        command += ["--partitions", os.environ["PERF_PARTITIONS"]]
    seconds, out = timed(lambda: subprocess.run(command, check=True, capture_output=True, text=True,
                                                cwd=perf["cluster"]["directory"], env=perf["cluster"]["env"]))
    report = json.loads(out.stdout)
    coverage = report["coverage"]
    assert len(coverage) == 1 and coverage[0]["unindexed_rows"] == 0, coverage
    perf["report"]["index"] = {"seconds": seconds, "segments": coverage[0]["segments"],
                               "partitions": int(os.environ.get("PERF_PARTITIONS", "0")) or None}


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
        if keep:
            assert all(keep(int(h["key"][1:])) for h in response["vectors"]), response["vectors"]
        if n < RECALL_SAMPLE:
            recalls.append(recall(data, query, TOPK, response["vectors"], keep))
    perf["report"][name] = {**summarize(latencies), "recall_at_k": statistics.fmean(recalls) if recalls else None}
    assert INDEXED or not recalls or statistics.fmean(recalls) >= 0.9


def test_query(perf):
    run_queries(perf, "query")


def test_query_with_metadata(perf):
    run_queries(perf, "query_return_metadata", returnMetadata=True)


def test_query_filtered(perf):
    run_queries(perf, "query_filtered_10pct", filter={"category": {"$eq": "c3"}})


def test_query_throughput(perf):
    args, queries = perf["args"], perf["queries"]
    results = {}
    for threads in CONCURRENCY:
        clients = [client(perf["endpoint"]) for _ in range(threads)]
        for s3v in clients:
            s3v.query_vectors(**args, queryVector={"float32": queries[0]}, topK=TOPK)
        work = queries * threads

        def run(worker):
            s3v = clients[worker]
            return [timed(lambda: s3v.query_vectors(**args, queryVector={"float32": query}, topK=TOPK))[0]
                    for query in work[worker::threads]]

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(threads) as pool:
            latencies = [s for chunk in pool.map(run, range(threads)) for s in chunk]
        elapsed = time.perf_counter() - start
        results[str(threads)] = {"qps": len(work) / elapsed, **summarize(latencies)}
    perf["report"]["query_throughput"] = results


def test_get_vectors(perf):
    s3v, args = perf["client"], perf["args"]
    rng = random.Random(3)
    size = min(100, VECTORS)
    s3v.get_vectors(**args, keys=["v0"])
    latencies = []
    for _ in range(max(1, QUERIES // 2)):
        keys = [f"v{i}" for i in rng.sample(range(VECTORS), size)]
        seconds, response = timed(lambda: s3v.get_vectors(
            **args, keys=keys, returnData=True, returnMetadata=True))
        latencies.append(seconds)
        assert len(response["vectors"]) == size
    perf["report"]["get_vectors"] = {**summarize(latencies), "keys": size}


def test_list_vectors(perf):
    args = perf["args"]
    results = {}
    for segments in (1, 4, 16):
        clients = [client(perf["endpoint"]) for _ in range(segments)]
        for s3v in clients:
            s3v.list_vectors(**args, maxResults=1)

        def scan(segment):
            extra = {"segmentCount": segments, "segmentIndex": segment} if segments > 1 else {}
            return list_all(clients[segment], args, **extra)

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(segments) as pool:
            listed = [key for keys in pool.map(scan, range(segments)) for key in keys]
        elapsed = time.perf_counter() - start
        assert_complete(listed)
        results[str(segments)] = {"seconds": elapsed, "vectors_per_s": VECTORS / elapsed}
    perf["report"]["list_vectors_segments"] = results


def test_delete_vectors(perf):
    s3v, args = perf["client"], perf["args"]
    size = min(100, VECTORS)
    keys = [f"v{i}" for i in random.Random(4).sample(range(VECTORS), size * min(5, VECTORS // size))]
    latencies = [timed(lambda: s3v.delete_vectors(**args, keys=keys[n:n + size]))[0]
                 for n in range(0, len(keys), size)]
    perf["report"]["delete_vectors"] = {**summarize(latencies), "keys": size}


def print_report(combo, report, path):
    cfg = report["config"]
    lines = [f"\n== S3 Vectors perf [{combo}]: {cfg['vectors']} x {cfg['dim']}d cosine, "
             f"topK {cfg['top_k']}, {cfg['cpus']} CPUs, load {cfg['load_1m']:.1f}"]
    index = report.get("index")
    if index:
        lines.append(f"index built in {index['seconds']:.1f} s, {index['segments']} segment(s)")
    ingest = report.get("ingest")
    if ingest:
        lines.append(f"ingest            {ingest['vectors_per_s']:9.0f} vectors/s  "
                     f"batch p50 {ingest['calls']['p50_ms']:.0f} ms, first tenth "
                     f"{ingest['first_tenth_mean_ms']:.0f} ms -> last tenth {ingest['last_tenth_mean_ms']:.0f} ms")
    for name in ("upsert_existing", "query", "query_return_metadata", "query_filtered_10pct",
                 "get_vectors", "delete_vectors"):
        if name in report:
            r = report[name]
            label = f"{name} ({r['keys']} keys)" if "keys" in r else name
            extra = f"  recall@k {r['recall_at_k']:.3f}" if r.get("recall_at_k") is not None else ""
            lines.append(f"{label:<26} p50 {r['p50_ms']:7.1f} ms  p95 {r['p95_ms']:7.1f} ms  "
                         f"p99 {r['p99_ms']:7.1f} ms{extra}")
    for threads, r in report.get("query_throughput", {}).items():
        lines.append(f"query x{threads:<3} threads    {r['qps']:7.1f} qps   p50 {r['p50_ms']:7.1f} ms  "
                     f"p95 {r['p95_ms']:7.1f} ms")
    for segments, r in report.get("list_vectors_segments", {}).items():
        lines.append(f"list {segments:>2} segments     {r['vectors_per_s']:9.0f} vectors/s  "
                     f"({r['seconds']:.2f} s)")
    lines.append(f"report: {path}")
    print("\n".join(lines))
