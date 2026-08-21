#!/usr/bin/env python3
"""PyIceberg half of the table lifecycle, run one phase per invocation.

The Go test calls this three times - write, verify, drop - and runs the
maintenance worker between the first two. Splitting it that way is the whole
point: a tally taken before compaction and the same tally taken after are the
only thing that catches a merge that rewrote every dictionary-encoded column
onto a single value and said nothing.
"""

import argparse
import hashlib
import json
import sys
import time

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

# Few enough distinct values in the two string columns that any writer worth
# the name dictionary-encodes them, which is the encoding that broke.
CATEGORIES = 7
VALUES = 13
ROWS_PER_BATCH = 4000
BATCHES = 3

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "category", StringType(), required=True),
    NestedField(3, "value", StringType(), required=True),
    NestedField(4, "ts", TimestamptzType(), required=True),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("category", pa.string(), nullable=False),
        pa.field("value", pa.string(), nullable=False),
        pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def batch(start, count):
    ids = list(range(start, start + count))
    return pa.Table.from_pydict(
        {
            "id": ids,
            "category": [f"cat-{i % CATEGORIES}" for i in ids],
            "value": [f"v-{i % VALUES}" for i in ids],
            # An hour apart, so the rows spread over months the way a real
            # table's do without needing a partition spec to prove it.
            "ts": [1772323200000000 + i * 3600000000 for i in ids],
        },
        schema=ARROW_SCHEMA,
    )


def tally(table):
    """Row count, per-column cardinality, and a digest of every row.

    The cardinalities catch a column collapsed onto one dictionary entry; the
    digest catches everything else, including a merge that keeps the right
    number of distinct values while handing them to the wrong rows. Every
    column goes into it, not just the two the cardinalities watch - compaction
    rewrites the whole row. ts goes in as microseconds so no timezone sits
    between the two runs.
    """
    scanned = table.scan().to_arrow()
    categories = scanned.column("category").to_pylist()
    values = scanned.column("value").to_pylist()
    rows = [
        f"{i}|{t}|{c}|{v}"
        for i, t, c, v in zip(
            scanned.column("id").to_pylist(),
            scanned.column("ts").cast(pa.int64()).to_pylist(),
            categories,
            values,
            strict=True,
        )
    ]
    digest = hashlib.md5(
        "\n".join(sorted(rows)).encode(), usedforsecurity=False
    ).hexdigest()
    return {
        "rows": scanned.num_rows,
        "categories": len(set(categories)),
        "values": len(set(values)),
        "digest": digest,
    }


def connect(args):
    properties = {
        "type": "rest",
        "uri": args.catalog_url,
        "warehouse": f"s3://{args.bucket}/",
        "prefix": args.bucket,
        "s3.endpoint": args.s3_endpoint,
        "s3.access-key-id": args.access_key,
        "s3.secret-access-key": args.secret_key,
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    }
    last = None
    for attempt in range(10):
        try:
            return load_catalog("rest", **properties)
        except Exception as err:  # the gateway may still be coming up
            last = err
            print(f"connect attempt {attempt + 1} failed: {err}", file=sys.stderr)
            time.sleep(2)
    raise last


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", required=True, choices=["write", "verify", "drop"])
    parser.add_argument("--catalog-url", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--secret-key", required=True)
    args = parser.parse_args()

    catalog = connect(args)
    identifier = f"{args.namespace}.{args.table}"

    if args.phase == "write":
        try:
            catalog.create_namespace(args.namespace)
        except NamespaceAlreadyExistsError:
            pass
        table = catalog.create_table(identifier, schema=SCHEMA)
        # One append per batch, so compaction has several files to merge
        # rather than one it would leave alone.
        for i in range(BATCHES):
            table.append(batch(i * ROWS_PER_BATCH + 1, ROWS_PER_BATCH))
        table = catalog.load_table(identifier)
        print(json.dumps(tally(table)))
        return

    if args.phase == "verify":
        print(json.dumps(tally(catalog.load_table(identifier))))
        return

    catalog.drop_table(identifier)
    try:
        catalog.load_table(identifier)
    except NoSuchTableError:
        pass
    else:
        raise SystemExit("the table is still in the catalog after a drop")
    catalog.drop_namespace(args.namespace)


if __name__ == "__main__":
    main()
