#!/usr/bin/env python3
"""Lance half of the table lifecycle, run one phase per invocation.

The Go test calls this for each step and maintains the table in between, so the
tally taken before maintenance and the tally taken after are directly
comparable. That comparison is the test: the Iceberg side of this suite exists
because compaction once rewrote a table's dictionary columns onto a single
value and every check we had still passed, and a Lance dataset is rewritten by
the same kind of job.

The maintain phase is a fallback. When the Rust worker can be built, the Go test
runs its handlers against this table instead and skips this phase - what runs
here are the same two lance calls the handlers make.
"""

import argparse
import hashlib
import json
import sys
import warnings
from datetime import timedelta

warnings.filterwarnings("ignore")

import lance
import lance_namespace as ln
import pyarrow as pa

# Low enough cardinality that these two columns are dictionary-encoded.
CATEGORIES = 7
VALUES = 13
ROWS_PER_BATCH = 4000
BATCHES = 3
DIM = 8


def rows(start, count):
    ids = list(range(start, start + count))
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "category": pa.array([f"cat-{i % CATEGORIES}" for i in ids]),
            "value": pa.array([f"v-{i % VALUES}" for i in ids]),
            "vector": pa.array(
                [[float(i % 97) + d for d in range(DIM)] for i in ids],
                type=pa.list_(pa.float32(), DIM),
            ),
        }
    )


def tally(dataset):
    """What the table holds, in a form two runs can be compared by.

    The cardinalities catch a column collapsed onto one value; the digest
    catches a rewrite that keeps the values and moves them to the wrong rows.
    Every column goes into the digest, the vectors included - compaction
    rewrites whole fragments, so leaving a column out leaves a place for it to
    go wrong unnoticed. Fragments come along because a compaction that merged
    nothing would otherwise let this test pass without having tested anything.
    """
    scanned = dataset.to_table(columns=["id", "category", "value", "vector"])
    ids = scanned.column("id").to_pylist()
    categories = scanned.column("category").to_pylist()
    values = scanned.column("value").to_pylist()
    vectors = scanned.column("vector").to_pylist()
    serialized = (
        f"{i}|{c}|{v}|{w}"
        for i, c, v, w in zip(ids, categories, values, vectors, strict=True)
    )
    digest = hashlib.md5(
        "\n".join(sorted(serialized)).encode(), usedforsecurity=False
    ).hexdigest()
    return {
        "rows": scanned.num_rows,
        "categories": len(set(categories)),
        "values": len(set(values)),
        "digest": digest,
        "fragments": len(dataset.get_fragments()),
        "version": dataset.version,
    }


def resolve(args):
    """Ask the namespace where the table lives, the way the worker does."""
    namespace = ln.connect("rest", {"uri": args.namespace_url})
    table_id = [args.bucket, args.namespace, args.table]
    described = namespace.describe_table(ln.DescribeTableRequest(id=table_id))
    return namespace, table_id, described.location, storage_options(args, described)


def storage_options(args, described=None):
    # The namespace vends an endpoint correct for its own host; this container
    # reaches the same gateway by another name. Credentials are filled in
    # because a deployment without STS vends none.
    options = dict((described.storage_options or {}) if described else {})
    options["aws_endpoint"] = args.s3_endpoint
    options["allow_http"] = "true"
    options.setdefault("aws_access_key_id", args.access_key)
    options.setdefault("aws_secret_access_key", args.secret_key)
    options.setdefault("aws_region", "us-east-1")
    return options


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=["write", "maintain", "verify", "drop"])
    parser.add_argument("--namespace-url", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--access-key", default="any")
    parser.add_argument("--secret-key", default="any")
    args = parser.parse_args()

    if args.phase == "write":
        namespace = ln.connect("rest", {"uri": args.namespace_url})
        for parent in ([args.bucket], [args.bucket, args.namespace]):
            namespace.create_namespace(ln.CreateNamespaceRequest(id=parent, mode="EXIST_OK"))
        table_id = [args.bucket, args.namespace, args.table]
        declared = namespace.declare_table(ln.DeclareTableRequest(id=table_id))
        options = storage_options(args)
        # A fragment per append, so the compaction that follows has something
        # to merge.
        for i in range(BATCHES):
            lance.write_dataset(
                rows(i * ROWS_PER_BATCH + 1, ROWS_PER_BATCH),
                declared.location,
                storage_options=options,
                mode="overwrite" if i == 0 else "append",
            )
        print(json.dumps(tally(lance.dataset(declared.location, storage_options=options))))
        return 0

    if args.phase == "maintain":
        _, _, location, options = resolve(args)
        dataset = lance.dataset(location, storage_options=options)
        dataset.optimize.compact_files()
        dataset = lance.dataset(location, storage_options=options)
        dataset.cleanup_old_versions(older_than=timedelta(seconds=0), delete_unverified=True)
        print(json.dumps(tally(lance.dataset(location, storage_options=options))))
        return 0

    if args.phase == "verify":
        _, _, location, options = resolve(args)
        print(json.dumps(tally(lance.dataset(location, storage_options=options))))
        return 0

    namespace, table_id, location, options = resolve(args)
    namespace.drop_table(ln.DropTableRequest(id=table_id))
    try:
        lance.dataset(location, storage_options=options)
    except ValueError as err:
        # pylance turns every load failure into a ValueError, so the message is
        # the only thing separating a dataset that is gone from credentials
        # that stopped working halfway through the test.
        if "was not found" in str(err):
            return 0
        print(f"FAIL: reading the dropped dataset failed for another reason: {err}",
              file=sys.stderr)
        return 1
    print("FAIL: the dataset is still readable after a drop", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
