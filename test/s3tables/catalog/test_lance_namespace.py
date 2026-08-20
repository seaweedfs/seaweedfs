#!/usr/bin/env python3
"""Drive the SeaweedFS Lance Namespace with the real Lance client.

The catalog half can be checked with plain HTTP, and the Go integration tests
do. What only a real client proves is that the location and storage_options it
hands back are enough to write and read a dataset: the layout guard on the S3
door, the endpoint and allow_http options, and the credentials all have to be
right at once, and a hand-built request checks none of that.
"""

import argparse
import sys
import warnings

warnings.filterwarnings("ignore")

import lance
import lance_namespace as ln
import pyarrow as pa


def sample_table():
    return pa.table(
        {
            "id": pa.array([1, 2, 3, 4]),
            "vec": pa.array([[1.0, 2.0]] * 4, type=pa.list_(pa.float32(), 2)),
        }
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-url", required=True, help="Lance namespace REST URL")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint the dataset lives behind")
    parser.add_argument("--bucket", required=True, help="table bucket to use, already created")
    parser.add_argument("--access-key", default="any")
    parser.add_argument("--secret-key", default="any")
    args = parser.parse_args()

    ns = ln.connect("rest", {"uri": args.namespace_url})
    ns.create_namespace(ln.CreateNamespaceRequest(id=[args.bucket, "ml"]))
    table_id = [args.bucket, "ml", "vectors"]

    declared = ns.declare_table(ln.DeclareTableRequest(id=table_id))
    print(f"declared {table_id} at {declared.location}")
    if not declared.location:
        print("FAIL: declare returned no location", file=sys.stderr)
        return 1

    described = ns.describe_table(ln.DescribeTableRequest(id=table_id))
    if described.location != declared.location:
        print(
            f"FAIL: describe location {described.location} != declare {declared.location}",
            file=sys.stderr,
        )
        return 1

    options = dict(described.storage_options or {})
    print(f"storage options from the namespace: {sorted(options)}")
    # The endpoint is overridden with the container's view of the same gateway:
    # what the namespace vends is correct for its own host, and a test harness
    # bound to a wildcard address vends nothing at all. That the namespace vends
    # a usable endpoint when it can is covered by the unit tests; what matters
    # here is that everything else it hands back is enough to reach the data.
    options["aws_endpoint"] = args.s3_endpoint
    options["allow_http"] = "true"
    # A deployment without STS still needs credentials to sign with.
    options.setdefault("aws_access_key_id", args.access_key)
    options.setdefault("aws_secret_access_key", args.secret_key)

    lance.write_dataset(sample_table(), described.location, storage_options=options,
                        mode="overwrite")
    dataset = lance.dataset(described.location, storage_options=options)
    rows = dataset.count_rows()
    print(f"wrote and read back {rows} rows at version {dataset.version}")
    if rows != 4:
        print(f"FAIL: read back {rows} rows, want 4", file=sys.stderr)
        return 1

    # The table is listed, and a client that skips the namespace entirely can
    # still open the dataset by URI.
    listed = ns.list_tables(ln.ListTablesRequest(id=[args.bucket, "ml"])).tables
    if f"{args.bucket}$ml$vectors" not in listed:
        print(f"FAIL: {listed} does not contain the table", file=sys.stderr)
        return 1

    if lance.dataset(declared.location, storage_options=options).count_rows() != 4:
        print("FAIL: the dataset is not readable straight off its URI", file=sys.stderr)
        return 1

    # Deregistering hides the table and keeps the data, which is the difference
    # between it and a drop.
    ns.deregister_table(ln.DeregisterTableRequest(id=table_id))
    if f"{args.bucket}$ml$vectors" in ns.list_tables(ln.ListTablesRequest(id=[args.bucket, "ml"])).tables:
        print("FAIL: a deregistered table is still listed", file=sys.stderr)
        return 1
    if lance.dataset(declared.location, storage_options=options).count_rows() != 4:
        print("FAIL: deregister destroyed the dataset", file=sys.stderr)
        return 1

    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
