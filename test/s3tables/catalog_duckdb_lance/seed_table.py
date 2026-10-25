#!/usr/bin/env python3
"""Declare a table through the namespace and write a dataset into it.

Also writes the same rows to a second path whose name ends in `.lance`, because
DuckDB's replacement scan recognises a dataset by that suffix and tables created
through this catalog do not have one. The test asserts both behaviours so a
change upstream is noticed rather than silently making the docs wrong.
"""

import argparse
import sys
import warnings

warnings.filterwarnings("ignore")

import lance
import lance_namespace as ln
import pyarrow as pa

DIM = 8


def rows(count):
    return pa.table(
        {
            "id": pa.array(list(range(count)), type=pa.int64()),
            "title": pa.array([f"row-{i}" for i in range(count)]),
            "vector": pa.array(
                [[float(i) + d for d in range(DIM)] for i in range(count)],
                type=pa.list_(pa.float32(), DIM),
            ),
        }
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-url", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--namespace", default="ml")
    parser.add_argument("--table", default="embeddings")
    parser.add_argument("--rows", type=int, default=128)
    parser.add_argument("--access-key", default="any")
    parser.add_argument("--secret-key", default="any")
    args = parser.parse_args()

    storage = {
        "aws_endpoint": args.s3_endpoint,
        "allow_http": "true",
        "aws_access_key_id": args.access_key,
        "aws_secret_access_key": args.secret_key,
        "aws_region": "us-east-1",
    }

    ns = ln.connect("rest", {"uri": args.namespace_url})
    ns.create_namespace(ln.CreateNamespaceRequest(id=[args.bucket], mode="EXIST_OK"))
    ns.create_namespace(
        ln.CreateNamespaceRequest(id=[args.bucket, args.namespace], mode="EXIST_OK")
    )
    declared = ns.declare_table(
        ln.DeclareTableRequest(id=[args.bucket, args.namespace, args.table])
    )
    lance.write_dataset(rows(args.rows), declared.location, storage_options=storage,
                        mode="overwrite")
    print(f"seeded {args.rows} rows at {declared.location}")

    # The same data under a name DuckDB's replacement scan recognises. Written
    # directly rather than declared, because a table name containing a dot is
    # not a valid catalog name.
    suffixed = f"s3://{args.bucket}/{args.namespace}/{args.table}-direct.lance"
    lance.write_dataset(rows(args.rows), suffixed, storage_options=storage,
                        mode="overwrite")
    print(f"seeded {args.rows} rows at {suffixed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
