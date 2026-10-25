#!/usr/bin/env python3
"""Read all rows of an Iceberg table via the SeaweedFS REST catalog.

Used by the ClickHouse integration test to prove that rows written by
ClickHouse are readable by another engine: PyIceberg is a strict reader that
requires spec-compliant manifests and either parquet field ids or a name
mapping. Prints one "id,label" line per row, ordered by id.
"""

import argparse
import sys

from pyiceberg.catalog import load_catalog


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog-url", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--access-key", required=True)
    p.add_argument("--secret-key", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--namespace", action="append", required=True)
    p.add_argument("--table", required=True)
    args = p.parse_args()

    catalog = load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": args.catalog_url,
            "warehouse": args.warehouse,
            "prefix": args.prefix,
            "credential": f"{args.access_key}:{args.secret_key}",
            "s3.access-key-id": args.access_key,
            "s3.secret-access-key": args.secret_key,
            "s3.endpoint": args.s3_endpoint,
            "s3.region": args.region,
            "s3.path-style-access": "true",
        },
    )

    table = catalog.load_table(tuple(args.namespace) + (args.table,))
    data = table.scan().to_arrow().to_pydict()
    rows = sorted(zip(data["id"], data["label"]))
    for row_id, label in rows:
        print(f"{row_id},{label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
