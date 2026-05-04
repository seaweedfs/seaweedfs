#!/usr/bin/env python3
"""Append rows to an existing Iceberg table via the SeaweedFS REST catalog.

Used by the Dremio integration test to materialize data files so a downstream
SELECT from Dremio verifies the read path against non-empty results.

Usage:
    python3 append_rows.py \\
        --catalog-url http://localhost:8181 \\
        --warehouse s3://my-bucket \\
        --prefix my-bucket \\
        --s3-endpoint http://localhost:8333 \\
        --access-key AKIA... --secret-key wJalr... \\
        --namespace foo --namespace bar \\
        --table events
"""

import argparse
import sys

import pyarrow as pa
from pyiceberg.catalog import load_catalog


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog-url", required=True)
    p.add_argument("--warehouse", required=True, help="s3://<bucket-name>")
    p.add_argument("--prefix", required=True, help="REST catalog prefix (table bucket name)")
    p.add_argument("--s3-endpoint", required=True, help="http://host:port")
    p.add_argument("--access-key", required=True)
    p.add_argument("--secret-key", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument(
        "--namespace",
        action="append",
        required=True,
        help="One per level (e.g. --namespace foo --namespace bar for foo.bar).",
    )
    p.add_argument("--table", required=True)
    args = p.parse_args()

    # `credential` triggers OAuth2 client_credentials against
    # <catalog_uri>/v1/oauth/tokens, matching the helper Go test uses for
    # REST-API table creation. The s3.* keys are needed for parquet writes.
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

    table_id = tuple(args.namespace) + (args.table,)
    table = catalog.load_table(table_id)

    # Match the Iceberg table schema: id is `required long`, label is
    # `optional string`. Default pyarrow columns are nullable, which fails
    # PyIceberg's required-field compatibility check.
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string(), nullable=True),
        ]
    )
    arrow_table = pa.Table.from_pydict(
        {
            "id": [1, 2, 3],
            "label": ["one", "two", "three"],
        },
        schema=arrow_schema,
    )
    table.append(arrow_table)
    print(f"appended {arrow_table.num_rows} rows to {'.'.join(table_id)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
