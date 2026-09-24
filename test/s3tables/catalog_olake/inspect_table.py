#!/usr/bin/env python3
"""Inspect an Iceberg table written by OLake, through the SeaweedFS REST catalog.

Two modes, because a strict reader cannot do both:

  rows      -- scan the table and print "id,region,amount" per row, ordered by
               id. Only valid while the table has no equality deletes.
  snapshots -- print one line per snapshot with its operation and delete-file
               counters, plus the manifest content kinds of the current
               snapshot.

The split exists because PyIceberg refuses to scan a table carrying equality
deletes (apache/iceberg#6568) while reading its metadata perfectly well. OLake
is a CDC tool, so its upsert path produces exactly those deletes -- asserting
the commit landed is the catalog's concern, and applying deletes on read is the
query engine's.
"""

import argparse
import sys

from pyiceberg.catalog import load_catalog


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["rows", "snapshots"])
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

    if args.mode == "rows":
        data = table.scan().to_arrow().to_pydict()
        # amount is decimal(10,2) in Postgres but arrives here as a float, whose
        # repr drops trailing zeros (120.5, not 120.50). Format it to the source
        # scale so the expected values in the Go test stay readable.
        for row_id, region, amount in sorted(
            zip(data["id"], data["region"], data["amount"])
        ):
            print("%s,%s,%.2f" % (row_id, region, float(amount)))
        return 0

    print("format-version=%d" % table.metadata.format_version)
    ids = table.metadata.schemas[-1].identifier_field_ids
    print("identifier-field-ids=%s" % ",".join(str(i) for i in ids))
    for snap in table.metadata.snapshots:
        s = snap.summary
        print(
            "snapshot operation=%s total-delete-files=%s added-delete-files=%s "
            "added-equality-deletes=%s total-records=%s"
            % (
                s.operation,
                s.get("total-delete-files", "0"),
                s.get("added-delete-files", "0"),
                s.get("added-equality-deletes", "0"),
                s.get("total-records", "0"),
            )
        )
    current = table.current_snapshot()
    kinds = [str(m.content).rsplit(".", 1)[-1] for m in current.manifests(table.io)]
    print("current-manifest-kinds=%s" % ",".join(kinds))
    return 0


if __name__ == "__main__":
    sys.exit(main())
