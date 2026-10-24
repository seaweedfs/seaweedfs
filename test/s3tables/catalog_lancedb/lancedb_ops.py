#!/usr/bin/env python3
"""Drive the SeaweedFS Lance Namespace with LanceDB.

The existing client test uses `lance_namespace` and `pylance` directly, which is
the protocol's reference client. LanceDB is what people actually point at a
catalog: it connects with `connect_namespace("rest", ...)`, lists what is there,
opens a table and searches it. This checks the catalog against that path, the
way the Spark, Trino and ClickHouse suites check the Iceberg one.

Everything it prints is either "PASS" or a line starting with FAIL, so the Go
harness can report the first real failure rather than a stack trace.
"""

import argparse
import sys
import warnings

warnings.filterwarnings("ignore")

import lance
import lance_namespace as ln
import lancedb
import pyarrow as pa

DIM = 8


def sample_rows(count):
    """A vector table, which is the only kind worth putting in Lance."""
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


def seed_table(namespace_url, storage, bucket, namespace, table, rows):
    """Declares a table through the namespace and writes a dataset into it.

    LanceDB reads through the catalog; the writing half is pylance, because the
    namespace records where a table lives and does not carry its data.
    """
    ns = ln.connect("rest", {"uri": namespace_url})
    ns.create_namespace(
        ln.CreateNamespaceRequest(id=[bucket], mode="EXIST_OK")
    )
    ns.create_namespace(
        ln.CreateNamespaceRequest(id=[bucket, namespace], mode="EXIST_OK")
    )
    table_id = [bucket, namespace, table]
    declared = ns.declare_table(ln.DeclareTableRequest(id=table_id))
    lance.write_dataset(
        sample_rows(rows), declared.location, storage_options=storage, mode="overwrite"
    )
    print(f"seeded {rows} rows at {declared.location}")
    return declared.location


def check(condition, message):
    if not condition:
        print(f"FAIL: {message}", file=sys.stderr)
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-url", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--namespace", default="ml")
    parser.add_argument("--table", default="embeddings")
    # Enough rows for an IVF_PQ index to be worth building, which is the point
    # of step 4: without one, a search is a brute-force scan.
    parser.add_argument("--rows", type=int, default=1024)
    parser.add_argument("--access-key", default="any")
    parser.add_argument("--secret-key", default="any")
    args = parser.parse_args()

    # The namespace vends an endpoint correct for its own host; a container
    # reaches the same gateway by another name, so the endpoint is overridden
    # here and the credentials filled in for a deployment without STS.
    storage = {
        "aws_endpoint": args.s3_endpoint,
        "allow_http": "true",
        "aws_access_key_id": args.access_key,
        "aws_secret_access_key": args.secret_key,
        "aws_region": "us-east-1",
    }

    location = seed_table(
        args.namespace_url, storage, args.bucket, args.namespace, args.table, args.rows
    )

    print(f"lancedb {lancedb.__version__} connecting to {args.namespace_url}")
    db = lancedb.connect_namespace(
        "rest", {"uri": args.namespace_url}, storage_options=storage
    )

    # 1. The catalog is browsable: the bucket is a namespace, and the table is
    #    in it under the name the namespace gave it.
    tables = list(db.table_names(namespace_path=[args.bucket, args.namespace], limit=100))
    print(f"table_names -> {tables}")
    check(
        any(args.table in name for name in tables),
        f"{args.table} is not listed in {tables}",
    )

    # 2. Opening it goes through the catalog: LanceDB asks the namespace where
    #    the table is and reads it from there.
    # storage_options is passed per call as well as on the connection: what the
    # namespace vends for a table is merged in, and a deployment without STS
    # vends no credentials, which is what the client would otherwise be left with.
    table = db.open_table(
        args.table,
        namespace_path=[args.bucket, args.namespace],
        storage_options=storage,
    )
    count = table.count_rows()
    print(f"open_table -> {count} rows")
    check(count == args.rows, f"read {count} rows, want {args.rows}")

    # 3. The schema survived the round trip, vector column included. This is the
    #    part a catalog that only records a location cannot fake.
    names = table.schema.names
    print(f"schema -> {names}")
    check("vector" in names and "title" in names, f"schema lost columns: {names}")

    # 4. Build a vector index, then search it. Both halves matter: an index
    #    writes files into a directory of the table the S3 door has to admit -
    #    the layout guard has refused a Lance directory before - and without one
    #    a search is a brute-force scan that proves nothing about the index path
    #    the maintenance worker exists to keep in shape.
    table.create_index(
        metric="l2",
        vector_column_name="vector",
        index_type="IVF_PQ",
        num_partitions=1,
        num_sub_vectors=4,
    )
    indices = table.list_indices()
    print(f"create_index -> {indices}")
    check(len(indices) >= 1, "no index was created")

    # Vectors are laid out so that id N sits near id N+1, so a query built from
    # id 1 should come back with its neighbourhood. The assertion is a
    # neighbourhood and not an exact id: an IVF_PQ index quantizes, so the
    # nearest hit is approximate by construction - with this data it answers 0
    # as readily as 1, and both are right.
    query = [float(1) + d for d in range(DIM)]
    hits = table.search(query).limit(3).to_list()
    ids = [hit["id"] for hit in hits]
    print(f"search -> {ids}")
    check(len(hits) == 3, f"search returned {len(hits)} hits, want 3")
    check(
        all(i <= 5 for i in ids),
        f"search returned {ids}, which is not the neighbourhood of the query",
    )

    # 5. A filtered scan, so it is not only the ANN path that works.
    filtered = table.search().where("id < 5").limit(10).to_list()
    print(f"filtered scan -> {len(filtered)} rows")
    check(len(filtered) == 5, f"filter returned {len(filtered)} rows, want 5")

    # 6. Creating a table. By default LanceDB declares it through the namespace
    #    and writes the data itself, which is exactly the split this catalog
    #    serves, so this has to work.
    created = db.create_table(
        "created_by_lancedb",
        data=sample_rows(4),
        namespace_path=[args.bucket, args.namespace],
        storage_options=storage,
    )
    check(created.count_rows() == 4, "create_table wrote the wrong number of rows")
    listed = list(db.table_names(namespace_path=[args.bucket, args.namespace], limit=100))
    check(
        any("created_by_lancedb" in name for name in listed),
        f"a table created through LanceDB is not listed: {listed}",
    )
    print(f"create_table -> {created.count_rows()} rows, listed by the catalog")

    # 7. The same creation with server-side pushdown, which asks the namespace
    #    to run CreateTable itself. That operation carries Arrow data and this
    #    catalog answers the spec's Unsupported for it. What matters is that the
    #    client is left with something coherent either way - it falls back to
    #    declare-and-write - rather than a hang, a 404, or a half-made table.
    pushdown = lancedb.connect_namespace(
        "rest",
        {"uri": args.namespace_url},
        storage_options=storage,
        namespace_client_pushdown_operations=["CreateTable"],
    )
    pushed_error = None
    try:
        pushdown.create_table(
            "pushed_by_lancedb",
            data=sample_rows(2),
            namespace_path=[args.bucket, args.namespace],
            storage_options=storage,
        )
    except Exception as err:  # noqa: BLE001 - the point is what the client sees
        pushed_error = err

    after = list(db.table_names(namespace_path=[args.bucket, args.namespace], limit=100))
    landed = any("pushed_by_lancedb" in name for name in after)
    print(f"create_table with pushdown: error={pushed_error!r}, catalog has it={landed}")

    if pushed_error is None:
        # The client fell back to declare-and-write, so the table is real and
        # has to be readable and complete.
        check(landed, "create_table reported success but the catalog has no table")
        rows = db.open_table(
            "pushed_by_lancedb",
            namespace_path=[args.bucket, args.namespace],
            storage_options=storage,
        ).count_rows()
        check(rows == 2, f"the pushed table holds {rows} rows, want 2")
    else:
        # Refused, which is what this catalog answers for a data-plane
        # operation. It has to be that refusal and not some other failure, and
        # it must not have left a half-made table behind.
        message = str(pushed_error).lower()
        check(
            "unsupported" in message or "501" in message or "not implemented" in message,
            f"pushdown failed for an unexpected reason: {pushed_error}",
        )
        check(not landed, "a refused create left a table behind in the catalog")

    # 8. And the dataset is still readable straight off its URI, which is what
    #    keeps the catalog optional.
    direct = lance.dataset(location, storage_options=storage).count_rows()
    check(direct == args.rows, f"direct read got {direct} rows, want {args.rows}")
    print(f"direct read without the catalog -> {direct} rows")

    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
