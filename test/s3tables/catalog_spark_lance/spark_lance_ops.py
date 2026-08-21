#!/usr/bin/env python3
"""Drive the SeaweedFS Lance Namespace with Spark.

Spark reaches the catalog through the Lance Spark connector's DSV2 catalog,
`org.lance.spark.LanceNamespaceSparkCatalog` with `impl=rest`, which speaks the
same routes this catalog implements. It is the Lance counterpart of the Spark
Iceberg suite next door.

Every step prints what happened. The last line is PASS, or a line starting with
FAIL that names the step, so the Go harness can report something useful rather
than a stack trace.
"""

import argparse
import sys

from pyspark.sql import SparkSession


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
    parser.add_argument("--access-key", default="any")
    parser.add_argument("--secret-key", default="any")
    parser.add_argument("--packages", required=True)
    parser.add_argument("--ivy-dir", default="/tmp/ivy")
    args = parser.parse_args()

    # storage.* is handed to lance as its object_store options, so these are
    # object_store's own key names rather than Spark's s3a ones.
    spark = (
        SparkSession.builder.appName("SeaweedFS Lance Namespace Test")
        .config("spark.jars.ivy", args.ivy_dir)
        .config("spark.jars.packages", args.packages)
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "rest")
        .config("spark.sql.catalog.lance.uri", args.namespace_url)
        .config("spark.sql.catalog.lance.storage.aws_endpoint", args.s3_endpoint)
        .config("spark.sql.catalog.lance.storage.allow_http", "true")
        .config("spark.sql.catalog.lance.storage.aws_access_key_id", args.access_key)
        .config("spark.sql.catalog.lance.storage.aws_secret_access_key", args.secret_key)
        .config("spark.sql.catalog.lance.storage.aws_region", "us-east-1")
        .config("spark.sql.catalog.lance.storage.virtual_hosted_style_request", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"spark {spark.version} connected to {args.namespace_url}")

    ns = f"lance.`{args.bucket}`.{args.namespace}"
    table = f"{ns}.{args.table}"

    # 1. A table bucket is the first level of the namespace path, so the
    #    namespace below it is created through the catalog like any other.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    namespaces = [row[0] for row in spark.sql(f"SHOW NAMESPACES IN lance.`{args.bucket}`").collect()]
    print(f"SHOW NAMESPACES -> {namespaces}")
    check(
        any(args.namespace in name for name in namespaces),
        f"{args.namespace} is not listed in {namespaces}",
    )

    # 2. Creating a table. The connector declares it through the namespace and
    #    writes the data itself, which is the split this catalog serves.
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, title STRING, vector ARRAY<FLOAT>) USING lance"
    )
    # SHOW TABLES gives back the namespace's own identifiers - bucket, namespace
    # and name joined by the delimiter - rather than a bare Spark table name.
    tables = [row[1] for row in spark.sql(f"SHOW TABLES IN {ns}").collect()]
    print(f"SHOW TABLES -> {tables}")
    check(
        any(args.table in name for name in tables),
        f"{args.table} is not listed in {tables}",
    )

    # 3. Write, and read back through the catalog.
    spark.sql(
        f"""INSERT INTO {table} VALUES
        (1, 'one',   array(1.0f, 2.0f, 3.0f)),
        (2, 'two',   array(2.0f, 3.0f, 4.0f)),
        (3, 'three', array(3.0f, 4.0f, 5.0f))"""
    )
    count = spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0]
    print(f"count -> {count}")
    check(count == 3, f"read back {count} rows, want 3")

    # 4. The schema survived the round trip, vector column included.
    schema = {field.name for field in spark.table(table).schema.fields}
    print(f"schema -> {sorted(schema)}")
    check({"id", "title", "vector"} <= schema, f"schema lost columns: {schema}")

    # 5. A filter, so it is not only a full scan.
    rows = spark.sql(f"SELECT id, title FROM {table} WHERE id >= 2 ORDER BY id").collect()
    print(f"filtered -> {[(r[0], r[1]) for r in rows]}")
    check(len(rows) == 2, f"filter returned {len(rows)} rows, want 2")
    check(rows[0][0] == 2, f"filter returned {rows[0][0]} first, want 2")

    # 6. Appending again, because a second commit is the one that fails when a
    #    store cannot order commits - this store can, so it must not.
    spark.sql(f"INSERT INTO {table} VALUES (4, 'four', array(4.0f, 5.0f, 6.0f))")
    count = spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0]
    print(f"count after a second commit -> {count}")
    check(count == 4, f"after appending, {count} rows, want 4")

    # 7. And the dataset is readable straight off its location, which is what
    #    keeps the catalog optional.
    location = f"s3://{args.bucket}/{args.namespace}/{args.table}"
    print(f"dataset location -> {location}")

    spark.stop()
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
