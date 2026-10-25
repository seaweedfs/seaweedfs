# Spark Lance Integration Test

Drives the SeaweedFS Lance Namespace with Spark, through the Lance Spark
connector's DSV2 catalog. The Lance counterpart of `catalog_spark`, which does
the same for the Iceberg REST catalog.

## Why Spark

Spark is the engine most likely to be pointed at a lakehouse, and it reaches the
catalog over the same Lance Namespace routes as every other client. Between this
and `catalog_lancedb`, the catalog is exercised by the two clients people
actually use, rather than only by the protocol's reference implementation.

## What it does

`TestSparkLanceNamespace`:

1. Starts a `weed mini` cluster with S3 and the Lance Namespace enabled.
2. Creates a table bucket declared `LANCE`.
3. Runs `spark_lance_ops.py` inside the stock `apache/spark:3.5.1` image, with
   the connector pulled from Maven at submit time.

Inside Spark:

| Step | What it proves |
| --- | --- |
| `CREATE NAMESPACE` / `SHOW NAMESPACES` | the bucket is the first level of the path, and the catalog is writable |
| `CREATE TABLE ... USING lance` | the connector declares through the namespace and writes the data itself |
| `INSERT` / `SELECT count(*)` | a write lands and reads back through the catalog |
| schema check | the vector column survived the round trip |
| `WHERE id >= 2` | the filter path, not only a full scan |
| a second `INSERT` | the commit a store that cannot order commits fails on |

## Configuration

The connector's catalog properties. The host names below are placeholders — the
test passes dynamically allocated `host.docker.internal` ports, and a real
deployment uses whatever address the gateway answers on:

```
spark.sql.catalog.lance                        org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.lance.impl                   rest
spark.sql.catalog.lance.uri                    http://seaweed:9101
spark.sql.catalog.lance.storage.aws_endpoint   http://seaweed:8333
spark.sql.catalog.lance.storage.allow_http     true
spark.sql.catalog.lance.storage.aws_access_key_id      …
spark.sql.catalog.lance.storage.aws_secret_access_key  …
```

Anything under `storage.` is handed to lance as object_store options, so those
are object_store's key names rather than Spark's `s3a` ones. Credentials belong
there because a gateway without STS vends none — see the note in
`../catalog_lancedb/README.md`.

## Running it

    cd test/s3tables/catalog_spark_lance
    (cd ../../../weed && go build .)     # the harness runs this binary
    go test -run TestSparkLanceNamespace -v -timeout 40m .

Skipped without Docker, and in `-short` mode. The first run downloads the Spark
image and the connector bundle, which is a few hundred megabytes; later runs
reuse both.
