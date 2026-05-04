# Apache Doris Iceberg Catalog Integration Test

This directory contains an Apache Doris integration smoke test for SeaweedFS's
Iceberg REST Catalog implementation.

## What It Tests

`TestDorisIcebergCatalog` verifies the Doris path end to end:

1. Starts a local SeaweedFS mini cluster with S3 Tables and Iceberg REST enabled.
2. Creates a SeaweedFS table bucket.
3. Creates an Iceberg namespace and an empty table through the SeaweedFS REST
   catalog OAuth flow.
4. Creates a second table and populates it with three rows by running a
   PyIceberg writer container (`Dockerfile.writer` + `append_rows.py`) before
   Doris bootstraps, so the snapshot is part of Doris's first scan.
5. Starts `apache/doris:doris-all-in-one-2.1.0` and waits for the FE MySQL
   query port and at least one alive BE.
6. Connects to Doris over the MySQL protocol and registers a Doris
   `EXTERNAL CATALOG` of `type=iceberg` and `iceberg.catalog.type=rest` that
   points at the SeaweedFS REST endpoint, using `credential = key:secret` for
   the OAuth2 client-credentials flow.
7. Runs subtests against the SeaweedFS-backed Iceberg tables:
   - `BasicSelect`: Doris is alive and answering SQL.
   - `CatalogVisible`: `SHOW CATALOGS` lists the SeaweedFS-backed catalog.
   - `DatabaseVisible`: the seeded namespace is exposed as a Doris database.
   - `TableVisible`: the seeded table appears under `SHOW TABLES FROM ...`.
   - `CountEmptyTable`: catalog-to-table resolution and a scan of an empty table.
   - `ColumnProjection`: `SELECT id, label` succeeds and the response columns
     are `id, label`. Failure here means Doris could not parse the schema
     returned by the SeaweedFS catalog.
   - `ReadWrittenDataCount` and `ReadWrittenDataValues`: Doris reads back the
     three PyIceberg-appended rows and the values match. This exercises the
     actual data path (parquet reads via S3), not just metadata.

The PyIceberg writer image is built on demand via Docker layer caching. The
first build pulls `python:3.11-slim` and pip-installs PyIceberg + PyArrow
(~1-2 min in CI); subsequent invocations are cheap.

## Running Locally

Build or install `weed`, then run:

```bash
cd test/s3tables/catalog_doris
go test -v -timeout 25m .
```

The test requires Docker. The GitHub Actions job runs on `ubuntu-22.04` and
executes the test for pull requests.

## Configuration

The test uses these fixed credentials for the local SeaweedFS IAM config:

- S3 access key: `AKIAIOSFODNN7EXAMPLE`
- S3 secret key: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
- Region: `us-west-2`
- Warehouse bucket: `iceberg-tables`

Doris ports:

- The all-in-one container exposes `9030` (FE MySQL/SQL) and `8030` (FE HTTP).
  Only `9030` is mapped to a host port (allocated dynamically) so the Go
  MySQL client can connect from the test process.
- The Iceberg REST endpoint and the S3 endpoint are reached from inside the
  Doris container via `host.docker.internal`, matching the Trino and Dremio
  test paths.

## Troubleshooting

- Ensure Docker is running: `docker version`
- Ensure `weed` is built or available on `PATH`
- The Doris all-in-one image is large (~1 GB) and the FE+BE need
  ~30-60 seconds to register before queries succeed; the test waits up to
  4 minutes for the query port and 60 seconds for at least one alive BE.
- If queries hang on metadata, run `REFRESH CATALOG iceberg_catalog` from a
  Doris MySQL client and retry.
- Container logs are printed in the failure message; you can also check
  `docker logs <seaweed-doris-...>` while the test is running.
