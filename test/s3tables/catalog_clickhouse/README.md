# ClickHouse Iceberg Catalog Integration Test

This directory contains a ClickHouse integration smoke test for SeaweedFS's
Iceberg REST Catalog implementation, using ClickHouse's `DataLakeCatalog`
database engine.

## What It Tests

`TestClickHouseIcebergCatalog` verifies the ClickHouse path end to end:

1. Starts a local SeaweedFS mini cluster with S3 Tables and Iceberg REST enabled.
2. Creates a SeaweedFS table bucket.
3. Creates an Iceberg namespace and an empty table through the SeaweedFS REST
   catalog OAuth flow.
4. Creates a second table and populates it with three rows by running a
   PyIceberg writer container (`Dockerfile.writer` + `append_rows.py`) before
   ClickHouse connects, so the snapshot is part of the catalog's first scan.
5. Starts the ClickHouse server container (`clickhouse/clickhouse-server:25.8`
   by default, overridable via `CLICKHOUSE_IMAGE`) and waits for the HTTP
   interface.
6. Attaches the catalog with `CREATE DATABASE ... ENGINE = DataLakeCatalog`
   (`catalog_type = 'rest'`), authenticating to the catalog via the OAuth2
   client-credentials flow (`catalog_credential` + `oauth_server_uri`) and to
   S3 via the engine's access/secret key arguments and `storage_endpoint`.
7. Runs subtests against the SeaweedFS-backed Iceberg tables:
   - `BasicSelect`: ClickHouse is alive and answering SQL.
   - `DatabaseVisible`: the DataLakeCatalog database exists.
   - `TableVisible`: seeded tables appear as `namespace.table` entries in
     `SHOW TABLES` (ClickHouse flattens Iceberg namespaces into table names).
   - `DescribeTable`: the Iceberg schema mapped to `id Int64` and
     `label Nullable(String)`. Failure here means ClickHouse could not parse
     the schema returned by the SeaweedFS catalog.
   - `CountEmptyTable`: catalog-to-table resolution and a scan of an empty table.
   - `ReadWrittenDataCount` and `ReadWrittenDataValues`: ClickHouse reads back
     the three PyIceberg-appended rows and the values match. This exercises the
     actual data path (parquet reads via S3), not just metadata.
   - `WriteReadBack`: ClickHouse inserts rows with its experimental Iceberg
     write support using default settings, which produces manifests without
     avro field-ids, bucket-relative paths, and parquet without field ids. The
     SeaweedFS catalog repairs the manifests at commit time and stamps a
     default name mapping on the table, so PyIceberg (`read_rows.py`, a strict
     reader) must return the rows ClickHouse wrote.
   - `CreateTableViaCatalog`: ClickHouse creates a table through the catalog
     (`CREATE TABLE ... ENGINE = IcebergS3(...)` with
     `write_full_path_in_iceberg_metadata = 1`), the test verifies it is
     registered in the REST catalog, inserts rows, reads them back, and has
     PyIceberg read them too. ClickHouse only issues the catalog createTable
     from 26.4, so the subtest skips on older servers.

Queries go through ClickHouse's HTTP interface (port 8123, mapped to a
dynamically allocated host port), so the test needs no ClickHouse client
driver. Tables are referenced as ``iceberg_catalog.`namespace.table` ``.

## Running Locally

Build or install `weed`, then run:

```bash
cd test/s3tables/catalog_clickhouse
go test -v -timeout 20m .
```

The ClickHouse image defaults to `clickhouse/clickhouse-server:25.8` and can
be overridden:

```bash
CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:latest go test -v -timeout 20m .
```

The test requires Docker. The GitHub Actions job runs on `ubuntu-22.04` and
executes the test for pull requests against both the pinned baseline image
and `latest`, so new ClickHouse releases are covered as they ship.

## Configuration

The test uses these fixed credentials for the local SeaweedFS IAM config:

- S3 access key: `AKIAIOSFODNN7EXAMPLE`
- S3 secret key: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
- Region: `us-west-2`
- Warehouse bucket: `iceberg-tables`

ClickHouse ports:

- Only `8123` (HTTP interface) is mapped to a host port (allocated
  dynamically) so the Go test can issue queries from the test process.
- The Iceberg REST endpoint and the S3 endpoint are reached from inside the
  ClickHouse container via `host.docker.internal`, matching the Doris, Trino,
  and Dremio test paths.

## Troubleshooting

- Ensure Docker is running: `docker version`
- Ensure `weed` is built or available on `PATH`
- `DataLakeCatalog` requires `allow_experimental_database_iceberg = 1`; the
  test passes it as a URL setting on the CREATE DATABASE request.
- Container logs are printed in the failure message; you can also check
  `docker logs <seaweed-clickhouse-...>` while the test is running.
