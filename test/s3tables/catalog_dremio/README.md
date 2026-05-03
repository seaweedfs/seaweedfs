# Dremio Iceberg Catalog Integration Test

This directory contains a Dremio integration smoke test for SeaweedFS's Iceberg REST Catalog implementation.

## What It Tests

`TestDremioIcebergCatalog` verifies the Dremio path end to end:

1. Starts a local SeaweedFS mini cluster with S3 Tables and Iceberg REST enabled.
2. Creates a SeaweedFS table bucket.
3. Creates an Iceberg namespace and empty table through the SeaweedFS REST catalog OAuth flow.
4. Starts `dremio/dremio-oss:25.2.0`.
5. Bootstraps a Dremio admin user and logs in.
6. Creates a Dremio `RESTCATALOG` source that points at the SeaweedFS catalog.
7. Submits Dremio SQL through `/api/v3/sql`, polls the job API, and reads job results.
8. Runs subtests against the SeaweedFS-backed Iceberg table:
   - `BasicSelect`: Dremio is alive and answering SQL.
   - `CountEmptyTable`: catalog-to-table resolution and a scan of an empty table.
   - `ColumnProjection`: `SELECT id, label` succeeds and the response schema reports both columns. Failure here means Dremio could not parse the schema returned by the SeaweedFS catalog.
   - `InformationSchemaColumns`: the table's columns are exposed through Dremio's metadata layer with the expected ordinal order.
   - `InformationSchemaTables`: the table is registered in Dremio's `INFORMATION_SCHEMA`.
   - `MultiLevelNamespace`: a 2-level Iceberg namespace (created via the REST API) is exposed by Dremio as nested folders, and a table inside it is queryable with dot-separated identifiers.

## Running Locally

Build or install `weed`, then run:

```bash
cd test/s3tables/catalog_dremio
go test -v -timeout 20m .
```

The test requires Docker. The GitHub Actions job runs on `ubuntu-22.04` and executes the test for pull requests.

## Configuration

The test uses these fixed credentials for the local SeaweedFS IAM config:

- S3 access key: `AKIAIOSFODNN7EXAMPLE`
- S3 secret key: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
- Region: `us-west-2`
- Warehouse bucket: `iceberg-tables`

The Dremio source is configured via `POST /api/v3/catalog`; it is not configured in `dremio.conf`.
The Dremio container starts with the `plugins.restcatalog.enabled` support key enabled, which is required for the Iceberg REST Catalog source in Dremio OSS 25.2.

## Troubleshooting

- Ensure Docker is running: `docker version`
- Ensure `weed` is built or available on `PATH`
- Check host-gateway routing if Dremio cannot reach SeaweedFS: `docker run --add-host host.docker.internal:host-gateway --rm alpine getent hosts host.docker.internal`
- Check Dremio logs from the failed test output; the harness prints the Dremio container tail on Dremio startup, source setup, or job failures.
