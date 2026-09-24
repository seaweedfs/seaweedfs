# OLake Iceberg Catalog Integration Test

An integration test for [OLake](https://github.com/datazip-inc/olake) against
SeaweedFS's Iceberg REST Catalog, in the same shape as `catalog_clickhouse`.

## Why OLake, given we already test five engines

Two things here are covered by nothing else in this directory.

**It is a strict Java Iceberg client.** OLake does not write Iceberg from Go —
its Go process spawns a Java sidecar over gRPC and writes through the official
Apache Iceberg library, because the Go library has no equality deletes and CDC
needs them. So this test exercises the client class that
`weed/s3api/iceberg/metadata_compliance.go` exists to serve: the one
that fails with *"Cannot parse missing long current-snapshot-id"* when the
catalog omits spec-required keys that `iceberg-go` strips via `omitempty`.

**It produces equality deletes.** OLake is a CDC tool. Its upsert path commits
`operation=overwrite` with an equality-delete file, and a delete manifest
alongside the data manifests. ClickHouse, StarRocks, Doris and DuckDB all only
append.

## What it asserts

`TestOLakeIcebergCatalog` runs six subtests against a `weed mini` cluster with
a pre-created table bucket and a Postgres source:

| Subtest | What a failure means |
|---|---|
| `CheckDestination` | `olake check` did not reach `SUCCEEDED`, or it passed without ever loading `org.apache.iceberg.rest.RESTSessionCatalog` — the second case means the destination was never actually contacted. |
| `Discover` | OLake could not enumerate the source table, or wrote no `streams.json`. |
| `FullSyncAppendsRows` | The sync read fewer than the three seeded rows, or committed no Iceberg snapshot. |
| `StrictReaderSeesRows` | PyIceberg could not read back what the Java writer committed, or the values differ. This is the data path, not just metadata. |
| `UpsertProducesEqualityDelete` | After an `UPDATE` and a re-sync, no snapshot recorded an overwrite carrying equality deletes, or the current snapshot has no delete manifest. |
| `CompliantWriterNeedsNoRepair` | The catalog rewrote a manifest the official Iceberg Java writer produced. That is a regression in the repair gate, not a problem with OLake. |

## The one thing this test deliberately does not check

It does **not** assert that a reader sees the updated row and no duplicate.

PyIceberg refuses to scan a table carrying equality deletes
([apache/iceberg#6568](https://github.com/apache/iceberg/issues/6568)) while
reading its metadata perfectly well — so a rows-mode read after the upsert would
raise, not pass. The alternative is an engine that applies equality deletes,
which for StarRocks means a 3 GB image and 12 GB of RAM in CI.

The line drawn instead: **recording the commit correctly is the catalog's
contract; applying deletes on read is the query engine's.** The metadata
assertions cover our half.

This was verified once by hand outside CI, on 2026-09-23, with StarRocks 4.1.4
attached to the same catalog: after the upsert it read 3 rows / 3 distinct ids
with `id=1` showing the updated value and `_op_type=u`. If someone later wants
that inside the gate, add a reader that supports equality deletes — **do not**
"upgrade" this test to a PyIceberg rows-mode read after the upsert. It would not
pass; and if PyIceberg ever starts silently skipping deletes instead of raising,
it would pass by not looking.

## What the config proves

Nothing in the destination config is SeaweedFS-specific:

```json
{
  "catalog_type": "rest",
  "rest_catalog_url": "http://HOST:ICEBERG_PORT",
  "iceberg_s3_path": "s3://olake-tables",
  "s3_endpoint": "http://HOST:S3_PORT",
  "rest_auth_type": "oauth2",
  "oauth2_uri": "http://HOST:ICEBERG_PORT/v1/oauth/tokens",
  "credential": "ACCESS_KEY:SECRET_KEY"
}
```

`catalog_type` is the generic `rest`, auth is the standard OAuth2
client-credentials flow, and `s3_path_style` does not even need setting —
OLake turns it on by itself whenever `s3_endpoint` is non-empty. As of
OLake v0.10.1 this works with no change on either side.

## Running it

```sh
go test ./test/s3tables/catalog_olake/ -run TestOLakeIcebergCatalog -v -timeout 25m
```

Needs Docker and a `weed` binary (at `weed/weed` under the repo root, or on
`PATH`). Takes about 35 seconds. It skips rather than fails when Docker is
absent, and `SEAWEEDFS_SKIP_OLAKE_TESTS=1` skips it outright.

Overrides: `OLAKE_IMAGE` (default `olakego/source-postgres:latest`),
`POSTGRES_IMAGE` (default `postgres:16`).

## In CI

Runs as `olake-iceberg-catalog-tests` in `.github/workflows/s3-tables-tests.yml`,
on a matrix of a pinned image plus `latest` — the same shape the ClickHouse job
uses, and for the same reason: OLake's Iceberg writer is a Java sidecar whose
library version moves independently of the Go release, so the `latest` leg is
what catches drift in the client rather than in OLake itself.

The job asserts the suite actually ran — at least one top-level `--- PASS` and
zero `--- SKIP` — rather than trusting a green exit. This suite skips itself
when Docker is unavailable, and a skipped suite reporting success is how a gate
quietly stops being one.
