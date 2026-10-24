# Table Lifecycle Integration Tests

One table, all the way through: created in the catalog, filled by a real client,
maintained, read again, dropped. Once for Iceberg and once for Lance. The
Iceberg half always maintains through the worker; the Lance half maintains
through the Rust worker or through the lance library, depending on what the
environment has - see below.

## Why this suite exists

[#10853](https://github.com/seaweedfs/seaweedfs/issues/10853) was a compaction
that rewrote every dictionary-encoded column onto a single value. It shipped.
The maintenance tests we had were thorough about the bookkeeping - sequence
numbers, added and deleted manifest entries, metadata versions, the manifest
list - and every one of them passed, because not one of them opened the parquet
file the worker had just written.

So the assertion here is the dull one nothing else was making: tally the table
before maintenance, tally it again after, and require the two to be equal. The
tally is a row count, the cardinality of each dictionary-encoded column, and an
md5 over whole rows. The cardinalities name the failure that happened; the
digest catches a rewrite that keeps every column's cardinality and hands the
values to the wrong rows.

The same shape covers Lance, because the exposure is the same: a compaction that
merges fragments can hand back a table that reads without complaint and answers
wrongly.

## What runs

`TestIcebergTableLifecycle` starts a `weed mini` cluster, declares an `ICEBERG`
table bucket, and runs two clients against it:

| Client | Why both |
| --- | --- |
| DuckDB | the client the bug was reported against, and the only one here that writes the deprecated `PLAIN_DICTIONARY` encoding - parquet-go normalizes it away on write, so a Go writer cannot produce it |
| PyIceberg | writes `RLE_DICTIONARY`, the modern spelling, so between the two the merge is checked against both dictionary encodings in the spec |

Between the write and the read, the test runs the worker's whole maintenance
cycle in-process against the live filer: compact, expire snapshots, remove
orphans, rewrite manifests. A compaction that merged nothing fails the test
rather than passing it - otherwise the read afterwards is checking a file the
worker never wrote.

`TestLanceTableLifecycle` does the same against a `LANCE` bucket: declare
through the namespace, write a fragment per append, maintain, read, drop.
Maintenance goes through the Rust worker's own handlers where cargo is
installed, and through the two lance calls those handlers wrap where it is not.
`WEED_LANCE_MAINTENANCE=library|worker` picks one instead of letting the test
guess; CI sets `library`, because a cold build of the lance crate costs more
than the layer it would be checking.

Both tests finish by dropping the table and checking the data actually left the
filer, which is the half of a lifecycle a catalog test never reaches.

## Running it

    cd test/s3tables/lifecycle
    (cd ../../../weed && go build .)     # the harness runs this binary
    go test -v -timeout 40m .

Skipped without Docker and in `-short` mode. The first run builds the two client
images and pulls `duckdb/duckdb:latest`; later runs reuse them. The DuckDB half
skips itself, rather than failing, on an image whose iceberg extension cannot
write through a REST catalog.

To watch it catch the bug it was written for, pin parquet-go back to the version
that had it:

    go mod edit -require=github.com/parquet-go/parquet-go@v0.30.1 && go mod tidy
    go test -run TestIcebergTableLifecycle/DuckDB -v .

    maintenance collapsed the category column: 7 distinct values -> 1
    maintenance collapsed the value column: 13 distinct values -> 1

The PyIceberg half still passes there, which is the reason both clients are in
this directory.
