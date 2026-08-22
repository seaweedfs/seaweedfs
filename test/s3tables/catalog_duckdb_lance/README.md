# DuckDB Lance Integration Test

Reads SeaweedFS Lance tables from DuckDB's `lance` core extension, the
counterpart of the DuckDB Iceberg tests in `../catalog/`.

## What this one proves that the others do not

The LanceDB and Spark suites go through the catalog. DuckDB does not: it reaches
the data over S3 with no namespace involved. That exercises the other half of the
design — a table bucket's layout is a valid Lance dataset directory, so a table
stays readable when the catalog is not in the path.

## What it does

`TestDuckDBLance`:

1. Starts a `weed mini` cluster with S3 and the Lance Namespace enabled.
2. Creates a table bucket declared `LANCE`, then declares a table through the
   namespace and writes a dataset into it with pylance.
3. Runs `duckdb_lance_ops.sql` in `duckdb/duckdb:latest`.

| Step | What it proves |
| --- | --- |
| `__lance_scan(s3://…)` | DuckDB reads a table this catalog created |
| `DESCRIBE` | the schema survived, vector column included |
| `WHERE id < 5` | the filter path |
| `lance_vector_search` | vector search over data behind SeaweedFS |
| a `.lance` path | the replacement scan works on a suffixed path |
| a suffix-less path | and does **not** see one without the suffix |

## The `.lance` suffix

DuckDB's replacement scan — `SELECT * FROM 's3://…'` — recognises a Lance dataset
by a `.lance` path suffix. Tables created through this catalog deliberately have
none: the catalog entry *is* the dataset directory, table names may not contain a
dot, and a suffix would leak into ARNs and policies.

So from DuckDB, a table in a SeaweedFS table bucket is read with
`__lance_scan('s3://bucket/namespace/table')` rather than the bare `FROM 's3://…'`
form. The test asserts both halves, so if the extension ever recognises a
suffix-less directory the test fails and tells us to update the documentation.

## Credentials

```sql
CREATE SECRET seaweedfs (
    TYPE lance,
    ACCESS_KEY_ID '…', SECRET_ACCESS_KEY '…',
    REGION 'us-east-1',
    ENDPOINT 'http://seaweed:8333',
    ALLOW_HTTP true,
    VIRTUAL_HOSTED_STYLE_REQUEST false
);
```

Those are object_store's key names, the same as everywhere else Lance touches
storage.

## Running it

    cd test/s3tables/catalog_duckdb_lance
    (cd ../../../weed && go build .)     # the harness runs this binary
    go test -run TestDuckDBLance -v -timeout 30m .

Skipped without Docker, in `-short` mode, and if the DuckDB image cannot load the
extension.
