# LanceDB Integration Test

Drives the SeaweedFS Lance Namespace with [LanceDB](https://lancedb.com), the way
`catalog_spark`, `catalog_trino` and `catalog_clickhouse` drive the Iceberg REST
catalog with their engines.

## Why a real client

Every serious bug in this catalog so far looked correct to a request written by
hand: a deregister that deleted the dataset, an S3 door that refused every Lance
file, a namespace that listed tables it would then deny. A hand-built HTTP test
checks the shape of a response. A client checks whether the response is *usable* —
that the location it hands back, the storage options beside it and the layout
rules on the S3 door all line up at once.

LanceDB connects with `connect_namespace("rest", ...)`, which speaks the routes
this catalog implements, so what runs here is the protocol rather than our own
idea of it.

## What it does

`TestLanceDBNamespace`:

1. Starts a `weed mini` cluster with S3 and the Lance Namespace enabled.
2. Creates a table bucket declared `LANCE`, so the catalog refuses tables of any
   other format in it.
3. Builds `Dockerfile.client` (LanceDB, pylance, lance-namespace) and runs
   `lancedb_ops.py` against the namespace.

Inside the container:

| Step | What it proves |
| --- | --- |
| seed a table | the namespace's location and credentials are enough to write |
| `table_names` | the catalog is browsable through LanceDB |
| `open_table` | LanceDB resolves a table through the catalog and reads it |
| schema check | the vector column survived the round trip |
| `create_index` | an IVF_PQ index builds, and its files land through the S3 door's layout rules |
| `search(...)` | approximate search over that index, on data behind SeaweedFS |
| `where("id < 5")` | so does the scan path, not only the index |
| `create_table` | LanceDB declares through the namespace and writes the data itself |
| `create_table` with pushdown | either the client falls back and the table is complete, or the catalog refuses with the spec's Unsupported and leaves nothing behind |
| direct `lance.dataset(uri)` | the catalog stays optional; the dataset opens without it |

The seeding is pylance rather than LanceDB, because the namespace records where
a table lives and does not carry its data. That split is the design, not a
limitation of the test.

## Running it

    cd test/s3tables/catalog_lancedb
    (cd ../../../weed && go build .)     # the harness runs this binary
    go test -run TestLanceDBNamespace -v -timeout 30m .

Skipped without Docker, and in `-short` mode. The first run builds the client
image, which takes a few minutes; later runs reuse it.
