# postgres_s3

The `postgres_s3` filer implementation uses postgres-specific features and data structures to improve upon previous SQL implementations based on the `abstract_sql` module. 

Of note, the `postgres2` filer implementation may leak directory hierarchy metadata when frequent inserts and deletes are
performed using the S3 API. If an application workload pattern creates directories, populates them temporarily, and then
deletes all objects in the directory, the `postgres2` filer implementation will continue to indefinitely maintain
information about the orphaned directories. While these directories will not be shown in S3 API list requests, the metadata
remains and places the burden of an unbounded number of unused rows on postgres.

Seaweedfs provides the `-s3.allowEmptyFolder=false` CLI argument to automatically clean up orphaned directory entries, but
this process necessarily races under high load and can cause unpredictable filer and postgres behavior.

To solve this problem, `postgres_s3` does the following:

1. One row in postgres _fully_ represents one object and its metadata
2. Insert, update, get, and delete operate on a single row
3. An array is stored of possible prefixes for each key
4. List requests leverage the prefixes to dynamically assemble directory entries using a complex `SELECT` statement

In order to efficiently query directory entries during list requests, `postgres_s3` uses special features of
postgres:

* An int64 array field called `prefixes` with a hash of each prefix found for a specific key
* GIN indexing that provides fast set membership information on array fields
* Special functions `split_part` (text parsing) and `cardinality` (length of the array field)

`postgres_s3` uses automatic upsert capability with `ON CONFLICT ... UPDATE` so that insert and update are the same
race-free operation.

In the filer metadata tables, all objects start with `/`, causing prefix calculation to include the empty string (`""`).
For space and index optimization, `postgres_s3` does not store the root prefix in the `prefixes` array, and instead
relies on the condition `cardinality(prefixes) < 1` to discover objects at the root directory.