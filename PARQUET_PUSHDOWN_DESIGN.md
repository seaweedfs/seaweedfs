# SeaweedFS-Aware Parquet Layout and Pushdown Design

## Status

Draft design note.

## Goal

Keep Parquet files fully compatible with existing engines such as Spark, Trino, DuckDB, PyArrow, and Iceberg, while allowing SeaweedFS-aware clients to take advantage of storage-side metadata, indexes, and pushdown execution.

The design avoids replacing Parquet with a custom format. Instead, SeaweedFS stores normal Parquet objects and builds auxiliary metadata/indexes beside them.

```text
Standard Parquet compatibility
+ SeaweedFS side indexes
+ pushdown-aware connectors
= faster scans, filtering, and hybrid vector search
```

## Non-Goals

- Do not require changes to the Parquet file format.
- Do not require existing Spark/Trino/DuckDB readers to change.
- Do not make SeaweedFS the only way to read the data.
- Do not rewrite all Parquet data into Lance or another vector-native format.

## Background

Parquet already contains useful metadata:

- file footer
- row group metadata
- column chunk offsets
- column statistics
- optional page indexes
- optional bloom filters

Typical readers use this flow:

```text
1. Read Parquet footer
2. Discover row groups and column chunks
3. Prune row groups using statistics
4. Read selected column chunks/pages
5. Decode/filter/project rows
```

SeaweedFS can optimize this process by caching metadata, exposing smarter range reads, and building additional side indexes.

## High-Level Architecture

```text
Spark / Trino / DuckDB / PyArrow / Custom Query Engine
        |
        | S3 API or SeaweedFS Pushdown API
        v
SeaweedFS S3 Gateway / Filer
        |
        | metadata/index lookup
        v
SeaweedFS Index Metadata
        |
        | range reads / local filtering
        v
SeaweedFS Volume Servers
        |
        v
Parquet data needles
```

The "local filtering" step on volume servers is new infrastructure introduced by this design. Today's volume servers serve byte ranges only; the pushdown path adds a filter/index-evaluation component colocated with the data. Where exactly it runs (volume server in-process, sidecar, or a separate index-serving tier) is covered in [Index Residency](#index-residency-and-failure-model).

There are two access modes:

### 1. Standard Object Access

Existing engines read Parquet through the normal S3-compatible API.

```text
engine -> SeaweedFS S3 -> Parquet object ranges
```

This preserves compatibility.

### 2. SeaweedFS-Aware Pushdown Access

Optimized connectors call a SeaweedFS pushdown API before reading data.

```text
engine -> Iceberg planning -> SeaweedFS pushdown API -> candidate files/ranges/rows
```

The engine still reads standard Parquet data, but reads much less of it.

## Physical Layout

The user-visible layout remains normal Iceberg/Parquet layout:

```text
/table/
  metadata/
    v1.metadata.json
    snapshots/...
  data/
    ds=2026-01-01/
      part-00001.parquet
      part-00002.parquet
```

SeaweedFS stores side indexes outside the table's S3 prefix so they do not appear in Iceberg listings or interfere with snapshot expiration / orphan-file removal. Two acceptable placements:

- a separate system bucket or filer mount (preferred), keyed by `(table_uuid, file_path, file_identity)`
- the same bucket under a top-level prefix that engines are configured to ignore (`/_sw_index/...`), never under the table prefix

Logical layout per data file:

```text
<system-prefix>/<table_uuid>/data/ds=2026-01-01/part-00001.parquet/<identity>/
  footer.cache
  page_index.timestamp
  bloom.user_id
  bitmap.tenant_id
  btree.timestamp
  inverted.message
  vector.embedding.ivf
```

`<identity>` is derived from the index identity rules in [Index Consistency](#index-consistency). The original Parquet file is not modified.

## Logical View for Planning

The Parquet object on disk is unchanged. For planning and pushdown purposes, SeaweedFS exposes a logical view of the file derived from the footer:

```text
Parquet file
 ├── footer metadata
 ├── row group 0
 │    ├── column chunk: id
 │    ├── column chunk: timestamp
 │    ├── column chunk: tenant_id
 │    └── column chunk: payload
 └── row group 1
      ├── column chunk: id
      ├── column chunk: timestamp
      ├── column chunk: tenant_id
      └── column chunk: payload
```

Each leaf maps to a `(file, offset, length)` byte range inside the original object:

```text
footer                          -> (file, footer_offset, footer_length)
row_group_0 / column_id         -> (file, offset, length)
row_group_0 / column_timestamp  -> (file, offset, length)
row_group_0 / column_tenant_id  -> (file, offset, length)
row_group_0 / column_payload    -> (file, offset, length)
row_group_1 / column_id         -> (file, offset, length)
...
```

This is a planning abstraction only. The Parquet file is not split into per-column-chunk needles, and the on-disk object is not modified. Reads are still issued as ranged GETs against the original object.

## Side Index Types

### 1. Footer Cache

Cache the parsed Parquet footer.

Purpose:

- avoid repeated footer reads
- speed up planning
- expose row group and column chunk offsets quickly

Index content:

```text
file path
file size
last modified / etag
schema
row groups
column chunks
statistics
byte offsets
```

### 2. Row Group Zone Map

Stores min/max/null count per row group and column.

Useful for predicates such as:

```sql
WHERE timestamp BETWEEN t1 AND t2
WHERE id > 1000
```

Skip condition:

```text
row_group.max < predicate_min OR row_group.min > predicate_max
```

### 3. Page-Level Index

Stores min/max and offsets per page.

Useful when row groups are large and predicates are selective.

Example:

```text
row_group_3 / column_timestamp:
  page_0 min=2026-01-01 max=2026-01-02
  page_1 min=2026-01-03 max=2026-01-04
  page_2 min=2026-01-05 max=2026-01-06
```

### 4. Bloom Filter Index

Useful for equality lookups.

Example:

```sql
WHERE user_id = 'u123'
```

If the bloom filter says the value is absent, SeaweedFS skips the row group or page.

### 5. Bitmap Index

Useful for low- or medium-cardinality columns.

Examples:

```sql
WHERE tenant_id = 'abc'
WHERE status IN ('failed', 'pending')
```

Bitmap operations:

```text
tenant_id=abc AND status=failed
```

### 6. B-Tree / Range Index

Useful for range predicates and point lookups on sortable columns.

Examples:

```sql
WHERE timestamp > t
WHERE id BETWEEN a AND b
```

The index maps values or ranges to:

```text
file
row group
page
optional row ids
```

### 7. Inverted Index

Useful for logs, text, tags, and JSON fields.

Examples (engine-side translations, since standard SQL has no `CONTAINS`):

```sql
-- substring search:    LIKE '%timeout%' or full-text MATCH
WHERE message LIKE '%timeout%'
-- array element match: array_contains(tags, 'gpu') / 'gpu' = ANY(tags)
WHERE array_contains(tags, 'gpu')
```

Index structure:

```text
token -> candidate row groups/pages/row ids
```

### 8. Vector Index

The embedding column is stored as a regular column inside the Parquet file. The vector index itself is a side file built from that column and stored alongside the other side indexes.

Example:

```sql
ORDER BY embedding <-> query_vector
LIMIT 20
```

Possible index types:

- IVF
- IVF+PQ
- HNSW
- scalar quantization

Index layout:

```text
/table/_seaweed_index/.../vector.embedding.ivf/
  centroids
  list_000001
  list_000002
  pq_codebooks
```

This allows SeaweedFS to perform approximate nearest neighbor search before reading Parquet columns.

## Pushdown Types

### File Pruning

Use Iceberg metadata plus SeaweedFS indexes to skip files.

### Row Group Pruning

Use row group statistics, bloom filters, and side indexes.

### Page Pruning

Use page-level indexes to read only matching pages.

### Column Pruning

Read only requested columns and predicate columns.

Example:

```sql
SELECT id, image_path
WHERE tenant_id = 'abc'
ORDER BY embedding <-> ?
LIMIT 10
```

Only these columns are needed:

```text
tenant_id
embedding
id
image_path
```

### Scalar Predicate Pushdown

Evaluate simple predicates using indexes:

```text
=, !=, <, <=, >, >=
IN
BETWEEN
IS NULL
CONTAINS
```

### Vector Search Pushdown

Search vector side indexes on volume servers or index-serving nodes.

Flow:

```text
1. Choose candidate vector partitions
2. Route to volume servers holding vector index/data needles
3. Compute local top-K
4. Merge global top-K
5. Fetch projected Parquet columns
```

### Hybrid Scalar + Vector Pushdown

Example:

```sql
SELECT image_path
WHERE tenant_id = 'abc'
  AND timestamp >= '2026-01-01'
ORDER BY embedding <-> query_vector
LIMIT 20
```

Execution:

```text
1. tenant_id bitmap filters candidate rows/pages
2. timestamp zone map or B-tree narrows candidates
3. vector index searches only candidate partitions
4. delete masks are applied if Iceberg delete files exist
5. top-K candidates are returned
6. projected columns are fetched
```

#### Filtered-ANN strategy

Step 3 hides a real engineering choice: most off-the-shelf IVF and HNSW indexes do not natively accept arbitrary scalar pre-filters. Three common strategies, in order of v1 preference:

- **Partitioned IVF (preferred for v1).** Build separate IVF indexes per partition key (e.g. one per `tenant_id`). Scalar filters that align with the partition key reduce search to a single partition; non-aligned filters fall back to post-filter.
- **Post-filter with overscan.** Search the full vector index for `k * overscan_factor` candidates, then apply scalar predicates and trim to `k`. Works for any scalar predicate but accuracy degrades when the scalar predicate is highly selective.
- **Filtered HNSW (e.g. ACORN-style).** Inline scalar predicate evaluation during graph traversal. Most flexible, but requires a custom index and is deferred past v1.

The pushdown planner picks per query: if the scalar predicate is aligned with a partitioned index, use strategy 1; otherwise fall back to strategy 2 with an overscan factor derived from estimated selectivity.

## API Sketch

### Pushdown Request

```go
type ParquetPushdownRequest struct {
    Table          string
    SnapshotId     int64
    Files          []string
    Columns        []string
    PredicateKind  PredicateKind // SUBSTRAIT or ICEBERG_EXPRESSION
    Predicate      []byte        // serialized per PredicateKind
    VectorQuery    *VectorQuery
    Limit          int
    RequestRowIds  bool          // include per-row refs in response (default false)
    MaxRowIds      int           // cap on returned row refs; server may truncate
}

type PredicateKind int32

const (
    PredicateUnspecified PredicateKind = 0
    PredicateSubstrait   PredicateKind = 1 // Substrait ExtendedExpression protobuf
    PredicateIceberg     PredicateKind = 2 // Iceberg Expression JSON
)

type VectorMetric int32

const (
    MetricL2     VectorMetric = 0
    MetricCosine VectorMetric = 1
    MetricDot    VectorMetric = 2
)

type VectorQuery struct {
    Column string
    Vector []float32
    Metric VectorMetric
    TopK   int
    NProbe int
}
```

v1 implementations should accept Substrait as the canonical wire format. Iceberg Expression JSON is supported as a convenience for connectors that already produce it.

### Pushdown Response

The response is range-oriented. Row-id lists are optional and bounded — they are returned only when the request opts in (e.g. for vector top-K) and the planner can verify the result fits within `MaxRowIds`.

```go
type ParquetPushdownResponse struct {
    FileRanges []FileRange
    RowGroups  []RowGroupRef
    Pages      []PageRef
    RowIds     []RowRef // optional; empty unless RequestRowIds set and within MaxRowIds
    Scores     []float32
    Truncated  bool     // true if row-id list was omitted/truncated due to size cap
    Stats      PushdownStats
}

type FileRange struct {
    File   string
    Offset int64
    Length int64
}

type RowGroupRef struct {
    File     string
    RowGroup int
}

type PageRef struct {
    File     string
    RowGroup int
    Column   string
    Page     int
    Offset   int64
    Length   int64
}

type RowRef struct {
    File     string
    RowGroup int
    RowId    int64
}
```

## Connector Behavior

### Existing Connector Path

```text
Spark/Trino/DuckDB
  -> Iceberg planning
  -> Parquet scan
  -> SeaweedFS S3 range reads
```

### Optimized Connector Path

```text
Spark/Trino/DuckDB connector
  -> Iceberg planning
  -> SeaweedFS pushdown API
  -> receive candidate row groups/pages/ranges/row ids
  -> read only needed Parquet ranges
```

## Index Consistency

Indexes must be tied to a stable file identity. Preferred identity, in order of strength:

```text
1. Iceberg manifest fields: file_path + file_size_in_bytes + record_count
   (+ snapshot id for cross-snapshot lookups)
2. S3 ETag (canonical for raw S3 access without an Iceberg manifest)
3. Content hash (optional; expensive but fully unambiguous)
```

Modification time is intentionally not used as identity: replication, re-upload, and metadata-only operations can change mtime without changing content, and content can change without mtime moving on some backends.

If the identity of a Parquet file changes, its indexes are invalidated and rebuilt. Iceberg makes this cheap because data files are immutable; new snapshots add or remove files rather than modifying files in place.

## Handling Iceberg Deletes

Iceberg uses two delete mechanisms, with different implications for indexing:

### Position deletes (precomputable)

Position deletes name `(data_file, row_position)` pairs. They can be merged into a per-data-file roaring bitmap and cached as a side index:

```text
/table/_seaweed_index/.../deletes.position.bitmap
```

Pushdown subtracts this bitmap from candidate row sets before returning results.

### Equality deletes (must evaluate at query time)

Equality deletes carry a predicate (e.g. `id = 42`) and apply to all data files in scope of the delete file's sequence number. They cannot be precomputed as a static bitmap per data file because:

- one equality delete file can affect many data files
- new data files added later may need to be filtered by the same delete predicate until compaction

Strategy: evaluate equality-delete predicates at query time, accelerated by the same scalar indexes (bloom, bitmap, B-tree). Optionally cache materialized result bitmaps keyed by `(data_file_id, equality_delete_file_id)` and invalidate on snapshot change.

### End-to-end flow

```text
1. Iceberg catalog returns data files plus position+equality delete files
2. For each data file:
     a. apply cached position-delete bitmap
     b. evaluate live equality-delete predicates against side indexes
3. Pushdown execution returns candidate sets minus the union of (a) and (b)
```

## Compaction and Maintenance

SeaweedFS can run background index maintenance:

```text
new Parquet file detected
  -> parse footer
  -> build row group stats
  -> optionally build page/bloom/bitmap/vector indexes
  -> publish index metadata
```

For Iceberg compaction:

```text
old files removed from snapshot
new compacted files added
old indexes become unreachable
new indexes are built lazily or eagerly
```

Garbage collection can remove indexes for files no longer referenced by active snapshots.

## Index Residency and Failure Model

Open question to resolve before Phase 2: where the side indexes physically live and how pushdown behaves when they are absent or stale.

Options:

- **Filer-managed.** Index metadata stored in the filer; index payloads stored as ordinary needles. Simplest. Read amplification is similar to a normal SeaweedFS file read.
- **Colocated on volume servers.** Index payloads live next to the data needles for the file they index, so filter evaluation runs on the same node that holds the data. Lowest network cost, highest deployment complexity.
- **Separate index-serving tier.** A pool of stateless workers that load index payloads from the filer on demand. Easiest to scale independently of storage.

In all cases, pushdown must degrade gracefully: if the requested side index is missing, stale, or unreachable, the server falls back to row-group-level pruning using the parsed footer (or, in the worst case, returns the request unmodified so the client performs a standard scan). The response should carry a `Stats` field indicating which indexes were used, missed, or skipped, so the client can decide whether to retry or just proceed.

## Cost Model and When Not to Push Down

Pushdown is not free. Each request adds a planning round-trip and forces the server to load and evaluate side indexes. For small or unselective queries this is a net loss versus a direct ranged GET.

Connectors should skip the pushdown API when:

- the table is small enough that a single Parquet scan is cheaper than the planning round-trip (rule of thumb: total file size below a few footer reads, ~10 MiB)
- the predicate is non-selective (estimated selectivity > ~0.5) and no projection prunes meaningful columns
- the query has no predicate and no vector clause — file pruning by Iceberg metadata alone already does the job

The pushdown API should expose its own selectivity and cost estimates in the response stats so connectors can learn when to use it; explicit configuration knobs (`min_table_size`, `min_selectivity`) can act as a fallback.

## Security and Access Control

Side indexes can leak information that the underlying object's ACL would otherwise hide:

- zone maps and B-trees expose min/max values, which can reveal date ranges or identifier ranges to clients with column-level but not row-level visibility
- bloom filters allow probing for the presence of specific values
- inverted indexes expose token vocabularies

For v1, side indexes inherit the same access controls as the underlying Parquet object — a client that cannot read the file cannot query its indexes either. Row- and column-level access policies (e.g. masking, row filters) are out of scope; tables that depend on row-level security should disable pushdown until a finer-grained authorization story exists.

## Rollout Plan

### Phase 1: Metadata Acceleration

- parsed-footer cache (row group stats already live in the footer; the win is avoiding repeated Thrift decode and offset lookups, not building new index data)
- column chunk range optimization
- expose file/range-level pushdown

### Phase 2: Scalar Indexes

- bloom filter side index
- bitmap index
- B-tree/range index
- basic predicate API

### Phase 3: Page-Level Pruning

- page statistics
- page offset map
- return page-level ranges

### Phase 4: Iceberg Delete Integration

- position delete masks
- equality delete masks
- snapshot-aware index validity

### Phase 5: Vector Indexes

- IVF side index for embedding columns
- local top-K search on volume servers
- global top-K merge
- hybrid scalar + vector search

### Phase 6: Query Engine Integrations

- SeaweedFS-aware DuckDB extension
- Trino connector pushdown
- Spark DataSource/Iceberg integration

## Benefits

- keeps Parquet compatibility
- accelerates existing Iceberg data
- avoids full file scans
- reduces network and disk I/O
- enables vector search over Parquet data
- lets SeaweedFS become more than object storage

## Tradeoffs

- side indexes require storage and maintenance
- consistency must be carefully tied to immutable file identity
- pushdown requires engine connector work
- page-level and row-level filtering are more complex than row-group pruning
- vector search on Parquet is less natural than LanceDB, but easier to adopt for existing lake data

## Summary

SeaweedFS should keep Parquet files standard and build auxiliary indexes beside them.

The first useful version can focus on footer caching and row group pruning. Later versions can add scalar, text, delete, and vector indexes.

This creates a practical path:

```text
Iceberg + Parquet compatibility today
+ SeaweedFS-aware pushdown
+ optional vector search acceleration
```
