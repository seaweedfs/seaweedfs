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

SeaweedFS may store side indexes internally or in a hidden namespace:

```text
/table/_seaweed_index/
  data/ds=2026-01-01/part-00001.parquet/
    footer.cache
    row_group_stats
    page_index.timestamp
    bloom.user_id
    bitmap.tenant_id
    btree.timestamp
    inverted.message
    vector.embedding.ivf
```

The original Parquet file is not modified.

## SeaweedFS-Aware Internal Mapping

A Parquet file can be represented internally as:

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

SeaweedFS needle mapping may be:

```text
footer                         -> cached metadata needle
row_group_0 / column_id         -> data needle(s)
row_group_0 / column_timestamp  -> data needle(s)
row_group_0 / column_tenant_id  -> data needle(s)
row_group_0 / column_payload    -> data needle(s)
row_group_1 / column_id         -> data needle(s)
...
```

This does not require physically splitting the Parquet file for compatibility. SeaweedFS can also map byte ranges inside one object to the underlying needles.

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

Examples:

```sql
WHERE message CONTAINS 'timeout'
WHERE tags CONTAINS 'gpu'
```

Index structure:

```text
token -> candidate row groups/pages/row ids
```

### 8. Vector Index

Useful for embedding columns stored inside Parquet.

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

## API Sketch

### Pushdown Request

```go
type ParquetPushdownRequest struct {
    Table       string
    SnapshotId  int64
    Files       []string
    Columns     []string
    Predicate   []byte
    VectorQuery *VectorQuery
    Limit       int
}

type VectorQuery struct {
    Column string
    Vector []float32
    Metric string // l2, cosine, dot
    TopK   int
    NProbe int
}
```

### Pushdown Response

```go
type ParquetPushdownResponse struct {
    FileRanges []FileRange
    RowGroups  []RowGroupRef
    Pages      []PageRef
    RowIds     []RowRef
    Scores     []float32
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

Indexes must be tied to immutable file identity:

```text
file path
file size
etag/version
modification time
Iceberg snapshot id
content hash, optional
```

If the Parquet file changes, indexes are invalidated and rebuilt.

For Iceberg tables, this is easier because data files are generally immutable. New snapshots add or remove files rather than modifying files in place.

## Handling Iceberg Deletes

Iceberg may use:

- position delete files
- equality delete files

SeaweedFS-aware pushdown should apply delete information when available.

Possible strategy:

```text
1. Iceberg catalog returns data files and delete files
2. SeaweedFS builds delete masks per data file
3. Pushdown execution removes deleted rows from candidate sets
```

Delete masks can be stored as side indexes:

```text
/table/_seaweed_index/.../deletes.position.bitmap
/table/_seaweed_index/.../deletes.equality.bitmap
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

## Rollout Plan

### Phase 1: Metadata Acceleration

- Parquet footer cache
- row group stats cache
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
