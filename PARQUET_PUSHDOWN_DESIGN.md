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
  page_index.fid_3      # column "timestamp", field id 3
  bloom.fid_5           # column "user_id", field id 5
  bitmap.fid_7          # column "tenant_id", field id 7
  btree.fid_3           # column "timestamp", field id 3
  inverted.fid_11       # column "message", field id 11
  vector.fid_42.ivf     # column "embedding", field id 42
```

Per-column index files are keyed by Iceberg field ID, not column name. A rename (e.g. `tenant_id` → `org_id`) does not invalidate the index, and a column dropped-and-re-added with the same name (a different field ID) does not silently reuse the wrong index. Side-index metadata may carry the human-readable name as a hint for logging. `<identity>` is derived from the index identity rules in [Index Consistency](#index-consistency). The original Parquet file is not modified.

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

Wraps Parquet's native `ColumnIndex` (per-page min/max/null-count) and `OffsetIndex` (per-page byte offset, length, and `first_row_index` within the row group). The side index materializes both for fast access without re-parsing the footer.

Useful when row groups are large and predicates are selective.

Predicate evaluation example:

```text
row_group_3 / column_timestamp:
  page_0 first_row=0       min=2026-01-01 max=2026-01-02
  page_1 first_row=120000  min=2026-01-03 max=2026-01-04
  page_2 first_row=240000  min=2026-01-05 max=2026-01-06
```

Page pruning is *row-range-driven*, not byte-range-driven. The flow:

1. Apply the predicate against the predicate column's `ColumnIndex` to pick surviving pages.
2. Translate surviving pages to row ranges using `first_row_index` (e.g. pages 1–2 above → row range `[120000, 360000)` within row group 3).
3. For each *projected* column, walk that column's `OffsetIndex` and select the pages whose `[first_row_index, next_first_row_index)` intersects the surviving row ranges. Page boundaries do not align across columns within a row group, so this translation step is mandatory; reusing the predicate column's page numbers as if they were column-independent is wrong.
4. Return per-column page byte ranges in the response.

Two Parquet-specific pitfalls the index must handle:

- **Dictionary pages.** A column chunk may begin with a dictionary page that is not described by `OffsetIndex` (which lists data pages only). The dictionary page must still be fetched whenever any data page from that column chunk is read, so the byte range returned for a surviving page set must include the dictionary page region of the chunk.
- **Repeated/nested fields.** For columns where one Parquet record spans multiple values (lists, maps), `first_row_index` refers to row positions in the row group, not value positions; row-range translation across columns still works because rows are aligned per row group, but downstream consumers must remember that data-page row counts and value counts are not the same.

If the file was written without `ColumnIndex`/`OffsetIndex`, page-level pruning is unavailable and the planner falls back to row-group pruning.

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
<system-prefix>/<table_uuid>/.../<identity>/vector.fid_42.ivf/
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

    // DataFiles is the authoritative list of files to scan. Each entry
    // carries enough identity for the server to validate that its cached
    // side indexes still apply, and enough delete-file context that the
    // server can compute the correct visible row set without re-running
    // Iceberg planning.
    DataFiles      []DataFileDescriptor

    Columns        []ColumnRef
    PredicateKind  PredicateKind // SUBSTRAIT or ICEBERG_EXPRESSION
    Predicate      []byte        // serialized per PredicateKind
    VectorQuery    *VectorQuery
    Limit          int
    RequestRowIds  bool          // include per-row refs in response (default false)
    MaxRowIds      int           // cap on returned row refs; server may truncate
}

type DataFileDescriptor struct {
    Path           string
    SizeBytes      int64  // Iceberg manifest file_size_in_bytes
    RecordCount    int64  // Iceberg manifest record_count
    ETag           string // optional, used when no Iceberg manifest is available

    // DataSequenceNumber is the Iceberg manifest entry's
    // data_sequence_number — the sequence number assigned when the
    // file's data was logically added. This is the field that drives
    // equality-delete and DV scope. It is NOT the same as
    // file_sequence_number (commit-time sequence number for the
    // manifest entry itself).
    DataSequenceNumber int64

    // PartitionSpecId and PartitionValues identify the partition this
    // data file belongs to, in the partition spec used at write time.
    // Required for delete applicability: a delete file with a
    // different (PartitionSpecId, PartitionValues) does not apply,
    // even if its sequence number rule matches. PartitionValues are
    // serialized as a Substrait/Iceberg literal map keyed by partition
    // field id, encoded the same way the planner sees them in the
    // manifest.
    PartitionSpecId int32
    PartitionValues []byte

    // Deletes is the union of position-delete files, equality-delete
    // files, and deletion vectors that apply to this data file.
    // Content type is discriminated by DeleteFileRef.Content.
    Deletes        []DeleteFileRef
}

type DeleteFileRef struct {
    Path           string
    SizeBytes      int64

    // PartitionSpecId and PartitionValues match the partition this
    // delete file was written for. A delete file applies to a data
    // file only when (delete.PartitionSpecId, delete.PartitionValues)
    // == (data.PartitionSpecId, data.PartitionValues). This holds for
    // both position-delete files and equality-delete files; deletion
    // vectors set ReferencedDataFile and so the partition match is
    // implicit but the fields should still be populated for
    // consistency.
    PartitionSpecId int32
    PartitionValues []byte

    // DataSequenceNumber is the delete file's data_sequence_number.
    // The scope rule is: this delete file applies to a data file iff
    //   data_file.DataSequenceNumber  <=  delete_file.DataSequenceNumber
    // for position deletes and deletion vectors (both are
    // position-based and may target data files committed in the same
    // snapshot), and
    //   data_file.DataSequenceNumber  <   delete_file.DataSequenceNumber
    // for equality deletes (predicate-based; only filters older data).
    DataSequenceNumber int64

    // Content matches Iceberg manifest "content" field. DVs are
    // POSITION_DELETES with FileFormat == FileFormatPuffin; there is
    // no separate "deletion vector" content value in Iceberg.
    Content        DeleteContent

    // FileFormat is the on-disk format of this delete file.
    FileFormat     FileFormat

    // EqualityFieldIds carries the Iceberg field IDs the equality
    // predicate is keyed on. Required for Content == EqualityDeletes;
    // empty for position deletes (file or DV).
    EqualityFieldIds []int32

    // Puffin-only fields, used when Content == PositionDeletes and
    // FileFormat == FileFormatPuffin (deletion vectors). The DV blob
    // lives at (Path, BlobOffset, BlobLength). BlobCRC32 is the
    // CRC-32 the Puffin footer records for this blob's
    // (decompressed) bytes; populated when present, used as a
    // tamper / corruption check, NOT as a content hash for cache
    // identity. Cache identity is (puffin file identity per Index
    // Consistency, BlobOffset, BlobLength) — Puffin files are
    // immutable, so that triple is sufficient.
    BlobOffset int64
    BlobLength int64
    BlobCRC32  uint32

    // ReferencedDataFile is set when this delete file targets a single
    // data file (mandatory for v3 deletion vectors; optional for v2
    // position-delete files). Empty when the delete file may target
    // many data files.
    ReferencedDataFile string
}

type DeleteContent int32

const (
    DeleteContentUnspecified DeleteContent = 0
    PositionDeletes          DeleteContent = 1 // Iceberg manifest content=1 (file or DV)
    EqualityDeletes          DeleteContent = 2 // Iceberg manifest content=2
)
// Iceberg v3 deletion vectors are POSITION_DELETES with
// FileFormat == FileFormatPuffin; there is no separate enum value.

type FileFormat int32

const (
    FileFormatUnspecified FileFormat = 0
    FileFormatParquet     FileFormat = 1
    FileFormatAvro        FileFormat = 2
    FileFormatOrc         FileFormat = 3
    FileFormatPuffin      FileFormat = 4
)

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
    Column ColumnRef
    Vector []float32
    Metric VectorMetric
    TopK   int
    NProbe int
}

// ColumnRef identifies a column by Iceberg field ID, which is stable
// across rename and reordering. The Path is an optional hint (the
// dotted Iceberg name path, e.g. "user.address.zip") for logging and
// for engines that cannot resolve field IDs; the server must trust
// FieldId when it is set and only fall back to Path when FieldId is
// zero (unspecified) — for example, when querying tables that were
// not written through Iceberg and have no field IDs in the Parquet
// file metadata.
type ColumnRef struct {
    FieldId int32  // Iceberg field ID; 0 means "use Path"
    Path    string // dotted Iceberg name path, optional hint
}
```

The client is responsible for Iceberg planning (resolving the snapshot to data files and delete files) and passes the resolved set in `DataFiles`. The server treats this list as authoritative and does not re-read the catalog. Each descriptor carries:

- enough identity (`SizeBytes`, `RecordCount`, optional `ETag`) for the server to verify a cached side index still matches the file,
- the Iceberg `DataSequenceNumber` (the manifest entry's `data_sequence_number`, not `file_sequence_number`) so equality-delete and DV scope can be resolved correctly,
- the `Deletes` list of position-delete files, equality-delete files, and deletion vectors that the client's planner has attached to this data file. Each entry carries the Iceberg `Content` discriminator (`PositionDeletes` or `EqualityDeletes`) plus the on-disk `FileFormat`, with the pair `(PositionDeletes, FileFormatPuffin)` denoting a v3 deletion vector. Content-specific fields cover equality field IDs and Puffin blob offset/length.

v1 implementations should accept Substrait as the canonical wire format. Iceberg Expression JSON is supported as a convenience for connectors that already produce it.

### Pushdown Response

The response is range-oriented. Row-id lists are optional and bounded — they are returned only when the request opts in (e.g. for vector top-K) and the planner can verify the result fits within `MaxRowIds`.

```go
type ParquetPushdownResponse struct {
    FileRanges []FileRange
    RowGroups  []RowGroupRef
    Pages      []PageRef
    RowRefs    []ScoredRowRef // optional; empty unless RequestRowIds set and within MaxRowIds
    Truncated  bool           // true if row-ref list was omitted/truncated due to size cap
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
    Column   ColumnRef
    Page     int
    Offset   int64
    Length   int64
}

// RowRef identifies a single row by its file-absolute row position,
// matching Iceberg's position-delete semantics. RowGroup is included
// for fast locality but is derivable from FilePosition + the file's
// row-group boundaries.
type RowRef struct {
    File        string
    RowGroup    int   // row group containing FilePosition
    FilePosition int64 // 0-based row index within the file (file-absolute)
}

// ScoredRowRef pairs a row reference with its similarity score. Used
// for vector-search results so score order is unambiguous; non-vector
// queries leave Score zero.
type ScoredRowRef struct {
    Ref   RowRef
    Score float32
}
```

Row identity uses **file-absolute** position (matching Iceberg position-delete files), not row-group-local. Row-group-local indexing is exposed via the convenience `RowGroup` field but is not authoritative — clients converting a `RowRef` back to a Parquet read should locate the row by `FilePosition` against the parsed footer's row-group boundaries.

`Scores` is no longer a parallel array. Pairing each score with its row ref via `ScoredRowRef` removes the ordering constraint and lets a single response mix scored (vector) and unscored (scalar) results without ambiguity.

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

Iceberg's delete model has evolved across spec versions. Pushdown must support all three forms:

| Form | Iceberg spec | Storage | Per-data-file? | Precomputable? |
|---|---|---|---|---|
| Position delete files | v2 | Parquet/Avro/ORC of `(file_path, position)` rows | No (one delete file may target many) | Yes (merge to bitmap) |
| Equality delete files | v2 | Parquet/Avro/ORC of equality-key rows | No (predicate scope by sequence number) | No (must evaluate per query) |
| Deletion vectors | v3 | Puffin blob (`deletion-vector-v1`) holding a roaring bitmap | Yes (one DV per data file) | Yes (it *is* the bitmap) |

### Position delete files (v2; precomputable)

Position deletes name `(data_file, row_position)` pairs. The set of position-delete files that apply to a given data file is

```text
{ pdf :
    pdf.DataSequenceNumber  >=  data_file.DataSequenceNumber           // sequence
    AND pdf.PartitionSpecId  ==  data_file.PartitionSpecId             // same spec
    AND pdf.PartitionValues  ==  data_file.PartitionValues             // same partition
    AND pdf.ReferencedDataFile in { empty, data_file.Path }            // file scope
}
```

Pushdown merges that set into a per-data-file roaring bitmap and caches it as a side index:

```text
<system-prefix>/<table_uuid>/.../<identity>/deletes.position.bitmap
```

The cached bitmap is only valid for the exact set of position-delete files that produced it — see [Cache key](#position-delete-bitmap-cache-key) below.

Pushdown subtracts this bitmap from candidate row sets before returning results.

### Deletion vectors (v3; precomputable)

Iceberg v3 replaces position delete *files* with deletion *vectors*: a roaring bitmap of file-absolute row positions for one specific data file, stored as a Puffin blob of type `deletion-vector-v1`. Because each DV already targets a single data file and is already a bitmap, no merge is needed — the cached form is just the decoded bitmap, keyed by the immutable triple `(puffin_file_identity, blob_offset, blob_length)`.

Multiple DV blobs may live in one Puffin file, so the per-data-file pointer is `(puffin_file_path, blob_offset, blob_length)`, not just a file path. The pushdown request must carry that triple per data file (see [DeleteFileRef](#pushdown-request)). The Puffin footer also records a CRC-32 per blob, which the server may verify on read; the CRC is *not* a content hash usable for cross-table cache lookup, only for tamper detection.

Strategy:

```text
1. resolve referenced Puffin file + blob offset/length from the manifest
2. range-GET the Puffin blob
3. decode roaring bitmap (cached)
4. subtract from candidate row sets
```

For tables that mix v2 position-delete files and v3 DVs (legal during migration), pushdown applies both: union the DV bitmap with the merged-position-delete bitmap before subtracting.

### Position-delete bitmap cache key

The merged position-delete bitmap is a *function of the set of position-delete files that target the data file at the requested snapshot*, not of the data file alone. New snapshots add or compact away position-delete files, which silently changes the merged result. A cache keyed only by data-file identity will hand back stale bitmaps after the first new position-delete file lands.

Cache key per data file:

```text
key = (
    data_file_identity,                  // see Index Consistency
    sorted_set_of_input_delete_file_ids, // each = (path, size, content_hash)
)
```

Equivalent shorthand for v3 DVs, which are intrinsically per-data-file: `(data_file_identity, puffin_file_identity, blob_offset, blob_length)`. Because Puffin files are immutable, that quadruple is sufficient — no separate content hash is required, and no such hash is exposed by Puffin metadata anyway. The Puffin per-blob CRC-32, when present, is used only to detect corruption on read.

`snapshot_id` is *not* a sufficient key — different snapshots can produce the same bitmap, and pinning the cache to snapshot id wastes work after no-op snapshots. Conversely, snapshot_id alone is *insufficient* across tables because position-delete files are not uniquely identified by snapshot number.

When pushdown receives a request, it computes the key from the position-delete entries of the data file's `Deletes` list (already supplied by the client's planner) and looks up or builds the bitmap. Cache eviction follows file identity: when a data file or any of its delete files goes out of scope (snapshot expiration, compaction), the corresponding cache entries are reclaimable.

### Equality deletes (must evaluate at query time)

Equality deletes carry a predicate (e.g. `id = 42`). The applicability rule is:

```text
{ edf :
    edf.DataSequenceNumber  >  data_file.DataSequenceNumber            // strict; sequence
    AND edf.PartitionSpecId ==  data_file.PartitionSpecId              // same spec
    AND edf.PartitionValues ==  data_file.PartitionValues              // same partition
}
```

The sequence rule is strict: data files whose `data_sequence_number` is equal to or greater than the delete file's are not affected — those rows simply never existed when the delete was written. (The Iceberg manifest entry's `file_sequence_number` is the commit-time sequence number for the manifest entry itself and is not used for delete scoping.) Partition matching is required as well: equality-delete files are written per partition, and a delete in one partition does not delete from another, even if the sequence rule matches. This is why the per-data-file pushdown request must carry both the data file's `DataSequenceNumber` / `PartitionSpecId` / `PartitionValues` and the delete files attached to it: the planner has already resolved this scoping.

Equality deletes cannot be precomputed as a static bitmap per data file because:

- one equality delete file can affect many older data files
- the predicate must be re-evaluated against current row content; a bitmap captured at one snapshot will not be reusable at a later snapshot if other equality deletes (or row-level updates expressed as deletes) alter the visible set

Strategy: evaluate equality-delete predicates at query time, accelerated by the same scalar indexes (bloom, bitmap, B-tree). Optionally cache materialized result bitmaps keyed by `(data_file_id, equality_delete_file_id, snapshot_id)` and invalidate on snapshot change.

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

The relevant cost input is the **planned scan after Iceberg pruning**, not the table's total size. A 100 TiB table where Iceberg planning has already narrowed the query to a single 4 MiB file is a poor candidate for pushdown; a 50 GiB table where Iceberg planning produces 5,000 surviving files is a great candidate even if each individual file is small.

Connectors should skip the pushdown API when:

- the planned scan is small enough that a direct ranged GET is cheaper than the planning round-trip (rule of thumb: total planned scan bytes below ~10 MiB *and* surviving file count ≤ 1)
- the predicate is non-selective (estimated selectivity > ~0.5) and no projection prunes meaningful columns
- the query has no predicate and no vector clause — file pruning by Iceberg metadata alone already does the job

The pushdown API should expose its own selectivity and cost estimates in the response stats so connectors can learn when to use it; explicit configuration knobs (`min_planned_scan_bytes`, `min_planned_file_count`, `min_selectivity`) can act as a fallback.

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
