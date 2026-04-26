# Parquet Pushdown Dev Plan

Companion to [PARQUET_PUSHDOWN_DESIGN.md](./PARQUET_PUSHDOWN_DESIGN.md). The design defines *what* the system does; this doc defines *how it lands in this codebase, in what order, with what milestones*.

## Status

Open questions resolved (see [Decisions Recorded](#decisions-recorded)). M0 is unblocked.

## Goals for the implementation

1. Ship the design's Phase 1 in production-quality form: a pushdown service that, given an Iceberg snapshot resolution, returns column-chunk byte ranges and prunes row groups using parsed-footer statistics.
2. Establish the package layout, wire protocol, and trust-mode plumbing so later phases (scalar indexes, page-level pruning, deletes, vector search) attach without reshaping the surface.
3. Avoid breaking any existing read path — pushdown is additive; standard S3 reads keep working.

## What already exists in the codebase

| Component | Path | Reuse |
|---|---|---|
| Iceberg-Go client and table/manifest types | `weed/s3api/iceberg/`, `weed/plugin/worker/iceberg/` | Catalog reads, manifest parsing, planning |
| Parquet reader (`parquet-go/parquet-go v0.28.0`) | `weed/mq/logstore/`, `weed/query/engine/parquet_scanner.go` | Footer parse, ColumnIndex/OffsetIndex |
| SQL/value type conversion | `weed/query/engine/parquet_scanner.go`, `weed/query/sqltypes/` | Predicate constant typing |
| Iceberg compaction worker (uses iceberg-go for planning) | `weed/plugin/worker/iceberg/planning_index.go` | Reference for snapshot → file-list resolution |
| Filer gRPC + protobuf scaffolding | `weed/pb/filer_pb/`, `weed/server/filer_grpc_server.go` | Wire-protocol pattern, auth |
| S3 IAM / bucket ACL | `weed/s3api/`, `weed/iam/` | Trust enforcement on data files |
| Substrait Go SDK (already a transitive dep) | `github.com/substrait-io/substrait-go/v7` | Predicate decode in v2 |

The dependencies needed for v1 (`parquet-go`, `iceberg-go`, `RoaringBitmap/roaring`, `substrait-go/v7`) are already in `go.mod` (some as indirect; will become direct).

## Architectural decisions for v1

These are commitments unless an open question below changes them.

- **Residency: catalog server (planning) + volume servers (heavy execution).** Pushdown is *not* a separate daemon. Planning (snapshot resolution, file-level pruning, side-index registry, footer cache, request validation) runs inside the existing catalog-server role — today colocated with the S3 gateway's Iceberg REST handlers in `weed/s3api/iceberg/`. Heavy execution (local index lookup, scalar predicate evaluation against indexes, vector distance + local top-K) runs on the volume server next to the data. The S3 gateway remains the compatibility path for unmodified readers. Rationale: the catalog already knows tables, snapshots, and table-to-files mapping, which is exactly what the planner needs — adding a separate daemon would duplicate that state. Heavy work goes to the volume servers so a single hot table cannot bottleneck cluster-wide planning. A standalone query-worker tier remains an option (design's Phase 4) only if measurements demand it.
- **Wire protocol: gRPC.** New service definition `parquet_pushdown_pb.ParquetPushdown` in `weed/pb/parquet_pushdown_pb/`. REST/HTTP shim only if a connector requires it. JSON variant deferred.
- **Trust mode for v1: catalog-validated, with connector-trusted as a developer-only flag.** The default deployment validates every request's `DataFiles` and `Deletes` against the Iceberg catalog at the requested snapshot before serving (full rules in [Trust Model](./PARQUET_PUSHDOWN_DESIGN.md#trust-model-and-catalog-validation)). Connector-trusted is supported but only enabled by an explicit flag for local development and benchmarks; it is not the default and is rejected in production builds. Manifest-signed is not on the roadmap.
- **Predicate engine for v1: built-in subset evaluator.** The wire format accepts Substrait `ExtendedExpression`, but v1 implements only: comparisons (`=, !=, <, <=, >, >=`), `IN`, `BETWEEN`, `IS NULL`, and boolean `AND/OR/NOT` over them. Anything outside this subset returns `unsupported predicate`, and the connector falls back to a standard scan. Full Substrait evaluation lands in Phase 2.
- **Side-index physical location:** co-located with the data in a hidden `.index/` directory next to each Parquet file or its parent folder, governed by the same filer ACLs as the data. Path layout matches the design's `<parquet_parent>/.index/<file_name>/<identity>/...` (file-scoped) and `<parquet_parent>/.index/<identity>/...` (folder-scoped) shapes — see [Physical Layout](./PARQUET_PUSHDOWN_DESIGN.md#physical-layout). The hidden-prefix name is configurable for deployments whose readers do not respect dot-prefix conventions.
- **Parquet library: `parquet-go/parquet-go`** (already in use elsewhere in the repo). Apache `arrow-go/v18/parquet` is *not* introduced.
- **Bitmap library: `RoaringBitmap/roaring`.** Used for position-delete bitmaps and DV decoding.

## Code layout

Code lives in two trees that match the architecture's two tiers. The proto stays shared.

**Catalog-server side** — registered on the gRPC server hosted by the catalog (today colocated with the S3 gateway's Iceberg REST handlers in `weed/s3api/iceberg/`):

```text
weed/s3api/iceberg/pushdown/
  service.go                  # SeaweedParquetPushdown service entry
  request_validation.go       # request-shape limits, trust-mode enforcement
  catalog/                    # Phase 1: catalog-validated mode
    validator.go              # cross-checks request DataFiles against manifest
  footer/
    cache.go                  # parsed-footer LRU, keyed by file identity
    parser.go                 # footer + ColumnIndex/OffsetIndex extraction
  predicate/
    substrait_decode.go       # decode Substrait ExtendedExpression
    iceberg_decode.go         # decode Iceberg Expression JSON
    eval_subset.go            # v1 built-in evaluator (comparisons + boolean)
    rowgroup_prune.go         # zone-map pruning for v1
  registry/                   # Phase 2: side-index registry
    schema.go                 # registry rows + filer-backed CRUD
    reconciler.go             # background freshness checker
  delete/                     # Phase 2: Iceberg delete metadata
    position.go               # tracks v2 position-delete files + DVs
    equality.go               # tracks v2 equality-delete files
  loader.go                   # FileHandle + Loader (filer-backed for production)
  stats.go                    # PushdownStats accumulation
```

**Volume-server side** (lands in Phase 3) — registered on the volume server's gRPC surface so index lookup, predicate evaluation, and vector compute run colocated with data:

```text
weed/server/volume_grpc_pushdown.go   # entry point on the volume server
weed/storage/pushdown/
  index/
    bloom/                    # Phase 3
    bitmap/                   # Phase 3
    btree/                    # Phase 3
    page/                     # Phase 3
    vector/ivf/               # Phase 3
  exec/
    predicate.go              # evaluate v1 predicate subset against indexes
    vector.go                 # local distance + top-K
    bitmap_merge.go           # delete-bitmap union / subtraction
```

**Shared proto** stays as today:

```text
weed/pb/parquet_pushdown.proto      # request/response messages
weed/pb/parquet_pushdown_pb/        # generated
```

Tests live alongside the code. Cross-tier integration tests live in `test/parquet_pushdown/`.

> **Existing code at `weed/parquet_pushdown/`** (M0 + M1) was scaffolded under the prior standalone-daemon decision. A code-move commit relocates the package to `weed/s3api/iceberg/pushdown/`, drops the `weed pushdown` subcommand and the standalone-daemon bootstrap, and rewires the service into the existing s3api gRPC server. That move is sequenced as the first task of M2 work in the revised milestones below.

## Milestones

Milestones are organized by the design's four-phase rollout: catalog candidates, catalog index registry, volume execution, optional workers. M0 and M1 already shipped under the previous standalone-daemon decision; **M2-rebase** is the first task of the revised plan and unblocks everything that follows.

### Already shipped under the prior architecture

Two milestones live at `weed/parquet_pushdown/` and need the rebase below before further work:

- **M0 (shipped):** proto + service skeleton + bufconn integration test.
- **M1 (shipped):** parsed-footer cache + file/range-level pushdown for predicate-less requests.

### M2-rebase — Move pushdown into the catalog server (1 PR)

- Move `weed/parquet_pushdown/` to `weed/s3api/iceberg/pushdown/`. Update import paths.
- Drop `weed pushdown` and `weed pushdown.ping`; remove `weed/parquet_pushdown/daemon/`. The standalone-daemon path is retired.
- Register `SeaweedParquetPushdown` on the s3api gRPC server alongside the existing Iceberg REST handlers, gated by a `-pushdown.enabled` flag (default off until Phase 1 ships).
- Update the existing M0/M1 tests to exercise the service via the s3api gRPC registration rather than a bufconn-only listener.
- Acceptance: existing M0/M1 tests pass against the relocated package; `weed s3 -pushdown.enabled` exposes the service.

### Phase 1: Catalog returns candidate files / row groups / ranges

#### M3 — Row-group zone-map pruning (1 PR)

- `predicate/eval_subset.go`: evaluator for the v1 predicate subset.
- `predicate/rowgroup_prune.go`: read row-group statistics from the parsed footer, drop row groups where the predicate cannot be true, return per-data-file `RowGroupRef` list.
- `predicate/substrait_decode.go`: decode just enough Substrait to feed the subset evaluator; everything else returns `unsupported`.
- Acceptance:
  - Predicate `WHERE timestamp BETWEEN t1 AND t2` against a 5M-row partitioned Iceberg table prunes to the expected row groups.
  - `unsupported predicate` returns a status the connector can recognize and fall back from.

#### M4 — Catalog-validated trust mode (1 PR; required for Phase 1)

- `catalog/validator.go`: read the Iceberg snapshot via the existing internal Iceberg package (decision 6 below), verify `(Path, SizeBytes, RecordCount, DataSequenceNumber, PartitionSpecId, PartitionValues)` for each `DataFile` and that the `Deletes` list matches the manifest's attached delete files.
- Configurable per-endpoint: `pushdown.trust=catalog-validated|connector-trusted` (default `catalog-validated`; `connector-trusted` requires an explicit dev-mode flag).
- Cache validated planning result keyed by `(table, SnapshotId)` to amortize manifest reads across requests in the same snapshot.
- Acceptance: a tampered request (omitted delete file, wrong sequence number, file not in manifest, wrong partition values) is rejected with `permission_denied`. Cache hit/miss visible in `PushdownStats`.

**End of design Phase 1.** Phase 1 does not ship without M2-rebase + M3 + M4.

### Phase 2: Catalog tracks side-index registry

#### M5 — Index registry schema and CRUD (1 PR)

- `registry/schema.go`: registry rows of shape `(table, file_identity, kind, field_id) -> (.index path, build identity, build_time, healthy)`. Stored in the filer under a system path, separate from the table's bucket prefix.
- CRUD APIs (gRPC + internal Go) for register, lookup, mark-stale, delete.
- Planner consults the registry to know which indexes exist; missing entries fall through gracefully and are reported in stats.
- Acceptance: register, lookup, list, and delete round-trips pass; planner stats show `indexes_used` / `indexes_missing` correctly populated.

#### M6 — Iceberg delete metadata in the registry (1 PR)

- Register position-delete files, equality-delete files, and v3 deletion vectors as registry entries attached to the data file they target.
- Planner resolves applicable deletes per the design's applicability rules (sequence + partition + file scope, with per-row `file_path` filter for multi-target position-delete files).
- Acceptance: planner returns the same data-file → delete-files mapping as a brute-force Iceberg manifest scan.

#### M7 — Background reconciler (1 PR)

- Worker that iterates the registry, verifies each `.index/` payload exists at the recorded path with the recorded identity, marks stale rows, and rebuilds when possible.
- Compaction / snapshot expiration eviction: registry rows whose `file_identity` is no longer referenced by any active snapshot are reclaimable.
- Acceptance: tampering with an index payload causes the next request to report it missing; reconciler restores the registry on the next pass.

### Phase 3: Volume servers execute local index/vector pushdown

#### M8 — Volume-server bloom-filter index (1–2 PRs)

- Build path: a worker reads Parquet column chunks and writes SplitBlock Bloom Filters to `.index/<file_name>/<identity>/bloom.fid_<id>` next to the data needles. Registry entry written on completion.
- Read path: volume server gains a `PushdownExec` gRPC service that, given a (path, identity, kind, field_id, predicate), returns a per-row-group bitmap of surviving rows. Catalog plans for predicates touching bloom-able columns include the volume-server endpoint.
- Acceptance: equality predicate `WHERE user_id = 'x'` against a 10M-row column with no matches reads zero column-chunk bytes after planning.

#### M9 — Volume-server bitmap and B-tree (1 PR each)

- `index/bitmap/` for low/medium-cardinality columns; integrates with `RoaringBitmap`.
- `index/btree/` for sortable columns; range and point-lookup predicates.
- Acceptance: ≥10× row-group reduction vs M3's footer-only pruning on the standard test workload.

#### M10 — Volume-server page-level pruning (1 PR)

- `index/page/`: wraps Parquet's `ColumnIndex` + `OffsetIndex` per [Page-Level Index](./PARQUET_PUSHDOWN_DESIGN.md#3-page-level-index); row-range translation across columns; dictionary-page byte-range inclusion.
- Volume server returns `PageRef` entries with byte ranges for each projected column.
- Acceptance: predicate selectivity ≥0.99 returns ≤1 page per surviving row group across all projected columns.

#### M11 — Volume-server delete bitmap evaluation (1 PR)

- v2 position-delete bitmap merge per the design's filter-by-`file_path` rule; cached with the registry's identity key.
- v3 deletion-vector (Puffin `deletion-vector-v1`) reader + cache.
- v2 equality-delete per-pair bitmap evaluation, accelerated by the volume server's scalar indexes.
- Acceptance: a reference table with both delete forms returns identical row sets to a Spark scan of the same snapshot.

#### M12 — Volume-server vector index (1 PR)

- Partitioned IVF index per [Filtered-ANN strategy](./PARQUET_PUSHDOWN_DESIGN.md#filtered-ann-strategy); per-partition top-K runs entirely on the volume server. Catalog merges per-partition top-Ks across volume servers.
- Acceptance: recall ≥0.95 vs brute force at top-20, 10× end-to-end speedup on the standard test set.

#### M13 — Hybrid scalar+vector pushdown (1 PR)

- Planner selects partitioned-IVF, post-filter+overscan, or filtered-HNSW per the design's strategy section based on selectivity estimate.
- Acceptance: hybrid query that combines `tenant_id = 'x' AND timestamp >= t` with `ORDER BY embedding <-> q LIMIT 20` returns recall-correct results in <2× the cost of the lighter clause alone.

**End of design Phase 3.**

### Phase 4: Optional standalone query workers

Built only if profiling shows planning, hot-path coordination, or cross-volume merges become a bottleneck. Out of scope until measurements demand it.

### Connector integrations (parallel with Phases 1–3)

These run in parallel with Phases 1–3 once the catalog API stabilizes (post-M4). Each consumer reads byte ranges directly via S3 (compatibility path) or dispatches to volume-server endpoints (hot path) per request.

- DuckDB extension as PoC; consumes the gRPC API directly.
- Trino connector pushdown.
- Spark DataSource / Iceberg integration.

## Test strategy

- **Unit tests:** every package, with parquet-go in-memory fixtures.
- **Integration tests:** `test/parquet_pushdown/` — spins up filer + a small Iceberg catalog (reuse `weed/s3api/iceberg/` infrastructure), generates Parquet data with `parquet-go` writers, compares pushdown results to a brute-force scan.
- **Correctness oracle:** every milestone validates that pushdown output is a *strict subset* of a brute-force Iceberg+Parquet scan with the same predicate, and that the projected-column byte ranges decode to the same rows. The oracle is the safety net that lets us refactor index internals freely.
- **Performance suite:** runs in CI weekly, not on every PR. Tracks footer-cache hit rate, row-group reduction, page reduction, end-to-end query latency vs a baseline standard scan.

## Decisions Recorded

These were the open questions before M0. All have been resolved; recording for traceability and to anchor future amendments.

1. **Residency:** catalog server (planning, registry, footer cache) + volume servers (local index lookup, predicate eval, vector compute). Standalone daemons are *not* introduced; query workers as an extra tier remain a future option contingent on measurements. (See [Architectural decisions](#architectural-decisions-for-v1).)
2. **Service surface:** new `weed/pb/parquet_pushdown_pb/` package; not extending `filer_pb`.
3. **Trust mode:** catalog-validated is the default and required for Phase 1 shipping; connector-trusted is a dev-only flag rejected in production builds.
4. **Side-index path:** co-located in a hidden `.index/` directory next to the Parquet data, under the same parent folder. File-scoped: `<parquet_parent>/.index/<file_name>/<identity>/...`. Folder-scoped: `<parquet_parent>/.index/<identity>/...`. Hidden-prefix name configurable for non-dot-aware readers.
5. **Predicate subset for v1:** `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN`, `IS NULL`, boolean `AND/OR/NOT`. `LIKE` deferred to the inverted-index milestone. Wire format is Substrait `ExtendedExpression` from M0.
6. **Iceberg snapshot integration:** route through the existing internal Iceberg packages (`weed/s3api/iceberg/`, `weed/plugin/worker/iceberg/`); do not add a parallel `iceberg-go` integration in `weed/parquet_pushdown/`.
7. **Auth:** reuse the filer's existing auth. The request principal must have read access to every `DataFile` listed; the daemon enforces this by re-using the filer's authorization on every blob fetch.
8. **Test corpus:** generated from scratch in `test/parquet_pushdown/fixtures/`, parameterized over (with-position-deletes, with-equality-deletes, with-DVs, partitioned/unpartitioned).
9. **Phase 1 done bar:** passing CI tests + an I/O-reduction benchmark on a reference query. Connector PoC slips to M12.
10. **Performance regression budget for v1:** ≤10% added latency vs a direct ranged GET on a 10 MiB single-file scan in the worst case (predicate prunes nothing). The threshold is exposed as a configuration knob (`pushdown.max_overhead_pct`, default `10`) so deployments can tighten or loosen it without code changes.

## Risk register

- **Iceberg-Go maturity for v3 deletion vectors / Puffin.** If `iceberg-go v0.5.0` does not expose Puffin DV blobs, M8 needs a vendored Puffin parser. Spike during M0 to verify.
- **parquet-go ColumnIndex/OffsetIndex completeness.** M6 depends on full ColumnIndex/OffsetIndex support. If the library returns nil for some files, fall back to row-group-only pruning per the design.
- **Catalog read latency.** M3's per-request manifest read must be cacheable per snapshot, otherwise the catalog-validated mode adds >100ms per request. Cache eviction strategy is part of M3's spec.
- **Trust boundary leakage.** A pushdown response must not return data references that a direct read would deny. The auth check (open question 7) is load-bearing; integration tests in M0 must include negative cases.
