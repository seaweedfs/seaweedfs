# Parquet Pushdown Dev Plan

Companion to [PARQUET_PUSHDOWN_DESIGN.md](./PARQUET_PUSHDOWN_DESIGN.md). The design defines *what* the system does; this doc defines *how it lands in this codebase, in what order, with what milestones*.

## Status

Draft. Open questions remain — see [Open Questions](#open-questions). Coding does not start until those are resolved.

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

- **Residency: separate `weed pushdown` daemon.** The pushdown service runs as its own process, scaled and operated independently of the filer. It talks to the filer over the existing filer gRPC API to read Parquet data and side-index blobs, and to the Iceberg catalog (via the existing internal package — see decision 6 below) for snapshot resolution and request validation. Rationale: pushdown availability and resource consumption (cache memory, predicate evaluation CPU, per-tenant index loads) decouple from the filer's hot path, and the daemon can scale horizontally without touching the storage layer. The daemon is stateless on disk: caches are in-memory; durable side indexes live in the filer per [Side-index path](#side-index-path).
- **Wire protocol: gRPC.** New service definition `parquet_pushdown_pb.ParquetPushdown` in `weed/pb/parquet_pushdown_pb/`. REST/HTTP shim only if a connector requires it. JSON variant deferred.
- **Trust mode for v1: catalog-validated, with connector-trusted as a developer-only flag.** The default deployment validates every request's `DataFiles` and `Deletes` against the Iceberg catalog at the requested snapshot before serving (full rules in [Trust Model](./PARQUET_PUSHDOWN_DESIGN.md#trust-model-and-catalog-validation)). Connector-trusted is supported but only enabled by an explicit flag for local development and benchmarks; it is not the default and is rejected in production builds. Manifest-signed is not on the roadmap.
- **Predicate engine for v1: built-in subset evaluator.** The wire format accepts Substrait `ExtendedExpression`, but v1 implements only: comparisons (`=, !=, <, <=, >, >=`), `IN`, `BETWEEN`, `IS NULL`, and boolean `AND/OR/NOT` over them. Anything outside this subset returns `unsupported predicate`, and the connector falls back to a standard scan. Full Substrait evaluation lands in Phase 2.
- **Side-index physical location:** under a separate filer namespace `/__pushdown__/<table_uuid>/...`, governed by the same filer ACLs. Out of any user-visible bucket prefix. Path layout matches the design's `<system-prefix>/<table_uuid>/.../<identity>/` shape.
- **Parquet library: `parquet-go/parquet-go`** (already in use elsewhere in the repo). Apache `arrow-go/v18/parquet` is *not* introduced.
- **Bitmap library: `RoaringBitmap/roaring`.** Used for position-delete bitmaps and DV decoding.

## Code layout

New code lives under `weed/parquet_pushdown/`:

```text
weed/parquet_pushdown/
  service.go                  # gRPC service entry; dispatches to subsystems
  request_validation.go       # request-shape limits, trust-mode enforcement
  catalog/                    # Phase 1.5: catalog-validated mode
    validator.go              # cross-checks request DataFiles against manifest
  footer/
    cache.go                  # parsed-footer LRU, keyed by file identity
    parser.go                 # footer + ColumnIndex/OffsetIndex extraction
  index/
    types.go                  # SideIndex interface, registry
    bloom/                    # Phase 2
    bitmap/                   # Phase 2
    btree/                    # Phase 2
    page/                     # Phase 3
    vector/ivf/               # Phase 5
  predicate/
    substrait_decode.go       # decode Substrait ExtendedExpression
    iceberg_decode.go         # decode Iceberg Expression JSON
    eval_subset.go            # v1 built-in evaluator (comparisons + boolean)
    rowgroup_prune.go         # zone-map pruning for v1
  delete/
    position_bitmap.go        # Phase 4 — v2 position-delete merge + cache
    deletion_vector.go        # Phase 4 — v3 Puffin DV reader + cache
    equality_eval.go          # Phase 4 — equality-delete evaluator
  storage/
    filer_blob_store.go       # read/write side-index blobs through filer
  stats.go                    # PushdownStats accumulation
  daemon/
    server.go                 # gRPC server bootstrap for the pushdown daemon
    config.go                 # flag parsing for the `weed pushdown` subcommand
    filer_client.go           # filer gRPC client for reading data + side-index blobs

weed/pb/parquet_pushdown_pb/
  parquet_pushdown.proto      # request/response messages
  parquet_pushdown.pb.go      # generated
  parquet_pushdown_grpc.pb.go # generated

weed/command/pushdown.go      # `weed pushdown` daemon entry point
                              #   plus build/inspect side-index subcommands
```

Tests live alongside the code (`*_test.go`). Integration tests for end-to-end paths land in `test/parquet_pushdown/`.

## Milestones

Each milestone is a shippable PR (or small chain of PRs). M0–M2 together implement the design's Phase 1.

### M0 — Infrastructure (1 PR)

- `parquet_pushdown.proto` with `Pushdown` RPC, request/response messages mirroring [API Sketch](./PARQUET_PUSHDOWN_DESIGN.md#api-sketch). Generate Go bindings.
- Empty service implementation that validates request shape (size limits, MaxRowIds cap, trust-mode tag) and returns `unimplemented` for the actual work.
- New `weed pushdown` subcommand: standalone daemon binary that boots its own gRPC server, opens a filer client connection (master + filer endpoints from flags, mirroring the existing pattern in `weed mount` / `weed s3`), and registers the pushdown service. Daemon is off by default; users start it explicitly.
- `weed pushdown ping` CLI subcommand for smoke testing the running daemon.
- Acceptance: integration test starts a filer + a pushdown daemon, calls `Pushdown` over gRPC, gets the expected `unimplemented` error path with stats populated. Daemon shuts down cleanly on SIGTERM.

### M1 — Parsed-footer cache + file/range-level pushdown (1 PR)

- `footer/parser.go`: open Parquet file via the filer chunk reader, parse footer, ColumnIndex, OffsetIndex.
- `footer/cache.go`: LRU keyed by `(path, size, etag-or-recordcount)` per [Index Consistency](./PARQUET_PUSHDOWN_DESIGN.md#index-consistency).
- Service handles requests with `Predicate == nil`: return `FileRanges` covering the requested `Columns` for each `DataFile` (one entry per column chunk), no pruning yet.
- Acceptance:
  - Round-trip integration test against a 3-file Iceberg table built in `test/parquet_pushdown/fixtures/`.
  - Footer cache hit-rate test (second call avoids re-parse; verified via `PushdownStats.FooterCacheHits`).

### M2 — Row-group zone-map pruning (1 PR)

- `predicate/eval_subset.go`: evaluator for the v1 predicate subset.
- `predicate/rowgroup_prune.go`: read row-group statistics from the parsed footer, drop row groups where the predicate cannot be true, return per-data-file `RowGroupRef` list.
- `predicate/substrait_decode.go`: decode just enough Substrait to feed the subset evaluator; everything else returns `unsupported`.
- Acceptance:
  - Predicate `WHERE timestamp BETWEEN t1 AND t2` against a 5M-row partitioned Iceberg table prunes to the expected row groups (verified by counting `RowGroupRef`).
  - `unsupported predicate` returns a status the connector can recognize and fall back from.

### M3 — Catalog-validated trust mode (1 PR; required for Phase 1)

Lands before M2 ships externally. Sequenced after M1 because catalog validation needs the parsed-footer cache to verify `RecordCount` and column-chunk metadata against the manifest.

- `catalog/validator.go`: read the Iceberg snapshot via the existing internal Iceberg package (decision 6 below), verify `(Path, SizeBytes, RecordCount, DataSequenceNumber, PartitionSpecId, PartitionValues)` for each `DataFile` and that the `Deletes` list matches the manifest's attached delete files.
- Configurable per-endpoint: `pushdown.trust=catalog-validated|connector-trusted` (default `catalog-validated`; `connector-trusted` requires an explicit dev-mode flag).
- Cache validated planning result keyed by `(table, SnapshotId)` to amortize manifest reads across requests in the same snapshot.
- Acceptance: a tampered request (omitted delete file, wrong sequence number, file not in manifest, wrong partition values) is rejected with `permission_denied`. Cache hit/miss visible in `PushdownStats`.

**End of design Phase 1.** Phase 1 does not ship without M3.

### M4 — Bloom-filter side index (1–2 PRs; design Phase 2 starts)

- `index/bloom/`: builder reads Parquet column chunks, produces a SplitBlock Bloom Filter (Parquet's standard) per column chunk, stores via `storage/filer_blob_store.go` keyed by `bloom.fid_<id>`.
- Predicate evaluator consults bloom for `=` / `IN` predicates.
- Background builder triggered on new Iceberg snapshot (hooks into the existing iceberg compaction worker — TODO: confirm hook point).
- Acceptance: equality predicate with no matches against a 10M-row column reads zero data needles; cache miss and miss-build also tested.

### M5 — Bitmap and B-tree side indexes (1 PR each)

- `index/bitmap/` for low/medium-cardinality columns; integrates with `RoaringBitmap`.
- `index/btree/` for sortable columns; range and point-lookup predicates.
- Acceptance: micro-benchmarks vs M2's row-group-only pruning showing ≥10× row-group reduction on the standard test workload.

### M6 — Page-level pruning (1 PR; design Phase 3)

- `index/page/`: wraps Parquet's ColumnIndex + OffsetIndex; row-range translation across columns; dictionary-page byte-range inclusion per [Page-Level Index](./PARQUET_PUSHDOWN_DESIGN.md#3-page-level-index).
- Service returns `PageRef` entries with byte ranges for each projected column.
- Acceptance: predicate selectivity ≥0.99 returns ≤1 page per surviving row group across all projected columns.

### M7–M9 — Iceberg deletes (3 PRs; design Phase 4)

- M7: position-delete bitmap caching, keyed per [Position-delete bitmap cache key](./PARQUET_PUSHDOWN_DESIGN.md#position-delete-bitmap-cache-key).
- M8: Puffin DV reader + cache (use `iceberg-go` Puffin support if present, otherwise vendor a minimal Puffin parser).
- M9: equality-delete evaluation accelerated by Phase 2 indexes.
- Acceptance: reference table with both delete forms returns identical row sets to a Spark scan of the same snapshot.

### M10–M11 — Vector indexes (design Phase 5)

- M10: partitioned IVF index per [Filtered-ANN strategy](./PARQUET_PUSHDOWN_DESIGN.md#filtered-ann-strategy); per-tenant partitions, top-K merge, score correctness vs brute-force.
- M11: hybrid scalar+vector pushdown; planner selects partitioned-IVF vs post-filter+overscan based on selectivity estimate.
- Acceptance: recall ≥0.95 vs brute force at top-20, 10× speedup on the standard test set.

### M12–M13 — Connector integrations (design Phase 6)

- M12: DuckDB extension as PoC; consumes the gRPC API directly.
- M13: Trino + Spark connectors; both use the same gRPC client.

## Test strategy

- **Unit tests:** every package, with parquet-go in-memory fixtures.
- **Integration tests:** `test/parquet_pushdown/` — spins up filer + a small Iceberg catalog (reuse `weed/s3api/iceberg/` infrastructure), generates Parquet data with `parquet-go` writers, compares pushdown results to a brute-force scan.
- **Correctness oracle:** every milestone validates that pushdown output is a *strict subset* of a brute-force Iceberg+Parquet scan with the same predicate, and that the projected-column byte ranges decode to the same rows. The oracle is the safety net that lets us refactor index internals freely.
- **Performance suite:** runs in CI weekly, not on every PR. Tracks footer-cache hit rate, row-group reduction, page reduction, end-to-end query latency vs a baseline standard scan.

## Open Questions

These need answers before M0 starts. Most have defaults proposed above; confirm or override.

1. **Residency for v1 — confirm filer-managed?** Alternative: a separate `weed pushdown` daemon. Filer-managed is simpler but couples pushdown availability to filer availability. *Default: filer-managed.*
2. **Service surface — extend `filer_pb` or new `parquet_pushdown_pb` package?** New package is cleaner but adds a discovery hop for clients. *Default: new package.*
3. **Trust mode for the very first integration tests — connector-trusted only, or do we need M3 (catalog-validated) before any test exercises the trust boundary?** *Default: connector-trusted for M0–M2, M3 lands before any external connector PoC (M12).*
4. **Side-index path root.** `/__pushdown__/` is my proposal; is there an existing convention I should match? (I see `/.snapshots/`, `/.iam/`, etc. in other parts of the repo.)
5. **Predicate evaluator scope for v1 — confirm the subset listed above (`=, !=, <, <=, >, >=`, `IN`, `BETWEEN`, `IS NULL`, boolean ops)?** Anything else you want supported up-front (e.g. `LIKE`)? *Default: subset above; `LIKE` deferred to inverted-index milestone.*
6. **Iceberg snapshot integration in M3 — use `iceberg-go` directly, or call into the existing `weed/s3api/iceberg/` and `weed/plugin/worker/iceberg/` code?** The latter has table cache + lock support already. *Default: route through the existing internal package, do not add a parallel iceberg-go integration.*
7. **Auth surface for the gRPC endpoint — reuse the filer's existing auth (mTLS / JWT), or add a pushdown-specific token?** *Default: reuse filer auth; the request principal must have read access to every `DataFile` listed.*
8. **Test corpus for M1.** Is there an existing Iceberg-with-deletes fixture in `test/` we should standardize on, or do we generate one from scratch in `test/parquet_pushdown/fixtures/`? *Default: generate from scratch, parameterized over (with-position-deletes, with-equality-deletes, with-DVs, partitioned/unpartitioned).*
9. **Phase 1 acceptance bar.** What's the definition of done that lets us call Phase 1 shipped — passing tests in CI, a benchmark showing measurable I/O reduction on a reference query, or a connector PoC? *Default: tests + I/O-reduction benchmark; PoC slips to M12.*
10. **Performance regression budget.** Pushdown adds work on the read path. What overhead is acceptable when the predicate misses everything (worst case for pushdown)? *Default: ≤10% added latency vs a direct ranged GET on a 10 MiB single-file scan.*

## Risk register

- **Iceberg-Go maturity for v3 deletion vectors / Puffin.** If `iceberg-go v0.5.0` does not expose Puffin DV blobs, M8 needs a vendored Puffin parser. Spike during M0 to verify.
- **parquet-go ColumnIndex/OffsetIndex completeness.** M6 depends on full ColumnIndex/OffsetIndex support. If the library returns nil for some files, fall back to row-group-only pruning per the design.
- **Catalog read latency.** M3's per-request manifest read must be cacheable per snapshot, otherwise the catalog-validated mode adds >100ms per request. Cache eviction strategy is part of M3's spec.
- **Trust boundary leakage.** A pushdown response must not return data references that a direct read would deny. The auth check (open question 7) is load-bearing; integration tests in M0 must include negative cases.
