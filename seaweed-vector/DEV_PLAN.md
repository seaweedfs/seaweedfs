# S3 Vectors ANN — Dev Plan

## Goal

Approximate nearest-neighbour search for S3 Vectors indexes, built on Lance
vector indexes, so that query latency stops growing linearly with the index.
Target: 90%+ recall@10 (what AWS states for S3 Vectors), exact results below
a size threshold and for rows written since the last index build, and one
code path that serves both the open-source single engine and the Enterprise
distributed query layer. Writes stay unchanged.

## Status (2026-11-04)

| Phase | Branch | State |
|---|---|---|
| 0 Build and measure | `vector-ann-phase0` | open: `vector_index` builder, `weed-vector-cli index`, `PERF_INDEX`, `PERF_CLUSTERS` |
| 1 Probe policy | `vector-ann-phase1` | open: policy in `query::scanner`, `--nprobes`, one Lance session per engine |
| 2 Build job | `vector-ann-phase2` | open: `lance_vector_index` in `weed-worker`, admin-driven end-to-end test, worker CI legs |
| 3 Keep it covering | `vector-ann-phase3` | open: relative budget, retrain rule, end-to-end steps |
| 4 Docs and gates | — | after the above merge |
| 5 Enterprise sharded builds | — | the Enterprise repository |

What the measurements changed:

- **IVF needs structure in the data.** On independent Gaussian vectors, the
  perf suite's default, recall through an IVF index tracks the fraction of
  partitions probed: at 50k × 384 with 224 partitions, 8 probes gave 0.13 and
  64 probes 0.56, and latency did not fall with fewer probes. On vectors drawn
  around 100 shared centres, 8 probes gave recall 1.0; with eight clients
  the index served 145 queries/s against 52 exact at 50k and 70 against 11
  at 200k, and the 10% filtered query at 200k fell from 445 to 265 ms,
  while one client's latency stayed at the request path's floor on the
  shared host. Embeddings cluster; the perf suite grows `PERF_CLUSTERS`
  and the end-to-end test draws its rows around centres. The probe rule
  stands as written.
- **The engine opened every table in a fresh Lance session**, so the index
  cache died with each request. Phase 1 gives the engine one session, sized
  by `--index-cache-mb`.
- **Compaction keeps the index covering** (Lance 10 remaps it), so the
  compaction risk below did not materialise.

## Where things stand (2026-11-04)

What already exists, with the files that hold it:

- **The engine is index-aware; nothing builds an index.** `QueryVectors` in
  `crates/server/src/service.rs` always calls `weed_vector_core::query::ann`,
  which runs a Lance scan with `use_index(true)`: ANN on fragments an index
  covers, a flat pass on the rest, decided per fragment by Lance
  (`crates/core/src/query.rs`). `nprobes` is `None`. `CreateIndex` writes an
  empty Lance dataset and no vector index, so today every fragment is "tail"
  and every query is an exact scan.
- **The maintenance worker only extends indexes that exist.** The
  `lance_optimize_indices` job in `seaweed-worker/crates/lance/src/jobs/indices.rs`
  skips a table whose `load_indices()` is empty and otherwise calls
  `optimize_indices(&OptimizeOptions::default())` once `num_unindexed_rows`
  exceeds its `max_unindexed_rows` budget (default 100,000).
- **Coverage and segments are modelled.** `crates/core/src/coverage.rs` reports,
  per index name, the fragments covered and the uncovered rows; it requires the
  fragment bitmap each Lance `IndexMetadata` delta carries.
  `crates/core/src/segments.rs` lists physical segments (one per delta) with
  their names and bitmaps, and partitions them across workers. `weed-vector-cli
  coverage` and `weed-vector-cli query --flat` expose both.
- **Filters are applied before ranking.** `query.rs` sets `prefilter(true)`
  unless the request asks for post-filtering, which S3 Vectors never does.
  Lance's IVF search honours the prefilter, so this is already the behaviour
  AWS calls an `ENHANCED` index.
- **Enterprise consumes exactly this model.** In the Enterprise repository,
  `enterprise/weed/pb/vector_coord.proto` defines `SegmentInfo{id,
  resident_bytes, rows, fragment_ids}` and `SegmentList{tail_fragment_ids,
  tail_bytes}`; `VectorQuery` already carries `nprobes`; a `Task` is a set of
  segment ids plus fragment ids. `sw-vector-distributed/src/plan.rs` assigns
  a fragment covered by several segments to the lowest segment and never mixes
  index names in one scan; `resident_bytes` is the index files' size.
  `datasets.rs` warms a slot with `prewarm_index_segments` per index name and
  warms the tail by scanning it. `sw-vector-distributed/src/index.rs` holds
  `plan_index` (shard the uncovered fragments across workers),
  `build_index_task` (one delta per shard, named
  `<column>-w<worker>-v<version>`, built with `create_index_builder(..)
  .fragments(..)`) and `optimize_policy` (append deltas until `MAX_DELTAS = 8`,
  then Lance's default options, which promise no merge; see phase 3).
  Nothing calls these three yet; `tests/index.rs`,
  `tests/plan_search.rs` and `tests/engine_query.rs` exercise them with
  `ivf_flat(4)` indexes.
- **Lance 10.0.0 offers** `VectorIndexParams::ivf_flat`, `ivf_pq`,
  `with_ivf_sq_params`, `with_ivf_rq_params`, `ivf_hnsw` and HNSW variants;
  scanner knobs `nprobes`, `minimum_nprobes`, `maximum_nprobes`, `ef`,
  `refine`; `OptimizeOptions{num_indices_to_merge, index_names, retrain}`.
- **Measured today** (384-d, cosine, top-10, one 16-core host, exact scans):
  one engine answers in 42 ms at 50k vectors and 82 ms at 200k; filtered
  queries take 81 and 259–336 ms. Four Enterprise engines split four ways
  bring 200k to 58 ms and the filtered query to 73 ms. Recall is 1.000.
- **Tests assume exactness only on tiny tables.** `test/s3vectors/s3vectors_test.py`
  asserts exact result order on indexes of a handful of vectors; the perf
  suite asserts recall@10 ≥ 0.9 against a client-side exact search.
- **AWS**: "90%+ average recall for most datasets"; `CLASSIC` and `ENHANCED`
  index modes, the latter pre-filtering; no algorithm named.

## Design

### The contract every consumer relies on

An S3 Vectors index gains a Lance vector index on its `vector` column, and
everything downstream keeps working because it only ever sees:

1. **Segments**: Lance index deltas with fragment bitmaps. Every build and
   every merge must leave `coverage_known == true`; a delta without a bitmap
   turns the whole index into "rebuild" for the Enterprise planner.
2. **Names**: one logical index name per shard, never two shards under one
   name within a build. Scans and warm-ups group by name.
3. **Tail**: fragments no segment covers, searched exactly. Rows written
   after a build land here, so recall never drops to zero between builds.
4. **Bytes**: `resident_bytes` of a segment is its index files' size. The
   controller's W sizing (`W_mem`, `W_scan`) and the engine's
   `--capacity-bytes` budget use it unchanged.
5. **Versions**: a build or merge commits a new dataset version. Routers cache
   segment lists per version, plans pin versions with the lease tags in
   `crates/core/src/maintenance.rs`, and cleanup respects those leases. No
   new mechanism is needed, but the build job must go through the same
   commit path as any Lance write.

### Index type and parameters

- **IVF_FLAT first.** Distances inside probed partitions are exact, so recall
  is governed by `nprobes` alone; the only training is k-means over the
  centroids; resident bytes stay close to the raw vectors, which keeps the
  Enterprise W sizing in the same range as today. `IVF_PQ` becomes an
  operator option later for indexes whose raw size is the constraint. HNSW
  variants are out of scope: their build cost and memory do not fit the
  maintenance-worker model.
- **Metric from the table.** The vector field's `distance_metric` metadata
  (`COSINE`, `DOT_PRODUCT`, else L2) is the metric the index is built with,
  exactly as `service.rs` resolves it for queries.
- **Partitions**: start at `clamp(round(sqrt(rows)), 1, 4096)` for the whole
  table, and for a shard, `sqrt` of the shard's rows. Phase 0 measures
  recall and latency against this and adjusts before it becomes a default.
- **Probes**: one count per query, `max(8, ceil(partitions / 10))` clamped to
  the partition count, taken from the largest delta of the index. A delta
  with no more partitions than that count is searched in full; a larger one
  at that count. Lance searches at least that many partitions. Under a
  filter it may widen to four times the count until it has k rows, which a
  selective filter needs; an open upper bound made Lance prepare every
  partition for that and cost as much as the exact scan (201 ms against
  104 ms for the 10% filter at 50k × 384). Without a filter it stops at the
  count.

### Who builds, and when

The Lance maintenance worker (`weed-worker`) gets a new job type,
`lance_vector_index`, beside `lance_optimize_indices`:

- **Detection**: tables in a vector bucket's `vector` namespace (ids of the
  form `<bucket>$vector$<index>`) whose vector column has no index and at
  least `min_rows` live rows (default 50,000). Below the threshold the index
  stays exact, which also keeps the existing small-table tests exact.
- **Execution**: one `create_index` call with IVF_FLAT, the table's metric,
  the partition rule above, and a fixed name (`vector_idx`). Progress and
  counters follow the pattern of `indices.rs`; a new `indexes_built_total`
  metric and an admin observation of index coverage sit beside the row and
  fragment counts the worker already reports.
- **Why the worker and not the engine or the gateway.** `CreateIndex` has no
  rows to index. The engine serves queries and should not run minute-long
  k-means builds. The worker already has the catalog client, credentials,
  the admin UI settings form, metrics, retries and dedupe, and plugin jobs
  start disabled until an operator turns them on, which is the rollout
  switch we want.
- **Enterprise sharded builds** (phase 5): the same job, when Enterprise
  engines are registered, splits the uncovered fragments with `plan_index`
  and has each engine run `build_index_task` for its shard, producing one
  named delta per shard. This is what `index.rs` was written for; the worker
  drives it, the engines build, and the shared `optimize_policy` keeps each
  name's delta count under eight.

### Keeping it covering

`lance_optimize_indices` already does the work once an index exists. Two
changes:

- The budget becomes relative for vector tables: reindex when uncovered rows
  exceed `min(absolute budget, 10% of indexed rows)`, so a 60k-row index
  reindexes after 6k new rows instead of waiting for 100k.
- A retrain once the table has grown to about 4× the rows the centroids were
  trained on, so partitions do not go stale. No row count is recorded: the
  retrain is due when the partition rule for the current rows gives twice
  the partitions the index has, and it rebuilds a single IVF_FLAT name
  through the builder at that count. Past 2,048 partitions (about 4.2M
  rows) the rule cannot double under the 4,096 cap, so those centroids are
  not retrained again: that would need the trained row count recorded and a
  full rebuild of a table that size, which is left for later.

### Deletes and upserts

A delete marks rows in Lance deletion files; an upsert is a delete plus an
append. Deleted rows are excluded through the prefilter on indexed and
unindexed fragments alike, and appended rows are tail until the next
optimize. Phase 1 adds a test that proves both, since this is where ANN
systems silently return stale hits.

### API surface

No new S3 Vectors API parameter is needed to reach AWS parity. Two optional
items for later: accept and echo AWS's `indexMode`/`queryMode` (both behave
as `ENHANCED` here, because filters are always applied first), and a
non-AWS per-request `nprobes` for the perf suite and the CLI, carried in the
engine's `QueryVectorsRequest` (the Enterprise `VectorQuery` already has the
field).

### Enterprise compatibility requirements

These hold for every phase, and the mono s3vectors legs (go/rust ×
single/distributed) are the gate:

- Builds produce deltas with fragment bitmaps; merges keep them. A test in
  `tests/index.rs` asserts `coverage_known` after build, append and merge.
- Index names never mix within a shard set; the router and `plan.rs` group by
  name, and `with_index_segments` rejects mixed names.
- Segment count per index name stays under `MAX_DELTAS`: at the limit
  `optimize_policy(deltas)` passes `OptimizeOptions::merge(deltas)`, where
  `deltas` is the name's delta count, so every delta of that name folds
  into one with the new rows; Lance's default options may append another
  delta instead of merging. A test runs repeated append-and-optimize cycles
  and checks the actual segment counts.
- Coverage is measured as the union over an index's names. After a sharded
  build each name misses the other shards' fragments, so a per-name count
  would extend every shard over the whole table and duplicate the index.
  An optimize extends one name only, the one with the fewest deltas; a test
  checks that two disjoint shards stay disjoint through it.
- `resident_bytes` is the index files' size and `tail_bytes` the uncovered
  fragments' size; the controller's `max_scan_mb` and memory sizing need no
  change for IVF_FLAT, and PQ only makes slots smaller.
- A merge replaces segment UUIDs, which is a new placement epoch. The
  existing make-before-break (target groups warm with
  `prewarm_index_segments` while serving groups keep answering) covers it;
  phase 5 measures how long a merged segment takes to warm and sets
  expectations for `warmup_timeout`.
- Workers read index files the way they read data files, directly from the
  filers and volume servers, and the files are write-once,
  so the read cache and the warm path apply unchanged.
- Query semantics are identical in both modes: each engine applies the same
  probe policy to the segments it owns, or the count the router passes in
  `VectorQuery.nprobes`, plus a flat pass over its tail fragments; the
  gateway merges the partial top-k lists. Each delta is probed on its own
  centroids, so the merged answer is never worse than one engine's.
- The open-source `weed-vector` stays the writer and the fallback engine, and
  it serves ANN through the same `query::ann` the workers call.

## Phases

### Phase 0 — Measure before deciding (small)

- `weed-vector-cli index build --partitions N --name ..` on a local table,
  reusing the code paths the tests already use, so builds can be tried
  without the worker.
- Perf suite: `PERF_INDEX=1` builds the index after ingest and reports exact
  versus ANN latency and recall@10 at 50k, 200k and 1M × 384, unfiltered and
  with the 10% filter, for a grid of partitions and `nprobes`.
- Verify Lance behaviour we depend on: deletions excluded under ANN;
  compaction (`lance_compact`) keeps bitmaps and coverage; `optimize_indices`
  with `append` versus merge versus `retrain`.
- Exit: chosen defaults for `min_rows`, partitions and probes with numbers
  behind them, and a written answer to each verification item.

### Phase 1 — Engine query path (small)

- Probe policy when an index exists: `minimum_nprobes` is the per-query
  count from the Probes rule above, taken from the largest delta;
  `maximum_nprobes` is four times it, which Lance uses only under a
  prefilter.
- `nprobes` in `QueryVectorsRequest` for tests and the CLI; zero means the
  policy.
- Per-query log line and counters: segments searched, tail rows scanned,
  index hit or not. Without these, recall regressions are invisible.
- Tests in `crates/core`: ANN versus flat recall on a 50k synthetic table;
  delete and upsert correctness under ANN; filtered query with a selective
  filter still returns k results.

### Phase 2 — Build job in the maintenance worker (medium)

- `lance_vector_index` job: detection, execution, settings form (`min_rows`,
  index type, partition rule), metrics, admin observation of coverage.
- Unit tests after the pattern of the other jobs; an integration test in
  `test/s3vectors` that ingests above the threshold, runs the worker, and
  asserts coverage through the CLI and recall ≥ 0.9 through the API.
- Exit: a fresh cluster with the job enabled indexes a 100k-vector table on
  its own, and the small-table tests stay exact.

### Phase 3 — Keep it covering (small)

- Relative budget and `retrain` policy in `lance_optimize_indices` for vector
  tables. The retrain rebuilds through the builder with partitions for the
  current row count; Lance's `OptimizeOptions::retrain()` does not size them.
- Union-based coverage over an index's names, extension scoped to one name,
  and `optimize_policy` with an explicit merge of all deltas at
  `MAX_DELTAS`, shared with the Enterprise builds in the worker crate.
- Tests: append and optimize nine times and check the delta counts climb to
  the limit and fold; two names on disjoint fragments stay disjoint through
  an optimize; the end-to-end test runs the job after a tenth of the index
  is appended and again after the table has grown fivefold.

### Phase 4 — Gateway, docs and gates (small)

- Optional `indexMode`/`queryMode` acceptance; `GetIndex` echoes the mode.
- Wiki and site docs: "Queries are exact" becomes "exact below the threshold,
  ANN above it, with the tail exact", with the recall and latency measured
  in phase 0. The performance page gets an ANN column.
- CI: a `test/s3vectors` leg with the worker and a low threshold, gated on
  recall.

### Phase 5 — Enterprise distributed builds and placement (medium)

- Drive `plan_index` / `build_index_task` from the worker job when engines
  are registered; replace the Enterprise copy of `optimize_policy` with the
  shared one, which merges at the limit.
- Measure four engines on 1M × 384 with ANN: latency, warm time of a merged
  segment, placement epochs per merge.
- mono s3vectors legs with the index enabled, go and rust volume servers,
  single and distributed, plus the hardened TLS legs.

## Risks

- **Filtered recall.** AWS's weak spot is selective filters. Pre-filtering
  keeps recall but costs probes; `minimum_nprobes`/`maximum_nprobes` is the
  lever, and phase 0 has to show the latency is acceptable at 1M rows.
- **Memory.** IVF_FLAT resident bytes are about the raw vectors: 1M × 384 ×
  4 bytes is 1.5 GB per copy. Enterprise spreads it by `capacity_bytes`; the
  single open-source engine needs that much RAM or PQ.
- **Compaction and coverage.** Settled in phase 0: Lance 10 keeps the
  bitmaps through `compact_files`, and coverage stays complete.
- **Build cost on the worker.** k-means over 1M × 384 is CPU-bound; execution
  concurrency stays at one and the job reports progress so the admin UI shows
  it working rather than hung.
- **Placement churn after merges.** Every merge is a new epoch; too frequent
  merging makes engines re-warm constantly. `MAX_DELTAS = 8` bounds it; the
  phase 5 measurement decides whether to raise it.

## Out of scope

HNSW indexes, GPU builds, per-request recall parameters in the AWS API
surface, and building an index at `CreateIndex` time.
