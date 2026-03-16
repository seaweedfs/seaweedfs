# S3 Tables Maintenance Roadmap

## Context

SeaweedFS already ships an Iceberg maintenance worker with support for:

- data-file compaction
- snapshot expiration
- orphan-file removal
- manifest rewriting
- delete-aware compaction

This document captures the comparison against common open source table-maintenance systems and lays out the next execution phases after the current PR stack.

Reference docs used for comparison:

- [Apache Iceberg Spark procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Apache Iceberg Flink table maintenance](https://iceberg.apache.org/docs/nightly/flink-maintenance/)
- [Delta Lake optimizations](https://docs.delta.io/latest/optimizations-oss.html)
- [Delta Lake utility commands](https://docs.delta.io/latest/delta-utility.html)
- [Apache Hudi cleaning](https://hudi.apache.org/docs/cleaning/)
- [Apache Hudi clustering](https://hudi.apache.org/docs/clustering/)
- [Apache Hudi metadata table](https://hudi.apache.org/docs/metadata/)
- [Apache Amoro self-optimizing](https://amoro.apache.org/docs/latest/self-optimizing/)
- [Apache Amoro optimizer groups](https://amoro.apache.org/docs/0.7.1/managing-optimizers/)

## Comparison Matrix

| Capability | Seaweed current + queued PRs | Iceberg OSS baseline | Delta/Hudi/Amoro baseline | Gap |
| --- | --- | --- | --- | --- |
| Atomic metadata commit | Covered by filer preconditions | Required by catalog commit protocol | Required | Closed in current stack |
| Compaction on spec-evolved tables | Covered by planner fix | Supported | Supported | Closed in current stack |
| Ref-aware snapshot expiration | Covered by lifecycle parity PR | Supported | Equivalent retention semantics exist | Closed in current stack |
| Orphan dry-run | Covered by lifecycle parity PR | Supported by Iceberg procedures | Common | Closed in current stack |
| Expired metadata cleanup | Covered by lifecycle parity PR | Supported by Iceberg procedures | Common | Closed in current stack |
| Dedicated delete-file rewrite | Not implemented | `rewrite_position_delete_files` | Similar delete cleanup exists | Open |
| Predicate-scoped maintenance | Not implemented | `where` support in rewrite procedures | Delta `OPTIMIZE ... WHERE` | Open |
| Sort-aware layout rewrite | Not implemented | Iceberg supports sort-strategy rewrites | Delta Z-order, Hudi clustering | Open |
| Fast planning/indexed orphan scan | Not implemented | Externalized in some deployments | Hudi metadata table, Amoro planning | Open |
| Resource groups / quotas | Not implemented | Not core Iceberg | Amoro optimizer groups | Open |

## Current PR Stack

1. `codex/s3tables-maint-conditional-update`
   - add server-side `UpdateEntry` preconditions
   - use them for table metadata commits
2. `codex/s3tables-maint-planner-multispec`
   - stop rejecting mixed-spec manifest lists during compaction planning
3. `codex/s3tables-maint-lifecycle-parity`
   - make expiration ref-aware
   - add `clean_expired_metadata`
   - add `remove_orphans_dry_run`

## Remaining Parity Design

### 1. Dedicated Delete-File Rewrite

Goal: add an Iceberg-style `rewrite_position_delete_files` operation without changing the existing data compaction semantics.

#### API shape

- Add `rewrite_position_delete_files` to `operations`.
- Add worker config:
  - `delete_target_file_size_mb`
  - `delete_min_input_files`
  - optional `delete_max_file_group_size_mb`

#### Planner

- Read current manifest list and collect delete manifests only.
- Group delete files by:
  - partition spec ID
  - partition tuple
  - referenced data file path for position deletes
- Schedule when a group has either:
  - at least `delete_min_input_files`, or
  - aggregate size below the target but spread across many small files

#### Executor

- Read position delete parquet files and concatenate rows into larger delete files.
- Preserve equality delete files in phase 1 unless they share a compatible schema fingerprint.
- Write new delete parquet files under `data/`.
- Write replacement delete manifests under `metadata/`.
- Commit by adding new delete manifests and removing the old ones in one metadata update.

#### Implementation constraint

`iceberg-go` currently hardcodes data-manifest content in `WriteManifest`, so SeaweedFS needs one of:

- a small upstream helper for delete-manifest writing, or
- a local helper mirroring manifest serialization with explicit content selection

Recommended path: add a local helper first, then upstream it.

#### Tests

- rewrite multiple position delete files for the same target data file
- mixed delete-manifest and data-manifest lists
- retry cleanup after commit conflict
- skip behavior when only one delete file exists

### 2. Predicate-Scoped Maintenance

Goal: add a narrow, safe `where` filter for rewrite-style maintenance operations.

#### Scope

Phase 1 should support partition-only predicates:

- `field = literal`
- `field IN (...)`
- conjunctions with `AND`

Do not support arbitrary row predicates in phase 1. That would require full data scans and can surprise operators.

#### API shape

- Add worker config `where`.
- Apply it only to:
  - `compact`
  - `rewrite_manifests`
  - `rewrite_position_delete_files`
- Reject `where` when combined with:
  - `expire_snapshots`
  - `remove_orphans`

#### Planner and executor rules

- Planner evaluates the predicate against manifest partition summaries first.
- Executor re-validates each candidate file against the exact partition tuple.
- If a table is unpartitioned and `where` is set, return a validation error.

#### Tests

- single-partition compaction
- multi-spec tables with compatible partition field names
- invalid predicates
- executor-side revalidation when manifest summaries are coarse

## Extra Feature Design

### 1. Sort-Aware Rewrite

This is the strongest non-parity feature worth adding after the parity gaps.

#### Why it is worth doing

- SeaweedFS already rewrites data files and applies deletes.
- Reordering rows during rewrite can improve scan pruning and compression.
- It gives a Delta/Hudi-style clustering story without introducing a separate service.

#### Design

- Reuse compaction bins.
- Add `rewrite_strategy` with values:
  - `binpack`
  - `sort`
- For `sort`, default to the table sort order when present.
- If no sort order exists, allow an explicit field list in config.

Phase 1 should stay partition-local and avoid global shuffles.

### 2. Indexed Planning

This is the highest-leverage scale feature once table counts grow.

#### Design

- Persist a compact maintenance index per table with:
  - latest metadata version
  - manifest counts
  - delete-file counts
  - last orphan scan watermark
- Update it opportunistically during successful maintenance commits.
- Use it to skip full directory walks when nothing relevant changed.

This is a better first step than a full Hudi-style metadata table.

### 3. Optimizer Groups and Quotas

This is useful when multiple maintenance types start competing for IO.

#### Design

- Introduce worker pools by operation class:
  - metadata-only
  - rewrite-light
  - rewrite-heavy
- Add per-pool concurrency and byte-budget limits.
- Keep scheduling local to the worker plugin before introducing a new control plane.

## Execution Plan

### Phase 0: Land the current stack

1. Merge commit-safety PR.
2. Merge spec-evolved planner PR.
3. Merge lifecycle parity PR.

### Phase 1: Finish Iceberg parity

1. Add `rewrite_position_delete_files`.
2. Add partition-scoped `where` support for rewrite operations.
3. Extend detection summaries and execution metrics for delete-file rewrite.

### Phase 2: Improve layout quality

1. Add sort-aware rewrite strategy.
2. Add rewrite caps by bytes, file count, and partitions touched.
3. Add richer admin reporting for before/after file counts and bytes.

### Phase 3: Improve scale and control

1. Add indexed planning to avoid repeated deep scans.
2. Add resource groups and quotas.
3. Revisit streaming output and schema-evolution support in the compaction engine if table sizes require it.

## Recommended PR Split For Remaining Work

1. `codex/s3tables-maint-delete-rewrite`
   - dedicated delete-file rewrite op
   - delete-manifest writer helper
   - planner and executor tests
2. `codex/s3tables-maint-where-filter`
   - predicate parser
   - planner and executor validation
   - config/UI plumbing
3. `codex/s3tables-maint-sort-strategy`
   - sort-aware rewrite strategy
   - metrics and safety limits
4. `codex/s3tables-maint-planning-index`
   - incremental planning cache
   - scan fast-paths
5. `codex/s3tables-maint-resource-groups`
   - worker pool limits
   - scheduler policy
