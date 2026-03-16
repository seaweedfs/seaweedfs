# S3 Tables Planning Index Design

## Problem

The maintenance worker currently discovers work by repeatedly scanning table metadata, manifests, and storage paths. That is acceptable for small deployments, but it becomes increasingly expensive as the number of tables, manifests, and object listings grows.

SeaweedFS does not need a full Hudi-style metadata table to improve this. A compact per-table planning index can eliminate most obviously unnecessary scans while preserving the current worker model.

## Goals

- Add a lightweight maintenance index per table.
- Use the index to skip deep planning work when nothing relevant changed.
- Keep the index advisory rather than authoritative so corruption or staleness cannot break maintenance correctness.
- Minimize write amplification by updating the index opportunistically after successful maintenance or metadata refreshes.

## Non-Goals

- Building a globally queryable metadata service.
- Replacing the source-of-truth Iceberg metadata files.
- Making maintenance depend on the index being present.

## Index Scope

The index should be stored per table and contain only maintenance-relevant summaries:

- latest table metadata version
- current snapshot ID
- data manifest count
- delete manifest count
- small-file candidate count estimate
- delete-file candidate count estimate
- last successful orphan scan watermark
- last successful maintenance timestamps by operation

The index is a cache, not a source of truth. Any mismatch between the index and table metadata must fall back to normal planning.

## Storage Model

Store the index as a compact JSON blob in the table’s existing maintenance metadata area.

Recommended location:

- an internal extended attribute on the table entry when the payload remains small
- otherwise a dedicated file under `metadata/maintenance-index.json`

Phase 1 should prefer the dedicated file to avoid xattr size creep and to keep schema evolution of the index straightforward.

## Read Path

Planning flow:

1. Load current table metadata.
2. Read the planning index if present.
3. Compare index version and snapshot markers against current metadata.
4. Use the index to decide whether expensive scans can be skipped.
5. If any mismatch or parse failure occurs, discard the index for this run and perform normal planning.

The worker must never trust an index built against a different metadata version.

## Skip Decisions

Examples of fast-path skips:

- no snapshot change since the last successful manifest rewrite planning pass
- no change in metadata version and no delete-file candidate estimate above threshold
- orphan scan watermark still inside the configured safety window

The index should only enable safe skips. It should not be used to synthesize positive work without re-checking current metadata.

## Update Path

Index writes should happen:

- after a successful maintenance commit
- after a no-op planning pass that fully inspected current metadata
- after a successful orphan scan

Index updates should be best-effort:

- maintenance success must not fail solely because the index write fails
- the index write should use the same metadata version marker so stale updates are rejected

## Schema Versioning

The index needs its own schema version field.

Rules:

- unknown schema version means ignore the index
- additive fields are allowed
- incompatible schema changes require a version bump

This keeps rollout and future extensions cheap.

## Operation Integration

The index should accelerate these operations first:

- `compact`
- `rewrite_position_delete_files`
- `rewrite_manifests`
- `remove_orphans`

Snapshot expiration benefits less from indexing because it already depends on current metadata traversal rather than deep data-path listing.

## Failure and Recovery

The index must be safe to lose, corrupt, or ignore.

If any of these happen:

- parse failure
- version mismatch
- stale metadata marker
- missing file

the worker logs a debug reason, falls back to full planning, and optionally rewrites the index after the run.

## Metrics and Reporting

Add visibility into index effectiveness:

- `planning_index_hits`
- `planning_index_misses`
- `planning_index_stale`
- `planning_index_rebuilds`
- `planning_scans_skipped`

This is important because the first implementation is only worthwhile if operators can confirm it is actually reducing expensive work.

## Test Plan

Coverage should include:

- index miss on first run
- index hit with safe skip
- stale index rejected after metadata version change
- corrupt index ignored and rebuilt
- best-effort index write failure that does not fail maintenance
- orphan-scan watermark behavior around the safety window

## Implementation Plan

1. Define the index schema and versioning rules.
2. Add read/write helpers with best-effort semantics.
3. Integrate safe skip checks into compaction, delete rewrite, and manifest rewrite planning.
4. Add orphan-scan watermark support.
5. Surface hit/miss metrics and debug logging.
6. Add unit tests and targeted maintenance integration coverage.

## Follow-On Work

Future phases can extend the index with:

- partition-level hot-spot summaries
- estimated bytes by operation class
- worker-local scheduling hints

## Open Questions

- Should the index write live in the same metadata CAS path as maintenance commits, or should it remain a separate best-effort side write with its own stale-write rejection?
- Do we want to persist per-partition summaries in phase 1, or is table-level summary data enough to justify the feature?
