# S3 Tables Delete-File Rewrite Design

## Problem

SeaweedFS can already apply Iceberg delete files while compacting data files, but it does not have a dedicated maintenance path for rewriting delete files themselves. That leaves tables with many small position-delete files in a degraded state even when data-file compaction is not otherwise warranted.

Iceberg exposes this as `rewrite_position_delete_files`. SeaweedFS needs the same maintenance primitive so operators can reduce delete-file fanout without forcing data rewrites.

## Goals

- Add a first-class `rewrite_position_delete_files` worker operation.
- Rewrite many small position-delete files into fewer larger files.
- Preserve snapshot semantics by committing new delete manifests and removing old ones in a single metadata update.
- Reuse the existing maintenance planner, execution loop, metrics, and retry model where that still fits.

## Non-Goals

- Rewriting data files as part of delete rewrite.
- Supporting arbitrary row-level filtering during delete rewrite.
- Rewriting equality-delete files in phase 1 when their schemas differ.
- Introducing upstream `iceberg-go` changes as a hard prerequisite.

## User-Facing API

Add a new operation name:

- `rewrite_position_delete_files`

Add worker config fields:

- `delete_target_file_size_mb`
- `delete_min_input_files`
- `delete_max_file_group_size_mb`
- `delete_max_output_files`

Phase 1 defaults should match the current data-compaction posture:

- target size defaults to the data-compaction target when unset
- minimum input files defaults to `2`
- max file-group size defaults to a bounded multiple of target size

## Planner

The planner should read the current manifest list and extract delete manifests only. Each delete entry is mapped into a rewrite group keyed by:

- partition spec ID
- exact partition tuple
- referenced data-file path for position deletes

Grouping by referenced data file keeps positional semantics simple and avoids mixing unrelated row-position streams.

The planner schedules a group when either of these is true:

- file count is at least `delete_min_input_files`
- aggregate bytes are below the target size and the file count exceeds a small-file threshold

The planner skips a group when:

- it contains only one delete file
- the aggregate input size exceeds `delete_max_file_group_size_mb`
- any file is not a position-delete file

## Execution Model

For each planned group:

1. Load all input delete parquet files.
2. Decode delete rows into a shared stream ordered by `(file_path, pos)`.
3. Write new larger delete parquet files under `data/`.
4. Write replacement delete manifests under `metadata/`.
5. Commit by atomically swapping old delete manifests for new ones in table metadata.

Delete rewrite should remain partition-local and group-local. It should not require a global shuffle or a full table scan.

## Equality Deletes

Phase 1 behavior:

- position deletes are fully supported
- equality deletes are left untouched unless every input file in the candidate group has the same schema fingerprint and equality-field IDs

If equality-delete rewrite is skipped, the operation should still succeed and report that the files were left unchanged.

## Manifest Writing

`iceberg-go` currently assumes data-manifest content in the existing writer helper. SeaweedFS should add a local delete-manifest writer with explicit manifest content selection rather than blocking on an upstream dependency.

Local helper requirements:

- set manifest content to deletes
- preserve sequence numbers and snapshot metadata required by the current table format version
- emit delete-file entries with correct lower-level metrics when available

Once stabilized, this helper can be proposed upstream, but the SeaweedFS implementation should not wait on that work.

## Metadata Commit

Delete rewrite should use the same metadata update protocol as other maintenance operations:

- read current metadata and snapshot state
- build replacement manifests against the current snapshot
- write new delete files and manifests
- commit metadata with the existing versioned compare-and-swap path
- on commit conflict, clean up uncommitted outputs and retry from a fresh metadata read

The operation should not delete input files until the new metadata commit succeeds.

## Failure Handling

- Planner-time validation failures are surfaced as configuration or table-shape errors.
- Executor failures before commit leave input files intact and best-effort delete temporary outputs.
- Commit conflicts trigger the same bounded retry loop as existing maintenance operations.
- Cleanup failures after a conflict are reported in the result string and metrics but do not mask the original commit outcome.

## Metrics and Reporting

Add per-operation metrics:

- `delete_files_rewritten`
- `delete_files_written`
- `delete_bytes_rewritten`
- `delete_groups_planned`
- `delete_groups_completed`
- `delete_groups_skipped`

The result string should include both file counts and byte counts so operators can distinguish a no-op run from a low-impact rewrite.

## Test Plan

Unit and integration coverage should include:

- rewrite several position-delete files for the same target data file
- skip a single-file group
- mixed data and delete manifests in the same snapshot
- conflict-and-retry cleanup for staged outputs
- equality-delete groups with matching and mismatched schema fingerprints
- multi-spec tables where delete groups stay isolated by spec ID

## Implementation Plan

1. Extend config and operation parsing for `rewrite_position_delete_files`.
2. Add delete-file grouping logic to the planner.
3. Add parquet read/write helpers for position-delete rewrite.
4. Add a local delete-manifest writer.
5. Wire commit, cleanup, metrics, and progress reporting into the handler.
6. Add end-to-end maintenance tests under `test/s3tables/maintenance`.

## Open Questions

- Should equality-delete rewrite stay hidden behind a second config flag in phase 1, or is silent skip behavior sufficient?
- Do we want a hard cap on the number of referenced data files touched in a single run, in addition to byte-based caps?
