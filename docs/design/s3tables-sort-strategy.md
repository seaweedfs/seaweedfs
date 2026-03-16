# S3 Tables Sort-Aware Rewrite Design

## Problem

SeaweedFS currently rewrites data files with a bin-pack strategy only. That reduces small-file pressure, but it does not improve data locality inside each rewritten file.

Once Iceberg parity gaps are closed, the next high-value maintenance feature is a sort-aware rewrite mode that can improve pruning, compression, and scan efficiency without introducing a separate clustering service.

## Goals

- Add a `sort` rewrite strategy alongside the existing bin-pack behavior.
- Keep phase 1 strictly partition-local to avoid global shuffles.
- Reuse the existing compaction pipeline where possible.
- Expose clear safety limits so a sort rewrite cannot silently expand into an unbounded job.

## Non-Goals

- Global table clustering across partition boundaries.
- Delta-style Z-order in phase 1.
- Rewriting delete files as part of the sort design.
- Introducing new table metadata beyond what Iceberg already stores for sort order.

## Strategy Surface

Add a new config field:

- `rewrite_strategy`

Supported values:

- `binpack`
- `sort`

Add optional config for explicit sort keys when the table does not declare a sort order:

- `sort_fields`

Phase 1 should reject `rewrite_strategy=sort` when:

- the table is unpartitioned and the candidate scope is too large for configured rewrite caps
- neither table sort order nor explicit `sort_fields` is present
- any configured sort field cannot be projected from the current schema

## Sort Key Resolution

When strategy is `sort`, key resolution follows this order:

1. table-declared Iceberg sort order
2. explicit `sort_fields` config

If both are present, explicit config should override the table order only when the operator opts in with a separate `sort_override` flag. That prevents accidental divergence from the table’s declared layout contract.

## Planner Behavior

The planner should continue using the existing file binning rules to decide which files are eligible for rewrite. Sort strategy changes how a bin is executed, not how table-wide file eligibility is discovered.

Phase 1 planner additions:

- cap the number of files per bin for sort rewrites
- cap total input bytes for a sort bin
- cap partitions touched per run when combined with `where`

These limits keep sort rewrite bounded and predictable.

## Executor Behavior

For each eligible bin:

1. Read rows from all input files in the bin.
2. Materialize rows into a bounded in-memory or spillable sort buffer.
3. Sort rows by the resolved key list.
4. Write output parquet files using the existing target-size policy.
5. Commit replacement data manifests through the standard metadata CAS path.

Phase 1 should stay partition-local. Each bin is sorted independently, and no rows move between partitions.

## Memory and Spill Strategy

Phase 1 cannot assume that all sort bins fit comfortably in memory.

Design requirements:

- keep the existing bin-size caps lower for `sort` than for `binpack`
- add a temporary spill path when buffered rows exceed a memory threshold
- sort spill runs individually, then merge them during output writing

If spill support is deferred, the phase 1 implementation must use conservative byte caps and reject bins that exceed the safe threshold.

## Schema Evolution

Sort execution must tolerate compatible schema evolution across input files in the same bin.

Phase 1 rules:

- allow additive schema evolution when the writer schema can project missing fields as null
- reject bins with incompatible physical types for the sort keys
- prefer sorting on fields present in all candidate files

This keeps the first implementation aligned with existing compaction constraints while avoiding avoidable false positives.

## Metrics and Reporting

Add metrics that make sort rewrites operationally visible:

- `rewrite_strategy`
- `sort_bins_planned`
- `sort_bins_completed`
- `sort_input_bytes`
- `sort_output_bytes`
- `sort_spill_bytes`

The result string should include whether the run used the table sort order or explicit fields.

## Test Plan

Coverage should include:

- table-declared sort order
- explicit `sort_fields`
- invalid sort-field validation
- partition-local sorting with multiple output files
- cap enforcement for oversized bins
- schema evolution with nullable additive columns
- spill or cap fallback behavior, depending on phase 1 implementation choice

## Implementation Plan

1. Add config parsing and validation for `rewrite_strategy`, `sort_fields`, and caps.
2. Resolve sort keys from table metadata or operator config.
3. Extend the compaction executor with a sort path.
4. Add bounded memory behavior, with spill support if phase 1 includes it.
5. Surface strategy-specific metrics and result details.
6. Add focused execution tests and maintenance integration coverage.

## Follow-On Work

Future phases can add:

- Z-order style multidimensional clustering
- global clustering jobs for large unpartitioned tables
- cost-based selection between bin-pack and sort

## Open Questions

- Should spill support be mandatory in the first implementation, or is a conservative cap-only version acceptable if the guardrails are explicit?
- Do we want to allow sort rewrite on unpartitioned tables in phase 1, or should that wait until spill and stronger byte caps are both in place?
