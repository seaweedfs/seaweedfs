# S3 Tables Predicate-Scoped Maintenance Design

## Problem

SeaweedFS maintenance currently operates at whole-table scope. That matches the simplest maintenance flow, but it is too coarse for large partitioned tables where operators only want to compact or rewrite a small slice of the table.

Iceberg and Delta both expose partition-scoped rewrite operations. SeaweedFS needs a similarly narrow `where` filter so maintenance can target a specific partition set without surprising row-level behavior.

## Goals

- Add a safe `where` filter for rewrite-style maintenance operations.
- Limit phase 1 to partition predicates that can be evaluated without scanning row data.
- Make planner decisions conservative and require executor-side revalidation.
- Keep unsupported operations and predicates explicit errors instead of silent no-ops.

## Non-Goals

- Supporting arbitrary row predicates in phase 1.
- Reusing the filter for snapshot expiration or orphan cleanup.
- Building a full SQL parser.

## Supported Operations

Phase 1 should allow `where` only for:

- `compact`
- `rewrite_manifests`
- `rewrite_position_delete_files`

The worker should reject `where` for:

- `expire_snapshots`
- `remove_orphans`

## Predicate Surface

Phase 1 predicates should be partition-only and intentionally small:

- `field = literal`
- `field IN (literal, ...)`
- conjunctions with `AND`

Unsupported expressions should fail validation:

- `OR`
- comparison ranges
- functions
- nested expressions
- references to non-partition fields

This keeps the implementation deterministic and makes planner behavior easy to reason about.

## Configuration

Add a new config field:

- `where`

Validation rules:

- empty string means no filter
- `where` is invalid for unpartitioned tables
- all referenced fields must exist in every active partition spec touched by the current snapshot

## Parsing and Normalization

The parser should produce a simple normalized structure:

- field name
- operator
- literal set

Normalization should:

- fold duplicate conjuncts
- sort `IN` literal lists for deterministic planning
- preserve literal types according to the partition field source type

Implementation should prefer a small local parser over a broad SQL dependency.

## Planner Rules

The planner evaluates the normalized predicate against manifest partition summaries first.

Planner outcomes:

- include a manifest when summaries prove it may contain matching partitions
- exclude a manifest when summaries prove it cannot match
- keep a manifest when summaries are coarse or incomplete

Summary-based filtering is only a first pass. The executor must still validate the exact partition tuple before rewriting a file.

## Executor Revalidation

Before a file enters a rewrite bin, the executor checks the concrete partition tuple against the normalized predicate.

If the executor finds a mismatch:

- skip the file
- increment a `where_revalidation_skips` metric
- continue the operation

This second gate protects correctness when manifest summaries are conservative.

## Multi-Spec Tables

Partition predicates must remain stable across spec evolution.

Phase 1 rules:

- match fields by logical partition field name, not by ordinal position
- reject filters when the referenced field is absent from any candidate spec
- allow filters when the same logical field exists across specs with compatible literal coercion

This keeps planner and executor behavior consistent on spec-evolved tables.

## Error Handling

Return validation errors for:

- malformed predicate syntax
- unsupported operators
- non-partition field references
- literal type mismatch
- unpartitioned tables with `where`

These should fail before any maintenance planning begins.

## Metrics and Reporting

Add metrics that make filtered maintenance auditable:

- `where_manifests_considered`
- `where_manifests_matched`
- `where_files_matched`
- `where_revalidation_skips`

Result strings should state when a filter produced no eligible candidates so operators can distinguish a successful no-op from an execution failure.

## Test Plan

Coverage should include:

- single-partition compaction using `field = literal`
- multi-partition compaction using `IN`
- mixed-spec tables with a shared partition field name
- invalid predicates and type mismatches
- executor-side skip when manifest summaries over-approximate a partition
- rejection on unpartitioned tables

## Implementation Plan

1. Add config plumbing and operation validation for `where`.
2. Implement a small parser and normalized predicate representation.
3. Add planner-side manifest summary evaluation.
4. Add executor-side exact partition tuple revalidation.
5. Surface metrics and result-string context.
6. Add focused planner and end-to-end tests for each supported operation.

## Open Questions

- Should phase 1 allow equality predicates on transformed partition fields only when the literal can be normalized into transform space, or should those cases wait for phase 2?
- Do we want a hard limit on the size of `IN` lists to avoid unexpectedly broad plans from very large operator inputs?
