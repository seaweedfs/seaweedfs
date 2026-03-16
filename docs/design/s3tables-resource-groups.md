# S3 Tables Resource Groups Design

## Problem

As the maintenance worker gains more rewrite operations, all work currently competes inside the same execution pool. That is manageable while only a small number of tables are active, but it will become noisy once metadata-only work, light rewrites, and heavy rewrites contend for the same CPU, memory, and IO budget.

SeaweedFS needs a local scheduling model that can isolate maintenance classes before introducing a larger control plane.

## Goals

- Add worker-local resource groups by operation class.
- Bound concurrency and byte budgets per group.
- Prevent heavy rewrite jobs from starving lighter metadata maintenance.
- Keep the design compatible with a single-process worker plugin.

## Non-Goals

- Cluster-wide centralized scheduling in phase 1.
- Cost-perfect fairness across all tables.
- Preemption of already-running maintenance jobs.

## Resource Group Model

Phase 1 groups:

- `metadata_only`
- `rewrite_light`
- `rewrite_heavy`

Suggested mapping:

- `expire_snapshots` -> `metadata_only`
- `rewrite_manifests` -> `rewrite_light`
- `compact` -> `rewrite_heavy`
- `rewrite_position_delete_files` -> `rewrite_light` or `rewrite_heavy` depending on planned bytes
- `remove_orphans` stays separate because it is listing-heavy rather than rewrite-heavy

The scheduler should classify an operation before execution using planner outputs and configured limits.

## Configuration

Add worker config for each group:

- max concurrency
- max in-flight bytes
- optional max tables in progress

Add a default class mapping that operators can override only if they explicitly opt in. That avoids unreviewed scheduling changes across deployments.

## Scheduling Rules

For each queued maintenance task:

1. compute its resource class
2. estimate planned bytes and file counts
3. check whether the target group has capacity
4. dispatch if both concurrency and byte budget allow it
5. otherwise leave the task queued and retry later

The scheduler should prefer simple admission control over complex fairness in phase 1.

## Queue Ordering

Within each group, order work by:

1. oldest eligible task
2. lower estimated bytes first when age is similar
3. lower retry count first

This improves throughput while still preventing starvation of long-waiting tasks.

## Admission Estimates

Admission control depends on planned cost estimates.

Phase 1 estimates should include:

- input bytes
- input file count
- partitions touched

If the planner cannot estimate bytes confidently, it should classify the task conservatively into the heavier group.

## Failure Handling

Group accounting must be released on:

- success
- planner rejection
- execution failure
- context cancellation

The scheduler should never permanently leak in-flight byte reservations because that would deadlock future maintenance.

## Observability

Add metrics for:

- queued tasks per resource group
- running tasks per resource group
- rejected admissions due to concurrency cap
- rejected admissions due to byte cap
- average wait time per resource group

Operator-facing results should show when a task was delayed for quota reasons instead of skipped for lack of eligible work.

## Rollout Plan

Phase 1 should ship with conservative defaults:

- small `metadata_only` concurrency greater than `1`
- low `rewrite_heavy` concurrency
- byte caps sized below the worker’s expected memory envelope

The initial implementation should also allow the entire feature to be disabled so deployments can fall back to the current global execution model.

## Test Plan

Coverage should include:

- metadata jobs continue making progress while heavy rewrites are queued
- byte-cap admission blocks a heavy job until capacity is released
- task accounting is released on failure and cancellation
- conservative classification when planned bytes are unknown
- configurable overrides for group limits

## Implementation Plan

1. Define resource-group config and defaults.
2. Add planner-side cost estimates needed for admission control.
3. Introduce a worker-local scheduler with per-group queues and reservations.
4. Wire metrics and logging into queue admission and release paths.
5. Add concurrency tests with fake planners and executors.

## Follow-On Work

Future phases can add:

- tenant-aware quotas
- cluster-level coordination
- dynamic reclassification from observed runtime cost

## Open Questions

- Should `remove_orphans` remain fully outside the resource-group model in phase 1 because of its listing-heavy profile, or should it get a dedicated scan group immediately?
- Do we want strict FIFO within each group for predictability, or size-aware ordering by default for better throughput?
