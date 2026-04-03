# CP13-9 Mode Normalization Under V2 Constraints

Date: 2026-04-03

Status: accepted

## Current Interpretation Rule

Before an explicit `V2 core` exists as a real code structure and live
event/command owner, current integrated tests are interpreted as:

1. validation of current `V1` runtime behavior under `V2` constraints
2. not proof that a completed `V2 runtime` already exists

`CP13-9` keeps that rule explicit.
It does not try to rewrite current constrained-runtime evidence into a claim that
the pure `V2 core` has already landed.

## Bounded Contract

`CP13-9` accepts one bounded thing:

1. explicit mode/publication normalization for the accepted chosen path

Scope remains bounded to:

1. `RF=2`
2. `sync_all`
3. current master / volume-server heartbeat path
4. `blockvol` as execution backend

It does not accept:

1. `Phase 14` pure `V2 core` extraction
2. broad launch approval
3. broad transport/product expansion

## Why This Checkpoint Exists

`CP13-8` and `CP13-8A` now prove:

1. one bounded real-workload package passes on the chosen path
2. assignment/readiness/publication closure is explicit enough for that path

What still needs freezing is the external mode meaning of the current path.

In particular:

1. a fresh volume before the first real replicated durability proof is not yet the
   same as replicated-healthy
2. `degraded` and `NeedsRebuild` are not interchangeable
3. lookup / heartbeat / tester / debug surfaces should not silently use different
   meanings of "healthy"

## Recommended Mode Contract

The semantic split below is the first-cut target.
Exact mode names may change, but the distinctions should remain explicit.

| Mode | Meaning | What it is allowed to claim |
|------|---------|-----------------------------|
| `allocated_only` | volume exists locally but runtime closure has not begun | existence only; not ready, not healthy |
| `bootstrap_pending` | assignment exists and the pair may need the first real replicated write/connect proof | not replicated-healthy; may be publishable only under bounded non-healthy wording |
| `replica_ready` | receiver / readiness closure exists on replica side | replica wiring is ready; not by itself proof of end-to-end healthy publication |
| `publish_healthy` | chosen-path publication conditions are closed | allowed to surface healthy publication on bounded chosen path |
| `degraded` | the bounded healthy path is not currently satisfied, but rebuild is not yet required | fail-closed for healthy replication claims |
| `needs_rebuild` | unrecoverable gap or equivalent fail-closed state | explicitly not healthy; normal replication path blocked |

## First-Write Bootstrap Rule

`CP13-9` should freeze this rule explicitly:

1. a freshly created `RF=2 sync_all` volume before the first real replicated write
   or equivalent bounded durability proof must not be overclaimed as
   replicated-healthy
2. if the current runtime needs the first replicated write to establish the first
   real sync/connect proof, that is a mode-policy fact that must be surfaced
   explicitly rather than hidden inside ambiguous degraded/healthy output

## Proof Shape

`CP13-9` should close with a bounded proof package:

| Proof | What it must show |
|-------|-------------------|
| Interpretation proof | current integrated evidence is described as constrained `V1` under `V2` constraints |
| Bootstrap proof | fresh volume before first replicated write is surfaced as bootstrap-pending or equivalent bounded non-healthy mode |
| Surface-consistency proof | lookup / heartbeat / tester / debug surfaces use one bounded mode meaning |
| Fail-closed proof | `publish_healthy`, `degraded`, and `needs_rebuild` remain distinct and do not overclaim health |

## Accepted Validation Summary

Tester verdict: `ACCEPT`

| Proof | Claim | Evidence |
|------|-------|----------|
| `AllocatedOnly` | `RF=1` maps to `allocated_only` | focused mode test |
| `BootstrapPending` (`Replicas` empty) | `RF=2` before replica set closure maps to `bootstrap_pending` | focused mode test |
| `BootstrapPending` (replica not ready) | `RF=2` with replica not ready maps to `bootstrap_pending` | focused mode test |
| `PublishHealthy` | ready + not transport degraded maps to `publish_healthy` | focused mode test |
| `Degraded` | transport degraded maps to `degraded` | focused mode test |
| `NeedsRebuild` | rebuilding role maps to `needs_rebuild` | focused mode test |
| `SurfaceConsistency` | mode / ready / degraded meaning stays aligned across transitions | focused transition checks |
| `InterpretationRule` | current integrated tests are constrained `V1` under `V2` constraints | explicit wording in contract + design docs |
| `NoOverclaim` | checkpoint does not claim pure `V2 core`, launch, or broad transport expansion | explicit boundedness wording |

Minor note kept bounded:

1. `assert_block_field` in the testrunner does not yet expose `volume_mode` as a first-class assert case
2. this does not block checkpoint acceptance because the bounded unit and API-surface proofs are already direct

## Relation to Earlier Checkpoints

| Prior checkpoint | What CP13-9 reuses |
|------------------|--------------------|
| `CP13-1..7` | accepted replication contract and fail-closed semantics |
| `CP13-8` | bounded real-workload pass on the chosen path |
| `CP13-8A` | assignment/readiness/publication closure |

`CP13-9` is therefore about policy/meaning on top of the corrected constrained
runtime, not about redoing replication correctness or workload validation.

## What CP13-9 Does NOT Close

- Pure `V2 core` extraction (`Phase 14`)
- Broad product launch approval
- Broad transport matrix claims
- Broad product-surface expansion beyond the chosen path
