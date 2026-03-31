# Phase 07

Date: 2026-03-30
Status: complete
Purpose: connect the broader runnable V2 engine stage to a real-system service slice and decide the first product path

## Why This Phase Exists

`Phase 06` completed the broader runnable engine stage:

1. planner/executor/resource contracts are real
2. selected real failure classes are validated through the engine path
3. cross-layer trusted-base / replayable-tail proof path is validated

What still does not exist is a real-system slice where the engine runs inside actual service boundaries with real control/storage surroundings.

So `Phase 07` exists to answer:

1. how the engine runs as a real subsystem
2. what the first product path should be
3. what integration risks remain before pre-production hardening

## Phase Goal

Establish a real-system integration slice for the V2 engine and make the first product-path decision without reopening protocol shape.

## Scope

### In scope

1. service-slice integration around `sw-block/engine/replication/`
2. real control-plane / lifecycle entry path into the engine
3. real storage-side adapter hookup into existing system boundaries
4. selected real-system failure replay and diagnosis
5. explicit product-path decision framing

### Out of scope

1. broad performance optimization
2. Smart WAL expansion
3. full V1 replacement rollout
4. broad backend redesign
5. production rollout itself

## Phase 07 Items

### P0: Service-Slice Plan

1. define the first real-system service slice that will host the engine
2. define adapter/module boundaries at the service boundary
3. choose the concrete integration path to exercise first
4. identify which current adapters are still mock/test-only and must be replaced first
5. make the first-slice identity/epoch mapping explicit
6. treat `blockvol` as execution backend only, not recovery-policy owner

Status:

- delivered
- planning artifact:
  - `sw-block/design/phase-07-service-slice-plan.md`
- implementation slice proposal:
  - engine core: `sw-block/engine/replication/`
  - bridge adapters: `sw-block/bridge/blockvol/`
  - real blockvol integration target: `weed/storage/blockvol/v2bridge/` (`P1`)
- adapter replacement order:
  - `control_adapter.go` (`P0`) done
  - `storage_adapter.go` (`P0`) done
  - `executor_bridge.go` (`P1`) deferred
  - `observe_adapter.go` (`P1`) deferred
- first-slice identity mapping is explicit:
  - `ReplicaID = <volume-name>/<server-id>`
  - not derived from any address field
- engine / blockvol boundary is explicit:
  - bridge maps intent and state
  - `blockvol` executes I/O
  - `blockvol` does not own recovery policy
- service-slice validation gaps called out for `P1`:
  - real blockvol field mapping
  - real pin/release lifecycle against reclaim/GC
  - assignment timing vs engine session lifecycle
  - executor bridge into real WAL/snapshot work

### P1: Real Entry-Path Integration

1. connect real control/lifecycle events into the engine entry path
2. connect real storage/base/recoverability signals into the engine adapters
3. preserve accepted engine authority/execution/recoverability contracts

Status:

- accepted
- real integration now established for:
  - reader via `weed/storage/blockvol/v2bridge/reader.go`
  - pinner via `weed/storage/blockvol/v2bridge/pinner.go`
  - catch-up executor path via `weed/storage/blockvol/v2bridge/executor.go`
- direct real-adapter tests now exist in:
  - `weed/storage/blockvol/v2bridge/bridge_test.go`
- accepted scope is explicit:
  - real reader
  - real retention hold / release
  - real WAL catch-up scan path
  - direct real bridge evidence for the integrated path
- still deferred:
  - `TransferSnapshot`
  - `TransferFullBase`
  - `TruncateWAL`
  - control intent from confirmed failover / master-side integration
- carry-forward limitation:
  - under interim `CommittedLSN = CheckpointLSN`, this slice proves a real bridge path, not general post-checkpoint catch-up viability
  - post-checkpoint catch-up semantics therefore remain narrower than final V2 intent and do not represent final V2 commit semantics

### P2: Real-System Failure Replay

1. replay selected real failure classes against the integrated service slice
2. confirm diagnosability from logs/status
3. identify any remaining mismatch between engine-stage assumptions and real system behavior

Status:

- accepted
- real service-path replay now accepted for:
  - changed-address restart
  - stale epoch / stale session invalidation
  - unrecoverable-gap / needs-rebuild replay
  - explicit post-checkpoint boundary replay under the interim model
- accepted with scoped limitation:
  - real `v2bridge` WAL-scan execution is proven
  - full integrated engine-driven catch-up semantics are not overclaimed under interim `CommittedLSN = CheckpointLSN`
- control-plane delivery remains simulated via direct `AssignmentIntent` construction
- carry-forward remains explicit:
  - post-checkpoint catch-up semantics are still narrower than final V2 intent

### P3: Product-Path Decision

1. choose the first product path for V2
2. define what remains before pre-production hardening
3. record what is still intentionally deferred

Status:

- accepted
- first product path chosen:
  - `RF=2`
  - `sync_all`
  - existing master / volume-server heartbeat path
  - V2 engine owns recovery policy
  - `v2bridge` provides real storage truth
- proposal is evidence-grounded and explicitly bounded by accepted `P0/P1/P2` evidence
- pre-hardening prerequisites are explicit:
  - real master control delivery
  - full integrated engine -> executor -> `v2bridge` catch-up chain
  - separation of committed truth from checkpoint truth
  - rebuild execution (`snapshot` / `full-base` / `truncation`)
  - pinner / flusher behavior under concurrent load
- intentionally deferred:
  - `RF>2`
  - Smart WAL optimizations
  - `best_effort` background recovery
  - performance tuning
  - full V1 replacement
- non-claims remain explicit:
  - not production-ready
  - no end-to-end rebuild proof yet
  - no general post-checkpoint catch-up proof
  - no real master heartbeat/control delivery proof yet
  - no full integrated engine -> executor -> `v2bridge` catch-up proof yet

## Guardrails

### Guardrail 1: Do not re-import V1 structure as the design owner

Use `weed/storage/block*` and `learn/projects/sw-block/` as constraints and validation sources, not as the architecture template.

### Guardrail 2: Keep catch-up narrow and rebuild explicit

Do not use integration work as an excuse to widen catch-up semantics or blur rebuild as the formal recovery path.

### Guardrail 3: Prefer real entry paths over test-only wrappers

The integrated slice should exercise real service boundaries, not only internal engine helpers.

### Guardrail 4: Observability must explain causality

Integrated logs/status must explain:

1. why rebuild was required
2. why proof was rejected
3. why execution was cancelled or invalidated
4. why a product-path integration failed

### Guardrail 5: Stable identity must not collapse back to address shape

For the first slice, `ReplicaID` must be derived from master/block-registry identity, not current endpoint addresses.

### Guardrail 6: `blockvol` executes I/O but does not own recovery policy

The service bridge may translate engine decisions into concrete blockvol actions, but it must not re-decide:

1. zero-gap / catch-up / rebuild
2. trusted-base validity
3. replayable-tail sufficiency
4. rebuild fallback requirement

## Exit Criteria

Phase 07 is done when:

1. one real-system service slice is integrated with the engine
2. selected real-system failure classes are replayed through that slice
3. diagnosability is sufficient for service-slice debugging
4. the first product path is explicitly chosen
5. the remaining work to pre-production hardening is clear

## Assignment For `sw`

Next tasks move to `Phase 08`.

## Assignment For `tester`

Next tasks move to `Phase 08`.
