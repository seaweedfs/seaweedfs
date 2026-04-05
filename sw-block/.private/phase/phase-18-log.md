# Phase 18 Log

Date: 2026-04-05
Status: complete

## 2026-04-05

### Start of Phase

Created the initial `Phase 18` control document.

Starting point recorded:

1. in-process RF2 failover runtime slice exists
2. `FailoverSession`, in-process driver, and runtime manager exist
3. review-base docs already reflect the current kernel boundary and current
   milestone

Initial execution rule:

1. move by major milestone
2. target `2-3` implementation steps per major milestone
3. update phase, log, decisions, and review-base docs after each major step

Current next step:

1. `M1` seam step for transport/session adapter boundary

### `M1` Adapter-Seam Package

Delivered in this update:

1. explicit failover adapter seam introduced in code:
   - `FailoverEvidenceAdapter`
   - `FailoverTakeoverAdapter`
   - `FailoverTarget`
2. first in-process adapter implementation delivered:
   - `NewInProcessFailoverTarget(...)`
3. `FailoverSession` now uses explicit targets as the primary path
4. failover driver and runtime manager now register/resolve targets as the
   primary path
5. existing healthy/gated runtime failover tests were moved onto the new target
   seam

Tests:

1. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

Current interpretation:

1. the transport/session adapter seam is now real in code
2. the in-process path is still the reference implementation behind that seam
3. the first non-in-process adapter remains the next required slice before `M1`
   can be treated as fully closed

### `M1` Delivered

Delivered in this update:

1. the failover-time query path now crosses a transport/session adapter boundary
2. `PromotionQuery` and `ReplicaSummary` no longer depend on direct orchestrator
   calls to `*Node` as the only implementation path
3. the first transport/session implementation is `InMemoryFailoverEvidenceTransport`
4. the runtime manager now registers nodes behind the evidence transport and
   executes failover through the transport-backed evidence path

Tests:

1. `TestTransportEvidenceAdapter_HealthyFailoverFlow`
2. `TestTransportEvidenceAdapter_GatedFailoverFlow`
3. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

Current interpretation:

1. `M1` is complete as a transport/session-backed failover-time evidence slice
2. this is still a bounded request/response transport implementation, not broad
   network-product proof
3. the next active work should move to `M2`

### `M2` Delivered

Delivered in this update:

1. one runtime-owned active Loop 2 session/controller now exists:
   - `Loop2RuntimeSession`
2. one bounded active runtime snapshot now exists:
   - `Loop2RuntimeSnapshot`
   - `Loop2RuntimeMode`
3. the runtime manager now owns active Loop 2 observation entry points and
   retained snapshots
4. the active Loop 2 slice is driven by bounded replica summaries rather than
   by hidden backend ownership

Tests:

1. `TestLoop2RuntimeSession_KeepUpOnHealthyReplicaSet`
2. `TestInProcessRuntimeManager_ObserveLoop2_CatchingUp`
3. `TestInProcessRuntimeManager_ObserveLoop2_NeedsRebuild`
4. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

Current interpretation:

1. `M2` is complete as the first active Loop 2 runtime slice
2. this is still bounded summary-driven runtime ownership, not full shipper or
   rebuild-task choreography
3. the next active work should move to `M3`

### `M3` Delivered

Delivered in this update:

1. one runtime-owned replicated continuity entry point now exists:
   - `ExecuteReplicatedContinuity(...)`
2. failover and active Loop 2 are now composed into one bounded continuity path
3. the continuity result captures:
   - pre-failover Loop 2 snapshot
   - failover result
   - selected primary
   - readback length
   - data match

Tests:

1. `TestInProcessRuntimeManager_ExecuteReplicatedContinuity_HappyPath`
2. `TestInProcessRuntimeManager_ExecuteReplicatedContinuity_GatedPath`
3. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

Current interpretation:

1. `M3` is complete as one bounded replicated continuity closure on the current
   runtime path
2. this is still a bounded continuity claim on the in-process/runtime-owned
   path, not broad RF2 product continuity proof
3. the next active work should move to `M4`

### `M4` Delivered

Delivered in this update:

1. one bounded RF2-facing runtime/product surface package now exists:
   - `RF2VolumeSurface`
   - `RF2SurfaceMode`
   - `RF2ContinuityStatus`
2. the runtime manager now projects:
   - active Loop 2 snapshot
   - failover snapshot
   - continuity snapshot
   into one compressed outward RF2 surface
3. continuity results are now retained as runtime-owned observable snapshots:
   - `ReplicatedContinuitySnapshot`

Tests:

1. `TestInProcessRuntimeManager_RF2VolumeSurface_HealthyPackage`
2. `TestInProcessRuntimeManager_RF2VolumeSurface_GatedPackage`
3. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

Current interpretation:

1. `M4` is complete as the first bounded RF2-facing runtime/product surface on
   the new runtime
2. the surface remains a compressed projection of runtime-owned truth rather than
   a new semantic owner
3. this is not a broad frontend/product approval or launch-readiness claim
4. the next active work should move to `M5`

### `M5` Delivered

Delivered in this update:

1. one bounded productionization / launch envelope now exists for the current
   `Phase 18` RF2 runtime-bearing path:
   - `v2-rf2-runtime-bounded-envelope.md`
2. one explicit bounded review result now exists:
   - `v2-rf2-runtime-bounded-envelope-review.md`
   - current result: `block expansion` / `not pilot-ready`
3. the productionization artifact set was rebound onto the new runtime path:
   - `v2-bounded-internal-pilot-pack.md`
   - `v2-pilot-preflight-checklist.md`
   - `v2-pilot-stop-conditions.md`
   - `v2-controlled-rollout-review.md`

Tests / review checks:

1. document-only milestone; no new runtime code added
2. consistency review anchored on delivered `M1-M4` code/docs

Current interpretation:

1. `M5` is complete as a bounded productionization artifact set around the new
   runtime path
2. the current judgment is explicitly:
   - `block expansion`
   - `not pilot-ready`
3. `Phase 18` is complete
