# Phase 18 Log

Date: 2026-04-05
Status: active

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
