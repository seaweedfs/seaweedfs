Purpose: append-only technical pack and delivery log for `Phase 17`
post-`Phase 16` separation tracking.

---

### `Phase 17` Start Note

Date: 2026-04-04
Intent: restore a continuous engineering log for the migration batches that
followed `Phase 16` runtime closure

This phase is intentionally a tracking phase.

It records the code-separation line that ran after `Phase 16` but before a new
single semantic/runtime claim had replaced it.

It exists so that:

1. `Batch 1-9` have one durable phase home
2. reviews can reference a stable migration timeline
3. the next seam can be chosen from a clear current-state snapshot

---

### Batch 1 Delivery Note

Date: 2026-04-04
Scope: canonical translation and contract ownership check

What changed:

1. canonical replica identity and recovery-target translation were confirmed to
   belong to `sw-block/bridge/blockvol`
2. adapter-side inline mapping was removed from `weed/storage/blockvol/v2bridge/control.go`
3. the remaining Batch 1 ports (`reader`, `pinner`, `executor`) were reviewed
   and found already aligned with the intended ownership split

Proof / evidence:

1. commit `a38e04c03`
2. no further code changes required for `Task B/C/D`

Conclusion:

1. Batch 1 closed semantic drift first, without forcing unnecessary code motion

---

### Batch 2 Delivery Note

Date: 2026-04-04
Scope: backend-binding shim reduction

What changed:

1. `v2bridge.Reader` now returns `bridge.BlockVolState` directly
2. `pinnerShimForRecovery` was removed from `weed/server/block_recovery.go`
3. executor binding was rechecked and kept as the already-correct thin binding

Proof / evidence:

1. commit `680b53031`
2. commit `519c84994`

Conclusion:

1. the backend-binding layer became thinner without changing semantic ownership

---

### Batch 3 Delivery Note

Date: 2026-04-04
Scope: reusable recovery coordination extraction

What changed:

1. `sw-block/engine/replication/runtime/pending.go` introduced
   `PendingCoordinator`
2. `sw-block/engine/replication/runtime/executor.go` introduced reusable
   recovery execution helpers
3. `weed/server/block_recovery.go` was later rewired so production code uses the
   runtime helpers directly
4. no-core execution was split into explicit legacy helpers instead of remaining
   implicit inline branches

Proof / evidence:

1. commit `6fea93e82`
2. commit `e200df779`
3. commit `e075d7761`
4. commit `3a5fbbfde`

Conclusion:

1. Batch 3 only became complete after the wiring fix; the final state removes
   helper duplication from the production path

---

### Batch 4 Delivery Note

Date: 2026-04-04
Scope: typed runtime boundary and host-shell reduction

What changed:

1. `PendingExecution` became fully typed
2. type assertions and `interface{}` drift were removed from the production
   recovery path
3. rebuild completion shaping moved into a dedicated runtime helper
4. recovery bundle assembly inside `block_recovery.go` was further reduced into
   a bounded helper

Proof / evidence:

1. commit `0bcfc678d`
2. commit `ded84b25e`

Conclusion:

1. Batch 4 made the recovery host shell easier to reason about and safer to
   test

---

### Batch 5 Delivery Note

Date: 2026-04-04
Scope: recovery binding factory extraction

What changed:

1. concrete construction of `Reader`, `Pinner`, `StorageAdapter`, and
   `Executor` moved behind `v2bridge.BuildRecoveryBundle()`
2. `weed/server/block_recovery.go` stopped assembling those concrete bindings
   directly

Proof / evidence:

1. commit `263611004`

Conclusion:

1. the backend-binding layer now owns its own assembly seam

---

### Batch 6 Delivery Note

Date: 2026-04-04
Scope: recovery context resolver extraction

What changed:

1. `resolveRecoveryContext()` consolidated host-side context assembly
2. inline derivation of `rebuildAddr` and related runtime inputs was removed
3. `runCatchUp()` and `runRebuild()` now follow a simple:
   - resolve
   - plan
   - branch
   structure

Proof / evidence:

1. commit `a48da0f67`
2. commit `41082bf92`

Conclusion:

1. the recovery host path became structurally thin enough to review by shape,
   not only by behavior

---

### Batch 7 Delivery Note

Date: 2026-04-04
Scope: command dispatch extraction

What changed:

1. the `engine.Command` switch moved out of `weed/server/volume_server_block.go`
2. new package `weed/server/blockcmd` became the server-adapter command
   dispatcher
3. host effects remained intentionally on the server side instead of being
   pushed into `v2bridge`

Proof / evidence:

1. commit `11c6aaf31`

Conclusion:

1. Batch 7 established the correct ownership seam:
   - dispatch in server adapter
   - backend bindings elsewhere
   - host effects still local

---

### Batch 8 Delivery Note

Date: 2026-04-04
Scope: `BlockVol` command-binding extraction

What changed:

1. concrete `BlockVol` command operations moved into
   `weed/storage/blockvol/v2bridge/command_bindings.go`
2. direct `WithVolume` execution for role apply / receiver startup / primary
   replication setup stopped living in `volume_server_block.go`

Proof / evidence:

1. commit `38b504299`

Conclusion:

1. `v2bridge` now owns concrete backend command bindings, which is the correct
   side of the seam

---

### Batch 9 Delivery Note

Date: 2026-04-04
Scope: non-`BlockVol` command-op extraction

What changed:

1. non-backend command operations moved into `weed/server/blockcmd/service_ops.go`
2. dispatcher rebinding no longer requires `volume_server_block.go` to own the
   full command-op surface
3. nil-safe service-op construction was added so typed nil pointers are not
   smuggled through interfaces

Proof / evidence:

1. commit `38b504299`
2. focused server proofs remained green after rebinding

Conclusion:

1. after Batch 9, `volume_server_block.go` is much closer to a host shell than
   a command-runtime implementation file

---

### Batch 10 Start Note

Date: 2026-04-04
Scope: `10A` host-effects adapter extraction

Execution rule:

1. extract only command-completion host effects
2. keep the slice bounded to:
   - `RecordCommand`
   - `EmitCoreEvent`
   - `PublishProjection`
   - projection-cache write routing
3. do not mix backend readiness mutation into the same cut unless the code
   proves it is already inseparable

Acceptance target:

1. `coreCommandEffects` disappears from `volume_server_block.go`
2. publish-projection cache writes stop being inline there
3. dispatcher keeps consuming a server-side host-effects object
4. focused proofs stay green

Why this is the next seam:

1. dispatch is already extracted
2. backend bindings are already extracted
3. the largest remaining concentrated non-shell logic in
   `volume_server_block.go` is now host effects

---

### Batch 10 Delivery Note

Date: 2026-04-04
Scope: `10A` host-effects adapter extraction

What changed:

1. concrete dispatcher-facing host effects moved into
   `weed/server/blockcmd/host_effects.go`
2. `volume_server_block.go` now wires host-effect callbacks and projection cache
   storage into that adapter instead of defining `coreCommandEffects` locally
3. server-owned projection cache writes moved behind
   `BlockService.StoreProjection()`

Proof / evidence:

1. `go test ./weed/server/blockcmd -count=1 -timeout 60s`
2. `go test ./weed/server -count=1 -timeout 120s -run "TestBlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)"`
3. result: `PASS`

Conclusion:

1. `volume_server_block.go` is thinner again, but host-effect semantics still
   remain explicitly on the server side
2. backend readiness mutation was intentionally left untouched in this slice

---

### Batch 11 Delivery Note

Date: 2026-04-04
Scope: stop-line review for remaining readiness-state mutation

What was reviewed:

1. `noteRoleApplied`
2. `markPrimaryTransportConfigured`
3. `markReceiverReady`
4. `ReadinessSnapshot` as the read-side consumer of the same local state

Decision:

1. do not extract these methods into `weed/server/blockcmd`

Why:

1. they are direct mutations of `BlockService.replStates`
2. they belong to adapter-local host state, not dispatcher-side orchestration
3. a further extraction would mostly replace direct method calls with callback
   plumbing while leaving ownership unchanged

Accepted stop line:

1. `blockcmd` keeps dispatch / service ops / host-effects adapter
2. `v2bridge` keeps concrete backend bindings
3. `weed/server` keeps local readiness/cache state and its mutation paths

Conclusion:

1. the current boundary is the correct stopping point for the separation line
2. the next meaningful work item should be cleanup within that boundary or a new
   semantic/runtime step, not another package shuffle

---

### Current State Snapshot

Date: 2026-04-04
State after Batch 10:

1. `sw-block` owns:
   - canonical bridge helpers
   - reusable recovery runtime helpers
2. `weed/storage/blockvol/v2bridge` owns:
   - concrete `BlockVol` recovery bundle assembly
   - concrete `BlockVol` command bindings
3. `weed/server/blockcmd` owns:
   - command dispatch
   - service-side command operations
   - host-effects adapter
4. `weed/server` still owns:
   - assignment ingress
   - projection/publication cache writes
   - host-owned cache/state fields
   - product-facing integration state

Open next seam:

1. no additional ownership move is currently justified on the readiness-state
   path
