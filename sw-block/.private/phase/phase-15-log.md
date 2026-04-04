Purpose: append-only technical pack and delivery log for `Phase 15` adapter
hook and projection rebinding work.

---

### `15A` Technical Pack

Date: 2026-04-03
Goal: connect one narrow live path from `weed/` into the explicit `V2 core`
and one bounded command/projection path back out, without attempting broad
runtime cutover

#### Layer 1: Semantic Core

`15A` accepts one bounded thing:

1. the explicit core is no longer isolated from the integrated path

It does not accept:

1. live runtime cutover
2. registry/lookup rebinding
3. broad product-surface migration

#### Narrow path chosen

Ingress:

1. `weed/server/volume_server_block.go`
2. `BlockService.ApplyAssignments()`

Egress:

1. `PublishProjectionCommand`
2. adapter-local projection cache on `BlockService`

Reason:

1. this is the narrowest stable live path after heartbeat delivery
2. it already owns assignment apply / receiver / shipper setup
3. it allows a real in-process `weed -> core -> adapter` loop without reopening
   master registry or product surfaces yet

#### `15A` Delivery Note Rev 1

Date: 2026-04-03
Scope: wire the explicit core into `BlockService.ApplyAssignments()` on one
narrow live path

What changed:

1. `BlockService` now owns an explicit `v2Core` and adapter-local core
   projection cache
2. `ApplyAssignments()` now sends bounded assignment and local observation
   events into the explicit core:
   - `AssignmentDelivered`
   - `RoleApplied`
   - `ReceiverReadyObserved`
   - `ShipperConfiguredObserved`
   - bounded `ShipperConnectedObserved` when observable
3. `PublishProjectionCommand` now has one real egress path back into `weed/`
   through the adapter-local core projection cache

Files changed:

1. `weed/server/volume_server_block.go`
   - added `v2Core`
   - added adapter-local projection cache
   - added narrow assignment-event delivery into the explicit core
   - cached `PublishProjectionCommand` output for live-path inspection
2. `weed/server/volume_server_block_test.go`
   - added narrow-path proofs for replica and primary assignment delivery
3. `sw-block/.private/phase/phase-15.md`
   - added phase/slice framing

Proofs added:

1. replica assignment narrow-path proof
   - live `ApplyAssignments()` updates core projection cache
   - resulting projection is `replica_ready`
   - publication stays non-healthy with reason `replica_not_primary`
2. primary assignment narrow-path proof
   - live `ApplyAssignments()` updates core projection cache
   - resulting projection carries applied role and shipper-configured truth
   - publication does not overclaim healthy without durable boundary closure

Validation:

1. targeted `weed/server` tests for the new narrow path
2. existing `sw-block/engine/replication` package tests stay green

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - one real adapter ingress now reaches the explicit core owner
2. overclaim avoided
   - this is not broad surface rebinding
   - the cache is adapter-local, not yet a product truth store
3. proof preserved
   - `Phase 14` core shell remains the semantic owner

---

#### `15A` Delivery Note Rev 2

Date: 2026-04-03
Scope: prove the adapter-local projection cache does not split from the explicit
core on the narrow live path

What changed:

1. extracted adapter command egress into a dedicated helper:
   - `applyCoreCommands`
2. added focused proofs that:
   - adapter-local projection cache equals the explicit core projection
   - repeated unchanged assignment does not make adapter cache and core diverge

Files changed:

1. `weed/server/volume_server_block.go`
   - extracted command egress helper for `PublishProjectionCommand`
2. `weed/server/volume_server_block_test.go`
   - strengthened replica/primary narrow-path tests with cache-vs-core equality
   - added unchanged-assignment consistency proof

Proofs strengthened:

1. adapter/core projection coherence
   - after live `ApplyAssignments()`, cached projection equals
     `bs.V2Core().Projection(path)`
2. unchanged-assignment coherence
   - repeated identical assignment keeps cache and core aligned
   - repeated identical assignment does not mutate the cached outward truth

Validation:

1. `go test ./weed/server -run "TestBlockService_ApplyAssignments_(UpdatesCoreProjection|RepeatedUnchangedStaysInSyncWithCore)"`
2. `go test ./...` in `sw-block/engine/replication`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the narrow adapter egress is now proven coherent with the explicit core
2. overclaim avoided
   - adapter-local cache is no longer merely assumed to reflect core truth
3. proof preserved
   - `15A Rev 1` ingress/egress proof remains intact

---

#### `15A` Delivery Note Rev 3

Date: 2026-04-03
Scope: make narrow-path adapter/core coherence explicitly checkable and record
the remaining semantic boundary around adapter-local `PublishHealthy`

What changed:

1. added `CoreProjectionMismatches(path)` on `BlockService`
   - compares only the fields that should already agree on the narrow `15A`
     path
   - intentionally excludes adapter-local `ReadinessSnapshot.PublishHealthy`
2. documented that `BlockReadinessSnapshot.PublishHealthy` is still an
   adapter-local bit and not the semantic owner for Phase 14 core publication
   health
3. strengthened the narrow-path tests to require zero adapter/core mismatches

Files changed:

1. `weed/server/volume_server_block.go`
   - added `CoreProjectionMismatches`
   - clarified `BlockReadinessSnapshot.PublishHealthy` semantics
2. `weed/server/volume_server_block_test.go`
   - replica narrow-path proof now asserts zero mismatches
   - primary narrow-path proof now asserts zero mismatches
   - repeated unchanged assignment proof now asserts zero mismatches

Proofs strengthened:

1. narrow-path aligned subset is now explicitly machine-checked
2. remaining semantic split is documented rather than hidden:
   - core publication owner = `engine.PublicationView`
   - adapter-local `PublishHealthy` remains a current-surface bit pending later
     rebinding

Validation:

1. `go test ./weed/server -run "TestBlockService_ApplyAssignments_(UpdatesCoreProjection|RepeatedUnchangedStaysInSyncWithCore)"`
2. `go test ./...` in `sw-block/engine/replication`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the narrow live path now has an explicit consistency oracle
2. overclaim avoided
   - we no longer imply that all adapter-local fields are already rebound
3. proof preserved
   - `15A Rev 1` and `Rev 2` proofs still pass

---

### `15B` Technical Pack

Date: 2026-04-04
Goal: make one existing `weed/` outward surface consume core-owned projection
truth instead of only adapter-local readiness bits

#### Layer 1: Semantic Core

`15B` accepts one bounded thing:

1. one real `weed/` read surface now prefers the explicit core projection when
   that projection exists on the live path

It does not accept:

1. master registry rebinding
2. master lookup/public API rebinding
3. broad runtime cutover
4. removal of all adapter-local convenience state

#### Chosen surface

Surface:

1. `weed/server/volume_server_block_debug.go`
2. `/debug/block/shipper`

Reason:

1. it is an existing explicit read-only `weed/` surface
2. it already exposes readiness/publication-adjacent fields
3. it is narrow enough to rebind without reopening master or product surfaces

#### `15B` Delivery Note Rev 1

Date: 2026-04-04
Scope: rebind one VS debug surface so it consumes core-owned projection truth on
the narrow live path

What changed:

1. added `BlockService.DebugInfoForVolume(path, vol)`
   - builds the outward debug view for one volume
   - prefers `CoreProjection(path)` when present
   - falls back to adapter-local readiness only when the core projection does
     not exist yet
2. `/debug/block/shipper` now uses that helper instead of assembling the
   surface directly from adapter-local readiness flags
3. the debug surface now carries bounded core-owned outward meaning:
   - `mode`
   - `publish_healthy`
   - `publication_reason`

Files changed:

1. `weed/server/volume_server_block_debug.go`
   - added `DebugInfoForVolume`
   - rebound debug surface assembly to core projection
   - added `mode` and `publication_reason` fields
2. `weed/server/volume_server_block_test.go`
   - added primary-path proof that debug `publish_healthy` follows core
     publication truth, not adapter-local convenience truth
   - added replica-path proof that debug role/mode/readiness/publication align
     with the cached core projection
3. `sw-block/.private/phase/phase-15.md`
   - marked `15A` delivered and `15B` active

Proofs added:

1. primary-path publication overclaim blocked on the real `weed/` surface
   - adapter-local readiness may still say `PublishHealthy=true`
   - debug surface now reports the core-owned publication result instead
   - this proves `assignment delivered != publish healthy` on the live path
2. replica-path projection rebinding
   - debug role/mode/readiness/publication now match the cached core projection
   - this proves one outward `weed/` surface is consuming core-owned truth

Validation:

1. `go test ./weed/server -run "TestBlockService_(ApplyAssignments|DebugInfoForVolume)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - one existing `weed/` surface now consumes explicit core projection truth
2. overclaim avoided
   - this is not yet registry/lookup rebinding
   - adapter-local readiness still exists as fallback and for unrebound paths
3. proof preserved
   - `15A` narrow ingress/egress/cache-coherence proofs still pass

---

#### `15B` Delivery Note Rev 2

Date: 2026-04-04
Scope: rebind the VS heartbeat address-publication gate so it consumes
core-owned readiness projection instead of adapter-local `publishHealthy`

What changed:

1. `CollectBlockVolumeHeartbeat()` no longer gates scalar replica transport
   addresses on adapter-local `publishHealthy` alone
2. added `heartbeatReplicaAddrs(path, state)`
   - prefers `CoreProjection(path)` when present
   - on primary path, heartbeat address publication follows core
     `Readiness.ShipperConfigured`
   - on replica path, heartbeat address publication follows core
     `Readiness.ReceiverReady`
   - falls back to legacy adapter-local behavior only when the core projection
     does not exist yet
3. added focused differential proofs that heartbeat still reports the correct
   addresses even when adapter-local `publishHealthy` is manually cleared

Files changed:

1. `weed/server/volume_server_block.go`
   - rebound heartbeat scalar address publication to core readiness projection
2. `weed/server/volume_server_block_test.go`
   - added primary-path heartbeat proof
   - added replica-path heartbeat proof

Proofs added:

1. primary-path heartbeat rebinding
   - core projection says `ShipperConfigured=true`
   - core publication still remains unhealthy
   - adapter-local `publishHealthy` is forcibly cleared in test
   - heartbeat still reports replica addresses, proving it no longer depends on
     adapter-local publication convenience truth
2. replica-path heartbeat rebinding
   - core projection says `ReceiverReady=true`
   - core publication remains unhealthy because replica is not the publication
     owner
   - adapter-local `publishHealthy` is forcibly cleared in test
   - heartbeat still reports receiver addresses, proving it follows the core
     readiness projection on the narrow live path

Validation:

1. `go test ./weed/server -run "TestBlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - one real report path from `weed/` to master now consumes core projection
     truth
2. overclaim avoided
   - heartbeat proto is not yet widened to carry full mode/publication objects
   - master registry/lookup are not yet rebound
3. proof preserved
   - `15B Rev 1` debug-surface rebinding still passes

---

#### `15B` Delivery Note Rev 3

Date: 2026-04-04
Scope: rebind the shared VS-side readiness snapshot so aligned fields prefer the
explicit core projection instead of adapter-local readiness state

What changed:

1. `ReadinessSnapshot(path)` now prefers `CoreProjection(path)` for the aligned
   readiness subset when the narrow Phase 15 path has already produced a
   projection:
   - `role_applied`
   - `receiver_ready`
   - `shipper_configured`
   - `shipper_connected`
   - `replica_eligible`
2. `PublishHealthy` remains adapter-local on `ReadinessSnapshot`
   - this keeps the publication ownership boundary explicit instead of silently
     rebinding it through a convenience struct
3. added focused proofs that manually corrupt adapter-local readiness state and
   show `ReadinessSnapshot()` still returns the core-owned aligned fields

Files changed:

1. `weed/server/volume_server_block.go`
   - rebound `ReadinessSnapshot()` aligned subset to core projection
   - clarified snapshot ownership boundary in comments
2. `weed/server/volume_server_block_test.go`
   - added primary-path readiness snapshot proof
   - added replica-path readiness snapshot proof

Proofs added:

1. primary-path shared snapshot rebinding
   - adapter-local `roleApplied` and `shipperConfigured` are forcibly cleared
   - `ReadinessSnapshot()` still returns them as true from the core projection
   - `PublishHealthy` stays false in the snapshot, proving publication was not
     silently rebound
2. replica-path shared snapshot rebinding
   - adapter-local `receiverReady` and `replicaEligible` are forcibly cleared
   - `ReadinessSnapshot()` still returns them as true from the core projection
   - `PublishHealthy` stays false in the snapshot, preserving the ownership
     boundary

Validation:

1. `go test ./weed/server -run "TestBlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the shared VS-side readiness snapshot now consumes the explicit core
     projection on the narrow live path
2. overclaim avoided
   - publication ownership still remains outside `ReadinessSnapshot`
   - master registry/lookup are still not rebound
3. proof preserved
   - `15B Rev 1` debug and `Rev 2` heartbeat rebinding proofs still pass

---

#### `15B` Delivery Note Rev 4

Date: 2026-04-04
Scope: rebind the heartbeat `replica_degraded` producer bit to the explicit core
mode and prove the master registry consume path accepts that rebinding

What changed:

1. `CollectBlockVolumeHeartbeat()` now also prefers the explicit core
   projection for the bounded degraded bit
2. added `heartbeatReplicaDegraded(path, current)`
   - maps `ModeDegraded` and `ModeNeedsRebuild` to heartbeat
     `ReplicaDegraded=true`
   - maps all other core modes to `false`
   - falls back to the runtime-local status bit when no core projection exists
3. added a producer-side proof that `heartbeatReplicaDegraded(..., false)` still
   returns `true` when the core projection enters `needs_rebuild`
4. added a minimal master-consume proof:
   - a `BlockService` heartbeat is produced after core degraded transition
   - `BlockVolumeRegistry.UpdateFullHeartbeat()` consumes that heartbeat
   - registry truth becomes `TransportDegraded=true`, `ReplicaDegraded=true`,
     `VolumeMode="degraded"`

Files changed:

1. `weed/server/volume_server_block.go`
   - rebound heartbeat degraded bit to explicit core mode
2. `weed/server/volume_server_block_test.go`
   - added bounded producer proof for core-driven degraded mapping
3. `weed/server/master_block_registry_test.go`
   - added bounded consume proof for registry ingest of the core-influenced
     heartbeat degraded bit

Proofs added:

1. producer degraded-bit rebinding
   - replica path enters core `needs_rebuild`
   - helper returns degraded even when the input `current` bit is `false`
   - this proves the heartbeat producer is no longer only echoing the runtime
     bit on the narrow live path
2. master consume closure
   - primary path enters core `degraded`
   - heartbeat exports `ReplicaDegraded=true`
   - registry consume derives degraded transport and degraded volume mode from
     that heartbeat

Validation:

1. `go test ./weed/server -run "Test(BlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded))"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the first bounded master-consume path now accepts a core-influenced
     heartbeat bit
2. overclaim avoided
   - registry mode derivation itself is not yet replaced by core-owned mode
   - lookup/public API surfaces are still not rebound
3. proof preserved
   - `15B Rev 1-3` VS-side rebinding proofs still pass

---

#### `15B` Delivery Note Rev 5

Date: 2026-04-04
Scope: close the other half of the first master-consume boundary by proving the
registry also consumes core-influenced ready heartbeats, not only degraded ones

What changed:

1. added a bounded ready-path consume proof in `master_block_registry_test.go`
2. the proof uses a real `BlockService` replica assignment path to produce a
   heartbeat whose replica addresses still publish even after adapter-local
   `publishHealthy` is manually cleared
3. `BlockVolumeRegistry.UpdateFullHeartbeat()` then consumes that heartbeat and
   closes the ready half of the contract:
   - replica detail becomes `Ready=true`
   - aggregate `ReplicaReady=true`
   - aggregate `ReplicaDegraded=false`
   - normalized `VolumeMode="publish_healthy"`

Files changed:

1. `weed/server/master_block_registry_test.go`
   - added `TestRegistry_UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady`

Proofs added:

1. master consume ready closure
   - VS producer emits replica addresses from the core-influenced ready path
     even after adapter-local publication convenience truth is cleared
   - registry consume converts that heartbeat into ready aggregate truth and
     `publish_healthy` outward mode
2. together with `Rev 4`, the first bounded master-consume edge now has both
   sides covered:
   - degraded consume
   - ready consume

Validation:

1. `go test ./weed/server -run "TestRegistry_(UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the first bounded master-consume edge now has explicit proof for both ready
     and degraded outcomes
2. overclaim avoided
   - registry is still consuming heartbeat-derived booleans/addresses, not full
     core mode/publication objects
   - lookup/public API remain unrebound
3. proof preserved
   - `15B Rev 4` degraded consume proof still passes unchanged

---

#### `15B` Delivery Note Rev 6

Date: 2026-04-04
Scope: extract the first explicit master-side consume helpers so registry
heartbeat semantics are no longer embedded only as inline logic inside
`UpdateFullHeartbeat()`

What changed:

1. extracted `applyPrimaryHeartbeatObservation(existing, info)`
   - names the primary-heartbeat -> registry consume contract
2. extracted `applyReplicaHeartbeatObservation(existing, server, existingName, info, result)`
   - names the replica-heartbeat -> registry consume contract
3. extracted `replicaReadyObservedFromHeartbeat(info)`
   - makes the current ready gate explicit:
     published replica receiver addresses => `Ready=true`
4. `UpdateFullHeartbeat()` now delegates to those helpers instead of carrying
   the full consume mapping inline

Files changed:

1. `weed/server/master_block_registry.go`
   - extracted explicit consume helpers from `UpdateFullHeartbeat()`

Proof / validation posture:

1. no new behavior claim
   - this revision is an extraction/clarification step, not a semantics change
2. existing master consume proofs remain the acceptance object:
   - `ReplicaReadyRequiresReplicaHeartbeat`
   - `ConsumesCoreInfluencedReplicaDegraded`
   - `ConsumesCoreInfluencedReplicaReady`

Validation:

1. `go test ./weed/server -run "TestRegistry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the first master consume edge is now explicit in code, not only in tests
2. overclaim avoided
   - registry derivation semantics are not replaced yet
   - lookup/public API are still unrebound
3. proof preserved
   - `15B Rev 4-5` consume proofs still pass after extraction

---

#### `15B` Delivery Note Rev 7

Date: 2026-04-04
Scope: push the first bounded closure from master consume into an outward
master read surface

What changed:

1. extracted `entryReplicaSurfaceInfo(e, primaryAlive)` in
   `master_server_handlers_block.go`
   - makes the current registry -> outward surface mapping explicit for:
     `ReplicaReady`, `ReplicaDegraded`, `VolumeMode`, `HealthState`
2. `entryToVolumeInfo()` now reads those outward replica-surface fields through
   the helper instead of inlining them
3. added two end-to-end outward-surface proofs:
   - core-influenced ready consume -> `entryToVolumeInfo()`
   - core-influenced degraded consume -> `entryToVolumeInfo()`

Files changed:

1. `weed/server/master_server_handlers_block.go`
   - added `entryReplicaSurfaceInfo`
   - rebound `entryToVolumeInfo` to the explicit outward surface helper
2. `weed/server/master_block_observability_test.go`
   - added ready-path outward closure proof
   - added degraded-path outward closure proof

Proofs added:

1. ready outward closure
   - VS emits a core-influenced ready heartbeat
   - registry consumes it into ready aggregate truth
   - `entryToVolumeInfo()` exposes:
     `ReplicaReady=true`, `ReplicaDegraded=false`,
     `VolumeMode=publish_healthy`, `HealthState=healthy`
2. degraded outward closure
   - VS emits a core-influenced degraded heartbeat
   - registry consumes it into degraded aggregate truth
   - `entryToVolumeInfo()` exposes:
     `ReplicaDegraded=true`, `VolumeMode=degraded`,
     `HealthState=degraded`

Validation:

1. `go test ./weed/server -run "Test(Registry_(UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume))"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - one bounded master outward read path now explicitly reflects the
     core-influenced consume chain
2. overclaim avoided
   - this is `entryToVolumeInfo()` closure only, not full REST/gRPC surface
     rebinding
   - lookup/public API transport remains otherwise unchanged
3. proof preserved
   - `15B Rev 4-6` producer/consume/extraction proofs remain valid

---

#### `15B` Delivery Note Rev 8

Date: 2026-04-04
Scope: close the first real HTTP handler proofs above the master outward helper

What changed:

1. added handler-level proof for `GET /block/volume/{name}`
   - proves lookup handler reflects the core-influenced ready path
2. added handler-level proof for `GET /block/volumes`
   - proves list handler reflects the core-influenced degraded path
3. both proofs reuse the same bounded chain already established in earlier
   revisions:
   - `BlockService` emits core-influenced heartbeat
   - `BlockVolumeRegistry.UpdateFullHeartbeat()` consumes it
   - outward handler returns the resulting truth

Files changed:

1. `weed/server/master_server_handlers_block_test.go`
   - added lookup-handler ready closure proof
   - added list-handler degraded closure proof

Proofs added:

1. lookup handler ready closure
   - replica assignment path produces a core-influenced ready heartbeat
   - registry consumes it
   - `GET /block/volume/{name}` returns:
     `ReplicaReady=true`, `ReplicaDegraded=false`,
     `VolumeMode=publish_healthy`
2. list handler degraded closure
   - primary path produces a core-influenced degraded heartbeat
   - registry consumes it
   - `GET /block/volumes` returns:
     `ReplicaDegraded=true`, `VolumeMode=degraded`

Validation:

1. `go test ./weed/server -run "TestBlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the bounded closure now reaches real HTTP handler surfaces
2. overclaim avoided
   - only two handler paths are proven so far
   - gRPC lookup response remains a separate surface
3. proof preserved
   - `15B Rev 7` outward helper closure remains the underlying contract

---

#### `15B` Delivery Note Rev 10

Date: 2026-04-04
Scope: extend the bounded closure from per-volume outward surfaces to the first
cluster-level aggregate outward surface

What changed:

1. added `TestBlockStatusHandler_ReflectsCoreInfluencedConsumeCounts`
2. the proof constructs two real bounded chains:
   - ready path: replica-side core-influenced heartbeat -> registry consume
   - degraded path: primary-side core-influenced heartbeat -> registry consume
3. `GET /block/status` is then verified to expose the resulting aggregate truth:
   - `VolumeCount=2`
   - `HealthyCount=1`
   - `DegradedCount=1`
   - `RebuildingCount=0`
   - `UnsafeCount=0`

Files changed:

1. `weed/server/master_block_observability_test.go`
   - added cluster-level status closure proof

Proofs added:

1. status-handler aggregate closure
   - two independent core-influenced consume chains are materialized in the
     registry
   - `blockStatusHandler` reports the expected aggregate health counts
   - this proves the bounded closure now reaches a cluster-level outward read
     surface, not only per-volume lookup/list surfaces

Validation:

1. `go test ./weed/server -run "TestBlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - a cluster-level outward aggregate now reflects the same bounded
     core-influenced consume chain
2. overclaim avoided
   - only the status-count surface is proven here
   - no broader dashboard/runbook claims are added by this revision
3. proof preserved
   - `15B Rev 8-9` per-volume outward surface proofs remain valid

---

#### `15B` Delivery Note Rev 11

Date: 2026-04-04
Scope: extract the first explicit cluster-level outward response helper

What changed:

1. extracted `statusResponseFromRegistry()` from `blockStatusHandler`
2. `blockStatusHandler` now delegates to that helper instead of assembling the
   aggregate response inline
3. this makes the current cluster-level outward mapping explicit for:
   - volume/server counts
   - promotion/barrier/queue aggregates
   - healthy/degraded/rebuilding/unsafe counts
   - NVMe-capable server count

Files changed:

1. `weed/server/master_server_handlers_block.go`
   - added `statusResponseFromRegistry()`
   - rebound `blockStatusHandler` to the helper

Proof / validation posture:

1. no new behavior claim
   - this revision is a contract extraction step for the status surface
2. existing status closure proof remains the acceptance object:
   - `TestBlockStatusHandler_ReflectsCoreInfluencedConsumeCounts`

Validation:

1. `go test ./weed/server -run "TestBlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the cluster-level outward aggregate now has an explicit code-level contract
2. overclaim avoided
   - no new status semantics are introduced in this revision
3. proof preserved
   - `15B Rev 10` status-handler closure proof still passes after extraction

---

#### `15B` Closeout Note

Date: 2026-04-04

Closeout judgment:

1. `15A` + `15B` are now treated as delivered
2. `weed/` now has one bounded integrated path where:
   - core-owned events enter from the live adapter path
   - bounded command/projection egress returns to the adapter
   - projection/store/outward surfaces consume core-owned truth on the selected
     path

Final focused validation sweep:

1. `go test ./weed/server -run "Test(BlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
2. result: `PASS`

Next phase handoff:

1. move to `Phase 16`
2. stop widening surface rebinding by default
3. start replacing one adapter-owned runtime-driving path with core-driven
   command ownership

---

#### `15B` Delivery Note Rev 9

Date: 2026-04-04
Scope: make the parallel gRPC lookup surface explicit as its own bounded outward
contract

What changed:

1. extracted `lookupResponseFromEntry(entry)` in
   `master_grpc_server_block.go`
   - this names the current `BlockVolumeEntry -> LookupBlockVolumeResponse`
     mapping explicitly instead of leaving it inline inside the gRPC handler
2. `LookupBlockVolume()` now delegates to that helper
3. added a focused test that proves the helper remains a publication-minimal
   outward surface:
   - it returns server/transport/capacity/replica-set/durability/NVMe fields
   - it does not attempt to become a second semantic owner for mode/readiness

Files changed:

1. `weed/server/master_grpc_server_block.go`
   - added `lookupResponseFromEntry`
   - rebound `LookupBlockVolume()` to the helper
2. `weed/server/master_grpc_server_block_test.go`
   - added `TestLookupResponseFromEntry_PublicationMinimalSurface`

Proofs added:

1. gRPC lookup outward contract
   - response helper preserves the current exposed fields:
     `VolumeServer`, `IscsiAddr`, `CapacityBytes`,
     `ReplicaServer`, `ReplicaFactor`, `ReplicaServers`,
     `DurabilityMode`, `NvmeAddr`, `Nqn`
   - response remains intentionally publication-minimal rather than trying to
     mirror the richer HTTP mode/readiness surface

Validation:

1. `go test ./weed/server -run "Test(Master_LookupBlockVolume|LookupResponseFromEntry_PublicationMinimalSurface|Master_LookupResponse_)"`
2. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - the parallel gRPC outward path now has an explicit code-level contract
2. overclaim avoided
   - gRPC lookup schema is not widened in this revision
   - mode/readiness/publication truth stay on the HTTP/helper side for now
3. proof preserved
   - `15B Rev 8` handler-level closures remain valid
