Purpose: append-only technical pack and delivery log for `Phase 16`
`V2`-native runtime closure work.

---

### `16A` Technical Pack

Date: 2026-04-04
Goal: replace one adapter-owned execution decision path with explicit
core-driven command ownership while keeping `blockvol` as the execution backend

#### Layer 1: Semantic Core

`16A` accepts one bounded thing:

1. one real integrated execution path now runs because the core emitted the
   command, not because adapter-local branching independently decided it

It does not accept:

1. full replacement of `blockvol` async runtime loops
2. broad runtime cutover across all adapter paths
3. all recovery/failover paths becoming `V2`-native at once

#### Candidate runtime-driving paths

Candidate commands already emitted by the explicit core:

1. `apply_role`
2. `start_receiver`
3. `configure_shipper`
4. `start_catchup`
5. `start_rebuild`

Selection rule:

1. prefer the narrowest path that is already real on the integrated `weed/`
   path
2. prefer a path where current execution is still primarily adapter-owned
3. prefer a path with direct proofable observation back into the core

#### Initial implementation rule

For the first `16A` slice:

1. do not rewrite `blockvol` internals
2. do not change flusher/shipper goroutine architecture yet
3. move runtime-driving ownership one step upward:
   - core emits command
   - adapter executes command
   - execution observation returns as explicit core event

#### Validation target

The first accepted `16A` path must prove:

1. the command was emitted by the core
2. the adapter executed because of that command
3. the resulting observation fed back into the core
4. outward surfaces stayed coherent with that same path

---

#### `16A` Delivery Note Rev 1

Date: 2026-04-04
Scope: first bounded runtime-driving command path on the integrated adapter

What changed:

1. `ApplyAssignments()` no longer performs role application as adapter-local
   branching when the explicit core is present
2. assignment delivery now executes `apply_role` from core command egress
3. replica assignment path also executes `start_receiver` from the same bounded
   command path
4. `publish_projection` caching now prefers the latest core projection so
   command-chain observations cannot be overwritten by an older inline
   projection snapshot

Files changed:

1. `weed/server/volume_server_block.go`
   - role application moved to `ApplyRoleCommand` execution
   - replica receiver startup moved to `StartReceiverCommand` execution
   - added bounded executed-command trace for focused runtime-ownership proofs
   - projection cache now prefers latest `v2Core.Projection()`
2. `weed/server/volume_server_block_test.go`
   - added focused proof for primary `apply_role` command ownership
   - added focused proof for replica `apply_role + start_receiver` ownership

Bounded contract:

`16A Rev 1` accepts only this:

1. one integrated assignment path now executes from core command egress
2. the adapter remains the executor, not the semantic owner
3. outward/readiness projections remain aligned after the command chain

It does not yet accept:

1. primary shipper configuration becoming fully core-command-driven
2. rebuild / catch-up / failover runtime closure
3. broad `Phase 16` completion

Validation:

1. `go test ./weed/server -run "TestBlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)"`
2. `go test ./weed/server -run "Test(BlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `Phase 16` requires one bounded path where core-owned semantics drive
     adapter execution rather than adapter-local truth
2. overclaim avoided
   - this revision does not claim full async runtime replacement or full
     failover/rebuild closure
3. proof preserved
   - existing `15A/15B` projection/store closure tests still pass after the new
     command-driven assignment path

---

#### `16A` Delivery Note Rev 2

Date: 2026-04-04
Scope: extend the same bounded assignment-side command path to primary shipper
configuration

What changed:

1. `ApplyAssignments()` no longer configures primary replication directly when
   the explicit core is present
2. primary shipper wiring now executes only from
   `engine.ConfigureShipperCommand`
3. the executed-command trace now proves a full bounded assignment-side command
   chain:
   - primary: `apply_role -> configure_shipper`
   - replica: `apply_role -> start_receiver`

Files changed:

1. `weed/server/volume_server_block.go`
   - added `executeConfigureShipperCommand()`
   - removed adapter-local primary replication setup from `ApplyAssignments()`
2. `weed/server/volume_server_block_test.go`
   - strengthened primary command-ownership proof to require
     `apply_role + configure_shipper`

Bounded contract:

`16A Rev 2` accepts only this:

1. the full assignment-side command chain is now core-command-driven on the
   integrated path
2. primary and replica setup no longer rely on adapter-local branching for the
   bounded assignment path
3. existing projection/store/outward-surface proofs remain intact

It does not yet accept:

1. catch-up runtime ownership
2. rebuild runtime ownership
3. invalidation / recovery-loop runtime closure

Validation:

1. `go test ./weed/server -run "TestBlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)"`
2. `go test ./weed/server -run "Test(BlockService_(ApplyAssignments|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `Phase 16` requires one bounded runtime path where command emission from
     the explicit core drives adapter execution
2. overclaim avoided
   - this revision still does not claim recovery-side runtime closure
3. proof preserved
   - `Phase 15B` surface consume-chain proofs remain green after moving primary
     shipper setup behind core command ownership

---

#### `16A` Delivery Note Rev 3

Date: 2026-04-04
Scope: first bounded recovery-side command path on the integrated runtime

What changed:

1. `InvalidateSessionCommand` is now executed on the live adapter path
2. when the core emits invalidation for a volume, the adapter invalidates the
   corresponding integrated sender sessions through the orchestrator registry
3. repeated identical failure transitions still remain bounded:
   one new failure transition triggers one invalidation command; repeated same
   failure does not re-execute it

Files changed:

1. `weed/server/volume_server_block.go`
   - added `executeInvalidateSessionCommand()`
   - wired `InvalidateSessionCommand` into command execution
2. `weed/server/volume_server_block_test.go`
   - added proof that `BarrierRejected` invalidates an active sender session
   - added proof that repeated same-reason rejection does not re-execute
     invalidation

Bounded contract:

`16A Rev 3` accepts only this:

1. one integrated failure-side path is now also core-command-driven
2. sender-session invalidation on the bounded path no longer depends on
   adapter-local branching alone
3. assignment-side command ownership and outward surface closure remain intact

It does not yet accept:

1. catch-up execution ownership
2. rebuild execution ownership
3. full recovery-loop runtime closure

Validation:

1. `go test ./weed/server -run "TestBlockService_(ApplyAssignments|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)"`
2. `go test ./weed/server -run "Test(BlockService_(ApplyAssignments|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `Phase 14` command semantics already required invalidation only on a new
     failure transition; `Phase 16` now makes that command real on the bounded
     integrated path
2. overclaim avoided
   - this revision invalidates sessions, but does not yet claim catch-up or
     rebuild execution ownership
3. proof preserved
   - `15B` projection/store/outward-surface proofs remain green after adding the
     failure-side command path

---

#### `16A` Closeout Note

Date: 2026-04-04

Closeout judgment:

1. `16A` is treated as delivered
2. the bounded integrated command-driven path now includes:
   - `apply_role`
   - `start_receiver`
   - `configure_shipper`
   - `invalidate_session`
3. this is enough to say one real runtime-driving command chain is now
   core-owned on the integrated path

Residual non-claims:

1. active recovery execution (`start_catchup`, `start_rebuild`) is not yet
   core-command-driven
2. `16A` does not close the full recovery loop by itself

---

#### `16B` Delivery Note Rev 1

Date: 2026-04-04
Scope: first live recovery observation path back into the explicit core

What changed:

1. `RecoveryManager.runCatchUp()` now feeds live recovery planning/escalation
   back into the core:
   - `CatchUpPlanned`
   - `NeedsRebuildObserved`
   - `CatchUpCompleted`
2. `RecoveryManager.runRebuild()` now feeds live rebuild lifecycle milestones
   back into the core:
   - `RebuildStarted`
   - `RebuildCommitted`
3. this makes the integrated recovery path update core-owned recovery/boundary
   truth instead of leaving those states test-only or documentation-only

Files changed:

1. `weed/server/block_recovery.go`
   - emit recovery events into `v2Core` from live catch-up/rebuild execution
2. `weed/server/block_recovery_test.go`
   - added focused proof that live catch-up updates core projection boundary
   - added focused proof that live needs-rebuild escalation updates core mode

Bounded contract:

`16B Rev 1` accepts only this:

1. one live recovery observation path now closes back into the core
2. core-owned boundary/recovery/mode fields update from real recovery work
3. existing `15B/16A` surface and command closure remain intact

It does not yet accept:

1. `start_catchup` command execution ownership
2. `start_rebuild` command execution ownership
3. full recovery-loop execution becoming core-command-driven

Validation:

1. `go test ./weed/server -run "TestP(4_LivePath_RealVol_ReachesPlan|16B_RunCatchUp_)"`
2. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `Phase 16` requires runtime observations to close back into core-owned
     truth instead of remaining adapter-local/runtime-local facts
2. overclaim avoided
   - this revision does not yet claim that recovery execution itself is
     core-command-driven
3. proof preserved
   - `15B` outward consume-chain proofs and `16A` command-driven proofs remain
     green after adding recovery event ingress

---

#### `16B` Delivery Note Rev 2

Date: 2026-04-04
Scope: bounded catch-up execution ownership on the live recovery path

What changed:

1. `RecoveryManager.runCatchUp()` no longer directly executes the catch-up plan
   when the explicit core is present
2. catch-up plans are now cached as bounded pending executions and consumed only
   from `StartCatchUpCommand`
3. if the core does not emit/consume `StartCatchUpCommand`, the pending plan is
   cancelled fail-closed instead of executing implicitly
4. compatibility is preserved for the old no-core path:
   legacy `P4` recovery tests still execute directly when `v2Core` is absent

Files changed:

1. `weed/server/block_recovery.go`
   - added bounded pending recovery execution cache
   - `runCatchUp()` now plans + emits core event, then waits for command
     consumption on the core-present path
   - added `ExecutePendingCatchUp()` and shared execution helpers
2. `weed/server/volume_server_block.go`
   - wired `StartCatchUpCommand` into command execution
3. `weed/server/block_recovery_test.go`
   - strengthened catch-up proof to require live `start_catchup` execution
   - preserved old `P4` live-path proof

Bounded contract:

`16B Rev 2` accepts only this:

1. one active recovery execution path (`catch-up`) is now core-command-driven on
   the integrated runtime
2. the corresponding live recovery observations still return back into the core
3. old no-core compatibility remains preserved for previously accepted tests

It does not yet accept:

1. `start_rebuild` execution ownership
2. full recovery-loop execution closure
3. broad multi-replica recovery ownership beyond the current bounded path

Validation:

1. `go test ./weed/server -run "TestP(4_LivePath_RealVol_ReachesPlan|16B_RunCatchUp_)"`
2. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `Phase 16` requires not only observation closure, but also one bounded
     active recovery execution path to run because the core emitted the command
2. overclaim avoided
   - only catch-up is moved behind core command ownership in this revision;
     rebuild remains explicitly outside the accepted closure
3. proof preserved
   - accepted `P4` no-core recovery tests still pass, and `15B/16A/16B` bounded
     consume-chain proofs remain green

---

#### `16B` Delivery Note Rev 3

Date: 2026-04-04
Scope: bounded rebuild execution ownership on the same recovery path

What changed:

1. pending recovery execution storage now carries engine-level catch-up / rebuild
   I/O interfaces instead of a concrete `v2bridge.Executor` only
2. `RecoveryManager.runRebuild()` now follows the same bounded pattern as
   catch-up on the core-present path:
   - plan rebuild
   - cache pending execution
   - emit `RebuildStarted`
   - execute only if `StartRebuildCommand` consumes the pending plan
3. if no fresh `StartRebuildCommand` is emitted, pending rebuild execution is
   cancelled fail-closed instead of executing implicitly
4. added focused rebuild proofs:
   - live `runRebuild()` caches pending rebuild, emits `RebuildStarted`,
     consumes `StartRebuildCommand`, and closes completion back into projection
   - no fresh command means no implicit rebuild execution

Files changed:

1. `weed/server/block_recovery.go`
   - split pending execution I/O into `engine.CatchUpIO` / `engine.RebuildIO`
   - reused shared execution helpers for rebuild ownership closure
2. `weed/server/block_recovery_test.go`
   - added bounded live-path rebuild ownership proof
   - added bounded rebuild fail-closed proof

Bounded contract:

`16B Rev 3` accepts only this:

1. bounded rebuild execution now runs from `StartRebuildCommand`
2. the corresponding rebuild completion observation still closes back into the
   core
3. if no fresh rebuild command is emitted, the pending rebuild plan does not run
   implicitly

It does not yet accept:

1. full recovery-loop closure
2. broad multi-replica rebuild ownership
3. launch / rollout readiness

Validation:

1. `go test ./weed/server -run "TestP(4_LivePath_RealVol_ReachesPlan|16B_(Run(CatchUp|Rebuild)_|StartRebuildCommand_))"`
2. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `Phase 16` now has bounded command ownership for both catch-up and rebuild
     execution on the selected recovery path
2. overclaim avoided
   - this revision proves bounded rebuild execution ownership, not full
     recovery-loop closure or broad rebuild runtime closure
3. proof preserved
   - accepted `P4` no-core recovery tests still pass, and the existing
     `15B/16A/16B` consume-chain proofs remain green

Review status:

1. this is a new working-state delivery beyond the previously reviewed
   catch-up-only checkpoint
2. external review has not yet been re-run for this widened `16B` state

---

#### `16` Checkpoint Review Note

Date: 2026-04-04

Current checkpoint judgment:

1. this is the first bounded integrated runtime checkpoint after `Phase 15`
   closeout
2. `16A` is delivered
3. `16B` has one accepted current closure:
   - live recovery observations close back into the core
   - bounded catch-up execution is core-command-driven
4. rebuild observation ingress exists, but rebuild execution ownership is not
   part of the accepted checkpoint

Recommended review target:

1. review this checkpoint as:
   - `Phase 15 delivered`
   - `16A delivered`
   - `16B current bounded closure`
2. do not review it as:
   - rebuild execution ownership
   - full rebuild execution ownership
   - full recovery-loop closure
   - launch / rollout readiness

Suggested commit boundary if review is accepted:

1. `sw-block/.private/phase/phase-15.md`
2. `sw-block/.private/phase/phase-15-log.md`
3. `sw-block/.private/phase/phase-16.md`
4. `sw-block/.private/phase/phase-16-log.md`
5. `weed/server/volume_server_block.go`
6. `weed/server/volume_server_block_test.go`
7. `weed/server/master_server_handlers_block.go`
8. `weed/server/master_block_observability_test.go`
9. `weed/server/block_recovery.go`
10. `weed/server/block_recovery_test.go`

---

#### `16B` Post-Review Fix Note

Date: 2026-04-04

Context:

1. prior `manager` review accepted widened `16B Rev 3` with two minor fixes
2. required fixes were:
   - add one positive live-path rebuild ownership proof
   - tighten `Phase 16` wording from `first bounded` to `current widened bounded`

What was changed:

1. `weed/server/block_recovery.go`
   - added a minimal test hook so focused tests can override freshly cached
     pending rebuild I/O without changing production ownership semantics
2. `weed/server/block_recovery_test.go`
   - replaced the seeded positive rebuild proof with
     `TestP16B_RunRebuild_UsesCoreStartRebuildCommandOnLivePath`
   - the test now proves the full live chain:
     `runRebuild()` -> pending rebuild cached -> `RebuildStarted` ->
     `StartRebuildCommand` -> adapter execution -> rebuild completion
3. `sw-block/.private/phase/phase-16.md`
   - tightened wording to `current widened bounded runtime checkpoint`
4. `sw-block/.private/phase/phase-16-rev3-review.md`
   - updated evidence summary to cite the live-path rebuild proof
5. `sw-block/.private/phase/phase-16-rev3-manager-rereview.md`
   - added a bounded delta note for `manager` re-review only

Validation:

1. focused recovery suite: `PASS`
2. combined `P4/15B/16A/16B` proof suite: `PASS`
3. lints: clean

Review intent:

1. this note does not broaden `16B`
2. it only closes the two minor gaps identified by `manager`
