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

1. this is the current widened bounded runtime checkpoint after `Phase 15`
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

---

#### `16C` Start Note Rev 1

Date: 2026-04-04
Scope: bounded rebuilding-assignment entry ownership on the core-present path

Why this slice exists:

1. `16B Rev 3` closed bounded catch-up/rebuild command ownership once recovery
   execution was already on the live path
2. one remaining adapter-local rebuild trigger still existed at assignment time:
   `RoleRebuilding` assignments could call legacy `BlockService.startRebuild()`
   directly
3. assignment handling also started recovery tasks before the local assignment
   apply path had completed

What changed:

1. `weed/server/volume_server_block.go`
   - rebuilding assignments no longer directly call legacy `startRebuild()` when
     the core is present
   - rebuilding assignments now enter `coreAssignmentEvent()` and therefore use
     the same bounded `apply_role` command path as other integrated assignments
   - recovery-task startup from orchestrator results is deferred until after the
     assignment apply loop completes
   - legacy direct `startRebuild()` is now explicitly reserved for no-core
     fallback only
2. `weed/server/volume_server_block_test.go`
   - added proof that core-present rebuilding assignment:
     - does not use legacy direct rebuild start
     - still applies local rebuilding role
     - reaches core-driven `start_rebuild`
   - added proof that no-core fallback still uses legacy direct rebuild start

---

#### `16C` Delivery Note Rev 2

Date: 2026-04-04
Scope: suppress false replica-ready receiver startup on rebuilding assignment

What changed:

1. `sw-block/engine/replication/event.go`
   - `AssignmentDelivered` now carries `RecoveryTarget`
2. `sw-block/engine/replication/engine.go`
   - rebuilding assignment with `RecoveryTarget=SessionRebuild` no longer emits
     `start_receiver`
   - bootstrap reason for that bounded state is now `awaiting_rebuild_start`
     instead of `awaiting_receiver_ready`
3. `weed/server/volume_server_block.go`
   - `RoleRebuilding` assignment now marks the core assignment event with
     `SessionRebuild`
4. `weed/server/volume_server_block_test.go`
   - tightened focused proof to require exactly:
     `apply_role`, then `start_rebuild`
   - explicitly rejects `start_receiver` on the rebuilding-assignment path

Bounded contract refinement:

`16C` now additionally accepts only this narrow improvement:

1. rebuilding assignment is not misclassified as receiver-start work
2. command egress for that path is the minimum bounded set needed for:
   - local rebuilding role apply
   - rebuild execution ownership

Validation:

1. `go test ./sw-block/engine/replication/...`
2. `go test ./weed/server -run "TestBlockService_ApplyAssignments_RebuildingRole_(UsesCoreRecoveryPathWithoutLegacyDirectStart|PreservesLegacyFallbackWithoutCore)"`
3. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments_(RebuildingRole_|ExecutesCoreCommands_)|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
4. result: `PASS`

---

#### `16D` Start Note Rev 1

Date: 2026-04-04
Scope: bounded rebuild recovery-task startup ownership

Why this slice exists:

1. `16C` closed rebuilding-assignment entry ownership
2. one bounded startup gap still remained:
   - recovery goroutine startup on the core-present path still came directly from
     orchestrator `SessionsCreated` / `SessionsSuperseded`
   - the core only owned later rebuild execution (`start_rebuild`), not the task
     startup itself
3. this is a runtime-loop ownership gap, but still narrow enough to fix only for
   rebuilding-assignment startup

What changed:

1. `sw-block/engine/replication/command.go`
   - added bounded `StartRecoveryTaskCommand`
2. `sw-block/engine/replication/state.go`
   - added command dedupe state for recovery-task startup
3. `sw-block/engine/replication/engine.go`
   - rebuilding assignment now emits `start_recovery_task`
   - that command is deduped by epoch / replica / recovery kind
4. `sw-block/engine/replication/phase14_command_test.go`
   - added command proof that rebuilding assignment emits:
     - `apply_role`
     - `start_recovery_task`
     - `publish_projection`
5. `weed/server/volume_server_block.go`
   - core-present path no longer starts recovery tasks from
     `SessionsCreated` / `SessionsSuperseded`
   - adapter now executes `start_recovery_task`
   - removed-sender drain remains preserved
   - legacy no-core startup path still uses `HandleAssignmentResult()`
6. `weed/server/block_recovery.go`
   - restored `HandleAssignmentResult()` as a no-core compatibility entry
   - added bounded `StartRecoveryTask()` entry for core-command execution
7. `weed/server/volume_server_block_test.go`
   - rebuilding-assignment proof now requires:
     - `apply_role`
     - `start_recovery_task`
     - `start_rebuild`

Bounded contract:

`16D Rev 1` currently accepts only this:

1. rebuild recovery-task startup is core-command-driven on the core-present
   rebuilding-assignment path
2. adapter no longer starts that task directly from orchestrator create/supersede
   results on the bounded path
3. no-core / old `P4` live-path ownership proofs are preserved
4. those legacy `P4` proofs now serve as compatibility guards only; they are not
   the semantic authority proof for the core-present `16D` path

It does not yet accept:

1. catch-up task startup ownership
2. full recovery-loop closure
3. broad end-to-end failover/recovery/publication closure
4. multi-replica recovery ownership
5. launch / rollout readiness

Validation:

1. `go test ./sw-block/engine/replication/...`
2. `go test ./weed/server -run "TestBlockService_ApplyAssignments_RebuildingRole_(UsesCoreRecoveryPathWithoutLegacyDirectStart|PreservesLegacyFallbackWithoutCore)"`
3. `go test ./weed/server -run "TestP4_(LivePath_RealVol_ReachesPlan|SerializedReplacement_DrainsBeforeStart|ShutdownDrain)"`
4. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments_(RebuildingRole_|ExecutesCoreCommands_)|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
5. result: `PASS`

---

#### `16E` Start Note Rev 1

Date: 2026-04-04
Scope: bounded catch-up recovery-task startup ownership on the core-present path

Why this slice exists:

1. `16D` closed rebuild recovery-task startup ownership
2. the parallel catch-up startup path still started from orchestrator
   `SessionsCreated` / `SessionsSuperseded`
3. this left the current `RF=2` primary-assignment path only partially
   command-driven

What changed:

1. `sw-block/engine/replication/engine.go`
   - `start_recovery_task` dedupe now resets on assignment change
   - startup command remains bounded to a single desired replica
   - startup command ordering now places `configure_shipper` before
     `start_recovery_task` on the primary catch-up path
2. `sw-block/engine/replication/phase14_command_test.go`
   - primary assignment now proves:
     - `apply_role`
     - `configure_shipper`
     - `start_recovery_task`
     - `publish_projection`
   - assignment change proof now requires a fresh `start_recovery_task`
   - catch-up planning proof now runs from the primary catch-up shape
3. `weed/server/volume_server_block.go`
   - primary assignment now marks `RecoveryTarget=SessionCatchUp` in the core
     assignment event on the bounded single-replica path
4. `weed/server/volume_server_block_test.go`
   - added focused proof that the core-present primary path executes:
     - `apply_role`
     - `configure_shipper`
     - `start_recovery_task`
     - `start_catchup`
   - proves sender reaches `in_sync` and projection returns to `RecoveryIdle`

Bounded contract:

`16E Rev 1` currently accepts only this:

1. catch-up recovery-task startup is core-command-driven on the core-present
   primary-assignment path
2. adapter no longer starts that task directly from orchestrator create/supersede
   results on that bounded path
3. no-core / legacy `P4` compatibility remains preserved

It does not yet accept:

1. multi-replica catch-up startup ownership
2. full recovery-loop closure
3. broad end-to-end failover/recovery/publication closure
4. launch / rollout readiness

Validation:

1. `go test ./sw-block/engine/replication/...`
2. `go test ./weed/server -run "TestBlockService_ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart|RebuildingRole_PreservesLegacyFallbackWithoutCore)"`
3. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_|ExecutesCoreCommands_)|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
4. result: `PASS`

Bounded contract:

`16C Rev 1` currently accepts only this:

1. rebuilding-assignment entry no longer bypasses the core on the core-present
   path
2. local role apply precedes recovery task start on that bounded path
3. old no-core fallback is preserved

It does not yet accept:

1. full recovery-loop closure
2. broad end-to-end failover/recovery/publication closure
3. multi-replica rebuild ownership
4. launch / rollout readiness

Validation:

1. `go test ./weed/server -run "TestBlockService_ApplyAssignments_RebuildingRole_(UsesCoreRecoveryPathWithoutLegacyDirectStart|PreservesLegacyFallbackWithoutCore)"`
2. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments_(RebuildingRole_|ExecutesCoreCommands_)|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
3. result: `PASS`

---

#### `16F` Start Note Rev 1

Date: 2026-04-04
Scope: replica-scoped recovery command addressing on the bounded core-present
paths

Why this slice exists:

1. `16E` moved recovery-task startup into the core command path
2. but `start_catchup` / `start_rebuild` and pending execution still address
   only one volume-wide slot
3. that volume-scoped slot blocks any later widening toward multi-replica
   startup ownership because concurrent replica targets would overwrite each
   other

Chosen implementation rule:

1. do not yet broaden startup ownership claims
2. first make the bounded recovery execution commands replica-scoped
3. keep proof posture narrow to the already-accepted single-replica primary
   path plus the bounded rebuilding path

---

#### `16F` Delivery Note Rev 1

Date: 2026-04-04
Scope: replica-scoped recovery command addressing on the bounded core-present
paths

What changed:

1. `sw-block/engine/replication/command.go`
   - `StartCatchUpCommand` now carries `replicaID`
   - `StartRebuildCommand` now carries `replicaID`
2. `sw-block/engine/replication/state.go`
   - command memory now tracks recovery-task / catch-up / rebuild issuance by
     replica target instead of one volume-scoped slot
3. `sw-block/engine/replication/engine.go`
   - bounded catch-up/rebuild execution commands now emit replica-scoped
     addressing
   - startup breadth is still bounded; this slice does not yet claim broad
     multi-replica startup ownership
4. `sw-block/engine/replication/runtime/pending.go`
   - `PendingCoordinator` now stores and matches pending execution by replica
     target
5. `weed/server/blockcmd`
   - recovery execution dispatch now forwards replica-scoped addressing
6. `weed/server/block_recovery.go`
   - pending catch-up/rebuild execution now takes and executes plans by
     `replicaID`

Proof / evidence:

1. `go test ./...` from `sw-block/engine/replication`
2. `go test ./weed/server/blockcmd -count=1 -timeout 60s`
3. `go test ./weed/server -count=1 -timeout 120s -run "Test(P16B_|BlockService_(ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart|RebuildingRole_PreservesLegacyFallbackWithoutCore)|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded))"`
4. result: `PASS`

Conclusion:

1. the bounded core-present recovery path no longer relies on a volume-scoped
   pending slot
2. the structural blocker for any future multi-replica startup-ownership
   widening is reduced
3. this slice still does not claim broad multi-replica startup ownership or
   full recovery-loop closure

---

#### `16G` Start Note Rev 1

Date: 2026-04-04
Scope: replica-scoped recovery observation events on the bounded core-present
paths

Why this slice exists:

1. `16F` made recovery commands and pending matching replica-scoped
2. but recovery planning / completion events still only identify the volume
3. that leaves the bounded recovery loop with one remaining volume-scoped seam
   before any later multi-replica widening can be evaluated cleanly

Chosen implementation rule:

1. make bounded recovery observation events carry `replicaID`
2. make `start_catchup` / `start_rebuild` command emission consume the
   event-scoped `replicaID`
3. do not yet claim broad multi-replica recovery ownership or a per-replica
   outward projection model

---

#### `16G` Delivery Note Rev 1

Date: 2026-04-04
Scope: replica-scoped recovery observation events on the bounded core-present
paths

What changed:

1. `sw-block/engine/replication/event.go`
   - bounded recovery planning / completion events now carry `replicaID`
2. `sw-block/engine/replication/engine.go`
   - bounded `start_catchup` / `start_rebuild` command emission now consumes
     the event-scoped `replicaID`
3. `sw-block/engine/replication/runtime`
   - runtime callbacks and rebuild-commit shaping preserve replica-scoped
     addressing
4. `weed/server/block_recovery.go`
   - recovery planning / completion events emitted back into the core now carry
     the source `replicaID`

Proof / evidence:

1. `go test ./...` from `sw-block/engine/replication`
2. `go test ./weed/server -count=1 -timeout 120s -run "Test(P16B_|BlockService_(ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart|RebuildingRole_PreservesLegacyFallbackWithoutCore)|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded))"`
3. result: `PASS`

Conclusion:

1. the bounded recovery loop no longer depends on a volume-only recovery event
   seam
2. both recovery command addressing and recovery observation addressing are now
   replica-scoped on the bounded path
3. this slice still does not claim broad multi-replica startup ownership or
   full recovery-loop closure

---

#### `16H` Start Note Rev 1

Date: 2026-04-04
Scope: conservative multi-replica catch-up observation aggregation on the
bounded core-present path

Why this slice exists:

1. `16F` made bounded recovery command addressing replica-scoped
2. `16G` made bounded recovery observation events replica-scoped
3. but the volume-level recovery view can still return to `idle` too early if
   more than one replica is catching up and the first one finishes

Chosen implementation rule:

1. track bounded catch-up observation internally per replica
2. aggregate the volume-level recovery view conservatively
3. stay in `catching_up` until all bounded catch-up replicas complete
4. do not yet claim broad multi-replica startup ownership

---

#### `16H` Delivery Note Rev 1

Date: 2026-04-04
Scope: conservative multi-replica catch-up observation aggregation on the
bounded core-present path

What changed:

1. `sw-block/engine/replication/state.go`
   - added internal per-replica catch-up observation tracking
2. `sw-block/engine/replication/engine.go`
   - catch-up planning / progress / completion now aggregate volume-level
     recovery state conservatively across bounded replicas
3. `sw-block/engine/replication/phase14_boundary_test.go`
   - added focused proof that one completed replica does not return the volume
     to `idle` or `publish_healthy` while another replica is still catching up

Proof / evidence:

1. `go test ./...` from `sw-block/engine/replication`
2. result: `PASS`

Conclusion:

1. the bounded multi-replica catch-up observation path no longer overclaims
   completion after the first replica finishes
2. this slice is still only an enabling aggregation step, not broad
   multi-replica startup ownership

---

#### `16I` Start Note Rev 1

Date: 2026-04-04
Scope: bounded multi-replica primary catch-up recovery-task startup ownership

Why this slice exists:

1. `16E` only bounded `start_recovery_task` ownership on the single-replica
   primary catch-up path
2. `16F-16H` made the downstream command / event / observation seams
   replica-scoped enough to support a bounded widening
3. the remaining gap is that primary assignment delivery still only starts one
   bounded recovery task even when multiple catch-up replicas are present

Chosen implementation rule:

1. keep the widening limited to the core-present primary catch-up path
2. emit one bounded `start_recovery_task` command per desired catch-up replica
   on assignment delivery
3. do not claim broad multi-replica recovery-loop closure beyond that startup
   ownership seam

---

#### `16I` Delivery Note Rev 1

Date: 2026-04-04
Scope: bounded multi-replica primary catch-up recovery-task startup ownership

What changed:

1. `sw-block/engine/replication/engine.go`
   - widened assignment-time `start_recovery_task` emission from a single
     bounded replica to all desired bounded recovery replicas
2. `weed/server/volume_server_block.go`
   - primary assignment delivery now marks bounded catch-up recovery intent when
     any stable replica set is present, including multi-replica assignments
3. `sw-block/engine/replication/phase14_command_test.go`
   - added focused proof that primary assignment emits one recovery-task command
     per bounded replica
4. `weed/server/volume_server_block_test.go`
   - added focused proof that the host starts two bounded catch-up paths from
     two emitted `start_recovery_task` commands on the multi-replica primary
     path

Proof / evidence:

1. `go test ./...` from `sw-block/engine/replication`
2. `go test ./weed/server -count=1 -timeout 120s -run "TestBlockService_(ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|PrimaryMultiReplica_UsesCoreStartRecoveryTaskPerReplica|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart)|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)"`
3. result: `PASS`

Conclusion:

1. bounded multi-replica primary catch-up startup ownership is now
   core-command-driven on the core-present path
2. this slice still does not claim broad multi-replica recovery-loop closure or
   broad failover/publication closure
