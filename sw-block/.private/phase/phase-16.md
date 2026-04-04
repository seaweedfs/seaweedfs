# Phase 16

Date: 2026-04-04
Status: active
Purpose: close one bounded `V2`-native runtime path where the explicit core
owns runtime-driving semantics and `blockvol` remains only the execution backend

## Why This Phase Exists

`Phase 14` made the explicit core real.

`Phase 15` then rebound one bounded set of integrated `weed/` surfaces so they
consume core-owned truth instead of silently inheriting adapter-local semantics.

That means the repo now has:

1. explicit core-owned state / command / projection semantics
2. bounded integrated surface rebinding across VS, registry, HTTP, gRPC, and
   cluster status

But it still does not yet have one bounded path where runtime-driving execution
ownership itself is `V2`-native.

## Phase Goal

Close one bounded integrated runtime path where:

1. the explicit core decides the runtime-driving command sequence
2. the adapter executes those commands against `blockvol`
3. runtime observations return back into the core
4. outward surfaces continue to reflect that core-owned runtime path

## Scope

### In scope

1. one bounded command-driven adapter execution path
2. one bounded observation-feedback path from execution back into the core
3. end-to-end proof that the bounded path behaves as a `V2`-owned runtime path

### Out of scope

1. no full replacement of all `blockvol` async executors
2. no broad runtime cutover across every `weed/` path
3. no protocol rediscovery
4. no launch / rollout approval

## Phase 16 Slices

### `16A`: Command-Driven Adapter Ownership

Goal:

1. replace one adapter-owned execution decision path with core-driven command
   ownership

Acceptance object:

1. one real integrated path executes because the core emitted the command
2. the adapter no longer decides that path only from its local branching
3. proof that command emission and command execution stay aligned

Current chosen path:

1. assignment-driven `apply_role` execution now runs from core command egress
2. replica-path `start_receiver` execution follows the same bounded command path
3. primary-path `configure_shipper` execution now also follows the bounded
   command path
4. failure-side `invalidate_session` execution now follows the bounded command
   path for the integrated sender path
5. catch-up / rebuild remain outside the current `16A` closure

Status:

1. delivered

### `16B`: Runtime Observation Closure

Goal:

1. make the bounded runtime path close back into the core through explicit
   observation semantics

Acceptance object:

1. one end-to-end failover/recovery/publication scenario runs on the
   core-driven path
2. proof that outward surfaces remain consistent with the same bounded runtime
   path

Current chosen path:

1. live recovery observations now return into the core on catch-up and rebuild
   entry/exit points
2. bounded catch-up execution now runs from `StartCatchUpCommand`
3. bounded rebuild execution now runs from `StartRebuildCommand`
4. full recovery-loop closure remains outside the current bounded path

Status:

1. delivered

### `16C`: Rebuild Assignment Entry Ownership

Goal:

1. remove one remaining adapter-local rebuild entry trigger from the core-present
   path
2. serialize rebuilding assignment local apply before the recovery task starts

Acceptance object:

1. `RoleRebuilding` assignment does not directly trigger legacy
   `BlockService.startRebuild()` when the core is present
2. the same assignment still applies local role through the core-driven command
   chain
3. recovery task start happens after the local assignment command path, not
   before it
4. old no-core fallback remains preserved

Current chosen path:

1. rebuilding assignment now participates in the same core assignment command
   chain as other bounded roles
2. direct legacy `startRebuild()` is now reserved for no-core fallback only
3. orchestrator-driven recovery task start is deferred until the assignment
   apply path has completed
4. rebuilding assignment no longer emits `start_receiver` as a false
   replica-ready side effect on the core-present path

Status:

1. delivered

### `16D`: Rebuild Task Startup Ownership

Goal:

1. move bounded rebuild recovery-task startup from direct orchestrator-result
   handling into the core command path
2. preserve old no-core recovery startup behavior for legacy proofs

Acceptance object:

1. on the core-present rebuilding-assignment path, recovery goroutine startup
   happens because the core emitted a command
2. the adapter no longer starts rebuild recovery tasks directly from
   `SessionsCreated` / `SessionsSuperseded` on that bounded path
3. no-core / older `P4` live-path proofs still pass unchanged

Current chosen path:

1. rebuilding assignment now emits a bounded `start_recovery_task` command
2. adapter executes that command by starting one recovery goroutine for the
   already-attached rebuild session
3. core-present path now uses:
   - `apply_role`
   - `start_recovery_task`
   - `start_rebuild`
4. old `HandleAssignmentResult()` startup behavior is retained only for no-core
   compatibility and legacy `P4` proof preservation

Status:

1. delivered

### `16E`: Catch-Up Task Startup Ownership

Goal:

1. move bounded catch-up recovery-task startup from direct orchestrator-result
   handling into the core command path
2. keep the slice bounded to the single-replica `RF=2` chosen path

Acceptance object:

1. on the core-present primary-assignment path, catch-up recovery-task startup
   happens because the core emitted a command
2. the bounded command sequence for that path becomes:
   - `apply_role`
   - `configure_shipper`
   - `start_recovery_task`
   - `start_catchup`
3. old no-core / legacy `P4` compatibility remains preserved

Current chosen path:

1. primary assignment with one replica now marks `RecoveryTarget=SessionCatchUp`
   in the core assignment event
2. the core emits `start_recovery_task` for that bounded catch-up path
3. the adapter starts the recovery goroutine from that command, not from
   orchestrator create/supersede results
4. assignment change resets the dedupe key for recovery-task startup, so endpoint
   change / reassign still emits a fresh task-start command
5. multi-replica startup ownership remains outside the current bounded path

Status:

1. delivered

### `16F`: Replica-Scoped Recovery Command Addressing

Goal:

1. remove the remaining volume-scoped recovery command/pending slot from the
   bounded core-present recovery path
2. make `start_catchup` / `start_rebuild` address the intended replica
   explicitly, even before broad multi-replica ownership is claimed

Acceptance object:

1. bounded core-emitted recovery execution commands identify the target
   `replicaID`
2. pending recovery execution is keyed by replica target instead of a single
   volume-wide slot
3. current single-replica catch-up and rebuilding paths remain green
4. this slice does not yet claim broad multi-replica startup ownership

Current chosen path:

1. `StartCatchUpCommand` carries `replicaID` on the bounded single-replica
   primary path
2. `StartRebuildCommand` carries `replicaID` on the bounded rebuilding path
3. `PendingCoordinator` matches pending execution by replica target
4. command dispatch / recovery manager execution follow the same replica-scoped
   addressing

Status:

1. delivered

Delivered result:

1. `StartCatchUpCommand` and `StartRebuildCommand` now carry `replicaID`
2. pending recovery execution is matched by replica target instead of one
   volume-wide slot
3. the bounded single-replica primary catch-up path and bounded rebuilding path
   continue to run unchanged in behavior, but now through replica-scoped
   recovery addressing

Evidence:

1. focused working-tree change after `145327498`

### `16G`: Replica-Scoped Recovery Observation Events

Goal:

1. remove the remaining volume-scoped recovery observation addressing on the
   bounded core-present path
2. make recovery planning / completion events identify the intended `replicaID`
   explicitly

Acceptance object:

1. bounded recovery observation events carry `replicaID`
2. bounded `start_catchup` / `start_rebuild` command emission consumes the
   event-scoped `replicaID`
3. current single-replica catch-up and rebuilding proofs remain green
4. this slice still does not yet claim broad multi-replica recovery ownership

Current chosen path:

1. `CatchUpPlanned` carries `replicaID`
2. `CatchUpCompleted` carries `replicaID`
3. `NeedsRebuildObserved` / `RebuildStarted` / `RebuildCommitted` carry
   `replicaID`
4. bounded runtime helpers and host callbacks preserve that addressing

Status:

1. delivered

Delivered result:

1. bounded recovery observation events now carry `replicaID`
2. bounded `start_catchup` / `start_rebuild` command emission now consumes the
   event-scoped `replicaID`
3. current single-replica catch-up and rebuilding paths still behave the same,
   but no longer depend on a volume-only recovery event seam

Evidence:

1. focused working-tree change after `b304b8e21`

### `16H`: Multi-Replica Catch-Up Observation Aggregation

Goal:

1. keep the volume-level recovery view on the bounded core path from returning
   to `idle` too early when more than one replica is still catching up
2. make the bounded recovery projection aggregate multi-replica catch-up
   progress conservatively enough for later startup-ownership widening

Acceptance object:

1. when multiple replica-scoped catch-up observations exist for the same
   volume, the recovery phase remains `catching_up` until all bounded replicas
   complete
2. bounded durable/progress fields do not overclaim completion after only one
   replica finishes
3. current single-replica catch-up and rebuilding proofs remain green
4. this slice still does not yet claim broad multi-replica startup ownership

Current chosen path:

1. bounded catch-up observation state is tracked per replica internally
2. volume-level recovery projection aggregates that state conservatively
3. aggregate recovery idles only after all bounded catch-up replicas complete

Status:

1. delivered

Delivered result:

1. bounded catch-up observation is tracked internally per replica
2. volume-level recovery stays `catching_up` until all bounded catch-up
   replicas complete
3. bounded durable/progress fields no longer overclaim completion after the
   first replica finishes on the multi-replica catch-up path

Evidence:

1. focused working-tree change after `16ba70f85`

### `16I`: Multi-Replica Catch-Up Task Startup Ownership

Goal:

1. widen bounded catch-up recovery-task startup ownership from the single-replica
   primary path to the bounded multi-replica primary path
2. keep the slice limited to task startup ownership, not broad multi-replica
   execution closure

Acceptance object:

1. on the core-present primary path with multiple replicas, the core emits one
   bounded `start_recovery_task` command per catch-up replica
2. the adapter starts those recovery goroutines because of the emitted commands,
   not from orchestrator create/supersede results
3. current single-replica and rebuilding proofs remain green
4. this slice still does not yet claim broad multi-replica recovery-loop
   closure

Current chosen path:

1. primary assignment with `len(replicas) > 1` now marks bounded
   `RecoveryTarget=SessionCatchUp` in the core assignment event
2. core assignment command emission widens `start_recovery_task` from one
   replica to all bounded catch-up replicas on that path
3. bounded multi-replica catch-up execution still closes through the already
   replica-scoped command / observation seams from `16F-16H`

Status:

1. delivered

Delivered result:

1. primary assignment delivery now marks bounded catch-up startup intent for all
   desired replicas, not only the single-replica path
2. the core emits one bounded `start_recovery_task` command per catch-up
   replica on that widened path
3. the bounded adapter path now starts multi-replica primary catch-up recovery
   work from those emitted commands and closes back through the existing
   replica-scoped observation seams

Evidence:

1. focused working-tree change after `92c006eb2`

### `16J`: Removed-Replica Recovery Drain Ownership

Goal:

1. close one more bounded recovery-loop gap by moving removed-replica recovery
   drain ownership off the direct orchestrator-result seam and onto an explicit
   core-owned path
2. keep the slice limited to removed-replica drain / invalidation on the
   core-present path, not broad recovery-loop closure

Acceptance object:

1. on the core-present path, replica removal no longer requires
   `HandleRemovedAssignments(result)` as the primary recovery-drain trigger
2. the host drains removed recovery work because of an explicit core-owned
   command or event seam
3. current bounded startup / execution / observation proofs remain green
4. this slice still does not yet claim broad failover/publication closure

Current chosen path:

1. define one bounded core-owned seam for removed-replica recovery drain
2. rebind the core-present host path to consume that seam instead of direct
   orchestrator-result removal handling
3. keep legacy no-core compatibility unchanged

Status:

1. delivered

Delivered result:

1. the core now emits a bounded `drain_recovery_task` command when assignment
   delivery removes a previously recovery-owned replica target
2. the core-present host path drains removed recovery work from that command
   instead of using direct `orchestrator.ProcessAssignment(...).Removed` as the
   primary trigger
3. legacy no-core compatibility remains isolated in `RecoveryManager`

Evidence:

1. focused working-tree change after `5fd9ec0ed`

### `16K`: Replica-Scoped Session Invalidation

Goal:

1. close one bounded multi-replica runtime gap by making per-replica failure
   invalidation explicit instead of broad volume-wide invalidation
2. keep the slice limited to replica-scoped invalidation for replica-scoped
   recovery events, not broad failover/publication closure

Acceptance object:

1. a replica-scoped recovery failure/escalation on the core-present path can
   invalidate only the affected replica session
2. volume-wide invalidation paths remain volume-wide where the event itself is
   volume-scoped
3. current bounded startup / execution / drain proofs remain green
4. this slice still does not yet claim broad failover/publication closure

Current chosen path:

1. widen `InvalidateSessionCommand` from volume-only addressing to optional
   replica-scoped addressing
2. emit replica-scoped invalidation from replica-scoped recovery events such as
   `NeedsRebuildObserved`
3. keep `BarrierRejected` and other volume-scoped invalidation paths unchanged

Status:

1. delivered

Delivered result:

1. `InvalidateSessionCommand` now supports bounded replica-scoped addressing in
   addition to volume-wide invalidation
2. replica-scoped recovery escalation now invalidates only the affected replica
   session on the core-present path
3. volume-scoped invalidation paths such as `BarrierRejected` remain unchanged

Evidence:

1. focused working-tree change after `5fd9ec0ed`

### `16L`: PublishHealthy Rebinding

Goal:

1. close one bounded publication seam by making `weed/server`
   `PublishHealthy` surfaces reflect the core-owned publication truth rather than
   an adapter-local convenience bit
2. keep the slice limited to publication-health rebinding at the current
   server/debug/readiness boundary, not broad failover/publication closure

Acceptance object:

1. `ReadinessSnapshot.PublishHealthy` reflects core publication truth when the
   core projection exists
2. mismatch/debug/readiness surfaces no longer intentionally exclude
   `PublishHealthy` from the core-owned publication owner
3. call sites that really need readiness/eligibility rather than publication
   health are updated to use the correct readiness field
4. this slice still does not yet claim broad failover/publication closure

Current chosen path:

1. rebind `PublishHealthy` from adapter-local state to core
   `Publication.Healthy` on the core-present path
2. update boundary comments and focused tests to match the rebinding
3. keep adapter-local readiness booleans only for truly local readiness state

Status:

1. delivered

Delivered result:

1. `ReadinessSnapshot.PublishHealthy` now mirrors core
   `Publication.Healthy` when the core projection exists
2. `CoreProjectionMismatches` no longer treats `PublishHealthy` as an excluded
   publication seam
3. call sites that only needed readiness/eligibility now use readiness fields
   rather than publication health as a proxy

Evidence:

1. focused working-tree change after `43dbebfa0`

### `16M`: ReplicaReady Heartbeat Truth Rebinding

Goal:

1. close one bounded failover/publication seam by making replica heartbeat
   consume carry an explicit `ReplicaReady` truth rather than forcing the master
   registry to infer readiness from replica transport address presence
2. keep the slice limited to the current heartbeat wire and master-registry
   consume path, not broad failover/promotion closure

Acceptance object:

1. `BlockVolumeInfoMessage` carries an explicit replica-ready bit on the
   heartbeat wire
2. `weed/server` heartbeat emission sets that bit from the same core-owned
   readiness truth already used at the server boundary on the core-present path
3. `master_block_registry` consumes explicit replica readiness from heartbeat
   first and uses address presence only as a backward-compat fallback
4. focused proofs show master-side `ReplicaReady` and `VolumeMode` follow the
   explicit heartbeat truth rather than a transport-address heuristic
5. this slice still does not yet claim broad failover/promotion closure

Current chosen path:

1. widen `master.proto` / heartbeat conversion with an additive
   `replica_ready` field
2. emit that field from `CollectBlockVolumeHeartbeat` using the current bounded
   core-owned readiness gate
3. update registry consume and focused tests without broadening into unrelated
   promotion logic

Status:

1. delivered

Delivered result:

1. `BlockVolumeInfoMessage` now carries additive explicit `replica_ready`
   heartbeat truth on the wire
2. `weed/server` heartbeat emission now exports explicit bounded
   `ReplicaReady` truth from the same core-owned readiness gate already used at
   the server boundary
3. `master_block_registry` now prefers explicit heartbeat `ReplicaReady` when
   present and falls back to transport-address inference only for older
   heartbeats without the field
4. focused proofs now show master-side `ReplicaReady` and `VolumeMode` follow
   explicit heartbeat truth rather than transport-address presence alone

Evidence:

1. focused working-tree change after `16L` closeout

### `16N`: NeedsRebuild Heartbeat Mode Preservation

Goal:

1. close one bounded failover/publication seam by preserving explicit
   `needs_rebuild` truth across the primary heartbeat/master consume boundary
   instead of collapsing it into a generic degraded bit
2. keep the slice limited to `needs_rebuild` preservation on the heartbeat wire
   and master-registry consume path, not broad `VolumeMode` rebinding

Acceptance object:

1. `BlockVolumeInfoMessage` carries an additive explicit `needs_rebuild` bit on
   the heartbeat wire
2. `weed/server` heartbeat emission sets that bit from the current core-owned
   mode truth on the core-present path
3. `master_block_registry` consumes explicit heartbeat `needs_rebuild` truth
   before the older replica-role / degraded-bit heuristic
4. focused proofs show primary `needs_rebuild` survives heartbeat/master consume
   even when the old heuristic would only yield `degraded`
5. this slice still does not yet claim broad `VolumeMode` heartbeat ownership or
   broad failover closure

Current chosen path:

1. widen `master.proto` / heartbeat conversion with an additive
   `needs_rebuild` field
2. emit that field from `CollectBlockVolumeHeartbeat` using the bounded core
   mode on the primary path
3. let master consume prefer explicit `needs_rebuild` truth while retaining the
   previous heuristic as backward-compatible fallback

Status:

1. delivered

Delivered result:

1. `BlockVolumeInfoMessage` now carries additive explicit `needs_rebuild`
   heartbeat truth on the wire
2. `weed/server` heartbeat emission now preserves explicit bounded
   `needs_rebuild` truth from the core-owned mode on the current core-present
   path
3. `master_block_registry` now prefers explicit heartbeat `needs_rebuild` truth
   over the older collapsed degraded-bit / replica-role heuristic while keeping
   the previous heuristic as backward-compatible fallback when the field is
   absent
4. focused proofs now show primary `needs_rebuild` survives heartbeat/master
   consume as `needs_rebuild` rather than collapsing into generic `degraded`

Evidence:

1. focused working-tree change after `16M` closeout

### `16O`: PublishHealthy Heartbeat Mode Preservation

Goal:

1. close one bounded failover/publication seam by preserving explicit
   `publish_healthy` truth across the primary heartbeat/master consume boundary
   instead of reconstructing that mode only from secondary readiness/degraded
   heuristics
2. keep the slice limited to publication-health preservation on the current
   heartbeat wire and master-registry consume path, not broad full
   `VolumeMode` heartbeat ownership

Acceptance object:

1. `BlockVolumeInfoMessage` carries an additive explicit `publish_healthy` bit
   on the heartbeat wire
2. `weed/server` heartbeat emission sets that bit from the current core-owned
   publication truth on the core-present path
3. `master_block_registry` consumes explicit heartbeat `publish_healthy` truth
   before reconstructing healthy publication from secondary heuristics
4. focused proofs show primary healthy publication survives heartbeat/master
   consume as explicit publication truth
5. this slice still does not yet claim full `VolumeMode` heartbeat ownership or
   broad failover closure

Current chosen path:

1. widen `master.proto` / heartbeat conversion with an additive
   `publish_healthy` field
2. emit that field from `CollectBlockVolumeHeartbeat` using the bounded core
   publication truth on the primary path
3. let master consume prefer explicit healthy publication truth while retaining
   the previous reconstruction as backward-compatible fallback

Status:

1. delivered

Delivered result:

1. `BlockVolumeInfoMessage` now carries additive explicit `publish_healthy`
   heartbeat truth on the wire
2. `weed/server` heartbeat emission now preserves explicit bounded
   healthy-publication truth from the core-owned publication owner on the
   current core-present path
3. `master_block_registry` now prefers explicit heartbeat `publish_healthy`
   truth over secondary readiness/degraded reconstruction while keeping the
   previous reconstruction as backward-compatible fallback when the field is
   absent
4. focused proofs now show primary healthy publication survives heartbeat/master
   consume as explicit publication truth rather than only as reconstructed mode

Evidence:

1. focused working-tree change after `16N` closeout

### `16P`: VolumeMode Heartbeat Truth Rebinding

Goal:

1. close one bounded failover/publication seam by preserving explicit
   `volume_mode` truth across the heartbeat/master boundary instead of
   reconstructing outward mode from multiple secondary heartbeat bits
2. keep the slice limited to outward `VolumeMode` preservation on the current
   heartbeat wire and master-registry consume path, not broader restart/failover
   closure

Acceptance object:

1. `BlockVolumeInfoMessage` carries an additive explicit `volume_mode` field on
   the heartbeat wire
2. `weed/server` heartbeat emission sets that field from the current core-owned
   mode truth on the core-present path
3. `master_block_registry` prefers explicit heartbeat `volume_mode` truth for
   outward mode while retaining the previous reconstruction as backward-compatible
   fallback when the field is absent
4. focused proofs show primary `bootstrap_pending`, `degraded`,
   `needs_rebuild`, and `publish_healthy` survive heartbeat/master consume as
   explicit outward mode truth
5. this slice still does not yet claim broad restart/disturbance or launch
   closure

Current chosen path:

1. widen `master.proto` / heartbeat conversion with an additive `volume_mode`
   field
2. emit that field from `CollectBlockVolumeHeartbeat` using the bounded core
   mode on the core-present path
3. let master outward `VolumeMode` prefer explicit heartbeat mode truth and keep
   the previous reconstruction only as backward-compatible fallback

Status:

1. delivered

Delivered result:

1. `BlockVolumeInfoMessage` now carries additive explicit `volume_mode` truth on
   the heartbeat wire
2. `weed/server` heartbeat emission now preserves explicit bounded
   core-owned `VolumeMode` truth on the current core-present path
3. `master_block_registry` now prefers explicit heartbeat `volume_mode` for
   outward mode while keeping the previous reconstruction as backward-compatible
   fallback when the field is absent
4. focused proofs now show primary `bootstrap_pending`, `degraded`,
   `needs_rebuild`, and `publish_healthy` survive heartbeat/master consume as
   explicit outward mode truth

Evidence:

1. focused working-tree change after `16O` closeout

### `16Q`: VolumeModeReason Heartbeat Truth Rebinding

Goal:

1. close one bounded failover/publication seam by preserving explicit
   `volume_mode_reason` truth across the heartbeat/master boundary instead of
   dropping the reason behind outward mode transitions
2. keep the slice limited to outward `VolumeModeReason` preservation on the
   current heartbeat wire and master-registry/API path, not broader restart or
   launch closure

Acceptance object:

1. `BlockVolumeInfoMessage` carries an additive explicit `volume_mode_reason`
   field on the heartbeat wire
2. `weed/server` heartbeat emission sets that field from the current core-owned
   mode/publication reason on the core-present path
3. `master_block_registry` and outward volume-info surfaces prefer explicit
   heartbeat `volume_mode_reason` truth while retaining empty/default behavior as
   backward-compatible fallback when the field is absent
4. focused proofs show primary bootstrap/degraded/needs_rebuild reason survives
   heartbeat/master consume as explicit outward reason truth
5. this slice still does not yet claim broad restart/disturbance or launch
   closure

Current chosen path:

1. widen `master.proto` / heartbeat conversion with an additive
   `volume_mode_reason` field
2. emit that field from `CollectBlockVolumeHeartbeat` using the bounded core
   reason on the core-present path
3. let master outward `VolumeModeReason` prefer explicit heartbeat reason truth
   and keep empty/default behavior as backward-compatible fallback

Status:

1. delivered

Delivered result:

1. heartbeat wire now carries additive explicit `volume_mode_reason` truth
2. volume-server heartbeat emission now preserves bounded core mode/publication
   reason on the core-present path
3. master consume and outward volume-info/API surfaces now preserve explicit
   `VolumeModeReason` truth instead of dropping the reason behind outward mode
   transitions
4. older heartbeats without the new field still keep empty/default behavior

Evidence:

1. focused working-tree change after `16P` closeout

### `16R`: Restart Auto-Register Truth Preservation

Goal:

1. close one bounded restart/disturbance seam by preserving explicit primary
   heartbeat truth when the master reconstructs a volume entry from a fresh
   registry after restart
2. keep the slice limited to master-side auto-register consume of already
   explicit heartbeat truth, not broader restart or failover closure

Acceptance object:

1. the master restart auto-register path preserves explicit primary heartbeat
   `needs_rebuild`, `publish_healthy`, `volume_mode`, and `volume_mode_reason`
   truth on the new registry entry
2. outward volume-info surfaces preserve that explicit truth after restart
   reconstruction instead of collapsing back to heuristic defaults
3. focused proofs show fresh-registry heartbeat reconstruction preserves the
   bounded explicit primary truth
4. this slice still does not yet claim broad restart/disturbance or launch
   closure

Current chosen path:

1. widen `UpdateFullHeartbeat` auto-register so fresh-registry reconstruction
   consumes the same explicit primary truth that existing-entry consume already
   prefers
2. prove restart reconstruction preserves outward mode/reason semantics on the
   bounded path

Status:

1. delivered

Delivered result:

1. master restart auto-register now preserves explicit primary heartbeat
   `needs_rebuild`, `publish_healthy`, `volume_mode`, and
   `volume_mode_reason` truth
2. fresh-registry reconstruction no longer drops those explicit fields and then
   falls back immediately to older heuristics
3. outward volume-info surfaces now preserve explicit mode/reason truth after
   bounded restart reconstruction

Evidence:

1. focused working-tree change after `16Q` closeout

### `16S`: Missing-Field Truth Retention

Goal:

1. close one bounded disturbance/compat seam by ensuring a primary heartbeat
   that omits newer explicit fields does not erase already accepted explicit
   truth on the master side
2. keep the slice limited to missing-field retention for bounded primary
   heartbeat truth, not entry deletion or broad restart policy

Acceptance object:

1. once explicit primary heartbeat `needs_rebuild`, `publish_healthy`,
   `volume_mode`, or `volume_mode_reason` truth has been accepted on an existing
   entry, a later heartbeat that omits those fields does not clear it
2. fresh entries with absent fields still keep backward-compatible fallback
   behavior
3. focused proofs show missing-field heartbeats preserve accepted explicit truth
   on the bounded primary path
4. this slice still does not yet claim broad restart/disturbance or launch
   closure

Current chosen path:

1. make existing-entry primary heartbeat consume preserve prior explicit truth
   when the corresponding field is absent
2. keep auto-register/fresh-entry behavior unchanged so absent fields still mean
   no explicit truth on first observation

Status:

1. delivered

Delivered result:

1. existing-entry primary consume now preserves already accepted explicit
   `needs_rebuild`, `publish_healthy`, `volume_mode`, and
   `volume_mode_reason` truth when a later heartbeat omits those fields
2. fresh-entry auto-register behavior remains backward-compatible: absent fields
   still do not invent explicit truth on first observation
3. bounded outward mode/reason surfaces no longer regress merely because a
   later heartbeat is field-sparse

Evidence:

1. focused working-tree change after `16R` closeout

### `16T`: Authoritative Empty-Inventory Guard

Goal:

1. close one bounded disturbance seam by separating authoritative empty block
   inventory from non-authoritative empty heartbeat observation on the
   master/volume-server seam
2. keep the slice limited to stale-delete eligibility for full block heartbeat
   reconciliation, not broader restart policy

Acceptance object:

1. full block heartbeat carries an additive explicit signal for whether the
   reported block inventory is authoritative
2. master stale-delete on full heartbeat runs only when the block inventory is
   authoritative
3. bounded proofs show non-authoritative empty full heartbeat does not delete an
   existing primary entry, while authoritative empty full heartbeat retains the
   old delete behavior
4. this slice still does not yet claim broad restart/disturbance or launch
   closure

Current chosen path:

1. widen `Heartbeat` with an additive `block_volume_inventory_authoritative`
   field
2. keep regular and shutdown-originated empty block inventory heartbeats
   authoritative on the current chosen path
3. make master full-heartbeat stale cleanup conditional on that explicit signal

Status:

1. delivered

Delivered result:

1. full block heartbeat now carries an explicit
   `block_volume_inventory_authoritative` signal
2. master full-heartbeat stale cleanup now runs only when block inventory is
   authoritative
3. current chosen-path regular and shutdown-originated empty block inventory
   heartbeats remain authoritative, while non-authoritative empty inventory can
   now be preserved without deleting entries

Evidence:

1. focused working-tree change after `16S` closeout

### `16U`: Real Non-Authoritative Inventory Path

Goal:

1. close one bounded runtime seam by making the new
   `block_volume_inventory_authoritative` signal true on a real sender-side
   path, not only as an unused wire capability
2. keep the slice limited to one truthful sender-side non-authoritative path,
   not broad restart policy

Acceptance object:

1. one real block-service startup path emits
   `block_volume_inventory_authoritative=false`
2. focused proofs show that sender-side path and the master-side authoritative
   guard compose correctly
3. this slice still does not yet claim broad restart-window policy

Current chosen path:

1. treat startup block-inventory scan failure as a truthful
   non-authoritative-inventory condition
2. emit that condition on the ordinary full block heartbeat path

Status:

1. delivered

Delivered result:

1. startup scan failure now yields a real sender-side
   non-authoritative block inventory heartbeat
2. the new authority bit is no longer only a wire capability; it now carries one
   real bounded runtime meaning

Evidence:

1. focused working-tree change after `16T` closeout

### `16V`: Restart Primary-Truth Rebase

Goal:

1. close one bounded restart seam by ensuring explicit primary truth is rebased
   to the winning primary during restart reconciliation
2. keep the slice limited to primary-swap explicit truth handling, not broad
   restart policy

Acceptance object:

1. when restart reconciliation promotes a new primary, explicit
   `needs_rebuild`, `publish_healthy`, `volume_mode`, and `volume_mode_reason`
   truth reflect the winning primary heartbeat
2. stale explicit truth from the old primary does not survive primary swap
3. sparse winning-primary heartbeats clear old explicit truth instead of
   retaining it incorrectly

Current chosen path:

1. rebase or clear explicit primary truth during `demoteExistingToReplica`
2. recompute outward surfaces immediately after primary swap

Status:

1. delivered

Delivered result:

1. restart reconciliation now rebinds explicit primary truth to the winning
   primary heartbeat
2. sparse winning-primary heartbeats no longer leave stale old-primary outward
   truth attached to the entry

Evidence:

1. focused working-tree change after `16T` closeout

### `16W`: Replica Explicit-Readiness Retention

Goal:

1. close one bounded replica-side compat seam by making accepted explicit
   `ReplicaReady` truth survive later sparse replica heartbeats
2. keep the slice limited to replica readiness presence/retention, not broad
   replica policy

Acceptance object:

1. once explicit replica readiness is accepted for an existing replica entry, a
   later sparse heartbeat does not revert it silently to address heuristics
2. fresh replica observations without explicit readiness still keep the old
   backward-compatible address fallback
3. focused proofs show replica-side readiness now follows the same bounded
   explicit-truth discipline as the primary side

Current chosen path:

1. track whether a replica entry has accepted explicit readiness
2. preserve that truth on sparse heartbeats only for existing replica entries
3. keep fresh-entry fallback unchanged

Status:

1. delivered

Delivered result:

1. existing replica entries now preserve accepted explicit `ReplicaReady` truth
   across sparse heartbeats
2. fresh replica entries still use address fallback when no explicit readiness
   has yet been observed

Evidence:

1. focused working-tree change after `16T` closeout

## Current Checkpoint Review Target

The current review target is the current widened bounded runtime checkpoint
after `Phase 15` closeout:

1. `Phase 15` delivered:
   - bounded surface/store/outward consume-chain rebinding
2. `16A` delivered:
   - bounded command-driven adapter ownership for:
     - `apply_role`
     - `start_receiver`
     - `configure_shipper`
     - `invalidate_session`
3. previously reviewed `16B` closure:
   - live recovery observations return into the core
   - bounded catch-up execution runs from `StartCatchUpCommand`
4. current working state extends that bounded path with:
   - bounded rebuild execution from `StartRebuildCommand`
   - rebuilding assignment entry ownership
   - rebuild recovery-task startup ownership
   - bounded catch-up recovery-task startup ownership on the single-replica
     primary path
   - replica-scoped recovery command addressing on those same bounded paths
   - conservative multi-replica catch-up observation aggregation on those same
     bounded paths
   - bounded multi-replica catch-up recovery-task startup ownership on the
     primary path

This checkpoint is intentionally still bounded:

1. broad recovery-loop closure is not yet claimed
2. broad end-to-end failover/recovery/publication proof is not yet claimed
3. broad multi-replica startup ownership is not yet claimed
4. launch / rollout readiness is not claimed

## Immediate Next Step

The current checkpoint is now good enough to take as the next stage/review
boundary:

1. `Phase 15` delivered
2. `16A` delivered
3. `16B` bounded recovery execution ownership:
   - live recovery observations close back into the core
   - bounded catch-up execution is core-command-driven
   - bounded rebuild execution is core-command-driven
4. `16C` delivered:
   - rebuilding assignment entry no longer bypasses the core
   - rebuilding assignment no longer emits false `start_receiver`
5. `16D` delivered:
   - rebuild recovery-task startup is core-command-driven
6. `16E` delivered:
   - catch-up recovery-task startup is core-command-driven on the
     single-replica primary path
7. `16F` delivered:
   - recovery execution commands / pending matching are replica-scoped on the
     same bounded paths
8. `16G` delivered:
   - recovery observation events are replica-scoped on those same bounded paths
9. `16H` delivered:
   - multi-replica catch-up observation is aggregated conservatively at the
     volume projection layer
10. `16I` delivered:
   - multi-replica primary catch-up startup ownership is core-command-driven
11. `16J` delivered:
   - removed-replica recovery drain is core-command-driven on the core-present
     path
12. `16K` delivered:
   - replica-scoped recovery invalidation no longer depends on a remaining
     volume-wide invalidation seam
13. `16L` delivered:
   - `PublishHealthy` is rebound from adapter-local status to the core-owned
     publication owner at the server boundary
14. `16M` delivered:
   - replica heartbeat/master consume now carries explicit bounded
     `ReplicaReady` truth with backward-compatible fallback for older
     heartbeats
15. `16N` delivered:
   - primary heartbeat/master consume now preserves explicit bounded
     `needs_rebuild` truth with backward-compatible fallback for older
     heartbeats
16. `16O` delivered:
   - primary heartbeat/master consume now preserves explicit bounded
     healthy-publication truth with backward-compatible fallback for older
     heartbeats
17. `16P` delivered:
   - heartbeat/master consume now preserves explicit bounded `VolumeMode` truth
     with backward-compatible fallback for older heartbeats
18. `16Q` delivered:
   - heartbeat/master/API path now preserves explicit bounded
     `VolumeModeReason` truth instead of dropping outward mode reasons at the
     master boundary
19. `16R` delivered:
   - master restart auto-register consume now preserves the same explicit
     primary heartbeat truth as the steady-state consume path
20. `16S` delivered:
   - steady-state primary consume now preserves already accepted explicit
     primary truth when later heartbeats omit those fields
21. `16T` delivered:
   - full heartbeat stale-delete now depends on an explicit block-inventory
     authoritative signal instead of empty inventory alone
22. `16U` delivered:
   - one real sender-side startup path now emits non-authoritative empty block
     inventory rather than leaving the new authority bit as wire-only
23. `16V` delivered:
   - restart primary swap now rebases explicit primary truth to the winning
     heartbeat instead of retaining stale old-primary truth
24. `16W` delivered:
   - replica-side accepted explicit readiness now survives sparse heartbeats
     without inventing explicit truth on fresh entries

After this checkpoint:

1. keep `legacy P4` only as a compatibility guard
2. stop the current bounded runtime closure package here unless a clearly
   smaller seam remains visible than the residual gaps below
3. do not yet claim full recovery-loop closure
4. do not broaden into launch claims

## Accepted Claim Set

At this checkpoint the bounded chosen path may now claim:

1. steady-state and restart reconstruction preserve accepted explicit primary
   heartbeat truth on the current heartbeat/master/API path
2. sparse primary and replica heartbeats no longer silently erase already
   accepted explicit truth on existing entries
3. empty full block inventory delete behavior is explicit rather than inferred
   from emptiness alone
4. these claims remain bounded to the current chosen path and do not imply broad
   runtime/product closure

## Explicit Non-Claims

This checkpoint still does NOT claim:

1. broad recovery-loop closure across all lifecycle branches
2. broad end-to-end failover/recovery/publication proof
3. full restart-window policy for all empty-inventory / not-yet-loaded cases
4. broad multi-replica startup ownership beyond the bounded proven path
5. launch / rollout readiness

## Residual Gaps After Checkpoint

The remaining visible gaps are now better treated as residuals unless they can
be cut smaller than the slices above:

1. broader recovery-loop closure
2. broader failover/publication whole-chain statement
3. long-window restart/disturbance policy and soak hardening
4. launch-envelope freeze and rollout gates

## Checkpoint Proof Commands

1. `go test ./weed/storage/blockvol -count=1 -run "TestInfoMessage_(ReplicaReady|NeedsRebuild|PublishHealthy|VolumeMode|VolumeModeReason)"`
2. `go test ./weed/server -count=1 -timeout 180s -run "Test(Registry_UpdateFullHeartbeat_(ConsumesCoreInfluencedReplicaReady|ReplicaReadyFallsBackToAddressesWhenFieldAbsent|ReplicaReadyMissingFieldPreservesAcceptedExplicitTruth|ReplicaReadyMissingFieldFreshEntryStillFallsBack|ConsumesExplicitNeedsRebuildFromPrimaryHeartbeat|NeedsRebuildFallsBackWhenFieldAbsent|ExplicitHealthySuppressesStaleNeedsRebuildHeuristic|ConsumesExplicitPublishHealthyFromPrimaryHeartbeat|ExplicitUnhealthySuppressesStalePublishHealthyHeuristic|ConsumesExplicitVolumeModeFromPrimaryHeartbeat|VolumeModeFallsBackWhenFieldAbsent|AutoRegisterPreservesExplicitPrimaryTruthOnRestart|MissingFieldsPreserveAcceptedExplicitPrimaryTruth|MissingFieldsDoNotInventExplicitTruthOnFreshEntry)|MasterRestart_(HigherEpochWins|HigherEpochRebasesExplicitPrimaryTruth|HigherEpochSparsePrimaryClearsOldExplicitTruth|LowerEpochBecomesReplica|SameEpoch_HigherLSNWins|SameEpoch_SameLSN_ExistingWins|SameEpoch_RoleTrusted)|StartBlockService_ScanFailureEmitsNonAuthoritativeInventory|CollectBlockVolumeHeartbeat_IncludesInventoryAuthority|Registry_UpdateFullHeartbeatWithInventoryAuthority_(NonAuthoritativeEmptyDoesNotDelete|AuthoritativeEmptyStillDeletes)|P11P3_HeartbeatReconstruction|P12P1_Restart_SameLineage)"`
3. `go test ./weed/server -count=1 -timeout 180s -run "Test(EntryToVolumeInfo_(ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(Get|List)Handler_ReflectsCoreInfluencedDegradedConsume|Master_ExpandCoordinated_B10_HeartbeatDoesNotDeleteDuringExpand|QA_Reg_FullHeartbeatEmptyServer)"`
