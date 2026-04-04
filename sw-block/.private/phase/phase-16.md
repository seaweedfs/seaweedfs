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

This checkpoint is intentionally still bounded:

1. broad recovery-loop closure is not yet claimed
2. broad end-to-end failover/recovery/publication proof is not yet claimed
3. multi-replica startup ownership is not yet claimed
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
6. `16E` current bounded refinement:
   - catch-up recovery-task startup is core-command-driven on the
     single-replica primary path
7. `16F` delivered:
   - recovery execution commands / pending matching are replica-scoped on the
     same bounded paths

After this checkpoint:

1. keep `legacy P4` only as a compatibility guard
2. the next bounded semantic/runtime decision is whether to widen startup
   ownership beyond the single-replica catch-up path now that recovery
   addressing is replica-scoped
3. do not yet claim full recovery-loop closure
4. do not broaden into launch claims
