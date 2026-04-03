Purpose: append-only technical pack and delivery log for `Phase 14` V2 core
extraction.

---

### `14A` Technical Pack

Date: 2026-04-03
Goal: freeze the first explicit `V2 core` shell inside
`sw-block/engine/replication` so current accepted semantic constraints become
executable state/event/command/projection ownership, not only design wording

#### Layer 1: Semantic Core

##### Problem statement

`Phase 13` accepted:

1. bounded replication correctness
2. bounded assignment/publication closure
3. bounded mode normalization

But those results are still interpreted mainly as:

1. constrained-`V1` runtime behavior under `V2` rules

`14A` accepts one narrower thing:

1. the first real `V2 core` semantic shell exists as code in
   `sw-block/engine/replication`

It does not accept:

1. live runtime cutover
2. adapter rebinding
3. product-surface migration
4. launch or performance claims

##### State / contract

`14A` must make these truths explicit in code:

1. one bounded `VolumeState` owns normalized mode, readiness, boundary, and
   desired replica truth
2. one bounded event set expresses assignment, readiness observation, durable
   boundary change, and rebuild escalation
3. one bounded command set expresses semantic decisions without runtime side
   effects
4. one bounded projection expresses outward publication meaning from the same
   state owner
5. the current interpretation remains:
   - explicit `V2 core` shell exists
   - integrated runtime authority is still `constrained_v1` until later phases

##### Must preserve

1. stable `ReplicaID` ownership
2. durable boundary truth is not inferred from diagnostic shipped progress
3. `publish_healthy` requires named readiness plus durable boundary closure
4. `degraded` and `needs_rebuild` remain distinct fail-closed modes
5. the code does not overclaim live `V2` runtime ownership

##### Reject shapes

Reject `14A` if:

1. the new core shell is only a naming wrapper with no deterministic state
   update path
2. `publish_healthy` can be reached from assignment or transport convenience
   without durable boundary truth
3. diagnostic sender progress is allowed to establish durable authority
4. `degraded` and `needs_rebuild` collapse into one ambiguous unhealthy bucket
5. the delivery wording implies live path cutover

#### Layer 2: Execution Core

##### Files in scope

Primary files:

1. `sw-block/engine/replication/state.go`
2. `sw-block/engine/replication/event.go`
3. `sw-block/engine/replication/command.go`
4. `sw-block/engine/replication/projection.go`
5. `sw-block/engine/replication/engine.go`
6. `sw-block/engine/replication/phase14_core_test.go`
7. `sw-block/engine/replication/doc.go`

Existing substrate kept in place:

1. `sw-block/engine/replication/registry.go`
2. `sw-block/engine/replication/sender.go`
3. `sw-block/engine/replication/session.go`
4. `sw-block/engine/replication/orchestrator.go`
5. nearby ownership/recovery tests

##### Execution order

`14A` follows the `Phase 14+` framework strictly:

1. explicit state
2. explicit events
3. explicit commands
4. explicit projection
5. deterministic engine loop
6. bounded structural tests

##### Acceptance basis

Keep the proof set small and structural:

1. identity / ownership
   - stable `ReplicaID`
   - endpoint change invalidates active ownership session
2. state eligibility
   - only eligible primary path can reach `publish_healthy`
3. durable boundary
   - barrier durability updates authority
   - diagnostic shipped progress stays diagnostic
4. fail-closed modes
   - `degraded` and `needs_rebuild` stay distinct and non-healthy
5. interpretation rule
   - the shell begins `V2 core`
   - it does not yet claim live runtime authority

##### Delivery posture

This phase uses the larger-slice execution model:

1. main developer owns semantic design and implementation
2. `sw` is used only for bounded support tasks if needed
3. `tester` validates the structural acceptance basis
4. `manager` challenges semantic adequacy and overclaim control

##### Review gate

Every `14A` code change or acceptance note should answer:

1. semantic constraint satisfied
2. overclaim avoided
3. accepted proof preserved

#### Starting point inventory

Current explicit shell already present in repo:

1. `state.go`
   - `RuntimeAuthority`
   - `VolumeRole`
   - `ModeName`
   - `ReadinessView`
   - `BoundaryView`
   - `ModeView`
   - `VolumeState`
2. `event.go`
   - assignment
   - readiness observation
   - barrier accepted / rejected
   - checkpoint advance
   - rebuild observation / commit
3. `command.go`
   - `ApplyRoleCommand`
   - `StartReceiverCommand`
   - `ConfigureShipperCommand`
   - `InvalidateSessionCommand`
   - `PublishProjectionCommand`
4. `projection.go`
   - `PublicationProjection`
5. `engine.go`
   - deterministic `ApplyEvent()`
   - recompute mode/readiness/publication
   - emit bounded commands and projection
6. `phase14_core_test.go`
   - structural acceptance basis for the shell

#### Immediate development target

The next development target under `14A` is not to broaden the shell.

It is to make the shell the clear semantic owner for the first complete chain:

1. `mode`
2. `readiness`
3. `publication`

and verify the package stays internally coherent before `14B` begins.

#### Verification status

Current package verification on 2026-04-03:

1. `go test ./...` in `sw-block/engine/replication`
2. result: `PASS`
3. interpretation:
   - the current explicit shell is a valid starting point for `Phase 14`
   - this verifies bounded internal coherence only
   - this does not claim live runtime cutover

---

### `14A` Delivery Note Rev 1

Date: 2026-04-03
Scope: strengthen the first `mode -> readiness -> publication` chain inside the
explicit `V2 core` shell without adding any live adapter hook

What changed:

1. publication is now explicit core-owned state, not only an implicit boolean
   threaded through readiness/projection
2. the engine now emits normalized publication-gate reasons for bootstrap and
   non-primary states
3. `RF=1 / no replicas -> allocated_only` is now frozen directly in the core
   shell, aligning the code with accepted `CP13-9` semantics

Files changed:

1. `sw-block/engine/replication/state.go`
   - added `PublicationView`
   - `VolumeState` now owns publication truth explicitly
2. `sw-block/engine/replication/projection.go`
   - `PublicationProjection` now carries explicit publication state
3. `sw-block/engine/replication/engine.go`
   - split publication recompute away from raw readiness bits
   - added explicit gate reasons:
     - `awaiting_role_apply`
     - `awaiting_shipper_configured`
     - `awaiting_shipper_connected`
     - `awaiting_barrier_durability`
     - `replica_not_primary`
     - `allocated_only`
   - enforced `no replicas => allocated_only`
4. `sw-block/engine/replication/phase14_core_test.go`
   - strengthened the primary publication chain proof with gate-reason checks
   - strengthened replica-ready proof with non-primary publication reason
   - added direct `allocated_only` proof for no-replica path

Proofs added or strengthened:

1. primary publication closure proof
   - assignment -> role applied -> shipper configured -> shipper connected ->
     barrier durability now produces the expected gate reason at each stage
2. replica-ready is not publication proof
   - `replica_ready` stays non-healthy with explicit reason
     `replica_not_primary`
3. `CP13-9` allocated-only proof
   - a primary assignment with no replicas remains `allocated_only`, not
     `bootstrap_pending`

Validation:

1. `gofmt -w state.go projection.go engine.go phase14_core_test.go`
2. `go test ./...`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `CP13-8A`: assignment/readiness/publication closure must be explicit
   - `CP13-9`: `allocated_only`, `bootstrap_pending`, `replica_ready`,
     `publish_healthy`, `degraded`, and `needs_rebuild` must stay bounded and
     non-overlapping
2. overclaim avoided
   - publication health can no longer be inferred from assignment presence,
     shipper connection alone, or replica readiness
   - RF=1/no-replica path no longer overclaims `bootstrap_pending`
3. proof preserved
   - barrier durability remains the authority for `publish_healthy`
   - diagnostic shipped progress remains non-authoritative
   - constrained-`V1` runtime interpretation remains explicit

---

### `14B` Delivery Note Rev 1

Date: 2026-04-03
Scope: freeze first bounded command-emission rules so the explicit `V2 core`
decides commands from semantic gaps, not from repeated event convenience

What changed:

1. repeated assignments no longer blindly reset semantic state and re-emit the
   same commands
2. command emission is now gap-driven:
   - apply role only when epoch/role command state is stale
   - start receiver only when replica path still needs receiver start for the
     current epoch
   - configure shipper only when primary path still needs current replica
     configuration
   - invalidate session only on a new failure transition, not every repeated
     degraded event
3. assignment changes still re-emit the needed command when semantic intent
   really changes

Files changed:

1. `sw-block/engine/replication/state.go`
   - added private command-state tracking to `VolumeState`
2. `sw-block/engine/replication/engine.go`
   - extracted assignment handling into gap-driven command logic
   - preserved readiness when the assignment is repeated without semantic change
   - reset only the relevant readiness edges when role/epoch/replica-set changes
   - deduplicated repeated invalidation commands for the same failure reason
3. `sw-block/engine/replication/phase14_command_test.go`
   - added exact command-sequence proofs

Proofs added:

1. primary repeated-assignment boundedness
   - first assignment emits:
     - `apply_role`
     - `configure_shipper`
     - `publish_projection`
   - repeated identical assignment emits only:
     - `publish_projection`
2. replica repeated-assignment boundedness
   - first replica assignment emits:
     - `apply_role`
     - `start_receiver`
     - `publish_projection`
   - repeated identical assignment emits only:
     - `publish_projection`
3. assignment-change selective reissue
   - changed replica endpoint on primary path reissues only
     `configure_shipper`, not the whole initial command bundle
4. repeated-failure boundedness
   - first `BarrierRejected(timeout)` emits `invalidate_session`
   - repeated `BarrierRejected(timeout)` does not emit duplicate invalidation

Validation:

1. `gofmt -w state.go engine.go phase14_command_test.go`
2. `go test ./...`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `Phase 14B`: command emission must come from semantic state, not runtime
     convenience
   - `CP13-8A`: assignment/readiness/publication closure must stay explicit
   - `CP13-9`: bounded mode meaning must not be destabilized by repeated command
     churn
2. overclaim avoided
   - repeated assignment no longer acts like proof that role apply / receiver
     start / shipper configure still need to happen
   - repeated failure does not create unbounded invalidation spam that looks like
     fresh semantic transitions
3. proof preserved
   - `14A` publication-gate proofs still hold
   - barrier durability is still the only path to `publish_healthy`
   - constrained-`V1` interpretation is still explicit, not broadened

---

### `14B` Delivery Note Rev 2

Date: 2026-04-03
Scope: tighten `publish_projection` so it is also emitted from semantic change,
not from raw event frequency

What changed:

1. `PublishProjectionCommand` is now emitted only when the outward projection
   actually changes
2. repeated identical events on an already-converged state now become true
   no-op command sequences

Files changed:

1. `sw-block/engine/replication/engine.go`
   - compare previous and new projection
   - emit `publish_projection` only on real outward change
2. `sw-block/engine/replication/phase14_command_test.go`
   - repeated identical primary assignment now expects no commands
   - repeated identical replica assignment now expects no commands
   - repeated identical failure now expects no commands after the first
     invalidation
   - added direct proof that repeated unchanged projection events emit no
     `publish_projection`

Proofs strengthened:

1. repeated identical assignment is now a true no-op command sequence
2. repeated identical failure is now a true no-op command sequence
3. publish emission is now tied to projection change, not event arrival

Validation:

1. `gofmt -w engine.go phase14_command_test.go`
2. `go test ./...`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `14B`: command emission is further frozen to semantic deltas only
2. overclaim avoided
   - repeated identical events no longer look like fresh publication work
   - projection emission no longer overstates outward change when nothing changed
3. proof preserved
   - all `14A` and `14B` proofs still pass
   - publication remains bounded by the same explicit state owner

---

### `14C` Delivery Note Rev 1

Date: 2026-04-03
Scope: make the first bounded boundary/recovery truths explicit in the core
shell so recovery-in-progress and rebuild closure affect mode/publication
semantics directly

What changed:

1. `BoundaryView` now carries more explicit boundary truth:
   - `CommittedLSN`
   - `TargetLSN`
   - `AchievedLSN`
   - plus the previously separated durable/checkpoint/diagnostic fields
2. `RecoveryView` is now an explicit core-owned state with bounded phases:
   - `idle`
   - `catching_up`
   - `needs_rebuild`
   - `rebuilding`
3. the event vocabulary now includes:
   - `CommittedLSNAdvanced`
   - `CatchUpPlanned`
   - `RecoveryProgressObserved`
   - `RebuildStarted`
   - extended `RebuildCommitted` with explicit achieved boundary support
4. recovery-in-progress now blocks `replica_ready` / publication overclaim
   through mode recompute:
   - active catch-up or rebuild forces `bootstrap_pending`
     with reason `recovery_in_progress`
   - rebuild-required stays `needs_rebuild`

Files changed:

1. `sw-block/engine/replication/state.go`
   - added explicit `RecoveryView`
   - expanded `BoundaryView`
2. `sw-block/engine/replication/event.go`
   - added boundary/recovery events
3. `sw-block/engine/replication/engine.go`
   - boundary truth is now updated explicitly and monotonically
   - recovery state now participates directly in mode/publication recompute
   - assignment changes clear stale recovery target/achieved truth
4. `sw-block/engine/replication/phase14_boundary_test.go`
   - added structural boundary/recovery proofs

Proofs added:

1. boundary-truth separation
   - `CommittedLSN`, `CheckpointLSN`, `DurableLSN`, and diagnostic shipped
     progress remain distinct truths
2. catch-up blocks ready overclaim
   - a replica with role applied + receiver ready still falls back to
     `bootstrap_pending` with reason `recovery_in_progress` while catch-up is
     active
3. rebuild boundary closure
   - `needs_rebuild` -> `rebuilding` -> `idle` is explicit in recovery truth
   - rebuild commit aligns achieved/durable/checkpoint boundaries
   - rebuild completion on replica returns to `replica_ready`, not
     `publish_healthy`

Validation:

1. `gofmt -w state.go event.go engine.go phase14_boundary_test.go`
2. `go test ./...`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - `CP13-3`: durable truth remains distinct from diagnostic sender progress
   - `CP13-7`: rebuild is explicit fail-closed truth, not an ambiguous degraded
     tail
   - `T14`: engine owns recovery policy and meaning, not backend convenience
2. overclaim avoided
   - receiver-ready during catch-up no longer looks like final ready state
   - rebuild-in-progress no longer risks being interpreted as ordinary
     bootstrap/readiness closure
   - rebuild completion on replica does not overclaim publication health
3. proof preserved
   - all `14A` and `14B` proofs still pass
   - publication remains derived from explicit core-owned truth

---

### `14C` Delivery Note Rev 2

Date: 2026-04-03
Scope: close the first bounded recovery-closure gap by making catch-up
completion explicit and projecting recovery truth outward

What changed:

1. `PublicationProjection` now carries `RecoveryView`, so recovery truth is part
   of outward normalized meaning rather than hidden only in internal state
2. catch-up now has an explicit closure event:
   - `CatchUpCompleted`
3. catch-up completion now:
   - advances achieved boundary
   - advances durable boundary on the bounded replica path
   - returns recovery phase to `idle`
   - allows mode to return from `bootstrap_pending` to `replica_ready`

Files changed:

1. `sw-block/engine/replication/projection.go`
   - projection now exposes `RecoveryView`
2. `sw-block/engine/replication/event.go`
   - added `CatchUpCompleted`
3. `sw-block/engine/replication/engine.go`
   - catch-up planning resets achieved progress for the new plan
   - catch-up completion explicitly closes recovery phase and updates boundaries
4. `sw-block/engine/replication/phase14_boundary_test.go`
   - strengthened catch-up proof with completion semantics
   - strengthened rebuild proof with outward recovery projection checks

Proofs strengthened:

1. recovery truth is projection-visible
   - `catching_up`, `rebuilding`, and `idle` are now asserted through outward
     projection, not only internal state snapshots
2. catch-up completion closure
   - replica catch-up returns to `replica_ready`
   - achieved and durable boundaries converge to the explicit completed target
   - no publication-health overclaim appears

Validation:

1. `gofmt -w projection.go event.go engine.go phase14_boundary_test.go`
2. `go test ./...`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - recovery closure is now expressed as explicit core-owned truth, not timing
     intuition
2. overclaim avoided
   - catch-up no longer stays indefinitely in an ambiguous in-progress state
   - recovery truth no longer disappears from outward projection
3. proof preserved
   - `14A`, `14B`, and `14C rev 1` proofs still pass

---

### `14C` Delivery Note Rev 3

Date: 2026-04-03
Scope: turn recovery start into explicit bounded command semantics so `catch-up`
and `rebuild` are not only state/projection truth but also first-class core
decisions

What changed:

1. added explicit recovery-start commands:
   - `StartCatchUpCommand`
   - `StartRebuildCommand`
2. recovery plan/start events now emit bounded commands:
   - `CatchUpPlanned(target)` -> `start_catchup` when the target is newly needed
   - `RebuildStarted(target)` -> `start_rebuild` when the target is newly needed
3. repeated identical recovery-start events are now true no-op command
   sequences

Files changed:

1. `sw-block/engine/replication/command.go`
   - added explicit recovery-start commands
2. `sw-block/engine/replication/state.go`
   - extended private command-state tracking for catch-up/rebuild targets
3. `sw-block/engine/replication/engine.go`
   - emits bounded recovery-start commands from recovery events
   - deduplicates repeated identical recovery-start requests
4. `sw-block/engine/replication/phase14_command_test.go`
   - added bounded catch-up start proof
   - added bounded rebuild start proof

Proofs strengthened:

1. catch-up start boundedness
   - first `CatchUpPlanned(55)` emits:
     - `start_catchup`
     - `publish_projection`
   - repeated identical `CatchUpPlanned(55)` emits no commands
2. rebuild start boundedness
   - first `RebuildStarted(80)` emits:
     - `start_rebuild`
     - `publish_projection`
   - repeated identical `RebuildStarted(80)` emits no commands

Validation:

1. `gofmt -w command.go state.go engine.go phase14_command_test.go`
2. `go test ./...`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - recovery policy is now explicit as both state truth and command decision
2. overclaim avoided
   - recovery-start intent no longer hides only in state mutation
   - repeated planning/start events no longer look like fresh work every time
3. proof preserved
   - all `14A`, `14B`, and `14C` proofs still pass

---

### `14C` Delivery Note Rev 4

Date: 2026-04-03
Scope: close the stale-recovery leakage gap so old recovery truth and old
recovery-start intent cannot survive into a new assignment/epoch cycle

What changed:

1. added proof that assignment change clears stale recovery truth:
   - recovery phase returns to `idle`
   - target and achieved boundaries are cleared
   - mode/publication fall back to the new assignment bootstrap state
2. added proof that a fresh assignment cycle may legitimately re-emit the same
   recovery-start command for the same target

Files changed:

1. `sw-block/engine/replication/phase14_boundary_test.go`
   - added stale-recovery-reset proof across assignment/epoch change
2. `sw-block/engine/replication/phase14_command_test.go`
   - added fresh-cycle recovery-start reissue proof

Proofs strengthened:

1. stale recovery does not leak across assignment cycles
2. recovery-start command dedupe is cycle-bounded rather than globally sticky

Validation:

1. `gofmt -w phase14_boundary_test.go phase14_command_test.go`
2. `go test ./...`
3. result: `PASS`

Constraint / overclaim / proof review:

1. semantic constraint satisfied
   - recovery truth and recovery command intent are now scoped to the active
     assignment/epoch cycle
2. overclaim avoided
   - old target/achieved/recovery phase cannot make a new assignment look
     partially recovered
   - dedupe state cannot suppress valid fresh-cycle recovery work
3. proof preserved
   - all previous `14A/14B/14C` proofs still pass

#### `Phase 14` first-round closure

At this point the first bounded `Phase 14` core shell is in place:

1. `14A` delivered
   - explicit mode / readiness / publication ownership
2. `14B` delivered
   - bounded command-emission rules
3. `14C` delivered
   - explicit boundary / recovery truth, projection visibility, recovery-start
     commands, and assignment-cycle reset rules

Interpretation:

1. this is a real explicit `V2 core` shell in `sw-block/engine/replication`
2. it is still not a live runtime cutover
3. the best next step is `Phase 15A` adapter ingress/egress rebinding on one
   narrow path

---

### Post-Closure Tightening

Date: 2026-04-03
Reason: manager review correctly identified two remaining risks:

1. `14A/14B/14C` slice-boundary blur in top-level phase wording
2. duplicated `publish_healthy` authority in core state/projection

Actions taken:

1. `sw-block/.private/phase/phase-14.md`
   - tightened `14A` so it owns only mode/readiness/publication shell closure
   - made `14B` the explicit owner of command-sequence closure
   - made `14C` the explicit owner of durable-boundary and recovery closure
2. `sw-block/engine/replication/state.go`
   - removed `ReadinessView.PublishHealthy`
   - documented `PublicationView` as the semantic owner for publication truth
3. `sw-block/engine/replication/projection.go`
   - removed duplicate top-level `PublishHealthy` convenience field
4. `sw-block/engine/replication/engine.go`
   - publication truth is now carried only through `PublicationView`
5. `phase14_*_test.go`
   - switched assertions to `Projection.Publication.Healthy`

Result:

1. `PublicationView` is now the single semantic owner for publication health
2. `ReadinessView` and `PublicationProjection` no longer carry parallel
   publication-health truth
3. top-level `Phase 14` wording now matches the actual `14A/14B/14C` ownership
   split more closely
