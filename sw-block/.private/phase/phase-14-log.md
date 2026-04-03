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
