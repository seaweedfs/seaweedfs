# Phase 10 Log

Date: 2026-04-02
Status: active
Purpose: record the technical pack and design notes for stronger control-plane closure after `Phase 09`

---

### P0 Technical Pack

Date: 2026-04-02
Goal: turn the accepted chosen-path backend execution into a stronger end-to-end control path where assignment intent, live runtime ownership, and externally visible control truth converge coherently

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted the chosen-path backend execution and live runtime ownership.

What still remains weak is the end-to-end control path:

1. real assignment ingress is accepted at `ProcessAssignments()`, but fuller heartbeat / gRPC-level closure is still bounded
2. local identity is still transport-shaped (`listenAddr`)
3. reassignment / result convergence is not yet proven deeply enough through the full live control path
4. repeated assignment behavior still has at least one bounded low-severity idempotence warning

`Phase 10` should close the main control-plane gap without reopening the accepted backend execution semantics.

##### Backend / system reality

1. engine policy is already accepted and should remain the policy owner
2. `RecoveryManager` already owns live runtime start/cancel/replace/cleanup on the chosen path
3. `ProcessAssignments()` is already the accepted local assignment-entry point
4. the remaining uncertainty is not "can the backend execute"
5. it is "does the real control path drive, replace, and report that execution coherently enough"

##### High-level algorithm

Use this control model:

1. master emits assignment / epoch / identity truth for the chosen path
2. the real control path delivers that truth into the volume server
3. the volume server converts it into stable local assignment/runtime truth
4. the orchestrator/runtime layer applies start / replace / cancel based on that truth
5. heartbeat/result reporting reflects the same truth back out
6. failover / reassignment should converge without split truth between:
   - master/control intent
   - local runtime owner
   - externally visible reported state

##### Pseudo code

```text
on control-plane assignment delivery:
    decode assignment truth
    normalize identity inputs
    apply assignment to local control/runtime owner

on local ownership change:
    start / replace / cancel accepted runtime owner
    keep engine policy unchanged

on reporting:
    publish state derived from current accepted owner + assignment truth
    do not report stale ownership after reassignment

on failover / reassignment:
    old control truth is invalidated
    new control truth becomes authoritative
    local runtime owner and reported state converge to it
```

##### State / convergence contract

`Phase 10` should make these truths explicit:

1. stable identity is not inferred casually from transport address shape
2. control ingress truth, runtime ownership truth, and reported truth should not diverge for the chosen path
3. reassignment must not leave stale local owner/reporting visible after control truth changes
4. accepted backend execution should remain a dependency, not be reopened as a new correctness debate

##### Reject shapes

Reject before implementation if the package still relies on:

1. proofs that stop at `ProcessAssignments()` without fuller control-path closure
2. identity that remains transport-shaped where stronger stable identity is required
3. local/runtime/reporting divergence after reassignment
4. reopening accepted `Phase 09` execution semantics as the main work item
5. expanding into product-surface rebinding or broad hardening

#### Layer 2: Execution Core

##### Current gaps `P0` identifies for `Phase 10`

1. fuller heartbeat / gRPC delivery proof is still missing on the chosen path
2. local identity is still too transport-shaped for stronger product/control claims
3. reassignment/result convergence through the real control path is still under-proven
4. repeated-assignment idempotence still has a bounded warning surface

##### Recommended slice order

1. `P1`: identity and control-truth closure
   - make local identity cleaner than transport-shaped `listenAddr`
   - tighten control truth so assignment identity and runtime identity converge

2. `P2`: reassignment / result convergence
   - prove failover / reassignment converge through the real control path
   - ensure reported state and runtime owner do not lag old truth

3. `P3`: bounded idempotence cleanup
   - close repeated-assignment/rebuild-server warning only if still needed after `P1` / `P2`

##### Reuse / update instructions

1. `weed/server/volume_grpc_client_to_master.go`
   - `update in place`
   - strengthen the proof around real control delivery if this is the active ingress path

2. `weed/server/volume_server_block.go`
   - `update in place`
   - keep this as the main local control/runtime application surface

3. `weed/storage/blockvol/v2bridge/control.go`
   - `update in place`
   - tighten identity and control-truth mapping only as needed

4. `weed/server/block_recovery.go`
   - `reference only` unless a narrow control/runtime convergence bug requires change
   - do not reopen accepted ownership semantics casually

5. `sw-block/engine/replication/*`
   - `reference only` unless the control-plane slice exposes a true engine-facing gap
   - keep policy in engine

6. copy guidance
   - prefer `update in place`
   - use `reference only` for accepted execution/runtime code
   - do not fork a second control/runtime path

##### Validation focus

Required proofs:

1. real control-ingress proof
   - heartbeat / gRPC-level delivery reaches the live local control/runtime path

2. identity proof
   - chosen-path local and remote identity are coherent enough for reassignment and reporting

3. reassignment proof
   - old truth is invalidated
   - new truth becomes authoritative
   - local owner/reporting converge accordingly

4. no-split-truth proof
   - do not leave different truths simultaneously visible across:
     - assignment ingress
     - runtime owner
     - reported heartbeat/control state

5. bounded-cleanup proof
   - if repeated-assignment idempotence is touched, prove the bounded cleanup directly

Reject if:

1. proof still stops at local assignment intake only
2. identity remains transport-shaped where stronger identity is required
3. reassignment leaves stale reporting or stale runtime owner visible
4. implementation reopens accepted backend execution semantics

##### Suggested first cut

1. define the exact real control ingress boundary to prove in `P1`
2. define the identity truth to carry across that boundary
3. define the visible reported-state convergence target after reassignment
4. keep idempotence cleanup as a bounded residual, not the main slice

##### Assignment For `sw`

1. Goal
   - deliver the first control-plane closure slice after `P0` is accepted

2. Required outputs
   - one real control-path proof
   - one identity/control-truth proof
   - one reassignment/result-convergence proof shape
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09` backend execution semantics
   - do not let bounded idempotence cleanup become the whole phase
   - keep product surfaces out of this phase

##### Assignment For `tester`

1. Goal
   - validate that `Phase 10` closes real control-path gaps rather than adding another layer of local-only bookkeeping

2. Validate
   - real control ingress
   - identity coherence
   - reassignment/result convergence
   - no split truth between ingress/runtime/reporting

3. Reject if
   - evidence stops at `ProcessAssignments()` only
   - identity remains transport-shaped without an accepted bound
   - claims exceed the chosen-path control closure target

---

### P1 Technical Pack

Date: 2026-04-02
Goal: close stable identity and control-truth on the real control ingress path so the chosen-path control chain preserves server identity end-to-end instead of relying on transport-shaped fallback

#### Layer 1: Semantic Core

##### Problem statement

The accepted chosen path already depends on stable identity semantics:

1. `v2bridge/control.go` defines `ReplicaID = <volume-path>/<ServerID>`
2. control tests already reject address-derived identity
3. `RecoveryManager` and accepted runtime ownership now assume that control truth is stable enough to drive start/replace/cancel on the right replica target

But the real control wire is still weaker than that accepted truth:

1. `BlockService.localServerID` is still initialized from `listenAddr`
2. `master.proto` block-assignment messages still do not carry stable `ServerID` fields
3. `block_heartbeat_proto.go` therefore drops stable replica identity on proto round-trip
4. master-side assignment producers currently populate replica addresses but not stable replica server identity

So the current system contains a real split:

1. accepted local V2 control semantics want stable `ServerID`
2. the actual heartbeat/gRPC assignment wire still mainly carries transport addresses

`P1` should close that split.

##### Concrete current gap

The main concrete gap is now explicit:

1. `weed/pb/master.proto`
   - `BlockVolumeAssignment` carries:
     - `replica_data_addr`
     - `replica_ctrl_addr`
     - `rebuild_addr`
     - `replica_addrs`
   - but it does not yet carry:
     - scalar `replica_server_id`
     - per-replica `server_id` in `ReplicaAddrMessage`
2. `weed/storage/blockvol/block_heartbeat_proto.go`
   - proto conversion currently round-trips addresses only
3. `weed/server/master_grpc_server_block.go` / `weed/server/master_block_failover.go`
   - assignment generation currently populates addresses but not stable server identity
4. `weed/server/volume_server_block.go`
   - local identity is still seeded from `listenAddr`

##### High-level algorithm

Use this identity/control-truth model:

1. choose one authoritative stable local server identity from the real control-plane/registry path
2. carry stable replica server identity through the assignment wire:
   - scalar RF=2 path
   - multi-replica path
3. decode that same stable identity on the volume server
4. feed it into `ControlBridge` and runtime ownership without address-derived fallback
5. fail closed if the chosen path requires a stable identity and the wire does not provide one

##### Pseudo code

```text
on master assignment build:
    emit stable replica server id(s) together with replica address(es)

on proto encode/decode:
    preserve server id(s) exactly

on volume server control ingest:
    derive authoritative local server id from stable control identity
    convert assignment using stable server id(s), not listen address

on ControlBridge conversion:
    build ReplicaID from <path>/<ServerID>
    reject missing identity instead of synthesizing address-based identity
```

##### State / convergence contract

`P1` should make these truths explicit:

1. stable identity on the chosen path is not advisory metadata; it is part of control truth
2. the same stable identity should survive:
   - master assignment generation
   - proto wire encode/decode
   - volume-server ingest
   - `ControlBridge` conversion
   - runtime owner target selection
3. local server identity should no longer be transport-shaped if `P1` claims identity closure
4. no chosen-path code should silently fall back to address-derived identity when a stable ID is required

##### Reject shapes

Reject before implementation if the proposed slice still does any of the following:

1. preserves stable `ServerID` only in tests, not on the real gRPC wire
2. leaves `localServerID` transport-shaped while claiming control-truth closure
3. silently reconstructs identity from address text
4. proves only local conversion helpers but not the real ingress chain
5. absorbs reassignment/result-convergence work that belongs to `P2`

#### Layer 2: Execution Core

##### Current gaps `P1` must close

1. `weed/pb/master.proto`
   - add the stable server-identity fields required by the chosen path

2. `weed/storage/blockvol/block_heartbeat_proto.go`
   - preserve the new stable identity fields in both directions

3. `weed/server/master_grpc_server_block.go`
   - populate stable server identity for created assignments, not addresses only

4. `weed/server/master_block_failover.go`
   - populate stable server identity on failover/reassignment-built assignments as well

5. `weed/server/volume_server_block.go`
   - stop treating `listenAddr` as the long-term local stable identity if the real control path can provide a better source

6. `weed/storage/blockvol/v2bridge/control.go`
   - keep fail-closed stable-ID mapping, but align it with the real incoming wire shape after the wire is fixed

##### Reuse / update instructions

1. `weed/pb/master.proto`
   - `update in place`
   - add only the stable identity fields needed by the chosen path

2. `weed/storage/blockvol/block_heartbeat_proto.go`
   - `update in place`
   - keep proto round-trip explicit and symmetric

3. `weed/server/master_grpc_server_block.go`
   - `update in place`
   - carry stable identity from registry/assignment source into the wire assignment

4. `weed/server/master_block_failover.go`
   - `update in place`
   - preserve the same identity rules during promotion/reassignment

5. `weed/server/volume_grpc_client_to_master.go`
   - `reference only` unless a narrow proof hook or assertion point is required
   - do not redesign the whole heartbeat loop

6. `weed/server/volume_server_block.go`
   - `update in place`
   - tighten the source of `localServerID` only as much as needed for the chosen path

7. `weed/storage/blockvol/v2bridge/control.go`
   - `update in place`
   - align comments/contracts with the real wire after stable-ID fields exist end-to-end

8. `weed/server/block_recovery.go`
   - `reference only`
   - accepted runtime ownership from `P4` is not the target of this slice

9. copy guidance
   - prefer `update in place`
   - no parallel control-bridge implementation
   - no address-derived compatibility shim that becomes the new truth

##### Validation focus

Required proofs:

1. proto round-trip proof
   - scalar assignment preserves `replica_server_id`
   - multi-replica assignment preserves per-replica `server_id`

2. master assignment proof
   - create path emits stable server IDs
   - failover/reassignment path emits stable server IDs

3. real ingress proof
   - heartbeat/gRPC delivery reaches the volume server with stable IDs intact
   - `ControlBridge` conversion builds the same `ReplicaID` the engine/runtime expect

4. local identity proof
   - local replica/rebuild assignment uses a stable local identity source
   - chosen-path control truth does not depend on `listenAddr` string shape

5. fail-closed proof
   - missing stable identity on the chosen path is rejected rather than synthesized from address text

Reject if:

1. stable identity still disappears on proto round-trip
2. tests only prove `ControlBridge` unit conversion without the real control wire
3. `localServerID` is still fundamentally transport-shaped after the slice claims closure
4. the slice claims full reassignment/result convergence rather than identity/control-truth closure

##### Suggested first cut

1. add stable-ID fields to the block assignment proto and Go conversion layer
2. wire stable IDs into master-side assignment production
3. choose and wire one authoritative local server ID source on the volume server
4. add one real heartbeat/gRPC ingress proof using those stable IDs
5. add one fail-closed proof for missing identity

##### Assignment For `sw`

1. Goal
   - deliver `P1` identity and control-truth closure on the real chosen-path control wire

2. Required outputs
   - real wire preservation of stable server identity
   - local server identity no longer transport-shaped on the chosen path
   - one real ingress proof
   - one fail-closed proof
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09` backend execution semantics
   - do not treat address text as stable identity
   - do not absorb reassignment/result-convergence proof into `P1`

##### Assignment For `tester`

1. Goal
   - validate that `P1` closes the real stable-identity/control-truth gap rather than only tightening local helper comments

2. Validate
   - stable server IDs survive the real proto wire
   - master-produced assignments preserve stable identity
   - volume-server ingest preserves stable identity
   - missing identity fails closed on the chosen path

3. Reject if
   - stable identity still exists only in tests or local structs
   - the real gRPC/heartbeat path still drops identity
   - claims exceed identity/control-truth closure

---

### P1 Completion Record

Date: 2026-04-02
Status: accepted (rev 3)
Revisions: 3 (rev 1: initial stable-ID wire change, rev 2: canonical local identity + generated proto required, rev 3: real ingress / canonical-local / fail-closed proofs added and handoff wording tightened)

#### Accepted contract

- stable identity on the chosen path is now part of the real block assignment control truth
- the chosen-path control wire now preserves:
  - scalar `replica_server_id`
  - per-replica `server_id`
- local block/control identity now uses the same canonical `volumeServerId` as the main volume server
- missing stable identity on the chosen path fails closed instead of falling back to address-derived identity
- `P1` does not claim full reassignment/result convergence

#### Files changed

| File | Action |
|------|--------|
| `weed/pb/master.proto` | Updated: additive stable-ID fields for block assignments |
| `weed/pb/master_pb/master.pb.go` | Regenerated: stable-ID fields carried in generated protobuf output |
| `weed/pb/master_pb/master_grpc.pb.go` | Regenerated alongside `master.pb.go` |
| `weed/storage/blockvol/block_heartbeat_proto.go` | Updated: preserve stable ID on proto encode/decode |
| `weed/server/master_grpc_server_block.go` | Updated: create-path assignments carry stable ID on chosen path |
| `weed/server/master_block_failover.go` | Updated: promotion/refresh chosen-path assignments carry stable ID |
| `weed/server/volume_server_block.go` | Updated: canonical block/control server identity setter |
| `weed/command/volume.go` | Updated: block service uses canonical `volumeServerId` |
| `weed/storage/blockvol/block_heartbeat_proto_test.go` | New/updated: stable-ID proto round-trip tests |
| `weed/server/qa_block_identity_test.go` | New: real ingress / canonical-local / fail-closed proofs |

#### Test inventory (6 P1 tests)

| Test | Proves |
|------|--------|
| `ProtoRoundTrip_ScalarServerID` | scalar stable ID survives proto/decode |
| `ProtoRoundTrip_MultiReplicaServerID` | per-replica stable ID survives proto/decode |
| `ProtoRoundTrip_MissingServerID_NotSynthesized` | missing ID stays empty |
| `RealIngress_ProtoToReplicaID` | proto -> decode -> `ProcessAssignments()` -> `ControlBridge` -> engine `ReplicaID` |
| `CanonicalLocalIdentity` | local block/control path uses canonical non-default local server ID |
| `FailClosed_MissingServerID` | missing ID is rejected without address-derived fallback |

#### Residual note

1. `P1` does not claim full reassignment/result convergence through the real control path
2. some scalar replica/rebuild assignment paths still rely on local chosen-path identity rather than carrying remote stable ID on every role shape

---

### P2 Technical Pack

Date: 2026-04-02
Goal: close reassignment / failover result convergence on the chosen path so control truth changes do not leave stale local owners or stale reported truth behind

#### Layer 1: Semantic Core

##### Problem statement

`P1` closed stable identity and control-truth on the chosen block assignment wire.

What is still open is dynamic convergence when control truth changes:

1. after failover or reassignment, old local ownership may linger too long
2. externally visible heartbeat/control truth may lag the new assignment truth
3. local/runtime/reporting state may temporarily point at different realities

`P2` should close that dynamic convergence gap on the chosen path.

##### System reality

1. `ProcessAssignments()` is already the local assignment-application surface
2. `RecoveryManager` already provides accepted start / cancel / replace ownership
3. the remaining question is not whether replacement can happen
4. it is whether the full control path converges the whole system to the new truth without stale residue

##### High-level algorithm

Use this convergence model:

1. a new assignment / epoch / failover event defines new control truth
2. local control ingress applies the new truth
3. stale runtime owner is cancelled / replaced
4. visible reported state is refreshed so it reflects the same new truth
5. no old truth should remain externally visible after convergence completes

##### Pseudo code

```text
on reassignment / failover:
    receive new assignment truth
    invalidate stale local owner
    start or stop replacement owner as required
    refresh/report state derived from the new truth

after convergence:
    old owner is gone
    current owner matches new truth
    externally visible state matches new truth
```

##### State / convergence contract

`P2` should make these truths explicit:

1. old assignment truth must not remain authoritative after reassignment
2. stale runtime owner must not survive convergence
3. reported/heartbeat truth must not continue advertising stale ownership after convergence
4. chosen-path control truth should converge without reopening backend execution semantics

##### Reject shapes

Reject before implementation if the slice:

1. proves only local invalidation without proving reported/control convergence
2. relies on helper-only tests without the real control path
3. leaves stale owner or stale reporting visible after reassignment
4. turns into broad hardening or product-surface work
5. absorbs the rebuild-server idempotence warning as the main story

#### Layer 2: Execution Core

##### Current gaps `P2` must close

1. real failover/reassignment convergence is still under-proven on the chosen path
2. reported/control truth after reassignment is still weaker than the runtime/executor proof
3. the relation between:
   - assignment ingress
   - runtime owner replacement
   - heartbeat/control reporting
   still needs direct proof

##### Reuse / update instructions

1. `weed/server/volume_grpc_client_to_master.go`
   - `update in place`
   - use this if needed for fuller heartbeat/gRPC reassignment proof

2. `weed/server/volume_server_block.go`
   - `update in place`
   - keep this as the local convergence application surface

3. `weed/server/block_recovery.go`
   - `reference only` unless a true convergence bug requires a narrow fix
   - accepted replacement ownership from `P4` should not be casually reopened

4. `weed/server/master_block_failover.go`
   - `reference only` unless proof work exposes a real stale-truth bug in assignment production

5. `weed/storage/blockvol/v2bridge/control.go`
   - `reference only` unless a true control-truth mismatch remains after `P1`

6. copy guidance
   - prefer `update in place`
   - do not fork a second reassignment/control path
   - keep bounded idempotence cleanup separate unless required by this slice

##### Validation focus

Required proofs:

1. real reassignment proof
   - failover / reassignment changes real control truth on the chosen path

2. stale-owner proof
   - old runtime owner is gone after convergence

3. reported-truth proof
   - externally visible heartbeat/control truth reflects the new assignment truth

4. no-split-truth proof
   - assignment ingress, runtime owner, and reported state converge to the same current truth

5. bounded-fail-closed proof
   - if reassignment cannot converge cleanly, do not overclaim completion

Reject if:

1. proof stops at identity-only closure already accepted in `P1`
2. stale owner can remain alive after reassignment convergence
3. heartbeat/control reporting still reflects stale truth after reassignment
4. implementation reopens accepted backend execution semantics

##### Suggested first cut

1. define one real reassignment/failover scenario on the chosen path
2. prove runtime-owner replacement for that scenario
3. prove the corresponding reported/control truth update
4. keep repeated-assignment idempotence as a residual unless directly exposed

##### Assignment For `sw`

1. Goal
   - deliver `P2` reassignment / result convergence on the chosen path

2. Required outputs
   - one real reassignment/failover convergence proof
   - one stale-owner removal proof
   - one reported/control-truth convergence proof
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09` backend execution semantics
   - do not re-solve stable identity already accepted in `P1`
   - do not let idempotence cleanup become the whole slice

##### Assignment For `tester`

1. Goal
   - validate that `P2` closes real reassignment/result convergence rather than only proving local replacement bookkeeping

2. Validate
   - real reassignment/failover control path
   - stale owner removed
   - reported/control truth converges
   - no split truth after reassignment

3. Reject if
   - evidence remains identity-only
   - stale local or reported truth remains visible
   - claims exceed reassignment/result convergence

---

### P2 Completion Record

Date: 2026-04-02
Status: accepted (rev 2)
Revisions: 2 (rev 1: initial convergence package under-proved stale owner and reported truth, rev 2: live stale-owner drain proof + heartbeat-output proof + bounded wording)

#### Accepted contract

- reassignment/result convergence on the chosen path is now accepted at the volume-server-side ingress boundary
- stale runtime residue after control truth change is now directly tested as a live drain case
- reported truth is now checked on actual heartbeat output rather than helper-local state
- accepted no-split-truth for `P2` means:
  - new engine sender truth is present
  - stale runtime residue is gone
  - heartbeat output reflects the new truth
- `P2` does not claim a proved live replacement owner in every convergence proof, and does not claim full master-driven failover infrastructure closure

#### Files changed

| File | Action |
|------|--------|
| `weed/server/qa_block_convergence_test.go` | New/updated: real reassignment, live stale-owner drain, heartbeat-output convergence, bounded no-split-truth proofs |
| `weed/server/block_recovery.go` | Reused existing `OnBeforeExecute` hook from accepted `P4`; no new `P2` semantic change required |
| `weed/server/volume_server_block.go` | Reused accepted chosen-path ingress/reporting surfaces; no new `P2` semantic change required |

#### Test inventory (4 P2 tests)

| Test | Proves |
|------|--------|
| `ReassignmentConvergence_EpochBump` | old sender removed, new sender created on chosen-path reassignment |
| `StaleOwnerRemoval` | live old recovery goroutine is drained during reassignment |
| `ReportedTruth_HeartbeatConverges` | `CollectBlockVolumeHeartbeat()` reflects new replica truth |
| `NoSplitTruth` | engine truth + stale-runtime residue removed + heartbeat truth converge without stale residue |

#### Residual note

1. full master-driven failover/gRPC infrastructure closure remains outside accepted `P2` scope
2. bounded repeated-assignment / rebuild-server relisten cleanup remains open for `P3`

---

### P3 Technical Pack

Date: 2026-04-02
Goal: close the remaining bounded repeated-assignment / idempotence gap on the chosen path so repeated primary assignments do not emit avoidable rebuild-server relisten noise or ambiguous duplicate runtime side effects

#### Layer 1: Semantic Core

##### Problem statement

`P1` accepted stable identity on the chosen path.
`P2` accepted reassignment/result convergence on the chosen path.

What still remains open is a bounded idempotence/runtime-cleanup gap during repeated primary assignment:

1. repeated primary assignment for the same volume can still try to relisten/start rebuild-server state that is already active
2. this produces low-severity warning noise and weakens the claim that unchanged control truth is handled idempotently
3. the remaining issue is not backend execution correctness
4. it is whether repeated chosen-path control truth is applied cleanly enough without duplicate side effects

`P3` should close that bounded gap without turning into broad runtime polish.

##### System reality

1. `ProcessAssignments()` remains the accepted chosen-path local ingress
2. `RecoveryManager` already owns accepted start/cancel/replace semantics and should not be broadly reopened
3. rebuild-server/runtime setup currently still has at least one repeated-assignment relisten warning surface
4. unchanged or effectively unchanged chosen-path truth should be absorbed idempotently rather than re-triggering avoidable side effects

##### High-level algorithm

Use this idempotence model:

1. compare the incoming chosen-path primary assignment with the currently applied live assignment/runtime state for the same volume
2. if the authoritative truth is unchanged in the fields that matter to the live chosen path, treat the update as idempotent refresh, not restart
3. if the authoritative truth changed, apply the existing accepted replacement/update path exactly once
4. do not relaunch rebuild-server/runtime side effects when the server is already correctly active for the same truth
5. keep reporting truth aligned with the accepted current owner after the idempotent path

##### Pseudo code

```text
on ProcessAssignments(primary assignment):
    load current chosen-path runtime state for volume

    if incoming truth == current authoritative truth:
        keep existing rebuild/runtime owner
        refresh bookkeeping only if needed
        do not relisten or duplicate side effects
        return

    if incoming truth changes authoritative runtime inputs:
        apply accepted replace/update path once
        update reporting truth to the new owner

after repeated assignment:
    there is at most one authoritative live owner
    rebuild/runtime side effects are not duplicated
    warning-free idempotent steady state is preferred for unchanged truth
```

##### State / convergence contract

`P3` should make these truths explicit:

1. repeated unchanged chosen-path control truth is not a new event that should restart everything
2. there should remain at most one authoritative live chosen-path owner/runtime side-effect set per volume
3. repeated assignment must not create stale duplicate runtime ownership or avoidable relisten noise
4. `P3` is bounded cleanup on top of accepted `P1`/`P2`, not a reopening of control ingress or backend execution semantics

##### Reject shapes

Reject before implementation if the slice:

1. turns into generic restart/runtime polish unrelated to repeated chosen-path assignment
2. reopens accepted `RecoveryManager` ownership semantics without a concrete repeated-assignment bug
3. claims full production hardening from removal of one warning surface
4. fixes warning text only while duplicate side effects or duplicate ownership are still possible
5. expands into new topology/mode work beyond the chosen path

#### Layer 2: Execution Core

##### Current gaps `P3` must close

1. repeated primary assignment can still attempt rebuild-server relisten/start on an already-active chosen-path volume
2. the current chosen-path tests do not yet prove idempotent steady state for repeated unchanged assignment
3. bounded runtime-side-effect cleanup is still under-specified relative to accepted `P2` convergence

##### Reuse / update instructions

1. `weed/server/volume_server_block.go`
   - `update in place`
   - keep this as the primary chosen-path assignment/runtime application surface
   - narrow target: repeated-assignment idempotence and rebuild-server/runtime side effects only

2. `weed/server/block_recovery.go`
   - `reference only` unless repeated-assignment proof exposes a real duplicate-owner bug
   - do not reopen accepted serialized ownership semantics casually

3. `weed/server/qa_block_convergence_test.go`
   - `update in place`
   - extend chosen-path convergence tests with one repeated-assignment/idempotence proof

4. `weed/server/*block*_test.go`
   - `update in place` only if a more focused live-path idempotence test is cleaner than overloading convergence tests

5. `weed/storage/blockvol/v2bridge/control.go`
   - `reference only`
   - no new mapping semantics unless repeated-assignment behavior exposes a true control-truth mismatch

6. copy guidance
   - prefer `update in place`
   - no parallel runtime-control path
   - no broad refactor whose main value is style cleanup

##### Validation focus

Required proofs:

1. unchanged repeated-assignment proof
   - same chosen-path primary assignment applied again does not relaunch duplicate rebuild/runtime side effects

2. changed-assignment proof
   - when control truth really changes, accepted replacement path still executes exactly once

3. no-duplicate-owner proof
   - repeated assignment does not leave two live owners or ambiguous runtime side effects

4. bounded-reporting proof
   - externally visible chosen-path state remains coherent after repeated assignment

5. boundedness proof
   - `P3` closes this residual without reopening accepted `P1`/`P2` or `Phase 09`

Reject if:

1. proof shows only warning suppression but not idempotent runtime behavior
2. unchanged repeated assignment still triggers avoidable relisten/start side effects
3. changed assignment breaks accepted `P2` convergence behavior
4. implementation expands into broad hardening or unrelated restart cleanup

##### Suggested first cut

1. identify the exact repeated primary-assignment path that still emits rebuild-server relisten noise
2. define the minimal equality/authoritative-truth check for "already active" chosen-path primary state
3. make unchanged repeated assignment idempotent at that application point
4. add one focused live-path test for unchanged repeated assignment and one guard test that real changed assignment still replaces correctly

##### Assignment For `sw`

1. Goal
   - deliver `P3` bounded repeated-assignment / idempotence cleanup on the chosen path

2. Required outputs
   - one live-path repeated-assignment idempotence proof
   - one no-duplicate-side-effect or no-duplicate-owner proof
   - one guard proof that changed assignment still uses the accepted replacement path
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09` backend execution semantics
   - do not reopen accepted `P1` stable-identity closure
   - do not reopen accepted `P2` reassignment convergence except for a narrow repeated-assignment bug
   - keep the slice bounded to repeated-assignment/idempotence cleanup

##### Assignment For `tester`

1. Goal
   - validate that `P3` closes the bounded repeated-assignment/idempotence gap instead of only silencing logs

2. Validate
   - unchanged repeated assignment is idempotent on the live chosen path
   - no duplicate rebuild/runtime side effects remain
   - changed assignment still converges through the accepted replacement path
   - claims remain bounded to residual cleanup

3. Reject if
   - evidence is log-text-only
   - unchanged repeated assignment still relaunches avoidable side effects
   - the slice overclaims production hardening or broader control-plane closure

---

### P3 Completion Record

Date: 2026-04-02
Status: accepted (rev 2)
Revisions: 2 (rev 1: V1-side relisten suppression only, under-closed chosen-path idempotence; rev 2: full chosen-path V2 + V1 repeated-assignment idempotence with bounded wording)

#### Accepted contract

- repeated unchanged chosen-path assignment is now treated as idempotent across the accepted V2 + V1 live path
- unchanged repeated assignment no longer re-enters duplicate V2 orchestrator/recovery work on the chosen path
- corresponding V1 replication/rebuild-server setup for unchanged truth is also skipped
- changed assignment still takes the accepted replacement/update path
- `P3` remains bounded residual cleanup and does not claim broad multi-replica idempotence or general production hardening

#### Files changed

| File | Action |
|------|--------|
| `weed/server/volume_server_block.go` | Updated: assignment-key tracking and chosen-path repeated-assignment idempotence at the live ingress point |
| `weed/server/qa_block_idempotence_test.go` | New/updated: repeated unchanged assignment proof, changed-assignment guard, heartbeat coherence after repeated assignment |

#### Test inventory (3 P3 tests)

| Test | Proves |
|------|--------|
| `RepeatedAssignment_Idempotent` | repeated identical chosen-path assignment does not grow V2 event count and leaves V1 state unchanged |
| `ChangedAssignment_StillReplaces` | changed assignment still triggers accepted replacement behavior |
| `HeartbeatCoherent_AfterRepeated` | heartbeat output remains coherent after repeated unchanged assignment |

#### Residual note

1. `P3` does not close the remaining full master-driven heartbeat/gRPC control-loop proof gap
2. bounded chosen-path master-originated delivery closure remains open for `P4`

---

### P4 Technical Pack

Date: 2026-04-02
Goal: close the remaining master-driven heartbeat / gRPC control-loop gap on the chosen path so accepted control truth is proven from master-originated assignment production through live delivery into the volume server and back out through visible state

#### Layer 1: Semantic Core

##### Problem statement

`P1` accepted stable identity and control-truth on the chosen wire shape.
`P2` accepted reassignment/result convergence at the volume-server-side chosen-path ingress boundary.
`P3` accepted bounded repeated-assignment/idempotence cleanup on that same live path.

What still remains open is the last larger control-plane bound:

1. accepted proofs still center mainly on the volume-server-side ingestion boundary
2. the fuller master-originated heartbeat / gRPC delivery loop is not yet accepted as a direct one-chain proof target
3. the remaining question is not backend execution or local replacement semantics
4. it is whether real master-produced chosen-path truth reaches the live volume-server path coherently enough through the actual control loop

`P4` should close that gap without turning `Phase 10` into broad cluster-hardening work.

##### System reality

1. master-side assignment production for the chosen path already exists
2. accepted stable-ID wire fields already exist from `P1`
3. accepted VS-side convergence and idempotence already exist from `P2` / `P3`
4. the remaining missing proof is the one-chain link across:
   - master-produced assignment truth
   - heartbeat / gRPC delivery
   - volume-server application
   - visible post-delivery state

##### High-level algorithm

Use this control-loop model:

1. master produces authoritative chosen-path assignment truth for one bounded scenario
2. the real heartbeat / gRPC delivery path carries that truth to the target volume server
3. the volume server applies the delivered truth through the already accepted local path
4. runtime/reporting state converge to the same truth
5. repeated heartbeat/control iterations do not silently rewrite that truth into something different

##### Pseudo code

```text
on master-side chosen-path state change:
    build authoritative assignment using accepted stable identity fields
    deliver assignment through the real heartbeat / gRPC path

on volume-server receipt through the live control loop:
    decode and preserve the same chosen-path truth
    apply accepted local control/runtime path

after delivery converges:
    engine/runtime/reporting reflect the same master-originated truth
    no stale pre-change truth remains visible
    repeated delivery of unchanged truth stays bounded under accepted P3 rules
```

##### State / convergence contract

`P4` should make these truths explicit:

1. master-originated chosen-path assignment truth is now part of the accepted end-to-end proof, not only local ingestion truth
2. the accepted `P1` identity fields must survive the real master -> volume-server delivery loop intact
3. the accepted `P2` convergence properties must still hold when the truth enters through the real master-driven path
4. the accepted `P3` idempotence properties must still hold for repeated unchanged delivery through that same loop
5. `P4` remains bounded to the chosen path and does not claim whole-cluster or product-surface hardening

##### Reject shapes

Reject before implementation if the slice:

1. still stops at direct local `ProcessAssignments()` calls without one real master-driven delivery proof
2. reopens accepted `P1` / `P2` / `P3` semantics as the main story instead of using them as dependencies
3. turns into broad heartbeat-loop refactoring, broad failover redesign, or cluster-hardening work
4. claims full distributed failover robustness beyond one bounded chosen-path control-loop proof
5. proves only transport reachability without proving resulting control/runtime/reporting truth

#### Layer 2: Execution Core

##### Current gaps `P4` must close

1. master-originated chosen-path assignment production is not yet tied to an accepted one-chain proof through the real heartbeat / gRPC delivery loop
2. accepted VS-side control truth is stronger than accepted master-to-VS loop proof
3. there is no accepted direct proof that the post-delivery visible state still matches the same master-originated truth after the real loop runs

##### Reuse / update instructions

1. `weed/server/volume_grpc_client_to_master.go`
   - `update in place`
   - use as the primary delivery-loop surface if proof hooks or deterministic test entry are needed
   - keep changes narrow and proof-oriented

2. `weed/server/master_grpc_server_block.go`
   - `reference only` unless one bounded proof bug requires a narrow fix in assignment production

3. `weed/server/master_block_failover.go`
   - `reference only` unless one bounded chosen-path failover proof requires a narrow fix

4. `weed/server/volume_server_block.go`
   - `reference only` unless the real delivery proof exposes a direct mismatch between delivered truth and local application

5. `weed/server/qa_block_*test.go`
   - `update in place`
   - add one or two bounded end-to-end control-loop proofs rather than broad scenario explosion

6. `weed/storage/blockvol/v2bridge/control.go`
   - `reference only`
   - accepted control mapping from `P1` should remain stable unless the real loop exposes a true mismatch

7. copy guidance
   - prefer `update in place`
   - no parallel control loop
   - no large mock-only harness that bypasses the actual chosen path

##### Validation focus

Required proofs:

1. master-driven delivery proof
   - one real chosen-path assignment generated on the master side reaches the live volume-server path through heartbeat / gRPC delivery

2. identity preservation proof
   - accepted `P1` stable identity survives that real delivery loop intact

3. post-delivery convergence proof
   - accepted `P2` runtime/reporting convergence still holds after real master-driven delivery

4. repeated-delivery boundedness proof
   - accepted `P3` repeated unchanged assignment idempotence still holds when the same truth is redelivered through the real loop

5. boundedness / no-overclaim proof
   - `P4` closes this remaining chosen-path control-loop gap without claiming full cluster-hardening closure

Reject if:

1. evidence still begins at manual `ProcessAssignments()` only
2. master-side production is bypassed in the claimed end-to-end proof
3. identity/convergence/idempotence accepted in `P1` / `P2` / `P3` do not survive the fuller loop
4. implementation expands into unrelated cluster/product work

##### Suggested first cut

1. choose one bounded master-driven chosen-path scenario:
   - create-path assignment delivery, or
   - one bounded failover/reassignment delivery
2. expose the narrowest deterministic proof seam in the real heartbeat / gRPC loop
3. prove delivered truth arrives intact at the live volume-server path
4. prove post-delivery runtime/reporting truth matches the same master-originated truth
5. prove repeated unchanged delivery stays bounded under accepted `P3`

##### Assignment For `sw`

1. Goal
   - deliver `P4` master-driven heartbeat / gRPC control-loop closure on the chosen path

2. Required outputs
   - one real master-originated delivery proof
   - one identity-preservation-through-delivery proof
   - one post-delivery convergence/reporting proof
   - one repeated-unchanged-delivery boundedness proof if the same loop naturally redelivers truth
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09` backend execution semantics
   - do not reopen accepted `P1` / `P2` / `P3` semantics except for narrow proof-exposed bugs
   - keep `P4` bounded to one real chosen-path master/heartbeat/gRPC control-loop proof package
   - do not let `P4` turn into broad cluster-hardening or product-surface work

##### Assignment For `tester`

1. Goal
   - validate that `P4` closes the remaining master-driven chosen-path control-loop proof gap rather than only adding new helper-level plumbing

2. Validate
   - real master-originated chosen-path delivery
   - stable-ID preservation through that loop
   - post-delivery runtime/reporting convergence
   - bounded repeated-delivery/idempotence behavior if applicable

3. Reject if
   - evidence still starts at local ingestion only
   - master-side truth is not part of the claimed proof chain
   - the slice overclaims full cluster-hardening or product readiness

---

### P4 Completion Record

Date: 2026-04-02
Status: accepted (rev 5 + corrected adversarial validation)
Revisions: 5 (rev 1-4 progressively expanded real master-produced delivery, promoted-VS proof, and bounded idempotence; rev 5 closed separate promoted-VS proof and full-set idempotence; corrected adversarial validation confirmed existing epoch-regression protection on the promoted volume)

#### Accepted contract

- real master-produced chosen-path assignment truth is now part of the accepted one-chain control proof
- accepted create-path proof now covers:
  - real master assignment production
  - real queue/proto preservation
  - real VS application
  - visible epoch/heartbeat convergence
- accepted failover-path proof now covers:
  - real master failover assignment production
  - delivery into a separate promoted volume-server surface
  - promoted volume epoch convergence
  - promoted volume-server heartbeat convergence
- accepted `P1` stable identity survives the master-originated delivery path
- accepted `P3` repeated unchanged delivery remains bounded/idempotent on the proven slice
- corrected adversarial validation confirms stale lower-epoch delivery to the promoted volume is rejected by the existing `ErrEpochRegression` guard; no new V1 bug was confirmed
- `P4` remains bounded chosen-path control-loop closure and does not claim full live transport-stream deployment proof, broad cluster hardening, or product-surface readiness

#### Files changed

| File | Action |
|------|--------|
| `weed/server/qa_block_control_loop_test.go` | New/updated: real master-produced create/failover delivery, separate promoted-VS proof, identity preservation, bounded repeated-delivery proof |
| `weed/server/qa_block_control_loop_adversarial_test.go` | New/updated: corrected promoted-volume stale-epoch adversarial guard |
| `weed/server/volume_server_block.go` | Updated: bounded repeated-delivery idempotence on the live path, including full-set comparison for multi-replica assignment equality |

#### Test inventory (5 tests)

| Test | Proves |
|------|--------|
| `TestP10P4_MasterCreate_FullDelivery` | real master-produced create assignment reaches live VS apply and heartbeat truth |
| `TestP10P4_MasterFailover_Convergence` | real failover assignment reaches separate promoted VS, and promoted epoch/heartbeat converge |
| `TestP10P4_IdentityPreservation` | stable identity survives master-produced delivery without address-derived fallback |
| `TestP10P4_RepeatedDelivery_Idempotence` | repeated unchanged master-produced delivery remains bounded/idempotent on the proven slice |
| `TestAdversarial_P10P4_StaleEpochAfterFailover` | corrected promoted-volume stale lower-epoch delivery is rejected; epoch does not regress |

#### Residual note

1. `P4` is accepted as bounded chosen-path control-loop closure, not as whole-cluster or full live transport-stream deployment proof
2. `Phase 10` is now closed
3. the next planned phase is `Phase 11` product-surface rebinding
