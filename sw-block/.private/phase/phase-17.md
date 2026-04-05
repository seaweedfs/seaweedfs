# Phase 17

Date: 2026-04-04
Status: active
Purpose: turn the bounded `Phase 16` runtime checkpoint into a bounded
product-claim checkpoint with explicit recovery/failover scope, disturbance
policy, and launch-envelope boundaries

## Why This Phase Exists

`Phase 16` closed the visible bounded runtime seams on the chosen path:

1. steady-state and bounded restart reconstruction preserve accepted explicit
   truth
2. sparse heartbeats no longer silently erase accepted truth
3. empty full-inventory delete behavior is explicit rather than heuristic

That is enough to stop `Phase 16`.

It is not enough to make a stronger product statement yet.

The next missing work is larger than heartbeat-field closure:

1. broader recovery-loop closure across more lifecycle branches
2. failover/publication whole-chain statement
3. long-window restart/disturbance policy
4. first-launch envelope freeze

This phase exists to package those larger objects explicitly instead of
continuing indefinite micro-slicing.

## Supersession Note

This document supersedes the earlier `Phase 17` separation-tracking draft.

That older draft was useful as an engineering migration record, but it is no
longer the right active phase object after the `Phase 16` finish-line
checkpoint.

For current planning:

1. use this file as the active `Phase 17` definition
2. treat any older separation-tracking notes only as historical context

## Relationship To Phase 16

`Phase 16` answered:

1. who owns bounded runtime semantics on the chosen path
2. whether the heartbeat/master/API path can preserve accepted explicit truth

`Phase 17` is different.

It answers:

1. which broader recovery/failover branches are actually closed
2. what stronger outward/publication statement is supportable
3. what long-window disturbance behavior is policy, not accident
4. what the first supported launch envelope really is

In short:

1. `Phase 16` = bounded runtime checkpoint
2. `Phase 17` = bounded product-claim checkpoint

## Phase Goal

Produce one bounded post-`Phase 16` checkpoint where:

1. the broader recovery-loop branch map is finite and explicitly classified
2. at least one stronger failover/publication whole-chain statement is defined
   and proven
3. long-window restart/disturbance behavior is reduced to explicit policy or
   explicit non-claim
4. the first supported launch envelope is frozen from accepted evidence

## Scope

### In scope

1. broader recovery-loop branch mapping and classification
2. stronger outward/publication consistency statement after failover
3. restart/rejoin/repeated-failover policy on the chosen path
4. supported-envelope and explicit exclusion freeze
5. proof-package and review artifact for the resulting claim boundary

### Out of scope

1. broad protocol rediscovery
2. broad transport-matrix expansion
3. `RF>2` general product closure
4. indefinite soak/pilot execution inside this phase
5. silent widening of runtime scope beyond the chosen path

## Phase 17 Workstreams

### `17A`: Broader Recovery-Loop Closure Map

Goal:

1. replace the current implicit branch set with one explicit recovery/lifecycle
   map

Acceptance object:

1. the main recovery/lifecycle branches are listed explicitly
2. each branch is classified as:
   - closed and proven
   - partially proven
   - residual / out of scope
3. there is no hidden "probably supported" branch left in wording only

Target branch classes:

1. steady-state failover
2. restart same-lineage reconstruction
3. restart after ownership change
4. replica rejoin after demotion/promotion
5. repeated failover in one disturbance window
6. startup not-yet-authoritative window
7. degraded-but-not-rebuild path
8. rebuild-entry / rebuild-exit path

Status:

1. delivered as first branch-map slice

Current chosen map:

1. steady-state failover
   - classification: partially proven
   - current evidence:
     - `TestP12P1_FailoverPublication_Switch`
     - `TestP11P3_Failover_PublicationSwitches`
     - failover timer/promotion tests in `qa_block_cp11b3_adversarial_test.go`
   - current gap:
     - stronger whole-chain outward publication contract still belongs to `17B`
2. restart same-lineage reconstruction
   - classification: closed and proven on the bounded chosen path
   - current evidence:
     - `TestP12P1_Restart_SameLineage`
     - `TestP11P3_HeartbeatReconstruction`
     - `TestMasterRestart_HigherEpochWins`
   - current boundary:
     - bounded chosen path only, not generic restart-product proof
3. restart after ownership change
   - classification: partially proven
   - current evidence:
     - `TestMasterRestart_HigherEpochRebasesExplicitPrimaryTruth`
     - `TestMasterRestart_HigherEpochSparsePrimaryClearsOldExplicitTruth`
     - `TestMasterRestart_LowerEpochBecomesReplica`
   - current gap:
     - ownership truth rebasing is proven, but broader outward failover statement
       is not yet frozen
4. replica rejoin after demotion/promotion
   - classification: partially proven
   - current evidence:
     - `TestMasterRestart_ReplicaHeartbeat_AddedCorrectly`
     - `TestMasterRestart_DuplicateReplicaHeartbeat_NoDuplicate`
     - `TestQA_CP82_MasterRestart_ReconstructReplicas_ThenFailover`
   - current gap:
     - rejoin semantics are only boundedly covered, not elevated to a full branch
       contract
5. repeated failover in one disturbance window
   - classification: partially proven
   - current evidence:
     - `TestP12P1_RepeatedFailover_EpochMonotonic`
     - `TestQA_T2_RF3_OrphanedPrimary_BestReplicaPromoted`
     - `TestQA_T3_OrphanDeferredTimer_FiresAndPromotes`
   - current gap:
     - broader repeated-disturbance publication coherence is not yet a closed
       product claim
6. startup not-yet-authoritative window
   - classification: partially proven
   - current evidence:
     - `TestStartBlockService_ScanFailureEmitsNonAuthoritativeInventory`
     - `TestRegistry_UpdateFullHeartbeatWithInventoryAuthority_NonAuthoritativeEmptyDoesNotDelete`
     - `TestQA_Reg_FullHeartbeatEmptyServer`
   - current gap:
     - one real sender path exists, but long-window startup policy remains for
       `17C`
7. degraded-but-not-rebuild path
   - classification: partially proven
   - current evidence:
     - bounded `Phase 15/16` readiness/publication/mode tests
     - `EntryToVolumeInfo` and block-volume handler coherence proofs
   - current gap:
     - current evidence proves bounded surface truth, not full lifecycle policy
8. rebuild-entry / rebuild-exit path
   - classification: partially proven
   - current evidence:
     - `16B-16K` bounded recovery execution ownership
     - `TestQA_Rebuild_FullCycle_CreateFailoverRecoverRebuild`
     - `TestQA_RF3_Rebuild_DeadReplicaCatchesUp`
   - current gap:
     - branch exists and is exercised, but broader recovery-loop closure is not
       yet claimed

Delivered result:

1. `Phase 17` now has one explicit recovery/lifecycle branch inventory instead
   of an implicit "some broader runtime remains" statement
2. the current state is now separated into:
   - one branch already closed on the bounded chosen path
   - several branches with real bounded evidence but not yet product-grade
     closure
3. this narrows the next work:
   - `17B` should focus on outward failover/publication statement
   - `17C` should focus on long-window policy for the partially proven branches

Evidence basis:

1. `Phase 16` finish-line review and proof package
2. restart and heartbeat tests in `master_block_registry_test.go`
3. disturbance/publication QA tests in `weed/server/qa_block_*_test.go`

### `17B`: Failover / Publication Whole-Chain Statement

Goal:

1. strengthen from internal truth preservation to an outward statement that can
   be used in product review

Acceptance object:

1. one explicit failover/publication contract is written down
2. the contract names which outward surfaces must stay coherent:
   - mode
   - reason
   - readiness
   - publish health
   - publication/lookup visibility
3. at least one full failover chain is proven against that contract
4. any allowed transient inconsistency window is explicit

Status:

1. delivered as first contract-draft slice

Current chosen contract:

On the bounded chosen path, once failover has completed and the winning primary
assignment has been delivered/applied, the following must hold for one named
volume:

1. publication ownership
   - outward lookup/publication points to the winning primary, not the old
     primary
2. publication address coherence
   - publication-facing transport fields exposed by lookup agree with the
     registry entry for the winning primary
3. failover surface coherence
   - failover changes publication visibility/address truth rather than leaving
     stale old-primary publication outwardly visible
4. restart reconstruction compatibility
   - heartbeat reconstruction and restart-era registry truth do not break the
     same bounded publication contract

Current bounded whole-chain:

1. create on chosen path
2. establish primary publication truth
3. trigger failover
4. promote winning primary
5. deliver winning-primary assignment through the real VS path
6. verify outward lookup/publication now points to the new primary and agrees
   with registry truth

Bounded proven surfaces:

1. `LookupBlockVolume()`
2. registry-backed publication fields
3. heartbeat reconstruction path used by restart recovery

Explicitly not yet included in this first contract draft:

1. full list/status/UI surface coherence after failover
2. long-window transient behavior before the winning assignment is delivered
3. every repeated-failover publication sequence
4. generic frontend/transport matrix guarantees beyond the chosen path

Allowed transient window on the current contract:

1. before failover completion and winning-primary assignment delivery, this
   contract does not yet require all outward surfaces to have converged
2. after that point, bounded lookup/publication truth must reflect the winning
   primary and must not still expose stale old-primary publication

Delivered result:

1. `Phase 17` now has one explicit failover/publication whole-chain statement
   instead of only a general "publication should switch" expectation
2. the strongest currently supportable statement is now bounded to:
   - failover completion
   - winning assignment delivered/applied
   - lookup/registry publication coherence on the chosen path
3. this makes the remaining work explicit:
   - widen to more outward surfaces only with named evidence
   - move long-window and pre-convergence behavior to `17C`

Evidence basis:

1. `TestP11P3_Failover_PublicationSwitches`
2. `TestP12P1_FailoverPublication_Switch`
3. `TestP11P3_HeartbeatReconstruction`
4. failover/promotion tests in `qa_block_cp11b3_adversarial_test.go`

Current gap after first contract draft:

1. the contract is strong enough for one bounded product-review statement
2. it is not yet a broad whole-surface publication proof
3. `17C` must define the long-window and pre-convergence policy around this
   contract

### `17C`: Long-Window Restart / Disturbance Policy

Goal:

1. turn restart/disturbance behavior into explicit policy instead of continuing
   local seam repair

Acceptance object:

1. startup/restart/rejoin/disturbance cases are grouped into a finite policy set
2. each case has one of:
   - explicit runtime rule
   - explicit temporary inconsistency policy
   - explicit non-claim
3. "not yet authoritative" states are described as policy, not inferred only
   from code shape

Target disturbance classes:

1. startup inventory not yet authoritative
2. repeated restart before convergence
3. rejoin with stale ownership/publication context
4. repeated failover during one disturbance window
5. long-window degraded heartbeat sparsity

Status:

1. delivered as first policy-draft slice

Current chosen policy table:

1. startup inventory not yet authoritative
   - policy type: explicit runtime rule
   - rule:
     - non-authoritative empty full heartbeat must preserve existing entries
     - authoritative empty full heartbeat may still drive stale-delete
   - evidence:
     - `TestStartBlockService_ScanFailureEmitsNonAuthoritativeInventory`
     - `TestRegistry_UpdateFullHeartbeatWithInventoryAuthority_NonAuthoritativeEmptyDoesNotDelete`
     - `TestRegistry_UpdateFullHeartbeatWithInventoryAuthority_AuthoritativeEmptyStillDeletes`
   - current non-claim:
     - this does not yet define broad long-window startup behavior for every
       delayed-load or multi-step bootstrap case
2. repeated restart before convergence
   - policy type: explicit temporary inconsistency policy
   - rule:
     - until winning-primary assignment is delivered/applied, full outward
       convergence is not yet required by the current bounded contract
     - after delivery/applied, registry epoch/publication truth must not regress
   - evidence:
     - `TestP12P1_Restart_SameLineage`
     - `TestP11P3_HeartbeatReconstruction`
     - `TestMasterRestart_HigherEpochWins`
   - current non-claim:
     - this is not yet generic proof for arbitrarily long repeated restart
       windows
3. rejoin with stale ownership/publication context
   - policy type: explicit runtime rule
   - rule:
     - stale old-epoch or stale old-role input must not overwrite the winning
       ownership/publication truth
     - replica rejoin may reconstruct bounded replica state without becoming the
       new publication owner merely by reconnecting
   - evidence:
     - `TestP12P1_StaleSignal_OldEpochRejected`
     - `TestMasterRestart_LowerEpochBecomesReplica`
     - `TestMasterRestart_ReplicaHeartbeat_AddedCorrectly`
   - current non-claim:
     - broader rejoin policy across all frontend/publication surfaces remains
       outside this first draft
4. repeated failover during one disturbance window
   - policy type: explicit temporary inconsistency policy
   - rule:
     - repeated failover may create a bounded convergence window
     - epoch must still move monotonically and duplicate-promotion shapes must
       not become accepted steady state
   - evidence:
     - `TestP12P1_RepeatedFailover_EpochMonotonic`
     - `TestQA_T2_RF3_OrphanedPrimary_BestReplicaPromoted`
     - `TestQA_T3_OrphanDeferredTimer_FiresAndPromotes`
   - current non-claim:
     - this is not yet a broad user-visible publication-stability guarantee under
       arbitrary oscillation
5. long-window degraded heartbeat sparsity
   - policy type: explicit runtime rule
   - rule:
     - once accepted on the bounded path, explicit degraded/mode/readiness truth
       must not be silently erased by later sparse heartbeats on existing entries
     - degraded state remains degraded until bounded readiness/publication truth
       closes again
   - evidence:
     - `TestRegistry_UpdateFullHeartbeat_MissingFieldsPreserveAcceptedExplicitPrimaryTruth`
     - `TestRegistry_UpdateFullHeartbeat_ReplicaReadyMissingFieldPreservesAcceptedExplicitTruth`
     - degraded surface proofs in `master_block_observability_test.go` and
       `master_server_handlers_block_test.go`
   - current non-claim:
     - this does not yet claim indefinite sparse-heartbeat tolerance on every
       lifecycle branch

Delivered result:

1. `Phase 17` now has a finite disturbance-policy table instead of only a
   general "restart/disturbance still remains" statement
2. the current policy shape is explicit about which cases are:
   - hard runtime rules
   - bounded temporary inconsistency windows
   - still non-claims
3. this reduces the remaining ambiguity before launch-envelope work

Evidence basis:

1. `Phase 16` finish-line proof package
2. restart/disturbance tests in `qa_block_disturbance_test.go`
3. restart/heartbeat truth-retention tests in `master_block_registry_test.go`
4. expand/empty-heartbeat disturbance tests in `qa_block_expand_adversarial_test.go`

Current gap after first policy draft:

1. the policy table is sufficient for bounded claim hygiene
2. it is not yet a broad production hardening or soak statement
3. `17D` must freeze the supported launch envelope using these explicit rules
   and non-claims

### `17D`: Launch Envelope Freeze

Goal:

1. freeze the first supported product envelope from accepted `Phase 12-17`
   evidence

Acceptance object:

1. supported topology/transport matrix is explicit
2. explicit exclusions are written down
3. launch-blocking vs post-launch items are separated
4. every launch claim maps back to accepted evidence
5. missing evidence remains an explicit constraint rather than silent support

Status:

1. delivered as first launch-envelope draft

Current chosen launch envelope:

1. replication and durability envelope
   - supported:
     - `RF=2`
     - `sync_all`
   - evidence basis:
     - `C-RF2-SYNCALL-CONTRACT`
     - `CP13-1..9`
     - `C-PHASE16-RUNTIME-CHECKPOINT`
2. control/runtime envelope
   - supported:
     - existing master / volume-server heartbeat path
     - bounded `Phase 16` runtime checkpoint
     - bounded `Phase 17A-17C` claim/policy envelope
   - evidence basis:
     - `Phase 10` accepted control-plane closure
     - `Phase 16` finish-line review
     - current `phase-17.md`
3. backend/runtime envelope
   - supported:
     - `blockvol` as execution backend
     - `v2bridge` as backend-binding adapter
     - explicit `V2 core` as semantic owner
   - evidence basis:
     - `Phase 09` execution closure
     - `Phase 14-16` accepted checkpoints
4. frontend/product-surface envelope
   - supported on the bounded chosen path:
     - `iSCSI`
     - bounded `CSI` integration
     - bounded `NVMe` publication/integration already accepted on the chosen path
   - current launch-reading rule:
     - these surfaces are only supported inside the same bounded chosen envelope,
       not as generic transport-matrix approval
5. operating-mode envelope
   - supported:
     - bounded failover/publication statement from `17B`
     - bounded restart/disturbance policy from `17C`
   - current launch-reading rule:
     - use the explicit `17B` contract and `17C` policy table as the launch
       interpretation boundary

Explicit exclusions in the first draft:

1. `RF>2`
2. broad transport/frontend matrix support beyond the chosen path
3. broad whole-surface failover/publication proof
4. generic long-window soak or pilot success as production proof
5. broad restart-window behavior outside the explicit `17C` policy table
6. broad launch approval beyond the named bounded envelope

Launch-blocking items:

1. no review outcome yet for the full `Phase 17` package
2. no pilot pack/preflight/stop-condition artifact yet
3. no controlled-rollout review artifact yet
4. no explicit broader failover/publication claim beyond the bounded `17B`
   contract

Explicitly not launch-blocking inside this first draft:

1. lack of generic `RF>2` support
2. lack of broad transport-matrix support
3. lack of broad rollout approval
4. lack of indefinite soak proof inside this phase

Claim-mapping rule:

1. any first-launch claim must map back to:
   - `Phase 12 P4` bounded floor / rollout-gate evidence
   - `CP13` bounded contract and workload evidence
   - `Phase 16` bounded runtime checkpoint
   - `Phase 17A-17C` branch/contract/policy framing
2. if a claim cannot map back to one of those named evidence anchors, it belongs
   in exclusions or later productionization work, not in the first-launch
   envelope

Delivered result:

1. the first launch envelope is now finite instead of implied
2. supported scope, exclusions, and launch blockers are all named in one place
3. this gives the product line a bounded pre-pilot statement without pretending
   that broad launch approval already exists
4. the phase now has explicit review/checkpoint and supported-matrix artifacts:
   - `sw-block/.private/phase/phase-17-checkpoint-review.md`
   - `sw-block/design/v2-first-launch-supported-matrix.md`

Evidence basis:

1. `Phase 12 P4` bounded floor / rollout-gate package
2. `CP13-1..9` bounded contract/workload/mode evidence
3. `Phase 16` finish-line checkpoint
4. `Phase 17A-17C` branch/contract/policy drafts
5. `v2-protocol-claim-and-evidence.md`
6. `sw-block/.private/phase/phase-17-checkpoint-review.md`
7. `sw-block/design/v2-first-launch-supported-matrix.md`

Current gap after first envelope draft:

1. the envelope is frozen as a bounded draft, not yet a full launch decision
2. pilot pack, preflight, stop conditions, and controlled rollout review remain
   for productionization
3. broader claims still require either explicit new evidence or explicit
   exclusion handling

## Stop-Line Rule

Do not keep widening `Phase 17` if a task requires:

1. broad failover architecture redesign
2. broad transport-matrix expansion
3. generic production proof from pilot/soak behavior
4. implicit launch approval without explicit evidence mapping

If one of those appears:

1. stop the current slice
2. record it as a residual or productionization item
3. do not hide it inside a runtime-logic patch

## Proof Shape

The target proof posture for `Phase 17` is still an engineering proof package,
not a mathematical proof.

Required shape:

1. branch map
   - finite recovery/lifecycle branch inventory
2. contract
   - explicit failover/publication statement
3. policy table
   - explicit disturbance and startup-window rules
4. envelope
   - supported matrix and exclusions
5. review
   - one checkpoint review artifact with claims, non-claims, residuals, and
     exact proof commands

## Phase Closeout Target

`Phase 17` should close only when one checkpoint can credibly say:

1. broader recovery-loop branches are named and classified
2. at least one stronger failover/publication whole-chain statement is proven
3. long-window disturbance behavior is explicit as rule or non-claim
4. the first launch envelope is frozen from accepted evidence
5. residual gaps are named instead of hidden

## Non-Claims

`Phase 17` should still not claim by default:

1. broad generic production readiness
2. support for every restart/failover/disturbance branch
3. `RF>2` product closure
4. broad transport-matrix support
5. pilot success as generic production proof

## Immediate Next Step

After the first `17A` branch map, first `17B` contract draft, first `17C`
policy draft, and first `17D` envelope draft, stop `Phase 17` and package the
checkpoint before widening anything else.

Reason:

1. `17A` now makes the branch inventory explicit
2. `17B` now makes one bounded failover/publication contract explicit
3. `17C` now makes the long-window and pre-convergence policy explicit
4. `17D` now freezes the first supported launch envelope from those explicit
   claims and non-claims
5. anything broader than that should enter productionization or a new explicit
   contradiction-driven slice, not silently widen `Phase 17`
6. the next artifacts after this phase are:
   - review outcome on `phase-17-checkpoint-review.md`
   - productionization documents driven by `v2-first-launch-supported-matrix.md`
