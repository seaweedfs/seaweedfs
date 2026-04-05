# Phase 17 Checkpoint Review

Date: 2026-04-04
Status: ready for review

## Review Object

Review the current `Phase 17` checkpoint as:

1. `Phase 16` finish-line checkpoint accepted as the bounded runtime stop-line
2. `17A` delivered as a broader recovery/lifecycle branch map
3. `17B` delivered as a bounded failover/publication whole-chain contract draft
4. `17C` delivered as a long-window restart/disturbance policy draft
5. `17D` delivered as a first launch-envelope draft

This checkpoint should be judged as a bounded product-claim checkpoint, not as a
broad production-readiness or rollout-approval review.

## What Is In Scope

### Current checkpoint claim

The checkpoint may now claim that, for the bounded chosen path:

1. the broader recovery/lifecycle branches are explicitly enumerated and no
   longer hidden in implementation-only reasoning
2. one bounded failover/publication whole-chain statement is explicit and tied
   to named evidence
3. long-window restart/disturbance handling is expressed as explicit runtime
   rule, explicit temporary inconsistency policy, or explicit non-claim
4. the first launch envelope is finite, with supported scope, exclusions, and
   launch blockers written down

### Expected judgment

1. the checkpoint is a real product-claim-shaping step, not only wording
2. claims, non-claims, and blockers are evidence-backed and bounded
3. the stop-line remains disciplined: nothing here should silently broaden into
   generic launch approval

## What Is Explicitly Out Of Scope

Do NOT review this checkpoint as claiming:

1. broad generic production readiness
2. support for every restart/failover/disturbance branch
3. broad transport/frontend matrix support
4. `RF>2` product closure
5. pilot success or soak success as generic production proof

## Primary Files

Checkpoint framing:

1. `sw-block/.private/phase/phase-17.md`
2. `sw-block/.private/phase/phase-17-checkpoint-review.md`
3. `sw-block/design/v2-first-launch-supported-matrix.md`
4. `sw-block/design/v2-product-completion-overview.md`
5. `sw-block/design/v2-protocol-truths.md`
6. `sw-block/design/v2-protocol-claim-and-evidence.md`

Primary evidence code/tests:

1. `weed/server/master_block_registry.go`
2. `weed/server/master_block_registry_test.go`
3. `weed/server/qa_block_publication_test.go`
4. `weed/server/qa_block_disturbance_test.go`
5. `weed/server/qa_block_cp11b3_adversarial_test.go`
6. `weed/server/volume_server_test.go`

## Accepted Claim Set

1. broader recovery/lifecycle branches on the chosen path are now classified as
   closed, partially proven, or residual
2. one bounded failover/publication contract is explicit:
   after failover completion and winning-primary assignment delivery/applied,
   lookup/publication must point to the winning primary and agree with registry
   truth
3. one bounded disturbance policy table is explicit for startup
   non-authoritative inventory, repeated restart before convergence, stale rejoin
   input, repeated failover windows, and degraded sparse heartbeat handling
4. one first-launch envelope draft is explicit for the bounded chosen path

## Explicit Non-Claims

1. broad whole-surface failover/publication proof
2. broad restart-window behavior outside the explicit `17C` policy table
3. broad transport/frontend approval beyond the named bounded envelope
4. launch approval, pilot approval, or rollout approval

## Residual Gaps

1. stronger whole-surface publication proof across more outward surfaces
2. broader restart/rejoin/repeated-disturbance closure beyond the current policy
   table
3. pilot-pack, preflight, stop-condition, and controlled-rollout artifacts
4. any broader launch claim that cannot map directly to named accepted evidence

## Evidence Summary

### `17A` branch map

1. branch inventory is derived from `Phase 16` finish-line residuals plus named
   restart/disturbance/failover tests in `weed/server`
2. result:
   - restart same-lineage reconstruction is classified closed on the bounded
     chosen path
   - the remaining major branches are classified partially proven rather than
     silently implied

### `17B` failover/publication contract

1. `TestP11P3_Failover_PublicationSwitches`
2. `TestP12P1_FailoverPublication_Switch`
3. `TestP11P3_HeartbeatReconstruction`
4. failover/promotion tests in `qa_block_cp11b3_adversarial_test.go`
5. result:
   - one bounded whole-chain publication statement is supportable

### `17C` disturbance policy

1. `TestStartBlockService_ScanFailureEmitsNonAuthoritativeInventory`
2. `TestRegistry_UpdateFullHeartbeatWithInventoryAuthority_(NonAuthoritativeEmptyDoesNotDelete|AuthoritativeEmptyStillDeletes)`
3. `TestP12P1_(Restart_SameLineage|RepeatedFailover_EpochMonotonic|StaleSignal_OldEpochRejected)`
4. missing-field truth-retention tests in `master_block_registry_test.go`
5. result:
   - the main long-window disturbance classes are now policy-shaped rather than
     only code-shaped

### `17D` launch envelope

1. `Phase 12 P4` bounded floor / rollout-gate package
2. `CP13-1..9`
3. `Phase 16` finish-line checkpoint
4. `Phase 17A-17C` branch/contract/policy package
5. result:
   - first supported envelope, exclusions, and launch blockers are finite and
     named

## Review Questions

### For `sw`

Please check implementation and checkpoint coherence:

1. Is the `Phase 17` package coherent as one bounded product-claim checkpoint?
2. Are the envelope exclusions and blockers disciplined enough to avoid silent
   overclaim?
3. Is any part of the current package still too vague to support review or later
   global-doc synchronization?

### For `tester`

Please challenge the proof posture:

1. Is the `17B` contract actually supported by the cited tests, or only loosely
   suggested by them?
2. Does the `17C` policy table faithfully separate runtime rule from temporary
   inconsistency window?
3. Are there any obvious missing outward surfaces that make the current launch
   envelope too optimistic even in bounded form?

### For `manager`

Please challenge scope and stop-line discipline:

1. Is `Phase 17` the right place to stop this checkpoint package before
   productionization?
2. Are the current launch blockers and explicit non-claims sufficient to prevent
   the package from being misread as launch approval?
3. Should any current item be moved out of `Phase 17` and into productionization
   instead?

## Requested Output Shape

Please reply with one of:

1. `ACCEPT`
2. `ACCEPT WITH MINOR FIXES`
3. `REJECT`

If not `ACCEPT`, list findings ordered by severity and keep them bounded to this
checkpoint's actual claim set.
