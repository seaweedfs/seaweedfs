# V2 Acceptance Evidence Map

Date: 2026-04-09
Status: active

## Purpose

This note maps the current V2 evidence across three layers:

1. protocol/simulation proof
2. `weed/` component and server proof
3. real-hardware operational proof

It is intentionally not another scenario backlog. Its goal is to answer:

1. what is already proven
2. where the proof lives
3. what is still missing enough that it should drive next work

## 1. Protocol And Simulation Coverage

Primary design references:

1. `sw-block/design/v2-acceptance-criteria.md`
2. `sw-block/design/v2_scenarios.md`

Current state:

1. `A1` through `A12` are defined at the protocol/design layer in `v2-acceptance-criteria.md`.
2. `v2_scenarios.md` currently marks `S1` through `S20` as covered in `distsim` and related model/protocol tests.
3. This means the broad semantic matrix is already mostly closed at the simulation layer.

What this proves:

1. committed-prefix and lineage rules are modeled
2. stale epoch and stale traffic rejection are modeled
3. failover, restart, partition, and mixed-state rules are modeled
4. restart-during-catchup and restart-during-rebuild are modeled

What this does not prove by itself:

1. real `weed/` runtime integration
2. real `BlockVol` data installation and WAL replay behavior
3. control-plane convergence latency on real heartbeat paths
4. operational comparisons between `V1`, `V1.5`, and `V2`

## 2. weed Runtime And Component Coverage

These tests provide engine-to-runtime and storage/backend proof beyond the pure protocol layer.

### Component-Level Proof

Relevant paths:

1. `weed/storage/blockvol/test/component/rebuild_failover_rejoin_test.go`
2. `weed/storage/blockvol/test/component/rebuild_matrix_gaps_test.go`
3. `weed/storage/blockvol/test/component/fast_rejoin_catchup_test.go`
4. `weed/storage/blockvol/test/component/rebuild_e2e_test.go`
5. `weed/storage/blockvol/test/component/rebuild_realistic_test.go`

Current high-value coverage:

1. failover -> old primary rejoins -> rebuild -> data converges
2. stale replica restart beyond retained WAL -> rebuild
3. connection drop mid-base -> partial rebuild fails closed -> fresh rebuild converges
4. quick rejoin with retained WAL -> catch-up chosen instead of rebuild
5. rebuild transport and realistic volume rebuild cases

New explicit complement added in this pass:

1. `TestFastRejoinCatchUp_FailoverRejoinUsesRetainedWAL`
   - proves the old primary can return after failover and still use bounded catch-up when retained WAL covers the gap
   - proves the real shipper catch-up path converges without starting rebuild

### Server-Level Recovery Proof

Relevant path:

1. `weed/server/block_recovery_test.go`

Current high-value coverage:

1. live-path catch-up planning and execution
2. fact-driven escalation from catch-up to rebuild
3. probe-driven rebuild session installation
4. command-driven rebuild start on the live path
5. fail-closed behavior when no fresh `start_rebuild` command exists

New explicit retry-stability proof added in this pass:

1. `TestP16B_RunRebuild_ExecutionFailureAllowsFreshRetry`
   - proves a rebuild execution failure does not wedge the sender/runtime
   - proves a fresh rebuild session can be installed afterward
   - proves the second rebuild can still reach `InSync`

## 3. Integration And Scenario Evidence

Relevant paths:

1. `weed/storage/blockvol/testrunner/scenarios/internal/v2-fast-rejoin-catchup.yaml`
2. `weed/storage/blockvol/testrunner/scenarios/internal/v2-rebuild-failure-retry.yaml`
3. `weed/storage/blockvol/testrunner/scenarios/internal/v2-rebuild-rejoin.yaml`

Current status:

1. `v2-rebuild-rejoin.yaml` is the full real-system rebuild-rejoin story.
2. `v2-fast-rejoin-catchup.yaml` now asserts that rejoin converges via catch-up and that no remote rebuild start was sent.
3. `v2-rebuild-failure-retry.yaml` now asserts that the retry path includes more than one rebuild attempt, instead of only checking the final data state.

Why these matter:

1. they bridge the gap between narrow unit/component tests and real multi-process orchestration
2. they expose heartbeat, assignment, probe, and retry timing effects that protocol tests do not see

## 4. Real-Hardware Evidence

Current proved run:

1. `v2-rebuild-rejoin`

Observed result:

1. full lifecycle passed end-to-end on real hardware
2. create -> write -> failover -> write more -> old primary rejoin -> rebuild -> verify
3. rebuilt replica preserved both the pre-failover and post-failover checkpoints

Important interpretation:

1. the data-plane rebuild itself was fast
2. the larger wall-clock recovery time was dominated by control-plane cadence:
   heartbeat propagation, assignment refresh, probe timing, and master-visible healthy convergence

So the current real-hardware result proves:

1. the primary-direct remote rebuild route works
2. the replica-side install path works
3. data continuity survives the full failover/rejoin cycle

It does not yet fully characterize:

1. control-plane latency breakdown as an explicit acceptance artifact
2. fast-rejoin catch-up on real hardware as distinct from rebuild-rejoin
3. RF=3 multi-replica operational behavior on hardware

## 5. Remaining Meaningful Gaps

These are the remaining gaps worth prioritizing. They are not generic matrix expansion.

### Gap 1: Operational Catch-Up After Failover

Needed:

1. explicit real integration evidence for old-primary quick return after failover where retained WAL is still sufficient

Why it matters:

1. it is the natural complement to rebuild-rejoin
2. it proves the system does not over-escalate to rebuild when bounded replay is enough

Current state:

1. component proof exists
2. scenario proof now exists at the testrunner level
3. hardware-grade acceptance evidence for this exact path is still worth collecting

### Gap 2: Control-Plane Convergence Visibility

Needed:

1. explicit breakdown for restart -> first heartbeat
2. assignment refresh -> probe start
3. probe start -> session accepted
4. session completed -> master healthy observed

Why it matters:

1. current operational latency is dominated by control-plane cadence, not data transfer
2. optimization work should target measured convergence phases, not the rebuild data plane blindly

### Gap 3: V1 / V1.5 / V2 Operational Comparisons

Still called out by `v2_scenarios.md`:

1. changed-address restart
2. same-address transient outage
3. slow reassignment recovery

Why it matters:

1. these are operational regressions/improvements, not just protocol semantics
2. they explain why V2 is better, not only that V2 is internally consistent

### Gap 4: RF=3 Operational Recovery Evidence

Needed:

1. targeted proof that the same recovery rules hold when more than one replica exists
2. especially for one stale replica, one healthy replica, and one active primary during rejoin/rebuild

Why it matters:

1. many current proofs are strongest in RF=2
2. the design explicitly supports RF=3 and per-replica recovery facts

## 6. Recommended Next Evidence Work

Order:

1. collect one hardware-grade fast-rejoin catch-up run
2. add explicit control-plane latency timestamps to recovery scenarios
3. add the highest-value `V1` / `V1.5` / `V2` operational comparison cases
4. add one RF=3 recovery acceptance slice before broadening anything else

## 7. Bottom Line

Current position:

1. protocol matrix: broadly covered
2. `weed` runtime/component proof: strong and improving
3. real-hardware rebuild-rejoin proof: achieved

So the next work should focus on:

1. the fast-rejoin catch-up operational path
2. control-plane convergence evidence
3. a small set of operational comparison cases

The project does not currently need a broad new matrix. It needs sharper proof on the remaining operational edges.
