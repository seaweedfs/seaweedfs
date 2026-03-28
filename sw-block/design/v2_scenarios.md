# WAL V2 Scenarios

Date: 2026-03-26
Status: working scenario backlog
Purpose: define the scenario set that proves why WAL V2 exists, what it must do better than WAL V1, and what it should handle better than rebuild-heavy systems

Execution note:
- active implementation planning for these scenarios lives under `../.private/phase/`
- `design/` is the design/source-of-truth view
- `.private/phase/` is the execution/checklist view for `sw`

## Why This File Exists

V2 should not grow by adding random simulations.

Each new scenario should prove one of these claims:

1. committed data is never lost
2. uncommitted data is never falsely revived
3. epoch and promotion lineage are safe
4. short-gap recovery is cheaper and cleaner than rebuild
5. catch-up vs rebuild boundary is explicit and correct
6. historical data correctness is preserved

## Scenario Sources

The backlog draws scenarios from three sources:

1. **V1 / V1.5 real failures**
- real bugs and real-hardware gaps observed during Phase 12 / Phase 13
- these are the highest-value scenarios because they came from actual system behavior

2. **V2 design obligations**
- scenarios required by the intended V2 protocol shape
- examples:
  - reservations
  - lineage-first promotion
  - explicit catch-up vs rebuild boundary

3. **Distributed-systems adversarial cases**
- scenarios not yet seen in production, but known to be dangerous
- examples:
  - zombie primary
  - partitions
  - message reordering
  - multi-promotion lineage chains

This file is the shared backlog for anyone extending:

- `sw-block/prototype/fsmv2/`
- `sw-block/prototype/volumefsm/`
- `sw-block/prototype/distsim/`

For active development sequencing, see:
- `sw-block/.private/phase/phase-01.md`
- `sw-block/.private/phase/phase-02.md`
- `sw-block/design/v2-scenario-sources-from-v1.md`

Current simulator note:
- current `distsim` coverage already includes:
  - changed-address restart comparison across `V1` / `V1.5` / `V2`
  - same-address transient outage comparison
  - slow control-plane recovery comparison
  - stale-endpoint rejection
  - committed-prefix-aware promotion eligibility

## V2 Goals

Compared with WAL V1, V2 should improve:

1. state clarity
2. recovery boundary clarity
3. fencing and promotion correctness
4. testability of distributed behavior
5. proof of data correctness at a target `LSN`

Compared with rebuild-heavy systems, V2 should improve:

1. short-gap recovery cost
2. explicit progress semantics
3. catch-up vs rebuild decision quality

## Scenario Format

Each scenario should eventually define:

1. setup
2. event sequence
3. expected commit/ack behavior
4. expected promotion/fencing behavior
5. expected final data state at target `LSN`

Where possible, use synthetic 4K writes with:

- `value = LSN`

That makes correctness assertions trivial.

## Priority 1: Commit Safety

These scenarios prove the most important distributed claim:

- if the system ACKed a write under the configured policy, that write is not lost

### S1. ACK Then Primary Crash

Goal:
- prove a quorum-acknowledged write survives failover

Sequence:
1. primary commits a write
2. replicas durable-ACK enough nodes for policy
3. primary crashes immediately
4. coordinator promotes a valid replica

Expect:
- promoted node contains the committed `LSN`
- final state matches reference model at committed `LSN`

### S2. Non-Quorum Write Then Primary Crash

Goal:
- prove uncommitted data is not revived after failover

Sequence:
1. primary accepts a write locally
2. quorum durability is not reached
3. primary crashes
4. coordinator promotes another node

Expect:
- promoted node does not expose the uncommitted write
- committed `LSN` stays at previous value

### S3. Zombie Old Primary Is Fenced

Goal:
- prove old-epoch traffic cannot corrupt new lineage

Sequence:
1. primary loses lease
2. coordinator bumps epoch and promotes new primary
3. old primary continues trying to send writes / barriers

Expect:
- all old-epoch traffic is rejected
- no stale write becomes committed under the new epoch

## Priority 2: Short-Gap Recovery

These scenarios justify V2 over rebuild-heavy designs.

### S4. Brief Disconnect, WAL Catch-Up Only

Goal:
- prove a short outage recovers via WAL catch-up, not rebuild

Sequence:
1. replica disconnects briefly
2. primary continues writing
3. gap stays inside recoverable window
4. replica reconnects and catches up

Expect:
- `CatchingUp -> PromotionHold -> InSync`
- no rebuild required
- final state matches reference at target `LSN`

### S5. Flapping Replica Stays Recoverable

Goal:
- prove transient disconnects do not force unnecessary rebuild

Sequence:
1. replica disconnects and reconnects repeatedly
2. gaps stay within reserved recoverable windows

Expect:
- replica may move between `Lagging`, `CatchingUp`, and `PromotionHold`
- replica does not enter `NeedsRebuild` unless recoverability is actually lost

### S6. Tail-Chasing Under Load

Goal:
- prove behavior when primary writes faster than catch-up rate

Sequence:
1. replica reconnects behind
2. primary continues writing quickly
3. catch-up target may be reached or may fall behind again

Expect:
- explicit result:
  - converge and promote
  - or abort to `NeedsRebuild`
- never silently pretend the replica is current

## Priority 3: Catch-Up vs Rebuild Boundary

These scenarios justify the V2 recoverability model.

### S7. Recovery Initially Possible, Then Reservation Expires

Goal:
- prove `check -> reserve -> recover` is enforced

Sequence:
1. primary grants a recoverability reservation
2. catch-up starts
3. reservation expires or is revoked before completion

Expect:
- catch-up aborts
- replica transitions to `NeedsRebuild`
- no partial recovery is treated as success

### S8. Current Extent Cannot Recover Old LSN

Goal:
- prove the historical correctness trap

Sequence:
1. write block `B = 10` at `LSN 10`
2. later write block `B = 12` at `LSN 12`
3. attempt to recover state at `LSN 10` from current extent

Expect:
- mismatch detected
- scenario must fail correctness check

### S9. Snapshot + Tail Rebuild Works

Goal:
- prove correct long-gap reconstruction

Sequence:
1. take snapshot at `cpLSN`
2. later writes extend head
3. lagging replica rebuilds from snapshot
4. replay trailing WAL tail

Expect:
- final state matches reference at target `LSN`

## Priority 4: Quorum and Mixed Replica States

These scenarios justify V2 mode clarity.

### S10. Mixed States Under `sync_quorum`

Goal:
- prove `sync_quorum` remains available with mixed replica states

Sequence:
1. one replica `InSync`
2. one replica `CatchingUp`
3. one replica `Rebuilding`

Expect:
- writes may continue if durable quorum exists
- ACK gating follows quorum rules exactly

### S11. Mixed States Under `sync_all`

Goal:
- prove `sync_all` remains strict

Sequence:
1. same mixed-state setup as above

Expect:
- writes/acks block or fail according to `sync_all`
- no silent downgrade to quorum or best effort

### S12. Promotion Chooses Best Valid Lineage

Goal:
- prove promotion is correctness-first, not “highest apparent LSN wins”

Sequence:
1. candidate nodes have different:
   - flushed LSN
   - rebuild state
   - epoch lineage
2. coordinator chooses a new primary

Expect:
- only a valid-lineage node is promotable
- stale or inconsistent node is rejected

## Priority 5: Smart WAL / Recovery Classes

These scenarios justify V2’s future adaptive write path.

### S13. `WALInline` Window Is Recoverable

Goal:
- prove inline WAL payload replay works directly

Sequence:
1. missing range consists of `WALInline` records
2. planner grants reservation

Expect:
- catch-up allowed
- final state correct

### S14. `ExtentReferenced` Payload Still Resolvable

Goal:
- prove direct-extent records can still support catch-up when pinned

Sequence:
1. missing range includes `ExtentReferenced` records
2. payload objects / generations are still resolvable
3. reservation pins those dependencies

Expect:
- catch-up allowed
- final state correct

### S15. `ExtentReferenced` Payload Lost

Goal:
- prove metadata alone is not enough

Sequence:
1. missing range includes `ExtentReferenced` records
2. metadata still exists
3. payload object / version is no longer resolvable

Expect:
- planner returns `NeedsRebuild`
- catch-up is forbidden

## Priority 6: Restart and Rebuild Robustness

These scenarios justify operational resilience.

### S16. Replica Restarts During Catch-Up

Goal:
- prove restart does not corrupt catch-up state

Sequence:
1. replica is catching up
2. replica restarts
3. reconnect and recover again

Expect:
- no false promotion
- resume or restart recovery cleanly

### S17. Replica Restarts During Rebuild

Goal:
- prove rebuild interruption is safe

Sequence:
1. replica is rebuilding from snapshot
2. replica restarts mid-copy

Expect:
- rebuild aborts or restarts safely
- no partial base image is treated as valid

### S18. Primary Restarts Without Failover

Goal:
- prove restart with same lineage is handled explicitly

Sequence:
1. primary stops and restarts
2. coordinator either preserves or changes epoch depending on policy

Expect:
- replicas react consistently
- no stale assumptions about previous sender sessions

### S19. Chain Of Custody Across Multiple Promotions

Goal:
- prove committed data survives more than one failover lineage step

Sequence:
1. primary `A` commits writes
2. fail over to `B`
3. `B` commits additional writes
4. fail over to `C`

Expect:
- `C` contains all writes committed by `A` and `B`
- no committed data disappears across multiple promotions
- final state matches reference model at committed `LSN`

### S20. Network Partition With Concurrent Write Attempts

Goal:
- prove epoch fencing prevents split-brain writes during partition

Sequence:
1. cluster partitions into two live sides
2. old primary side continues trying to write
3. coordinator promotes a new primary on the surviving side
4. both sides attempt to send control/data traffic

Expect:
- only the current-epoch side can advance committed state
- stale-side writes are rejected or ignored
- no conflicting committed lineage appears

## Suggested Implementation Order

Implement in this order:

1. `S1` ACK then primary crash
2. `S2` non-quorum write then primary crash
3. `S3` zombie old primary fenced
4. `S4` brief disconnect with WAL catch-up
5. `S7` reservation expiry aborts catch-up
6. `S10` mixed-state quorum policy
7. `S9` long-lag rebuild from snapshot + tail
8. `S13-S15` Smart WAL recoverability

## Coverage Matrix

Status values:
- `covered`
- `partial`
- `not_started`
- `needs_richer_model`

| Scenario | Package | Test / Artifact | Status | Notes |
|---|---|---|---|---|
| `S1` ACK then primary crash | `distsim` | `TestQuorumCommitSurvivesPrimaryFailover` | `covered` | quorum commit survives failover |
| `S2` non-quorum write then primary crash | `distsim` | `TestUncommittedWriteNotPreservedAfterPrimaryLoss` | `covered` | no false revival |
| `S3` zombie old primary fenced | `distsim` | `TestZombieOldPrimaryWritesAreFenced` | `covered` | stale epoch traffic ignored |
| `S4` brief disconnect, WAL catch-up only | `distsim` | `TestReplicaCatchupFromPrimaryWAL` | `covered` | short-gap recovery |
| `S5` flapping replica stays recoverable | `distsim` | `TestS5_FlappingReplica_NoUnnecessaryRebuild`, `TestS5_FlappingWithStateTracking`, `TestS5_FlappingExceedsBudget_EscalatesToNeedsRebuild` | `covered` | both recoverable flapping and explicit budget-exceeded escalation are now asserted |
| `S6` tail-chasing under load | `distsim` | `TestS6_TailChasing_ConvergesOrAborts`, `TestS6_TailChasing_NonConvergent_Aborts`, `TestS6_TailChasing_NonConvergent_EscalatesToNeedsRebuild`, `TestP02_S6_NonConvergent_ExplicitStateTransition` | `covered` | explicit non-convergent `CatchingUp -> NeedsRebuild` path now asserted |
| `S7` reservation expiry aborts catch-up | `fsmv2`, `volumefsm`, `distsim` | `TestFSMReservationLostNeedsRebuild`, `TestModelReservationLostDuringCatchupAfterRebuild`, `TestReservationExpiryAbortsCatchup` | `covered` | present at 3 layers |
| `S8` current extent cannot recover old LSN | `distsim` | `TestCurrentExtentCannotRecoverOldLSN` | `covered` | historical correctness trap |
| `S9` snapshot + tail rebuild works | `distsim` | `TestReplicaRebuildFromSnapshotAndTail`, `TestSnapshotPlusTrailingReplayReachesTargetLSN` | `covered` | long-gap reconstruction |
| `S10` mixed states under `sync_quorum` | `volumefsm`, `distsim` | `TestModelSyncQuorumWithThreeReplicasMixedStates`, `TestSyncQuorumWithMixedReplicaStates` | `covered` | quorum stays available |
| `S11` mixed states under `sync_all` | `distsim` | `TestSyncAllBlocksWithMixedReplicaStates` | `covered` | strict sync_all behavior |
| `S12` promotion chooses best valid lineage | `distsim` | `TestPromotionUsesValidLineageNode`, `TestS12_PromotionChoosesBestLineage_NotHighestLSN`, `TestS12_PromotionRejectsRebuildingCandidate` | `covered` | lineage-first promotion now exercised beyond simple LSN comparison |
| `S13` `WALInline` window recoverable | `distsim` | `TestWALInlineRecordsAreRecoverable` | `covered` | inline payload recoverability |
| `S14` `ExtentReferenced` payload resolvable | `distsim` | `TestExtentReferencedResolvableRecordsAreRecoverable`, `TestMixedClassRecovery_FullSuccess` | `covered` | recoverable direct-extent and mixed-class recovery case |
| `S15` `ExtentReferenced` payload lost | `distsim` | `TestExtentReferencedUnresolvableForcesRebuild`, `TestRecoverableThenUnrecoverable`, `TestTimeVaryingAvailability` | `covered` | metadata alone not enough; active recovery can transition from recoverable to unrecoverable |
| `S16` replica restarts during catch-up | `distsim` | `TestReplicaRestartDuringCatchupRestartsSafely` | `covered` | safe recovery restart |
| `S17` replica restarts during rebuild | `distsim` | `TestReplicaRestartDuringRebuildRestartsSafely` | `covered` | rebuild interruption safe |
| `S18` primary restarts without failover | `distsim` | `TestS18_PrimaryRestart_SameLineage`, `TestS18_PrimaryRestart_ReplicasRejectOldEpoch`, `TestS18_PrimaryRestart_DelayedOldAck_DoesNotAdvancePrefix`, `TestS18_PrimaryRestart_InFlightBarrierDropped`, `TestP02_S18_DelayedAck_ExplicitRejection` | `covered` | delayed stale ack rejection and committed-prefix stability are now asserted directly |
| `S19` chain of custody across promotions | `distsim` | `TestS19_ChainOfCustody_MultiplePromotions`, `TestS19_ChainOfCustody_ThreePromotions` | `covered` | multi-promotion lineage continuity covered |
| `S20` live partition with competing writes | `distsim` | `TestS20_LivePartition_StaleWritesNotCommitted`, `TestS20_LivePartition_HealRecovers`, `TestS20_StalePartition_ProtocolRejectsStaleWrites`, `TestP02_S20_StaleTraffic_CommittedPrefixUnchanged` | `covered` | stale-side protocol traffic is explicitly rejected and committed prefix remains unchanged |

## Ownership Notes

When adding a scenario:

1. add or extend the relevant prototype test:
   - `fsmv2`
   - `volumefsm`
   - `distsim`
2. update this file with:
   - status
   - package location
3. keep correctness checks tied to:
   - committed `LSN`
   - reference model state

## Current Coverage Snapshot

Already covered in some form:

- quorum commit survives primary failover
- uncommitted write not preserved after primary loss
- zombie old primary fenced by epoch
- lagging replica catch-up from primary WAL
- reservation expiry aborts catch-up in distributed sim
- `sync_quorum` continues with one lagging replica
- `sync_all` blocks with one lagging replica
- `sync_quorum` with mixed replica states
- `sync_all` with mixed replica states
- rebuild from snapshot + tail
- promotion uses valid lineage node
- flapping recoverable vs budget-exceeded rebuild path
- tail-chasing explicit escalation to rebuild
- restart during catch-up recovers safely
- restart during rebuild recovers safely
- primary restart delayed stale ack rejection
- `WALInline` recoverability
- `ExtentReferenced` resolvable vs unresolvable boundary
- mixed-class Smart WAL recovery and time-varying payload availability
- delayed stale messages and selective drop behavior
- multi-node reservation expiry and rebuild-timeout behavior
- current extent cannot reconstruct old `LSN`

Still important to add:

- explicit coordinator-driven candidate selection among competing valid/invalid lineages
- control-plane latency scenarios derived from `CP13-8 T4b`
- explicit V1 / V1.5 / V2 comparison scenarios for:
  - changed-address restart
  - same-address transient outage
  - slow reassignment recovery

## V1.5 Lessons To Add Or Strengthen

These come directly from WAL V1.5 / Phase 13 behavior and should be treated as high-priority scenario drivers.

### L1. Replica Restart With New Receiver Port

Observed:
- replica VS restarts
- receiver comes back on a new random port
- primary background reconnect retries old address and fails

Implication:
- direct reconnect only works if replica address is stable

Backlog impact:
- strengthen `S18`
- add a restart/address-change sub-scenario under `S20` or a future network/control-plane recovery scenario

### L2. Slow Control-Plane Reassignment Dominates Recovery

Observed:
- sync correctness preserved
- write availability recovery waits for heartbeat/reassignment cycle

Implication:
- "recoverable in theory" is not enough
- recovery latency is part of protocol quality

Backlog impact:
- `S5` is now covered at current simulator level
- strengthen `S18`
- add long-running restart/rejoin timing scenarios

### L3. Background Reconnect Helps Only Same-Address Recovery

Observed:
- background reconnect is useful for transient network failure
- not sufficient for process restart with address change

Implication:
- scenarios must distinguish:
  - transient disconnect
  - process restart
  - address change

Backlog impact:
- keep `S4` as transient disconnect
- strengthen `S18` with restart/address-stability cases

### L4. Tail-Chasing And Retention Pressure Are Structural Risks

Observed:
- Phase 13 reasoning repeatedly exposed:
  - lagging replica may pin WAL
  - catch-up may not converge while primary keeps advancing

Implication:
- V2 must explicitly model convergence, abort, and rebuild boundaries

Backlog impact:
- strengthen `S6`
- add multi-node retention / timeout variants

### L5. Current Extent Is Not Historical State

Observed:
- using current extent to reconstruct old `LSN` can return later values

Implication:
- V2 must require version-correct base images or resolvable historical payloads

Backlog impact:
- already covered by `S8`
- should remain a permanent regression scenario

## Randomized Simulation

In addition to fixed scenarios, V2 should keep a randomized simulator suite.

Purpose:

1. discover paths that were not explicitly written as named scenarios
2. stress promotion, restart, and recovery ordering
3. check invariants after each random step

Current prototype:

- `sw-block/prototype/distsim/random.go`
- `sw-block/prototype/distsim/random_test.go`

Current invariants checked:

1. current committed `LSN` remains a committed prefix
2. promotable nodes match reference state at committed `LSN`
3. current primary, if valid/running, matches reference state at committed `LSN`

This does not replace named scenarios.
It complements them.

## Scenario Summary

When reviewing or adding scenarios, always record the source:

1. from real V1/V1.5 behavior
2. from explicit V2 design obligation
3. from adversarial distributed-systems reasoning

The best scenarios are the ones that come from real failures first, then are generalized into V2 requirements.

## Development Phases

Execution detail is tracked in:
- `sw-block/.private/phase/phase-01.md`
- `sw-block/.private/phase/phase-02.md`

High-level phase order:

1. close explicit scenario backlog
   - `S19`
   - `S20`
2. strengthen missing lifecycle scenarios
   - `S5`
   - `S6`
   - `S18`
   - stronger `S12`
3. extend protocol-state simulation and version comparison
   - `V1`
   - `V1.5`
   - `V2`
   - stronger closure of current `partial` scenarios
4. strengthen random/adversarial simulation
5. add timeout-based scenarios only when the execution path is modeled
