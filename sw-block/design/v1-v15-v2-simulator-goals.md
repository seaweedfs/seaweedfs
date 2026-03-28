# V1 / V1.5 / V2 Simulator Goals

Date: 2026-03-26
Status: working design note
Purpose: define how the simulator should be used against WAL V1, Phase-13 V1.5, and WAL V2

## Why This Exists

The simulator is not only for validating V2.

It should also be used to:

1. break WAL V1
2. stress WAL V1.5 / Phase 13
3. justify why WAL V2 is needed

This note defines what failures we want the simulator to find in each protocol generation.

## What The Simulator Can And Cannot Do

### What it is good at

The simulator is good at:

1. finding concrete counterexamples
2. exposing bad protocol assumptions
3. checking commit / failover / fencing invariants
4. checking historical data correctness at target `LSN`

### What it is not

The simulator is not a full proof unless promoted to formal model checking.

So the right claim is:

- "no issue found under these modeled runs"

not:

- "protocol proven correct in all implementations"

## Protocol Targets

### WAL V1

Core shape:
- primary ships WAL out
- lagging replica degrades quickly
- no real recoverability contract
- no strong short-gap catch-up window

Primary risk:
- a briefly lagging replica gets downgraded too early and forced into rebuild

### WAL V1.5 / Phase 13

Core shape:
- primary retains WAL longer for lagging replicas
- reconnect / catch-up exists
- rebuild fallback exists
- primary may wait before releasing WAL

Primary risks:
- WAL pinning
- tail chasing
- slow availability recovery
- recoverability assumptions that do not hold long enough

### WAL V2

Core shape:
- explicit state machine
- explicit recoverability / reservation
- catch-up vs rebuild boundary is formalized
- eventual support for `WALInline` vs `ExtentReferenced`

Primary goal:
- no committed data loss
- no false recovery
- cheaper and clearer short-gap recovery

## What To Find In WAL V1

The simulator should try to find scenarios where V1 fails operationally or structurally.

### V1-F1. Short Disconnect Still Forces Rebuild

Sequence:
1. replica disconnects briefly
2. primary continues writing
3. replica returns quickly

Expected ideal behavior:
- short-gap catch-up

What V1 may do:
- downgrade replica too early
- no usable catch-up path
- rebuild required unnecessarily

### V1-F2. Jitter Causes Avoidable Degrade

Sequence:
1. replica is alive but sees delayed/reordered delivery
2. primary interprets this as lag/failure

Failure signal:
- unnecessary downgrade or exclusion

### V1-F3. Repeated Brief Flaps Cause Thrash

Sequence:
1. repeated short disconnect/reconnect
2. primary repeatedly degrades replica

Failure signal:
- poor availability
- excessive rebuild churn

### V1-F4. No Efficient Path Back To Healthy State

Sequence:
1. replica becomes degraded
2. network recovers

Failure signal:
- control plane or protocol provides no clean short recovery path

## What To Find In WAL V1.5 / Phase 13

The simulator should stress whether retention-based catch-up is actually enough.

### V15-F1. Tail Chasing Under Ongoing Writes

Sequence:
1. replica reconnects behind
2. primary keeps writing
3. catch-up tries to close the gap

Failure signal:
- replica never converges
- stays forever behind
- no clean escalation path

### V15-F2. WAL Pinning Harms System Progress

Sequence:
1. replica lags
2. primary retains WAL to help recovery
3. lag persists

Failure signal:
- WAL window remains pinned too long
- reclaim stalls
- system availability or throughput suffers

### V15-F3. Catch-Up Window Expires Mid-Recovery

Sequence:
1. catch-up begins
2. primary continues advancing
3. required recoverability disappears before completion

Failure signal:
- protocol still claims success
- or lacks a clean abort-to-rebuild path

### V15-F4. Restart Recovery Too Slow

Sequence:
1. replica restarts
2. primary blocks writes correctly under `sync_all`
3. service recovery takes too long

Failure signal:
- correctness preserved
- but availability recovery is operationally unacceptable

### V15-F5. Multiple Lagging Replicas Poison Progress

Sequence:
1. more than one replica lags
2. retention and recovery obligations interact

Failure signal:
- one slow replica or mixed states poison the entire volume behavior

## What WAL V2 Should Survive

V2 should not merely avoid V1/V1.5 failures.
It should make them explicit and manageable.

### V2-S1. Short Gap Recovers Cheaply

Expected:
- brief disconnect -> catch-up -> promote
- no rebuild

### V2-S2. Impossible Catch-Up Fails Cleanly

Expected:
- not fully recoverable -> `NeedsRebuild`
- no pretend success

### V2-S3. Reservation Loss Forces Correct Abort

Expected:
- once recoverability is lost, catch-up aborts
- rebuild path takes over

### V2-S4. Promotion Is Lineage-First

Expected:
- new primary chosen from valid lineage
- not simply highest apparent `LSN`

### V2-S5. Historical Data Correctness Is Preserved

Expected:
- no rebuild from current extent pretending to be old state
- correct snapshot/base + replay behavior

## Simulation Strategy By Version

### For V1

Use simulator to:
- break it
- demonstrate avoidable rebuilds and downgrade behavior

The simulator is mainly a diagnostic and justification tool here.

### For V1.5

Use simulator to:
- stress retention-based catch-up
- find operational limits
- expose where retention alone is not enough

The simulator is a stress and tradeoff tool here.

### For V2

Use simulator to:
- validate named protocol scenarios
- validate random/adversarial runs
- confirm state + data correctness under failover/recovery

The simulator is a design-validation tool here.

## Practical Outcome

If the simulator finds:

### On V1
- short outages still lead to rebuild

Then conclusion:
- V1 lacks a real short-gap recovery story

### On V1.5
- retention helps but can still tail-chase or pin WAL too long

Then conclusion:
- V1.5 is a useful bridge, but not the final architecture

### On V2
- catch-up/rebuild boundary is explicit and safe

Then conclusion:
- V2 solves the protocol problem more cleanly

## Bottom Line

Use the simulator differently for each generation:

1. WAL V1: find where it breaks
2. WAL V1.5: find where it strains
3. WAL V2: validate that it behaves correctly and more cleanly

That is how the simulator justifies the architectural move from V1 to V2.
