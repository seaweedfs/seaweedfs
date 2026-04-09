# V2 Capability Map

Date: 2026-04-05
Status: active
Purpose: define the V2 capability expansion map that drives feature closure, test closure, and the transition from bounded scenario debugging to systematic product validation

## Why This Document Exists

If `V2` is a real system line, it needs more than:

1. accepted protocol truths
2. passing point fixes
3. a few successful scenarios

It also needs one explicit map that answers:

1. what product capabilities exist in the V2 line
2. in what order those capabilities should close
3. what "done" means for each capability
4. which tests prove the capability
5. which proofs are V2-owned versus runtime-specific

This document is that map.

It complements:

1. `v2-protocol-truths.md` for stable semantic rules
2. `v2-product-completion-overview.md` for product-level completion status
3. `v2-phase-development-plan.md` for active execution sequencing
4. `v2_scenarios.md` for scenario backlog and historical failure sources

## How To Use This Map

For any new feature, bug fix, or test expansion, ask:

1. which capability tier does this belong to
2. which closure claim does it strengthen
3. which proof tier should carry it
4. whether it is V2-owned truth or current-runtime integration

This prevents three common failures:

1. growing V2 by random scenario accumulation
2. confusing `weed` integration success with V2 semantic completion
3. re-testing everything from zero when the runtime boundary changes later

## Core Method

The map uses three linked ideas:

### 1. Capability expansion

V2 should expand from:

1. single-volume correctness
2. bounded RF=2 replication
3. failover and rejoin
4. multi-replica behavior
5. lifecycle operations
6. control-plane and operations closure
7. CSI and product-surface closure

### 2. Completion definition

A capability is not "done" because code exists.

It is only closed when all of these are true:

1. semantic rule is explicit
2. runtime path exists
3. observability exists
4. focused tests prove the rule
5. one product-level scenario proves the real path

### 3. Proof layering

Each capability should be proven across four proof tiers:

1. `Core semantic`
   - pure V2 truth
   - fastest feedback
   - should remain reusable if runtime changes
2. `Seam / adapter`
   - queue, heartbeat, registry, proto, assignment, bridge ownership
   - catches most integrated bugs cheaply
3. `Integrated runtime`
   - real `weed` path today
   - smaller number of high-value scenarios
4. `Soak / benchmark / adversarial`
   - slow, broad, or disturbance-heavy validation
   - not the daily development loop

## Capability Tiers

## Tier 0: Semantic Foundation

Goal:

1. make V2 the source of truth for replication semantics

Main closure claims:

1. epoch and lineage are authoritative
2. committed truth is explicit
3. catch-up versus rebuild boundary is explicit
4. stale authority fails closed
5. replica identity is stable across endpoint change

Done means:

1. truths are explicit in `v2-protocol-truths.md`
2. engine events and commands preserve those truths
3. core tests cover replay, stale events, fencing, and recovery choice

Primary proof tiers:

1. core semantic
2. seam only where identity/transport adaptation matters

Typical tests:

1. event -> projection -> command tests
2. recovery-choice tests
3. stale session / stale epoch rejection
4. stable `ReplicaID` versus mutable endpoint tests

## Tier 1: Single-Volume Base Capability

Goal:

1. prove one volume is correct before adding replication

Capabilities:

1. create/delete
2. single-node read/write
3. restart durability
4. publication correctness
5. bounded observability

Done means:

1. RF=1 write/read survives restart
2. publication reflects the true serving node
3. explicit health/publication state is observable

Primary proof tiers:

1. core semantic for boundaries
2. integrated runtime for real read/write/restart

Typical scenarios:

1. create -> write -> restart -> read
2. publication remains coherent after restart

## Tier 2: RF=2 Replication Base

Goal:

1. close the smallest useful HA replication unit

Capabilities:

1. primary/replica assignment
2. receiver readiness
3. shipper configuration
4. barrier semantics
5. explicit `publish_healthy`
6. explicit `degraded`
7. explicit `needs_rebuild`

Done means:

1. replica membership reaches the primary truthfully
2. `sync_all` cannot succeed vacuously with zero shippers
3. publication health depends on real closure, not optimistic state
4. RF=2 replicated write/read works on the integrated path

Primary proof tiers:

1. core semantic
2. seam
3. one integrated replicated IO scenario

Typical tests:

1. assignment-delivered membership tests
2. `RoleApplied`, `ReceiverReady`, `ShipperConfigured` closure tests
3. barrier strictness tests
4. replicated checksum scenarios

## Tier 3: RF=2 Recovery And Failover

Goal:

1. turn RF=2 replication into a fault-tolerant runtime path

Capabilities:

1. manual promote
2. auto failover
3. old primary fencing
4. old primary rejoin
5. catch-up-first reconnect
6. rebuild fallback
7. data continuity after failover

Done means:

1. promotion bumps epoch and fences stale authority
2. promoted primary regains replica membership after rejoin
3. reconnect chooses catch-up or rebuild explicitly
4. failover preserves committed data
5. one data-verified integrated scenario exists for each supported failover path

Primary proof tiers:

1. seam
2. integrated runtime
3. soak/adversarial for disturbance variants

Current note:

1. manual promote on the integrated `weed` path has now closed with data continuity verification
2. this tier remains broader than one passing scenario and still requires systematic matrix expansion

Typical scenarios:

1. kill primary -> promote replica -> restart old primary -> data verified
2. lease-expiry auto failover
3. rejoin with address change
4. rebuild fallback when catch-up path is unavailable

## Tier 4: Multi-Replica Runtime (`RF>=3`)

Goal:

1. extend the model from one replica to a replica set

Capabilities:

1. multi-replica membership
2. multi-shipper convergence
3. strict `sync_all`
4. `sync_quorum`
5. partial failure tolerance
6. replacement and rebuild target choice

Done means:

1. primary ownership and closure remain replica-scoped, not scalar-only
2. quorum/all durability rules hold under mixed replica states
3. failover and rejoin do not collapse back to RF=2-only assumptions

Primary proof tiers:

1. core semantic
2. seam
3. targeted integrated RF=3 scenarios

Typical tests:

1. multi-replica assignment closure
2. quorum durability tests
3. partial-failure promotion eligibility tests
4. RF=3 disturbance scenarios

## Tier 5: Lifecycle Capability

Goal:

1. prove that product operations remain correct under replication and recovery

Capabilities:

1. expand
2. truncate
3. snapshot
4. snapshot export/import
5. clone/restore style flows where supported

Done means:

1. lifecycle operations preserve V2 recovery truth
2. lifecycle metadata does not bypass fencing or recovery boundaries
3. lifecycle operations continue to hold under restart/failover

Primary proof tiers:

1. core semantic for boundary rules
2. seam where command ownership matters
3. integrated scenarios for user-visible lifecycle behavior

Typical scenarios:

1. snapshot then failover
2. expand under replicated volume
3. truncate under degraded or catch-up conditions

## Tier 6: Control Plane And Operations

Goal:

1. make the system diagnosable and operationally trustworthy

Capabilities:

1. heartbeat convergence
2. assignment queue correctness
3. registry truth coherence
4. publication truth coherence
5. debug surfaces
6. metrics and operator diagnosis
7. restart and disturbance policy clarity

Done means:

1. the control plane reports the same truth the runtime acts on
2. major failure classes are diagnosable from bounded logs/debug state
3. restart/rejoin behavior is policy-shaped, not accidental

Primary proof tiers:

1. seam
2. integrated runtime
3. soak for repeated disturbance

Typical tests:

1. registry/publication coherence tests
2. assignment queue confirm/refresh tests
3. reconnect/restart diagnosis tests
4. bounded failover observability tests

## Tier 7: Product Surfaces (`CSI`, `iSCSI`, `NVMe`)

Goal:

1. project V2 storage truth through real product interfaces

Capabilities:

1. volume create/publish through `CSI`
2. node stage/node publish
3. failover-visible remount or reconnect behavior
4. expansion through product surface
5. snapshot through product surface
6. front-end publication coherence

Done means:

1. product surfaces do not hide or weaken V2 truth
2. frontend publication follows actual authority after failover
3. product workflows survive supported restart/failover envelopes

Primary proof tiers:

1. seam
2. integrated runtime
3. slower end-to-end scenario pack

Typical scenarios:

1. CSI create/publish/write/failover/read
2. CSI expand under replicated volume
3. snapshot + restore + failover

## Tier 8: Launch Envelope

Goal:

1. convert bounded capability proof into a bounded support statement

Capabilities:

1. supported topology matrix
2. supported disturbance matrix
3. known unsupported branches
4. pilot stop conditions
5. rollout review evidence

Done means:

1. supported claims are explicit
2. unsupported areas are explicit
3. pilot and rollout review use the same capability map and proof layers

Primary proof tiers:

1. integrated runtime
2. soak / perf / operational review

## Capability Map Summary

| Tier | Scope | What closes here | Main proof emphasis |
|------|-------|------------------|---------------------|
| 0 | Semantic foundation | truth rules and fail-closed boundaries | core semantic |
| 1 | Single-volume base | RF=1 correctness and restart durability | core + integrated |
| 2 | RF=2 replication | receiver/shipper/barrier/publication closure | core + seam + one integrated path |
| 3 | RF=2 recovery/failover | promote, rejoin, catch-up, rebuild, data continuity | seam + integrated |
| 4 | RF>=3 runtime | multi-replica membership and durability semantics | core + seam + targeted integrated |
| 5 | Lifecycle | snapshot/expand/truncate under replication truth | mixed by feature |
| 6 | Control/ops | registry/heartbeat/publication/diagnosis closure | seam + integrated |
| 7 | Product surfaces | CSI and frontend projection of V2 truth | integrated |
| 8 | Launch envelope | bounded support and rollout claims | integrated + soak |

## Matrix Linkage

Use the three active documents in a fixed order:

1. protocol docs define the rule
2. this capability map defines which product tier owns the rule
3. `v2-validation-matrix.md` defines what must be proven for closure
4. `v2-integration-matrix.md` defines which real scenarios exercise the path

The goal is to make the chain explicit:

`protocol -> capability tier -> validation rows -> integration rows`

| Tier | Primary protocol refs | Validation rows | Integration rows | Practical meaning |
|------|------------------------|-----------------|------------------|-------------------|
| 0 | `v2-protocol-truths.md`, `v2-sync-recovery-protocol.md` | `V4`, `V5`, `V14` | feeds `I-V1` through `I-V6` | pure semantic truth and fail-closed rules |
| 1 | `v2-protocol-truths.md` | `V1` | `I-V1` | single-volume and bootstrap correctness |
| 2 | `v2-sync-recovery-protocol.md` | `V1`, `V2`, `V4` | `I-V1`, `I-V2` | RF=2 replication base and barrier/publication closure |
| 3 | `v2-sync-recovery-protocol.md`, `v2-rebuild-mvp-session-protocol.md` | `R1`-`R12`, `V3`, `V6`, `V7`, `V8`, `V11` | `I-R1`-`I-R8`, `I-V3`, `I-V4`, `I-V5` | recovery, rebuild, failover, and rejoin |
| 4 | `v2-sync-recovery-protocol.md` | `V9`, `V10` | future `RF>=3` integrated rows | aggregate multi-replica projection and durability semantics |
| 5 | `v2-rebuild-mvp-session-protocol.md`, snapshot/restore execution docs | `S1`-`S10` | `I-S1`-`I-S4` | snapshot, restore, and lifecycle operations |
| 6 | `v2-automata-ownership-map.md`, `v2-protocol-claim-and-evidence.md` | `V8`, `V12`, `V13` | `I-V4`, `I-V6` | control-plane truth, observability, and operator surfaces |
| 7 | product-surface and rollout docs | `V1`, `V2`, `V12`, `V13` | runner scenarios and product e2e packs | CSI/frontend projection of V2 truth |
| 8 | rollout/support docs | stage-gate summaries in validation matrix | chaos/perf rows `I-C1`-`I-C4`, `I-P1`-`I-P3` | bounded launch envelope and operational confidence |

## Test Expansion Strategy From This Map

This map should drive testing in a faster order than "one expensive scenario at a time."

### Fast lane

Run on most code changes:

1. core semantic tests for the touched rule
2. seam tests for ingress/egress/control delivery
3. one focused scenario only if the change crosses a real product seam

### Medium lane

Run on milestone closure for a tier:

1. representative integrated scenarios for that tier
2. checksum or historical-read validation where data continuity matters

### Slow lane

Run on nightly or bounded review:

1. disturbance matrix
2. soak
3. benchmark
4. larger product-surface packs

## What Must Stay Runtime-Agnostic

To avoid re-testing everything from zero when `weed` ownership shrinks later,
these proof categories must stay V2-owned:

1. assignment semantics
2. role/epoch/fencing semantics
3. recovery-choice semantics
4. publication closure semantics
5. data continuity contracts

The current `weed` path remains valuable as:

1. the present integrated runtime
2. one proof backend for product-level behavior

It must not become the only place where V2 truth is tested.

## Immediate Next Use

This map should be used to produce:

1. one capability-to-test taxonomy
2. one current coverage matrix marking which tiers are:
   - `strong`
   - `bounded`
   - `partial`
   - `not yet closed`
3. one reduced high-value integrated scenario pack aligned to tiers rather than ad hoc bug history

## Current Practical Reading

For near-term work, read in this order:

1. `v2-protocol-truths.md`
2. `v2-capability-map.md`
3. `v2-product-completion-overview.md`
4. `v2-phase-development-plan.md`
5. `v2_scenarios.md`
