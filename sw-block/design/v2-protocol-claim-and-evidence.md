# V2 Protocol Claim And Evidence

Date: 2026-04-03
Status: active
Purpose: keep one centralized ledger for the current chosen envelope, accepted claims, supporting evidence, invalidated evidence, and rerun obligations

## Why This Document Exists

`v2-protocol-truths.md` records stable protocol truths.
`v2-protocol-closure-map.zh.md` records the structural closure model.

What they do not track in one place is the current operational contract:

1. which claims are allowed right now
2. which baselines are accepted right now
3. which evidence supports each claim
4. which evidence has been narrowed or invalidated
5. which reruns are required before a claim can be restored

This document is that ledger.

## How To Use It

When reviewing any new slice, bug fix, workload run, or delivery note, ask:

1. which current claim does this change strengthen, narrow, or invalidate?
2. which evidence row should be updated?
3. does the change alter the current chosen envelope?
4. does any old claim now require rerun or reclassification?

If the answer changes the current state of the product, update this ledger in the same change.

## Current Chosen Envelope

This is the bounded envelope currently allowed for active V2 claims:

| Item | Current value | Source |
|------|---------------|--------|
| Replication factor | `RF=2` | `v2-protocol-closure-map.zh.md` |
| Durability mode | `sync_all` | `v2-protocol-closure-map.zh.md`, `Phase 13` |
| Control path | current master / volume-server heartbeat path | `v2-protocol-closure-map.zh.md` |
| Execution backend | `blockvol` | `v2-protocol-closure-map.zh.md`, `v2-reuse-replacement-boundary.md` |
| Frontend in active validation | iSCSI | `Phase 11`, `CP13-8` |
| Real-workload checkpoint | `CP13-8` | `Phase 13` |

Current explicit exclusions:

1. `RF>2` as a general accepted product claim
2. broad mode normalization before `CP13-9`
3. broad rollout / launch approval
4. broad transport matrix claims outside explicitly named evidence
5. treating synthetic benchmarks as substitutes for real workload validation

## Active Protocol Constraints

These are the currently binding constraints that later work must preserve.

| ID | Constraint | Source | Current status |
|----|------------|--------|----------------|
| `T1` | `CommittedLSN` is the external truth boundary | `v2-protocol-truths.md` | active |
| `T9` | truncation is a protocol boundary, not cleanup | `v2-protocol-truths.md` | active |
| `T14` | engine remains recovery authority; storage remains truth source | `v2-protocol-truths.md` | active |
| `T15` | reuse reality, not inherited semantics | `v2-protocol-truths.md` | active |
| `CP13-2` | stable identity must not be inferred from transport address shape | `Phase 13` | active |
| `CP13-3` | durable authority is `replicaFlushedLSN`, not legacy success inference | `Phase 13` | active |
| `CP13-4` | only eligible replica state may satisfy sync durability | `Phase 13` | active |
| `CP13-5` | reconnect must use explicit handshake / catch-up semantics | `Phase 13` | active |
| `CP13-6` | retention must fail closed for lagging replicas | `Phase 13` | active |
| `CP13-7` | unrecoverable gap must escalate to `NeedsRebuild` and block normal paths | `Phase 13` | active |
| `CP13-8A` | assignment delivered != receiver ready != publish healthy | `Phase 13` | active |

## Accepted Baselines

| Baseline | What it is allowed to say | Evidence location | Current validity |
|----------|---------------------------|-------------------|------------------|
| `CP13-1` replication baseline inventory | which tests originally passed/failed/`PASS*` before `CP13-2..7` closure | `sw-block/.private/phase/phase-13-cp1-baseline.md` | valid as baseline inventory, not as final product claim |
| `Phase 12 P4` bounded floor | one bounded performance floor and rollout-gate package on the accepted chosen path | `sw-block/.private/phase/phase-12-p4-floor.md`, `phase-12-p4-rollout-gates.md` | valid inside its named envelope |
| real-workload envelope draft | one bounded `ext4 + pgbench` package for `CP13-8` | `sw-block/.private/phase/phase-13-cp8-workload-validation.md` | active draft; full claim pending rerun after blockers close |

## Allowed Claims

These are the claims that may currently be made without overreach.

| Claim ID | Allowed claim | Scope boundary | Evidence anchor | Status |
|----------|---------------|----------------|-----------------|--------|
| `C-RF2-SYNCALL-CONTRACT` | the accepted `RF=2 sync_all` replication contract is closed at protocol/unit/adversarial level through `CP13-1..7` | protocol/unit/adversarial evidence only | `Phase 13` docs and tests | allowed |
| `C-WORKLOAD-DRAFT` | one bounded real-workload validation package is defined for `CP13-8` | package definition only, not final pass claim | `phase-13-cp8-workload-validation.md`, YAML scenario | allowed |
| `C-WORKLOAD-PASS` | the bounded real-workload package passes on the chosen path | only after rerun succeeds on corrected path | `CP13-8` rerun artifact | not yet allowed |
| `C-ADAPTER-CLOSURE` | assignment / readiness / publication closure is explicit on the chosen path | only after `CP13-8A` acceptance | `CP13-8A` proof package | in progress |
| `C-MODE-NORMALIZATION` | mode policy / normalization is closed | only in `CP13-9` or later | future | not allowed |
| `C-LAUNCH-APPROVAL` | broad product launch readiness | outside current phase | future | not allowed |

## Evidence Map

| Evidence area | What it proves | Primary evidence | Support evidence |
|---------------|----------------|------------------|------------------|
| Identity / addressing | stable identity and routable publication | `CP13-2` tests and docs | `qa_block_soak_test.go`, `sync_all_bug_test.go` |
| Durable progress | barrier durability truth and non-legacy authority | `CP13-3` tests and docs | protocol tests around barrier handling |
| State eligibility | only eligible replica state may satisfy sync durability | `CP13-4` tests and docs | adversarial state tests |
| Reconnect / catch-up | reconnect uses handshake/catch-up rather than bootstrap | `CP13-5` tests and docs | adversarial reconnect tests |
| Retention | lagging replica retains WAL or escalates fail closed | `CP13-6` tests and docs | retention protocol tests |
| Rebuild fallback | unrecoverable gap escalates to `NeedsRebuild` and blocks normal paths | `CP13-7` tests and docs | rebuild tests |
| Performance floor | one bounded measured floor and rollout-gate package | `Phase 12 P4` docs/tests | cited baseline artifact |
| Real-workload package | one bounded workload matrix exists | `CP13-8` scenario/doc | tester validation reports |
| Assignment/publication closure | assignment does not imply readiness/publication | `CP13-8A` code/tests/debug evidence | tester investigation, bug docs |

## Invalidated Or Narrowed Evidence

This section records evidence that cannot currently be used at full strength.

| ID | Affected claim/evidence | Narrowing reason | Scope | Action required |
|----|-------------------------|------------------|-------|-----------------|
| `INV-CP13-8A-01` | any weed-VS scenario claim that `block_promote` preserved replication automatically | promote path could leave new primary without replica shipper wiring; barrier then became vacuous with `0` shippers | recent weed-VS testrunner scenarios using `block_promote` | rerun after fix |
| `INV-CP13-8A-02` | bounded real-workload `CP13-8` pass claim | blocked by assignment/publication contradiction and then by promote/shipper closure issue | `CP13-8` only | rerun after `CP13-8A` blocker fixes |
| `INV-CLAIM-SPREAD-01` | claims embedded only in phase delivery notes | phase docs are not a reliable centralized current-state ledger | all scattered phase notes | migrate ongoing claim state here |

Unaffected evidence currently believed to remain valid:

1. standalone `iscsi-target` scenarios that used direct `assign + set_replica` wiring rather than weed-VS `block_promote`
2. protocol/unit/adversarial evidence from accepted `CP13-1..7`
3. performance-only scenarios that did not claim active cross-node replication through the broken promote path

## Open Contradictions And Blockers

| ID | Blocker | Current classification | Impact |
|----|---------|------------------------|--------|
| `BUG-CP13-8A-ADDR` | malformed/mock replica addresses in some QA allocators | test/adapter bug | narrows affected QA evidence; does not by itself close real workload |
| `BUG-CP13-8A-RECV-IDEMP` | repeated assignment delivery restarted replica receiver and hit bind conflict | adapter/runtime bug | blocks weed-VS replica from leaving degraded state until fixed |
| `BUG-CP13-8A-PROMOTE-SHIPPER` | post-promote assignment could leave new primary with no replica shipper configured | master/adapter bug | invalidates weed-VS `block_promote` replication claims until rerun |
| `CP13-8` | real bounded workload package still needs corrected rerun | blocked by `CP13-8A` issues | blocks real-workload pass claim |

## Rerun Queue

| Priority | Item | Why rerun is needed | Exit condition |
|----------|------|---------------------|----------------|
| `P0` | `CP13-8` bounded real-workload scenario | current pass claim is not yet allowed after `CP13-8A` blockers | bounded rerun passes or fails with attributable remaining cause |
| `P0` | weed-VS scenarios using `block_promote` from the recent testrunner enhancement work | prior replication interpretation may have been vacuous (`0` shippers) | affected scenarios are reclassified or rerun |
| `P1` | any recent degraded/perf interpretation derived from broken weed-VS promote path | performance interpretation may be based on RF=1 semantics | audit updated and affected numbers rerun or narrowed |

## Maintenance Rules

1. do not add a new claim anywhere else without adding or updating the corresponding row here
2. when a bug narrows evidence, record the invalidation here in the same change
3. when a rerun restores a claim, move the row from `Invalidated Or Narrowed Evidence` to `Allowed Claims` or update its status
4. keep this document bounded to the active chosen path; do not turn it into a future roadmap
