# V2 Protocol Claim And Evidence

Date: 2026-04-05
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

## Interpretation Rule For Current Integrated Evidence

Until an explicit `V2 core` exists as a real code structure and live event/command owner,
current integrated evidence should be interpreted as:

1. validation of current `V1` runtime behavior under `V2` constraints
2. not proof that a completed `V2 runtime` already exists

This means:

1. protocol truths and claim boundaries may already be `V2`-owned
2. workload and integration passes may still be about the constrained current runtime
3. later phases must keep separating:
   - semantic authority
   - constrained current-runtime validation
   - future pure-core extraction

## Current Chosen Envelope

This is the bounded envelope currently allowed for active V2 claims:

| Item | Current value | Source |
|------|---------------|--------|
| Replication factor | `RF=2` | `v2-protocol-closure-map.zh.md` |
| Durability mode | `sync_all` | `v2-protocol-closure-map.zh.md`, `Phase 13` |
| Control path | current master / volume-server heartbeat path | `v2-protocol-closure-map.zh.md` |
| Execution backend | `blockvol` | `v2-protocol-closure-map.zh.md`, `v2-reuse-replacement-boundary.md` |
| Frontend/product surfaces in bounded support envelope | iSCSI, CSI, NVMe on the chosen path | `Phase 11`, `Phase 17` |
| Real-workload checkpoint | `CP13-8` | `Phase 13` |

Current explicit exclusions:

1. `RF>2` as a general accepted product claim
2. broad mode normalization outside the accepted bounded `CP13-9` contract
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
| `CP13-9` | bounded external mode meaning must stay explicit and surface-consistent on the constrained current path | `Phase 13` | active |

## Accepted Baselines

| Baseline | What it is allowed to say | Evidence location | Current validity |
|----------|---------------------------|-------------------|------------------|
| `CP13-1` replication baseline inventory | which tests originally passed/failed/`PASS*` before `CP13-2..7` closure | `sw-block/.private/phase/phase-13-cp1-baseline.md` | valid as baseline inventory, not as final product claim |
| `Phase 12 P4` bounded floor | one bounded performance floor and rollout-gate package on the accepted chosen path | `sw-block/.private/phase/phase-12-p4-floor.md`, `phase-12-p4-rollout-gates.md` | valid inside its named envelope |
| `CP13-8` bounded real-workload pass | one bounded `ext4 + pgbench` package passes on the accepted chosen path | `sw-block/.private/phase/phase-13-cp8-workload-validation.md` | valid inside its named envelope and current constrained-`V1` interpretation |
| `CP13-9` bounded mode contract | one bounded external mode set is explicit on the constrained current path | `sw-block/.private/phase/phase-13-cp9-mode-normalization.md` | valid inside its named envelope and constrained-`V1` interpretation |

## Allowed Claims

These are the claims that may currently be made without overreach.

| Claim ID | Allowed claim | Scope boundary | Evidence anchor | Status |
|----------|---------------|----------------|-----------------|--------|
| `C-RF2-SYNCALL-CONTRACT` | the accepted `RF=2 sync_all` replication contract is closed at protocol/unit/adversarial level through `CP13-1..7` | protocol/unit/adversarial evidence only | `Phase 13` docs and tests | allowed |
| `C-WORKLOAD-DRAFT` | one bounded real-workload validation package is defined for `CP13-8` | package definition only, not final pass claim | `phase-13-cp8-workload-validation.md`, YAML scenario | allowed |
| `C-WORKLOAD-PASS` | the bounded real-workload package passes on the chosen path | bounded chosen path only; interpreted as current `V1` runtime under `V2` constraints | `CP13-8` rerun artifact | allowed |
| `C-ADAPTER-CLOSURE` | assignment / readiness / publication closure is explicit on the chosen path | bounded chosen path only; does not imply mode normalization or pure-core extraction | `CP13-8A` proof package | allowed |
| `C-CONSTRAINED-V1-RUNTIME` | current integrated checks are evaluating `V1` runtime behavior under `V2` constraints rather than validating a completed `V2 runtime` | current chosen path only, until explicit `V2 core` extraction | `v2_mini_core_design.md`, `Phase 13` docs | allowed |
| `C-MODE-NORMALIZATION` | one bounded mode-policy / normalization package is closed on the current constrained chosen path | bounded chosen path only; does not imply pure `V2 core` extraction or broad product policy | `CP13-9` docs/tests | allowed |
| `C-PHASE16-RUNTIME-CHECKPOINT` | the bounded heartbeat/master/API runtime path now preserves accepted explicit truth across the delivered `16M-16W` restart/disturbance seams | bounded chosen path only; excludes broad recovery-loop, broad failover/publication, and launch claims | `sw-block/.private/phase/phase-16-finish-review.md`, `phase-16.md`, focused `weed/server` tests | allowed |
| `C-PHASE17-PRODUCT-CHECKPOINT` | the current broader recovery-branch map, bounded failover/publication contract, bounded disturbance policy table, and first-launch envelope draft are explicit for the chosen path | bounded chosen path only; excludes broad production readiness, broad transport/frontend approval, and broad whole-surface failover/publication proof | `sw-block/.private/phase/phase-17.md`, `sw-block/.private/phase/phase-17-checkpoint-review.md` | allowed |
| `C-FIRST-LAUNCH-ENVELOPE-DRAFT` | one bounded first-launch supported matrix is frozen as a draft with explicit exclusions and launch blockers | bounded chosen path only; not a launch decision, pilot approval, or rollout approval | `sw-block/design/v2-first-launch-supported-matrix.md` | allowed |
| `C-PRODUCTIONIZATION-ARTIFACT-SET` | one bounded productionization artifact set now exists for internal pilot, preflight, stop-condition, and controlled-rollout discipline inside the frozen chosen envelope | bounded chosen path only; artifact existence only, not pilot success, rollout approval, or broader launch proof | `sw-block/design/v2-bounded-internal-pilot-pack.md`, `v2-pilot-preflight-checklist.md`, `v2-pilot-stop-conditions.md`, `v2-controlled-rollout-review.md` | allowed |
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
| Real-workload package | one bounded workload matrix passes on the corrected chosen path | `CP13-8` scenario/doc | tester validation reports |
| Assignment/publication closure | assignment does not imply readiness/publication and corrected wiring refreshes replication truth explicitly | `CP13-8A` code/tests/debug evidence | tester investigation, bug docs |
| Mode normalization | one bounded mode set is explicit and surface-consistent on the constrained current path | `CP13-9` contract/doc/tests | tester validation report |
| Runtime truth closure under restart/disturbance | accepted explicit truth survives the delivered bounded `Phase 16` heartbeat/restart seams through `16W` | `phase-16-finish-review.md`, `phase-16.md`, focused restart/heartbeat tests in `weed/server` | `v2-product-completion-overview.md`, `v2-protocol-truths.md` |
| Failover/publication bounded contract | one bounded whole-chain statement is explicit for publication ownership/address coherence after failover completion and winning assignment delivery | `phase-17.md`, `phase-17-checkpoint-review.md`, publication/disturbance tests in `weed/server` | `v2-first-launch-supported-matrix.md` |
| Disturbance policy table | startup/restart/rejoin/repeated-failover/degraded-sparsity behavior is explicit as runtime rule, temporary inconsistency policy, or non-claim | `phase-17.md`, `phase-17-checkpoint-review.md`, restart/disturbance tests in `weed/server` | `v2-product-completion-overview.md` |
| First-launch supported matrix | one bounded launch envelope draft is explicit with supported scope, exclusions, and launch blockers | `v2-first-launch-supported-matrix.md` | `phase-17-checkpoint-review.md`, `Phase 12 P4`, `CP13`, `Phase 16` |
| Productionization artifact set | one bounded artifact set defines pilot scope, preflight gate, stop/contain rules, and post-pilot rollout review discipline | `v2-bounded-internal-pilot-pack.md`, `v2-pilot-preflight-checklist.md`, `v2-pilot-stop-conditions.md`, `v2-controlled-rollout-review.md` | `v2-first-launch-supported-matrix.md`, `v2-product-completion-overview.md` |

## Invalidated Or Narrowed Evidence

This section records evidence that cannot currently be used at full strength.

| ID | Affected claim/evidence | Narrowing reason | Scope | Action required |
|----|-------------------------|------------------|-------|-----------------|
| `INV-CP13-8A-01` | any historical weed-VS scenario claim that `block_promote` preserved replication automatically before the promote/refresh fix | old promote path could leave new primary without replica shipper wiring; barrier then became vacuous with `0` shippers | historical weed-VS testrunner scenarios using old `block_promote` behavior | rerun or reclassify historical evidence as needed |
| `INV-CLAIM-SPREAD-01` | claims embedded only in phase delivery notes | phase docs are not a reliable centralized current-state ledger | all scattered phase notes | migrate ongoing claim state here |

Unaffected evidence currently believed to remain valid:

1. standalone `iscsi-target` scenarios that used direct `assign + set_replica` wiring rather than weed-VS `block_promote`
2. protocol/unit/adversarial evidence from accepted `CP13-1..7`
3. performance-only scenarios that did not claim active cross-node replication through the broken promote path

## Open Contradictions And Blockers

No active `Phase 13` blocker currently remains inside the accepted bounded chosen path.

## Rerun Queue

| Priority | Item | Why rerun is needed | Exit condition |
|----------|------|---------------------|----------------|
| `P0` | historical weed-VS scenarios using old `block_promote` semantics from the recent testrunner enhancement work | prior replication interpretation may have been vacuous (`0` shippers) before the refresh fix | affected scenarios are reclassified or rerun |
| `P1` | any recent degraded/perf interpretation derived from broken historical weed-VS promote path | performance interpretation may have been based on RF=1 semantics | audit updated and affected numbers rerun or narrowed |

## Maintenance Rules

1. do not add a new claim anywhere else without adding or updating the corresponding row here
2. when a bug narrows evidence, record the invalidation here in the same change
3. when a rerun restores a claim, move the row from `Invalidated Or Narrowed Evidence` to `Allowed Claims` or update its status
4. keep this document bounded to the active chosen path; do not turn it into a future roadmap
