# V3 Phase 14 S8 Assignment

Date: 2026-04-19
Status: draft
Owner: sw, with architect and tester review
Purpose: execute the final bounded P14 close by producing evidence, scenario classification, and a clean P15 handoff

## 1. One-Line Goal

Close P14 as a bounded internal single-active-master topology/control-plane loop, backed by L0/L1/L2 evidence and honest L3/P15 carry-forward.

## 2. Scope

Included:

1. S4-S7 route evidence consolidation
2. acceptance matrix for all P14 internal claims
3. final supported topology statement
4. unsupported/deferred list
5. V2 testrunner/scenario port classification
6. process smoke gaps review
7. 14A final targeted review packet
8. P15 handoff mapping

Excluded:

1. CSI implementation
2. external API implementation
3. user data-path production readiness
4. migration implementation
5. security implementation
6. multi-master / HA
7. performance or soak claims

## 3. Required Work

### A. Evidence Inventory

Create an evidence table that maps S4-S7 claims to concrete tests and commands.

Must include:

1. observation institution
2. durable authority institution
3. convergence institution
4. real-route restart closure
5. placement/rebalance/failover policy for accepted topology
6. unsupported topology evidence
7. old-slot/stale observation rejection
8. per-volume isolation

### B. Gap Classification

Classify every missing proof as one of:

1. P14 internal blocker
2. P14 S8 test/harness gap
3. P15 product-surface gap
4. 14A verification follow-up
5. explicit non-goal

Any P14 internal blocker must be fixed before S8 close. A P15 product-surface gap must be carried forward, not disguised as P14 closure.

### C. V2 Scenario Port Classification

Inspect V2 scenario/test assets and produce a table:

| V2 asset | P14 use | P15 use | Port shape | Blocker |
|---|---|---|---|---|

Start with:

1. `weed/storage/blockvol/testrunner/`
2. `weed/storage/blockvol/testrunner/scenarios/public/ha-restart-recovery.yaml`
3. `weed/storage/blockvol/testrunner/scenarios/public/ha-failover.yaml`
4. `weed/storage/blockvol/testrunner/scenarios/public/fault-partition.yaml`
5. `weed/storage/blockvol/test/component/`
6. `weed/server/qa_block_*`
7. `learn/test` evidence layout

### D. L2 Process Smoke Decision

Answer whether current `sparrow` hidden smoke surfaces are sufficient for S8 L2 internal close.

If not sufficient, add the minimum internal S8 smoke surface needed to prove the S4-S7 route without inventing P15 external product APIs.

The smoke must check:

1. real process starts
2. durable authority reloads
3. observation or fixture input drives controller
4. assignment reaches adapter through `VolumeBridge`
5. restart does not mint backward
6. structured output can be consumed by tester

### E. 14A Final Targeted Pass

Prepare a 14A checklist for:

1. stale authority after restart
2. stale observation after reassignment
3. old-slot delivery after authority move
4. unsupported topology no-op/evidence
5. convergence stuck/supersede clearing
6. publication honesty during transition
7. per-volume isolation

14A should review findings first. It should not expand P14 scope.

### F. P15 Handoff

Produce a P15 handoff table:

| Gap | Why not P14 | P15 owner track | Required first proof |
|---|---|---|---|

Must include:

1. CSI lifecycle
2. frontend data path
3. external volume API
4. security/auth
5. diagnostics/operator workflow
6. migration/coexistence
7. deployment/hardening
8. final cluster validation agent

## 4. Required Commands

At minimum run or document blockers for:

```text
go test ./core/engine ./core/adapter ./core/authority -count=1
go test ./cmd/sparrow ./core/authority -count=1
go test ./... -count=1
```

If sandbox or Windows filesystem behavior blocks durable-store tests, rerun outside the sandbox and record the reason.

If L3 hardware scenarios are not runnable, do not fake them. Record the scenario names and blockers.

## 5. Expected Files

Expected docs:

1. `sw-block/design/v3-phase-14-s8-final-bounded-close.md` — this scope document
2. `sw-block/design/v3-phase-14-s8-assignment.md` — this assignment
3. `sw-block/design/v3-phase-14-s8-closure.md` — final closure statement, created after implementation/evidence
4. updates to `v3-phase-14-checklist.md`
5. updates to `v3-phase-14-log.md`
6. updates to `v3-phase-14a-checklist.md` if 14A final pass finds coverage gaps

Expected code/tests only if needed:

1. internal process smoke helper in `cmd/sparrow`
2. component evidence consolidation tests in `core/authority` or a shared component package
3. scenario classification artifacts under the chosen test/runner directory

## 6. Acceptance

S8 can be accepted only when:

1. the evidence matrix exists and is honest
2. all P14 internal blockers are either fixed or explicitly rejected from the P14 claim
3. L0 and L1 tests pass
4. L2 process smoke is present or its absence is classified as a P15 product-surface blocker
5. L3 scenarios are classified with runnable/deferred/blocker status
6. 14A final targeted pass has no open P14 safety blocker
7. P15 handoff maps every product gap to a P15 track
8. final closure statement does not claim production readiness

## 7. Review Focus

Architect should review:

1. whether S8 closure is product-honest
2. whether any P15 work was pulled backward into P14
3. whether any P14 internal truth gap was pushed forward into P15
4. whether test evidence matches the claimed layer
5. whether V2 porting stays mechanism-only

Tester should review:

1. whether evidence is reproducible
2. whether commands and scenario blockers are specific
3. whether L2/L3 gaps are visible
4. whether final artifacts are enough to start P15 without re-litigating P14
