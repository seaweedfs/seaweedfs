# Phase 01 Decisions

Date: 2026-03-26
Status: active

## Purpose

Capture the key design decisions made during Phase 01 simulator work.

## Initial Decisions

### 1. `design/` vs `.private/phase/`

Decision:
- `sw-block/design/` holds shared design truth
- `sw-block/.private/phase/` holds execution planning and progress

Reason:
- design backlog and execution checklist should not be mixed

### 2. Scenario source of truth

Decision:
- `sw-block/design/v2_scenarios.md` is the scenario backlog and coverage matrix

Reason:
- all contributors need one visible scenario list

### 3. Phase 01 priority

Decision:
- first close:
  - `S19`
  - `S20`

Reason:
- they are the biggest remaining distributed lineage/partition scenarios

### 4. Current simulator scope

Decision:
- use the simulator as a V2 design-validation tool, not a product/perf harness

Reason:
- current goal is correctness and protocol coverage, not productization

### 5. Phase execution format

Decision:
- keep phase execution in three files:
  - `phase-xx.md`
  - `phase-xx-log.md`
  - `phase-xx-decisions.md`

Reason:
- separates plan, evidence, and reasoning
- reduces drift between roadmap and findings

### 6. Design backlog vs execution plan

Decision:
- `sw-block/design/v2_scenarios.md` remains the source of truth for scenario backlog and coverage
- `.private/phase/phase-01.md` is the execution layer for `sw`

Reason:
- design truth should be stable and shareable
- execution tasks should be easier to edit without polluting design docs

### 7. Immediate Phase 01 priorities

Decision:
- prioritize:
  - `S19` chain of custody across multiple promotions
  - `S20` live partition with competing writes

Reason:
- these are the biggest remaining distributed-lineage gaps after current simulator milestone

### 8. Coverage status should be conservative

Decision:
- mark scenarios as `partial` unless the test actually exercises the core protocol obligation, not just a simplified happy path

Reason:
- avoids overstating simulator coverage
- keeps the backlog honest for follow-up strengthening

### 9. Protocol-version comparison belongs in the simulator

Decision:
- compare `V1`, `V1.5`, and `V2` using the same scenario set where possible

Reason:
- this is the clearest way to show:
  - where V1 breaks
  - where V1.5 improves but still strains
  - why V2 is architecturally cleaner
