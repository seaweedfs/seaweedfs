# V2 Prototype Roadmap And Gates

Date: 2026-03-27
Status: historical prototype roadmap
Purpose: define the remaining prototype roadmap, the validation gates between stages, and the decision point between real V2 engine work and possible V2.5 redesign

## Current Position

V2 design/FSM/simulator work is sufficiently closed for serious prototyping, but not frozen against later `V2.5` adjustments.

Current state:

- design proof: high
- execution proof: medium
- data/recovery proof: low
- prototype end-to-end proof: low

Rough prototype progress:

- `25%` to `35%`

This is early executable prototype, not engine-ready prototype.

## Roadmap Goal

Answer this question with prototype evidence:

- can V2 become a real engine path?
- or should it become `V2.5` before real implementation begins?

## Step 1: Execution Authority Closure

Purpose:

- finish the sender / recovery-session authority model so stale work is unambiguously rejected

Scope:

1. ownership-only `AttachSession()` / `SupersedeSession()`
2. execution begins only through execution APIs
3. stale handshake / progress / completion fenced by `sessionID`
4. endpoint bump / epoch bump invalidate execution authority
5. sender-group preserve-or-kill behavior is explicit

Done when:

1. all execution APIs are sender-gated and reject stale `sessionID`
2. session creation is separated from execution start
3. phase ordering is enforced
4. endpoint bump / epoch bump invalidate execution authority correctly
5. mixed add/remove/update reconciliation preserves or kills state exactly as intended

Main files:

- `sw-block/prototype/enginev2/`
- `sw-block/prototype/distsim/`
- `learn/projects/sw-block/phases/phase-13-v2-boundary-tests.md`

Key gate:

- old recovery work cannot mutate current sender state at any execution stage

## Step 2: Orchestrated Recovery Prototype

Purpose:

- move from good local sender APIs to an actual prototype recovery flow driven by assignment/update intent

Scope:

1. assignment/update intent creates or supersedes recovery attempts
2. reconnect / reassignment / catch-up / rebuild decision path
3. sender-group becomes orchestration entry point
4. explicit outcome branching:
   - zero-gap fast completion
   - positive-gap catch-up
   - unrecoverable gap -> `NeedsRebuild`

Done when:

1. the prototype expresses a realistic recovery flow from topology/control intent
2. sender-group drives recovery creation, not only unit helpers
3. recovery outcomes are explicit and testable
4. orchestrator responsibility is clear enough to narrow `v2-open-questions.md` item 6

Key gate:

- recovery control is no longer scattered across helper calls; it has one clear orchestration path

## Step 3: Minimal Historical Data Prototype

Purpose:

- prove the recovery model against real data-history assumptions, not only control logic

Scope:

1. minimal WAL/history model, not full engine
2. enough to exercise:
   - catch-up range
   - retained prefix/window
   - rebuild fallback
   - historical correctness at target LSN
3. enough reservation/recoverability state to make recovery explicit

Done when:

1. the prototype can prove why a gap is recoverable or unrecoverable
2. catch-up and rebuild decisions are backed by minimal data/history state
3. `v2-open-questions.md` items 3, 4, 5 are closed or sharply narrowed
4. prototype evidence strengthens acceptance criteria `A5`, `A6`, and `A7`

Key gate:

- the prototype must explain why recovery is allowed, not just that policy says it is

## Step 4: Prototype Scenario Closure

Purpose:

- make the prototype itself demonstrate the V2 story end-to-end

Scope:

1. map key V2 scenarios onto the prototype
2. express the 4 V2-boundary cases against prototype behavior
3. add one small end-to-end harness inside `sw-block/prototype/`
4. align prototype evidence with acceptance criteria

Done when:

1. prototype behavior can be reviewed scenario-by-scenario
2. key V1/V1.5 failures have prototype equivalents
3. prototype outcomes match intended V2 design claims
4. remaining gaps are clearly real-engine gaps, not protocol/prototype ambiguity

Key gate:

- a reviewer can trace:
  - acceptance criteria -> scenario -> prototype behavior
  without hand-waving

## Gates

### Gate 1: Design Closed Enough

Status:

- mostly passed

Meaning:

1. acceptance criteria exist
2. core simulator exists
3. ownership gap from V1.5 is understood

### Gate 2: Execution Authority Closed

Passes after Step 1.

Meaning:

- stale execution results cannot mutate current authority

### Gate 3: Orchestrated Recovery Closed

Passes after Step 2.

Meaning:

- recovery flow is controlled by one coherent orchestration model

### Gate 4: Historical Data Model Closed

Passes after Step 3.

Meaning:

- catch-up vs rebuild is backed by executable data-history logic

### Gate 5: Prototype Convincing

Passes after Step 4.

Meaning:

- enough evidence exists to choose:
  - real V2 engine path
  - or `V2.5` redesign

## Decision Gate After Step 4

### Path A: Real V2 Engine Planning

Choose this if:

1. prototype control logic is coherent
2. recovery boundary is explicit
3. boundary cases are convincing
4. no major structural flaw remains

Outputs:

1. real engine slicing plan
2. migration/integration plan into future standalone `sw-block`
3. explicit non-goals for first production version

### Path B: V2.5 Redesign

Choose this if the prototype reveals:

1. ownership/orchestration still too fragile
2. recovery boundary still too implicit
3. historical correctness model too costly or too unclear
4. too much complexity leaks into the hot path

Output:

- write `V2.5` as a design/prototype correction before engine work

## What Not To Do Yet

1. no Smart WAL expansion beyond what Step 3 minimally needs
2. no backend/storage-engine redesign
3. no V1 production integration
4. no frontend/wire protocol work
5. no performance optimization as a primary goal

## Practical Summary

Current sequence:

1. finish execution authority
2. build orchestrated recovery
3. add minimal historical-data proof
4. close key scenarios against the prototype
5. decide:
   - V2 engine
   - or `V2.5`
