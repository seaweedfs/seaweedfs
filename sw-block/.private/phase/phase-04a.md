# Phase 04a

Date: 2026-03-27
Status: active
Purpose: close the critical V2 ownership-validation gap by making sender/session ownership explicit in both simulation and the standalone `enginev2` slice

## Goal

Validate the core V2 claim more deeply:

1. one stable sender identity per replica
2. one active recovery session per replica
3. endpoint change, epoch bump, and supersede rules invalidate stale work
4. stale late results from old sessions cannot mutate current state

This phase is not about adding broad new simulator surface.
It is about proving the ownership model that is supposed to make V2 better than V1.5.

## Why This Phase Exists

Current simulation is already strong on:

- quorum / commit rules
- stale epoch rejection
- catch-up vs rebuild
- timeout / race ordering
- changed-address recovery at the policy level

The remaining critical risk is narrower:

- the simulator still validates V2 strongly as policy
- but not yet strongly enough as owned sender/session protocol state

That is the highest-value validation gap to close before trusting V2 too much.

## Source Of Truth

Design:
- `sw-block/design/v2-first-slice-session-ownership.md`
- `sw-block/design/v2-acceptance-criteria.md`
- `sw-block/design/v2-open-questions.md`
- `sw-block/design/protocol-development-process.md`

Simulator / prototype:
- `sw-block/prototype/distsim/`
- `sw-block/prototype/enginev2/`

Historical / review context:
- `learn/projects/sw-block/phases/phase-13-v2-boundary-tests.md`
- `sw-block/design/v2-scenario-sources-from-v1.md`

## Scope

### In scope

1. explicit sender/session identity validation in `distsim`
2. explicit stale-session invalidation rules
3. bridge tests from `distsim` scenarios to `enginev2` sender/session invariants
4. doc cleanup so V2-boundary tests point to real simulator and `enginev2` coverage

### Out of scope

- Smart WAL expansion
- broad new timing realism
- TCP / disk realism
- V1 production integration
- new backend/storage engine work

## Critical Questions To Close

1. can an old session completion mutate state after a new session supersedes it?
2. does endpoint change invalidate or supersede the active session cleanly?
3. does epoch bump remove all authority from prior sessions?
4. can duplicate recovery triggers create overlapping active sessions?

## Assigned Tasks For `sw`

### P0

1. add explicit session identity to `distsim`
- model session ID or equivalent ownership token
- make stale session results rejectable by identity, not just by coarse state

2. add ownership scenarios to `distsim`
- endpoint change during active catch-up
- epoch bump during active catch-up
- stale late completion from old session
- duplicate recovery trigger while a session is already active

3. add bridge tests in `enginev2`
- same-address reconnect preserves sender identity
- endpoint bump supersedes or invalidates active session
- epoch bump rejects stale completion
- only one active session per sender

### P1

4. tighten `learn/projects/sw-block/phases/phase-13-v2-boundary-tests.md`
- point to actual `distsim` scenarios
- point to actual `enginev2` bridge tests
- state what remains real-engine-only

5. only add simulator mechanics if a bridge test exposes a real ownership gap

## Exit Criteria

Phase 04a is done when:

1. `distsim` explicitly validates sender/session ownership invariants
2. `enginev2` has bridge tests for the same invariants
3. stale session work is shown unable to mutate current sender state
4. V2-boundary doc no longer has stale simulator references
5. we can say with confidence that V2 ownership semantics, not just V2 policy, are validated at prototype level
