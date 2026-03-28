# Phase 04a Decisions

Date: 2026-03-27
Status: initial

## Core Decision

The next must-fix validation problem is:

- sender/session ownership semantics

This outranks:

- more timing realism
- more WAL detail
- broader scenario growth

## Why

V2's core claim over V1.5 is not only:

- better recovery policy

It is also:

- stable per-replica sender identity
- one active recovery owner
- stale work cannot mutate current state

If those ownership rules are not validated, the simulator can overstate confidence.

## Validation Rule

For this phase, a scenario is only complete when it is expressed at two levels:

1. simulator ownership model (`distsim`)
2. standalone implementation slice (`enginev2`)

Real `weed/` adversarial tests remain the system-level gate.

## Scope Discipline

Do not expand this phase into:

- generic simulator feature growth
- Smart WAL design growth
- V1 integration work

Keep it focused on the ownership model.
