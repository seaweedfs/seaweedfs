# Protocol Development Process

Date: 2026-03-27

## Purpose

This document defines how `sw-block` protocol work should be developed.

The process is meant to work for:

- V2
- future V3
- or a later block algorithm that is not WAL-based

The point is to make protocol work systematic rather than reactive.

## Core Philosophy

### 1. Design before implementation

Do not start with production code and hope the protocol becomes clear later.

Start with:

1. system contract
2. invariants
3. state model
4. scenario backlog

Only then move to implementation.

### 2. Real failures are inputs, not just bugs

When V1 or V1.5 fails in real testing, treat that as:

- a design requirement
- a scenario source
- a simulator input

Do not patch and forget.

### 3. Simulator is part of the protocol, not a side tool

The simulator exists to answer:

- what should happen
- what must never happen
- which old designs fail
- why the new design is better

It is not a replacement for real testing.
It is the design-validation layer before production implementation.

### 4. Passing tests are not enough

Green tests are necessary, not sufficient.

We also require:

- explicit invariants
- explicit scenario intent
- clear state transitions
- review of assumptions and abstraction boundaries

### 5. Keep hot-path and recovery-path reasoning separate

Healthy steady-state behavior and degraded recovery behavior are different problems.

Both must be designed explicitly.

## Development Ladder

Every major protocol feature should move through these steps:

1. **Problem statement**
- what real bug, limit, or product goal is driving the work

2. **Contract**
- what the protocol guarantees
- what it does not guarantee

3. **State model**
- node state
- coordinator state
- recovery state
- role / epoch / lineage rules

4. **Scenario backlog**
- named scenarios
- source:
  - real failure
  - design obligation
  - adversarial distributed case

5. **Prototype / simulator**
- reduced but explicit model
- invariant checks
- V1 / V1.5 / V2 comparison where relevant

6. **Implementation**
- production code only after the protocol shape is clear enough

7. **Real validation**
- unit
- component
- integration
- real hardware where needed

8. **Feedback loop**
- turn new failures back into scenario/design inputs

## Required Artifacts

For protocol work to be considered real progress, we usually want:

### Design

- design doc
- scenario doc
- comparison doc when replacing an older approach

### Prototype

- simulator or prototype code
- tests that assert protocol behavior

### Implementation

- production patch
- production tests
- docs updated to match the actual algorithm

### Review

- implementation gate
- design/protocol gate

## Two-Gate Rule

We use two acceptance gates.

### Gate 1: implementation

Owned by the coding side.

Questions:

- does it build?
- do tests pass?
- does it behave as intended in code?

### Gate 2: protocol/design

Owned by the design/review side.

Questions:

- is the logic actually sound?
- do tests prove the intended thing?
- are assumptions explicit?
- is the abstraction boundary honest?

A task is not accepted until both gates pass.

## Layering Rule

Keep simulation layers separate.

### `distsim`

Use for:

- protocol correctness
- state transitions
- fencing
- recoverability
- promotion / lineage
- reference-state checking

### `eventsim`

Use for:

- timeout behavior
- timer races
- event ordering
- same-tick / delayed event interactions

Do not duplicate scenarios blindly across both layers.

## Test Selection Rule

Do not choose simulator inputs only from failing tests.

Review all relevant tests and classify them by:

- protocol significance
- simulator value
- implementation specificity

Good simulator candidates often come from:

- barrier truth
- catch-up vs rebuild
- stale message rejection
- failover / promotion safety
- changed-address restart
- mode semantics

Keep real-only tests for:

- wire format
- OS timing
- exact WAL file behavior
- frontend transport specifics

## Version Comparison Rule

When designing a successor protocol:

- keep the old version visible
- reproduce the old failure or limitation
- show the improved behavior in the new version

For `sw-block`, that means:

- `V1`
- `V1.5`
- `V2`

should be compared explicitly where possible.

## Documentation Rule

The docs must track three different things:

### `learn/projects/sw-block/`

Use for:

- project history
- V1/V1.5 algorithm records
- phase records
- real test history

### `sw-block/design/`

Use for:

- active design truth
- V2 and later protocol docs
- scenario backlog
- comparison docs

### `sw-block/.private/phase/`

Use for:

- active execution plan
- log
- decisions

## What Good Progress Looks Like

A good protocol iteration usually has this pattern:

1. real failure or design pressure identified
2. scenario named and written down
3. simulator reproduces the bad case
4. new protocol handles it explicitly
5. implementation follows
6. real tests validate it

If one of those steps is missing, confidence is weaker.

## Bottom Line

The process is:

1. design the contract
2. model the state
3. define the scenarios
4. simulate the protocol
5. implement carefully
6. validate in real tests
7. feed failures back into design

That is the process we should keep using for V2 and any later protocol line.
