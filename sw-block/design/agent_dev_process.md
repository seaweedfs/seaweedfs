# Agent Development Process

Date: 2026-03-30
Status: active
Purpose: define the working split between `sw`, `tester`, and review/management roles so each phase and slice has a clear delivery path

## Why This Exists

The project is now beyond pure exploration.

The expensive part is no longer only writing code.
The expensive part is:

1. delivery
2. review
3. fixes
4. re-review

So the process must reduce repeated full-stack review and make each role responsible for a distinct layer.

## Roles

### `manager`

Primary role:

- phase/plan owner

Responsibilities:

1. define the phase/slice direction and scope
2. accept the planning package before coding starts
3. decide whether a carry-forward is acceptable or must become a gate
4. perform the final round review for overall logic, omissions, and product-path fit

### `architect`

Primary role:

- plan and technical reviewer

Responsibilities:

1. review the plan before implementation starts
2. tighten algorithm wording, scope edges, and expectation framing
3. review technical correctness during implementation
4. review API/state/resource/fail-closed behavior
5. catch semantic drift, scope drift, and V1/V1.5 leakage

### `sw`

Primary role:

- implementation owner

Responsibilities:

1. implement the accepted slice
2. state changed contracts
3. state fail-closed handling
4. state resources acquired/released
5. state carry-forward items
6. add or update tests

### `tester`

Primary role:

- evidence owner

Responsibilities:

1. define what the slice must prove before implementation starts
2. maintain the failure-class checklist
3. define reject conditions and required test level
4. confirm that implementation claims are actually covered by evidence

## Default Routine

Each slice should follow this order:

1. `manager` defines the plan direction
2. `architect` reviews and tightens the plan / algorithm / expectation framing
3. `tester` writes the expectation template
4. `manager` accepts the package and records it in the phase docs
5. `sw` implements and submits with the delivery template
6. `architect` reviews the technical layer until clean enough
7. `tester` performs validation and evidence closure
8. `manager` performs round-two review for overall logic and omissions

Urgent exception:

- if early work already shows major scope drift, protocol contradiction, or V1/V1.5 leakage, architecture review may short-circuit before implementation grows further

## Delivery Template For `sw`

Each delivery should include:

1. changed contracts
2. fail-closed handling added
3. resources acquired/released
4. test inventory
5. known carry-forward notes

This template is required between:

1. implementation
2. implementation/fail-closed review

It should accompany the delivery before reviewers start detailed review.

Suggested format:

```md
Changed contracts:
- ...

Fail-closed handling:
- ...

Resources acquired/released:
- ...

Test inventory:
- ...

Carry-forward notes:
- ...
```

## Phase Doc Usage

Use the three phase documents differently:

### `phase-xx.md`

Use for:

1. current execution direction
2. current scope
3. current guardrails
4. current accepted status
5. current assignments

Keep it short and execution-oriented.

### `phase-xx-log.md`

Use for:

1. detailed planning evolution
2. review feedback
3. carry-forward discussion
4. open observations
5. why wording or scope changed

This document may be longer and more detailed.

### `phase-xx-decisions.md`

Use for:

1. durable phase-level decisions
2. accepted boundaries that later rounds should inherit
3. gate decisions
4. decisions that should not be re-argued without new evidence

This document should stay compact and hold only the more important global decisions.

## Expectation Template For `tester`

Before or at slice start, `tester` should define:

1. must-pass expectations
2. failure-class checklist
3. required test level for each behavior
4. reject conditions

`tester` should re-engage after technical review is mostly clean, to confirm final evidence closure before the manager's second-round review.

Suggested format:

```md
Expectation:
- ...

Required level:
- entry path / engine / unit

Reject if:
- ...

Failure classes covered:
- ...
```

## Review Checklist For `architect`

Review these first:

1. nil handling
2. missing-resource handling
3. wrong-state / wrong-kind rejection
4. stale ID / stale authority rejection
5. resource pin / release symmetry
6. plan/execute/complete argument correctness
7. fail-closed cleanup on partial failure

## Failure-Class Checklist

This checklist should be kept active across phases.

Minimum recurring classes:

1. changed-address restart
2. stale epoch / stale session
3. missing resource pin
4. cleanup after failed plan
5. replay range mis-derived
6. false trusted-base selection
7. truncation missing but completion attempted
8. bounded catch-up not escalating

## Process Rules

### Rule 1: Do not wait until the end to define proof

Each slice should begin with a statement of:

1. what must be proven
2. which failure classes must stay closed

### Rule 2: Do not let convenience wrappers silently become model truth

Any convenience flow must be explicitly classified as:

1. test-only convenience
2. stepwise engine task
3. planner/executor split

### Rule 3: Prefer evidence quality over object growth

New work should preferentially improve:

1. traceability
2. diagnosability
3. failure-class closure
4. adapter contracts

not just add:

1. more structs
2. more states
3. more helper APIs

### Rule 4: Use V1 as validation source, not architecture template

Use:

1. `learn/projects/sw-block/`
2. `weed/storage/block*`

for:

1. constraints
2. failure gates
3. implementation reality

Do not use them as the default V2 architecture template.

### Rule 5: Reuse reality, not inherited semantics

When later implementation reuses existing `Seaweed` / `V1` paths:

1. reuse control-plane reality
2. reuse storage/runtime reality
3. reuse execution mechanisms

but do not silently inherit:

1. address-shaped identity
2. old recovery classification semantics
3. old committed-truth assumptions
4. old failover authority assumptions

Any such reuse should be reviewed explicitly as:

1. safe reuse
2. reuse with explicit boundary
3. temporary carry-forward
4. hard gate before later phases

## Current Direction

The project has moved from exploration-heavy work to evidence-first engine work.

From `Phase 06` onward, the default is:

1. plan first
2. review plan before coding
3. implement
4. review technical layer
5. close evidence
6. do final manager review
