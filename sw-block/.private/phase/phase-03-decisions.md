# Phase 03 Decisions

Date: 2026-03-27
Status: initial

## Why Phase 03 Exists

Phase 02 already covered the main protocol-state story:

- V1 / V1.5 / V2 comparison
- stale traffic rejection
- catch-up vs rebuild
- changed-address restart control-plane flow
- committed-prefix-safe promotion eligibility

The next simulator problems are different:

- timer semantics
- timeout races
- event ordering under contention

That deserves a separate phase so the model boundary stays clear.

## Initial Boundary

### `distsim`

Keep for:

- protocol correctness
- reference-state validation
- recoverability logic
- promotion / lineage rules

### `eventsim`

Grow for:

- explicit event queue behavior
- timeout events
- equal-time scheduling choices
- race exploration

## Working Rule

Do not move all scenarios into `eventsim`.

Only move or duplicate scenarios when:

- timer or event ordering is the real bug surface
- `distsim` abstraction hides the important behavior

## Accepted Phase 03 Decisions

### Same-tick rule

Within one tick:

- data/message delivery is evaluated before timeout firing

Meaning:

- if an ack arrives in the same tick as a timeout deadline, the ack wins and may cancel the timeout

This is now an explicit simulator rule, not accidental behavior.

### Timeout authority

Not every timeout that reaches its deadline still has authority to mutate state.

So we now distinguish:

- `FiredTimeouts`
  - timeout had authority and changed the model
- `IgnoredTimeouts`
  - timeout reached deadline but was stale and ignored

This keeps replay/debug output honest.

### Late barrier ack rule

Once a barrier instance times out:

- it is marked expired
- late ack for that barrier instance is rejected

That prevents a stale ack from reviving old durability state.

### Review gate rule for timer work

Timer/race work is easy to get subtly wrong while still having green tests.

So timer-related work is not accepted until:

- code path is reviewed
- tests assert the real protocol obligation
- stale and authoritative timer behavior are clearly distinguished
