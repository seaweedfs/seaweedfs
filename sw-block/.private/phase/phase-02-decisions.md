# Phase 02 Decisions

Date: 2026-03-26
Status: active

## Decision 1: Extend `distsim` Instead Of Forking A New Protocol Simulator

Reason:
- current `distsim` already has:
  - node/storage model
  - coordinator/epoch model
  - reference oracle
  - randomized runs
- the missing layer is protocol-state fidelity, not a new simulation foundation

Implication:
- add lightweight per-node replication state and protocol decisions to `distsim`
- do not build a separate fourth simulator yet

## Decision 2: Keep Coverage Status Conservative

Reason:
- `S20`, `S6`, and `S18` currently prove important safety properties
- but they do not yet fully assert message-level or explicit state-transition behavior

Implication:
- leave them `partial` until the model can assert protocol behavior directly

## Decision 3: Use Versioned Scenario Comparison To Justify V2

Reason:
- the simulator should not only say "V2 works"
- it should show:
  - where `V1` fails
  - where `V1.5` improves but still strains
  - why `V2` is worth the complexity

Implication:
- Phase 02 includes explicit `V1` / `V1.5` / `V2` scenario comparison work

## Decision 4: V2 Must Not Be Described As "Always Catch-Up"

Reason:
- that wording is too optimistic and hides the real V2 design rule
- V2 is better because it makes recoverability explicit, not because it retries forever

Implication:
- describe V2 as:
  - catch-up if explicitly recoverable
  - otherwise explicit rebuild
- keep this wording consistent in tests and docs
