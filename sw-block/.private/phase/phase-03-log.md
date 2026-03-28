# Phase 03 Log

Date: 2026-03-27
Status: active

## 2026-03-27

- Phase 03 created after Phase 02 core scope was effectively delivered.
- Reason for new phase:
  - remaining simulator work is about timer semantics and race behavior, not basic protocol-state coverage
- Initial target:
  - define `distsim` vs `eventsim` split more clearly
  - add explicit timeout semantics
  - add timer-race scenarios without bloating `distsim` ad hoc
- P0 delivered:
  - timeout model added for barrier / catch-up / reservation
  - timeout-backed scenarios added
  - same-tick ordering rule defined as data-before-timers
- First review result:
  - timeout semantics accepted only after making cancellation model-driven
  - late barrier ack after timeout required explicit rejection
- P0 hardening delivered:
  - recovery timeout cancellation moved into model logic
  - stale late barrier ack rejected via expired-barrier tracking
  - stale vs authoritative timeout distinction added:
    - `FiredTimeouts`
    - `IgnoredTimeouts`
- P1 delivered and reviewed:
  - promotion vs stale timeout race
  - rebuild completion vs epoch bump race
  - trace builder moved into reusable code
- Current suite state at latest accepted review:
  - 86 `distsim` tests passing
- Manager decision:
  - Phase 03 P0/P1 are accepted
  - next work should move to deliberate P2 selection rather than broadening the phase ad hoc
