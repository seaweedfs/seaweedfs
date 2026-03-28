# Phase 02 Log

Date: 2026-03-26
Status: active

## 2026-03-26

- Phase 02 created to move `distsim` from final-state safety validation toward explicit protocol-state simulation.
- Initial focus:
  - close `S20`, `S6`, and `S18` at protocol level
  - compare `V1`, `V1.5`, and `V2` on the same scenarios
- Known model gap at phase start:
  - current `distsim` is strong at final-state safety invariants
  - current `distsim` is weaker at mid-flow protocol assertions and message-level rejection reasons
- Phase 02 progress now in place:
  - delivery accept/reject tracking
  - protocol-level stale-epoch rejection assertions
  - explicit non-convergent catch-up state transition assertions
  - initial version-comparison tests for disconnect, tail-chasing, and restart/rejoin policy
- Next simulator target:
  - reproduce real `V1.5` address-instability and control-plane-recovery failures as named scenarios
- Immediate coding asks for `sw`:
  - changed-address restart failure in `V1.5`
  - same-address transient outage comparison across `V1` / `V1.5` / `V2`
  - slow control-plane reassignment scenario derived from `CP13-8 T4b`
- Local housekeeping done:
  - corrected V2 wording from "always catch-up" to "catch-up if explicitly recoverable; otherwise rebuild"
  - added explicit brief-disconnect and changed-address restart policy helpers
  - verified `distsim` test suite still passes with the Windows-safe runner
- Scenario status update:
  - `S20` now covered via protocol-level stale-traffic rejection + committed-prefix stability
  - `S6` now covered via explicit `CatchingUp -> NeedsRebuild` assertions
  - `S18` now covered via explicit stale `MsgBarrierAck` rejection + prefix stability
- Next asks for `sw` after this closure:
  - changed-address restart scenario tied directly to `CP13-8 T4b`
  - same-address transient outage comparison across `V1` / `V1.5` / `V2`
  - slow control-plane reassignment scenario
  - Smart WAL recoverable -> unrecoverable transition scenarios
- Additional closure completed:
  - `S5` now covered with both:
    - repeated recoverable flapping
    - budget-exceeded escalation to `NeedsRebuild`
  - Smart WAL transitions now exercised with:
    - recoverable -> unrecoverable during active recovery
    - mixed `WALInline` + `ExtentReferenced` success
    - time-varying payload availability
- Updated next asks for `sw`:
  - changed-address restart scenario tied directly to `CP13-8 T4b`
  - same-address transient outage comparison across `V1` / `V1.5` / `V2`
  - slow control-plane reassignment scenario
  - delayed/drop network beyond simple disconnect
  - multi-node reservation expiry / rebuild timeout cases
- Additional Phase 02 coverage delivered:
  - delayed stale messages after promote/failover
  - delayed stale barrier ack rejection
  - selective write-drop with barrier delivery under `sync_all`
  - multi-node mixed reservation expiry outcome
  - multi-node `NeedsRebuild` / snapshot rebuild recovery
  - partial rebuild timeout / retry completion
- Remaining asks are now narrower:
  - changed-address restart scenario tied directly to `CP13-8 T4b`
  - same-address transient outage comparison across `V1` / `V1.5` / `V2`
  - slow control-plane reassignment scenario
  - stronger coordinator candidate-selection scenarios
- Additional closure after review:
  - safe default promotion selector now refuses `NeedsRebuild` candidates
  - explicit desperate-promotion API separated from safe selection
  - changed-address and slow-control-plane comparison tests now prove actual data divergence / healing, not only policy shape
- New next-step assignment:
  - strengthen model depth around endpoint identity and control-plane reassignment
  - replace abstract repair helpers with more explicit event flow where practical
  - reduce direct recovery state injection in comparison tests
  - extend candidate selection from ranking into validity rules

## 2026-03-27

- Phase 02 core simulator hardening is effectively complete.
- Delivered since the previous checkpoint:
  - endpoint identity / endpoint-version modeling
  - stale-endpoint rejection in delivery path
  - heartbeat -> coordinator detect -> assignment-update control-plane flow
  - recovery-session trigger API for `V1.5` and `V2`
  - explicit candidate eligibility checks:
    - running
    - epoch alignment
    - state eligibility
    - committed-prefix sufficiency
  - safe default promotion now rejects candidates without the committed prefix
- Current `distsim` status at latest review:
  - 73 tests passing
- Manager bookkeeping decision:
  - keep Phase 02 active only for doc maintenance / wrap-up
  - treat further simulator depth as likely Phase 03 work, not unbounded Phase 02 scope creep
