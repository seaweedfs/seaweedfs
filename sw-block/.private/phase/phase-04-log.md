# Phase 04 Log

Date: 2026-03-27
Status: active

## 2026-03-27

- Phase 04 created to start the first standalone V2 implementation slice.
- Decision:
  - do not begin in `weed/storage/blockvol/`
  - begin under `sw-block/`
- first slice chosen:
  - per-replica sender ownership
  - explicit recovery-session ownership
- Initial slice delivered under `sw-block/prototype/enginev2/`:
  - sender
  - recovery session
  - sender group
- First review found:
  - sender/session epoch coherence gap
  - session lifecycle was shell-only, not enforcing real transitions
  - attach-session epoch mismatch was not rejected
- Follow-up delivered and accepted:
  - reconcile updates preserved sender epoch
  - epoch bump invalidates stale session
  - session transition map enforced
  - attach-session rejects epoch mismatch
  - enginev2 tests increased to 26 passing
- Phase 04a created to close the ownership-validation gap:
  - explicit session identity in `distsim`
  - bridge tests into `enginev2`
- Phase 04a ownership problem closed well enough:
  - stale completion rejected by session ID
  - endpoint invalidation includes `CtrlAddr`
  - boundary doc aligned with real simulator/prototype evidence
- Phase 04 P1 delivered and accepted:
  - sender-owned execution APIs added
  - all execution APIs fence on `sessionID`
  - completion now requires valid completion point
  - attach/supersede now establish ownership only
  - handshake range validation added
  - enginev2 tests increased to 46 passing
- Next phase focus narrowed to P2:
  - recovery outcome branching
  - assignment-intent orchestration
  - prototype end-to-end recovery flow
