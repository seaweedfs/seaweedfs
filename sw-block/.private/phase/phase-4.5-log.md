# Phase 4.5 Log

## 2026-03-29

### Accepted

1. `sw` `Phase 4.5 P0`
   - bounded `CatchUp` budget is semantic in `enginev2`
   - `FrozenTargetLSN` is a real session invariant
   - `Rebuild` is wired into sender execution and is exclusive from catch-up
   - rebuild completion goes through `CompleteRebuild`, not generic session completion

2. `tester` crash-consistency simulator strengthening
   - storage-state split introduced and accepted
   - checkpoint/restart boundary made explicit
   - recoverability upgraded from watermark-style logic to checkpoint + contiguous WAL replayability proof
   - core invariant tests for crash consistency now pass

3. `tester` evidence hardening and adversarial exploration
   - grouped simulator evidence for `A5-A8`
   - danger predicates added
   - adversarial search added and passing
   - adversarial search found a real `StateAt(lsn)` historical-state bug
   - `StateAt(lsn)` corrected so newer checkpoint/base state does not leak into older historical queries

4. `Phase 4.5` closeout judgment
   - prototype and simulator evidence are now strong enough to stop expanding `4.5`
   - next major step should move to engine-readiness review and engine slicing

### Remaining open work

1. low-priority cleanup
   - remove or consolidate redundant frozen-target bookkeeping if no longer needed
