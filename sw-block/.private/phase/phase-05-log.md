# Phase 05 Log

## 2026-03-29

### Opened

`Phase 05` opened as:

- V2 engine planning + Slice 1 ownership core

### Accepted

1. engine module location
   - `sw-block/engine/replication/`

2. Slice 1 ownership core
   - stable per-replica sender identity
   - stable recovery-session identity
   - sender/session fencing
   - endpoint / epoch invalidation
   - ownership registry

3. Slice 1 identity correction
   - registry now keyed by stable `ReplicaID`
   - mutable `Endpoint` separated from identity
   - real changed-`DataAddr` preservation covered by test

4. Slice 1 encapsulation
   - mutable sender/session authority state no longer exposed directly
   - snapshot/read-only inspection path in place

5. Slice 2 recovery execution core
   - connect / handshake / catch-up flow
   - explicit zero-gap / catch-up / needs-rebuild branching
   - stale execution rejection during active recovery
   - bounded catch-up semantics
   - rebuild execution shell

6. Slice 2 validation
   - corrected tester summary accepted
   - `12` ownership tests + `18` recovery tests = `30` total
   - Slice 2 accepted for progression to Slice 3 planning

7. Slice 3 data / recoverability core
   - `RetainedHistory` introduced as engine-level recoverability input
   - history-driven sender APIs added for handshake and rebuild-source selection
   - trusted-base decision now requires both checkpoint trust and replayable tail
   - truncation remains a completion gate / protocol boundary

8. Slice 3 validation
   - corrected tester summary accepted
   - `12` ownership tests + `18` recovery tests + `18` recoverability tests = `48` total
   - accepted boundary:
     - engine proves historical-correctness prerequisites
     - simulator retains stronger historical reconstruction proof
   - Slice 3 accepted for progression to Slice 4 planning

9. Slice 4 integration closure
   - `RecoveryOrchestrator` added as integrated engine entry path
   - assignment/update-driven recovery is exercised through orchestrator
   - observability surface added:
     - `RegistryStatus`
     - `SenderStatus`
     - `SessionSnapshot`
     - `RecoveryLog`
   - causal recovery logging now covers invalidation, escalation, truncation, completion, rebuild transitions

10. Slice 4 validation
   - corrected tester summary accepted
   - `12` ownership tests + `18` recovery tests + `18` recoverability tests + `11` integration tests = `59` total
   - Slice 4 accepted
   - `Phase 05` accepted as complete

### Next

1. `Phase 06` planning
2. broader engine implementation stage
3. real-engine integration against selected `weed/storage/block*` constraints and failure classes
