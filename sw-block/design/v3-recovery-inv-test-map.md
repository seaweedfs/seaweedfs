# Recovery INV ↔ Test Mapping (G7-redo POC)

Each invariant declared by `core/recovery/doc.go` (or by
`sw-block/design/v3-recovery-live-line-backlog-spec.md`) maps below to
the test function(s) that pin it. File paths are repo-relative;
line numbers are the `func TestXxx` declaration line at branch
`g7-redo/dual-lane-recovery-poc` HEAD (commits `42f351d` /
`dc7f550` / `a5b50db`).

Status legend:

- **Layer 1** — receiver-side mechanism (`core/recovery/`, layer-1 files).
- **Layer 2** — primary-side orchestration (coordinator + bridge).
- **Layer 3** — error taxonomy.
- **Substrate** — `core/storage/memorywal` contract conformance.
- **Forward** — declared but deliberately untested in this PR; pending later milestone.

---

## Layer 1 — receiver-side mechanism

| INV | Definition | Test(s) | Status |
|---|---|---|---|
| `INV-DUAL-LANE-WAL-WINS-BASE` | Same LBA: once WAL lane has applied (`bitmap.MarkApplied`), base lane MUST skip subsequent base blocks for that LBA. Symmetric: base-then-WAL → WAL data overwrites via substrate's last-writer-wins LSN ordering. | `TestRebuildSession_WALWinsThenBaseSkips` (`core/recovery/rebuild_session_test.go:29`)<br>`TestRebuildSession_BaseFirstThenWALOverwrites` (`core/recovery/rebuild_session_test.go:54`) | ✅ pinned both directions |
| `INV-SESSION-COMPLETE-ON-CONJUNCTION-LAYER1` | `TryComplete` returns `done=true` iff `baseDone ∧ walApplied ≥ targetLSN`. Latched: subsequent calls return `done=true` without state change. NOT including barrier-ack — that is layer-2's system-level confirmation. | `TestRebuildSession_TryComplete_Conjunction` (`core/recovery/rebuild_session_test.go:74`)<br>`TestRebuildSession_TryComplete_LatchesCompleted` (`core/recovery/rebuild_session_test.go:108`) | ✅ pinned (3 negative + 1 positive + latch) |
| `INV-BITMAP-NO-INDEPENDENT-LOCK` | `RebuildBitmap` exposes no synchronization; callers (`RebuildSession`) serialize access via session mutex. | `TestRebuildSession_ConcurrentLanes` (`core/recovery/rebuild_session_test.go:130`) | ✅ stress test (4 base + 4 WAL goroutines × 1024 LBAs); needs `-race` on Linux for full proof |
| `INV-WAL-CURSOR-MONOTONIC-FROM-PINLSN` | Sender's WAL pump rewinds cursor to `fromLSN` once at session start; cursor advances monotonically per `ScanLBAs` callback; never decreases. Receiver enforces matching wire-level monotonic discipline (4-case per `v3-recovery-unified-wal-stream-kickoff.md` v0.3 §5.1: `==applied+1` apply / `>applied+1` gap → `FailureContract` / `==applied` exact-duplicate → `FailureProtocol` / `<applied` backward → `FailureProtocol`). The only legitimate "rewind" is session-level `cursor:=pinLSN` at a NEW SessionStart, never in-stream. | Sender pump (memorywal substrate): `TestSender_PumpHappyPath_OnMemoryWAL`, `TestSender_LiveWritesDuringSession_OnMemoryWAL`, `TestSender_KindByte_FlipsOnceAtCatchUp`, `TestSender_StreamUntilHead_CtxCancel` (`core/recovery/unified_wal_test.go`).<br>Receiver 4-case discipline: `TestReceiver_RejectsBackwardLSN_InSession`, `TestReceiver_RejectsGap_InSession`, `TestReceiver_RejectsExactDuplicate_InSession` (same file). | ✅ pinned (sender pump + receiver 4-case discipline; added in g7-redo/unified-wal-impl §3.2 #3 milestone) |

---

## Layer 2 — primary-side orchestration

| INV | Definition | Test(s) | Status |
|---|---|---|---|
| `INV-PIN-EXISTS-ONLY-DURING-SESSION` | Primary maintains no `pin_floor` in steady state; recycle proceeds per retention. `pin_floor` appears only during an active session. | `TestCoordinator_MinPinAcrossActiveSessions` (`core/recovery/peer_ship_coordinator_test.go:159`) — `anyActive=false` when no sessions; emerges on `StartSession`; vanishes on `EndSession` | ✅ pinned |
| `INV-PIN-STABLE-WITHIN-SESSION` | Same session: `pin_floor` is monotonically non-decreasing. To pick a different anchor, invalidate session and start new lineage. | `TestCoordinator_SetPinFloor_Monotonic` (`core/recovery/peer_ship_coordinator_test.go:200`) — regression attempts ignored | ✅ pinned |
| `INV-PIN-ADVANCES-ONLY-ON-REPLICA-ACK` | `pin_floor` advances iff replica has emitted `BaseBatchAcked` for an LBA range whose base data is now installed. Primary cannot advance pin on its own. | `TestE2E_PinFloorAdvancesIncrementally` (`core/recovery/e2e_test.go:466`) — K=8 cadence drives multiple acks across 50-LBA backlog; pin advances through coord.SetPinFloor. Wire impl in `g7-redo/pin-floor` branch (`6cb89ee`). | ✅ pinned |
| `INV-PIN-COMPATIBLE-WITH-RETENTION` | `pin_floor ≥ retained(S)` always holds. A session demanding a pin below S MUST fail-loud → invalidate → new lineage. | Coord layer: `TestCoordinator_SetPinFloor_RejectsBelowRetention` (`core/recovery/peer_ship_coordinator_test.go:225`) — typed `*Failure(PinUnderRetention)` on `floor < primaryS`; `TestCoordinator_SetPinFloor_ZeroBoundaryDisablesCheck` (`core/recovery/peer_ship_coordinator_test.go:262`) — `S=0` legacy path.<br>Engine retry-budget gate (G7-redo priority #3 [retry] Option A): `TestT4d_PinUnderRetention_BypassesRetryBudget` + `TestT4d_PinUnderRetention_AtBudgetEdge_DoesNotEscalate` (`core/engine/recovery_pin_retention_test.go`) — `RecoveryFailurePinUnderRetention` skips retry, leaves `Attempts` unchanged, emits `PublishDegraded`, does NOT auto-flip Decision (counter-WALRecycled).<br>Boundary mapping: `TestClassifier_RecoveryPinUnderRetention_MapsToEngineKind` (`core/transport/classifier_pin_under_retention_test.go`) — `*recovery.Failure(PinUnderRetention)` → `engine.RecoveryFailurePinUnderRetention` (closes silent fall-through to retryable Transport). | ✅ pinned (coord + engine retry-bypass + boundary mapping) |
| `INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN` | Primary's WAL recycle floor = `min(pin_floor)` over active sessions; no sessions ⇒ pure retention. | `TestCoordinator_MinPinAcrossActiveSessions` (`core/recovery/peer_ship_coordinator_test.go:159`) — coord-level min/release.<br>`TestWALStore_RecycleGate_*` (`core/storage/walstore_recycle_pin_test.go`) — 6 cases: nil source / inactive source / active clamp / floor-above-proposed / no-regress / disable. Substrate `persistCheckpoint` honors gate.<br>`TestStore_RecycleGate_*` (`core/storage/memorywal/store_recycle_pin_test.go`) — 6 cases mirroring walstore on the in-memory substrate; pins gate interface + AdvanceWALTail clamp. | ✅ pinned (coord + walstore + memorywal substrate integration) |
| `INV-SESSION-COMPLETE-CLOSURE` | System-level done = layer-1 `TryComplete` ∧ barrier-ack(`achieved == targetLSN`). Both halves required before declaring InSync. | `TestCoordinator_CanEmitSessionComplete` (`core/recovery/peer_ship_coordinator_test.go:79`)<br>`TestE2E_RebuildHappyPath` (`core/recovery/e2e_test.go:33`) — full round-trip<br>`TestE2E_RebuildWithLiveWritesDuringSession` (`core/recovery/e2e_test.go:127`) — closure with live | ✅ pinned (closure predicate + e2e round-trip) |
| `INV-LIVE-CAUGHT-UP-IFF-FRONTIER-AT-BARRIER` | "Live caught up" is provable only via probe/barrier comparing `R_repr` against `H` at frozen time, not via shipper liveness. | `TestE2E_RebuildHappyPath` and `TestE2E_RebuildWithLiveWritesDuringSession` — both require barrier achieved ≥ target before passing | ✅ pinned indirectly (no test passes without barrier) |
| `INV-SESSION-TEARDOWN-IS-EXPLICIT` | Convergence → InSync → teardown session → drop `pin_floor` → recycle resumes. All transitions are explicit events. | `TestCoordinator_EndSession_Idempotent` (`core/recovery/peer_ship_coordinator_test.go:218`)<br>e2e tests check `coord.Phase == Idle` post-Run | ✅ pinned |
| `INV-SINGLE-FLIGHT-PER-REPLICA` | At most one active rebuild session per replica. Concurrent attempts rejected at session start. | `TestCoordinator_SingleFlightPerReplica` (`core/recovery/peer_ship_coordinator_test.go:63`)<br>`TestIntegrationStub_LifecycleCallbacks` (`core/recovery/integration_stub_test.go:22`) — second `StartRebuildSession` errors | ✅ pinned (coord level + bridge level) |

---

## Spec `CHK-*` checks (`v3-recovery-live-line-backlog-spec.md`)

| CHK | Definition | Test(s) | Status |
|---|---|---|---|
| `CHK-PHASE-NEVER-STEADY-BEFORE-DRAIN` | While recover session active, do NOT transition peer to `SteadyLiveAllowed` until `BacklogDrained ∧ baseDone`. | `TestCoordinator_PhaseTransitionRequiresDrainAndBaseDone` (`core/recovery/peer_ship_coordinator_test.go:10`) — both negative cases (drained-but-not-done, done-but-not-drained) | ✅ pinned |
| `CHK-BARRIER-BEFORE-CLOSE` | No `SessionClosedCompleted` without prior successful barrier `AchievedLSN ≥ target`. | `TestCoordinator_CanEmitSessionComplete` (`core/recovery/peer_ship_coordinator_test.go:79`) | ✅ pinned |
| `CHK-NO-FAKE-LIVE-DURING-BACKLOG` | During scripted backlog, steady sentinel path either paused or fed from same ordered queue. | `TestCoordinator_RouteLocalWrite` (`core/recovery/peer_ship_coordinator_test.go:114`) — phase=DrainingHistorical → SessionLane; phase=SteadyLiveAllowed → still SessionLane (barrier-pending); only Idle returns SteadyLive | ✅ pinned (with architect-ratified semantics: SteadyLiveAllowed is publication-permission, not routing) |

---

## Substrate (memorywal) contract conformance

| INV / Property | Test(s) | Status |
|---|---|---|
| `LogicalStorage` contract — Write / Read round-trip | `TestContract_WriteReadRoundTrip` (`core/storage/contract_test.go:91`) — runs across BlockStore / WALStore / **MemoryWAL** | ✅ pinned (3 impls) |
| `LogicalStorage` — read-unwritten-LBA returns zeros | `TestContract_ReadUnwrittenLBAReturnsZeros` (`core/storage/contract_test.go:111`) | ✅ pinned (3 impls) |
| `LogicalStorage` — Write advances LSN strictly increasing | `TestContract_WriteAdvancesLSN` (`core/storage/contract_test.go:124`) | ✅ pinned (3 impls) |
| `LogicalStorage` — Sync returns highest written LSN | `TestContract_SyncReturnsHighestWrittenLSN` (`core/storage/contract_test.go:140`) | ✅ pinned (3 impls) |
| `LogicalStorage` — Sync monotonic non-decreasing | `TestContract_SyncMonotonicNonDecreasing` (`core/storage/contract_test.go:160`) | ✅ pinned (3 impls) |
| `LogicalStorage` — Boundaries (R/S/H) reflect writes | `TestContract_BoundariesReflectWrites` (`core/storage/contract_test.go:182`) | ✅ pinned (3 impls) |
| `LogicalStorage` — ApplyEntry uses supplied LSN | `TestContract_ApplyEntryUsesSuppliedLSN` (`core/storage/contract_test.go:204`) | ✅ pinned (3 impls) |
| `LogicalStorage` — AdvanceFrontier bumps H | `TestContract_AdvanceFrontierBumpsH` (`core/storage/contract_test.go:235`) | ✅ pinned (3 impls) |
| `LogicalStorage` — AdvanceWALTail moves S | `TestContract_AdvanceWALTailMovesS` (`core/storage/contract_test.go:253`) | ✅ pinned (3 impls) |
| `LogicalStorage` — AllBlocks current snapshot | `TestContract_AllBlocksReturnsCurrentSnapshot` (`core/storage/contract_test.go:271`) | ✅ pinned (3 impls) |
| `LogicalStorage` — Close idempotent | `TestContract_CloseIsIdempotent` (`core/storage/contract_test.go:286`) | ✅ pinned (3 impls) |
| `LogicalStorage` — Recover on fresh store returns 0 | `TestContract_RecoverOnFreshStoreReturnsZero` (`core/storage/contract_test.go:297`) | ✅ pinned (3 impls) |
| **MemoryWAL-specific:** ScanLBAs emits write-time LSN | `TestScanLBAs_EmitsWriteTimeLSN` (`core/storage/memorywal/store_test.go:59`) | ✅ pinned |
| **MemoryWAL-specific:** 3 writes to same LBA → 3 ScanLBAs entries (V2-faithful per-LSN, NOT state-convergence dedup) | `TestScanLBAs_ThreeWritesSameLBA_EmitsThreeEntries` (`core/storage/memorywal/store_test.go:98`) | ✅ pinned |
| **MemoryWAL-specific:** AdvanceWALTail → ErrWALRecycled gate (typed envelope + errors.Is compat) | `TestAdvanceWALTail_RecycledGate` (`core/storage/memorywal/store_test.go:166`) | ✅ pinned (typed `*RecoveryFailure` + `errors.Is(err, ErrWALRecycled)` via Unwrap) |
| **MemoryWAL-specific:** AppliedLSNs returns per-LBA highest LSN (BlockStore returns sentinel) | `TestAppliedLSNs_TracksHighestPerLBA` (`core/storage/memorywal/store_test.go:224`) | ✅ pinned |
| **MemoryWAL-specific:** RecoveryMode reports WALReplay (not StateConvergence) | `TestRecoveryMode_WALReplay` (`core/storage/memorywal/store_test.go:214`) | ✅ pinned |

---

## Layer 3 — failure taxonomy

| INV | Definition | Test(s) | Status |
|---|---|---|---|
| Failure retryable matrix (9 kinds) | Wire / Substrate / Contract → retryable; Protocol / Cancelled / SingleFlight / WALRecycled / **PinUnderRetention** / Unknown → not retryable. | `TestFailure_RetryableMatrix` (`core/recovery/failure_test.go:11`) | ✅ pinned (PinUnderRetention added in `g7-redo/pin-floor`) |
| Typed `*Failure` envelope: nil-safe Error/Retryable, errors.Is via Unwrap | `TestFailure_NilSafeAndUnwrap` (`core/recovery/failure_test.go:33`) | ✅ pinned |
| `AsFailure(err)` extraction works on chains; nil/plain → nil | `TestFailure_AsFailureExtraction` (`core/recovery/failure_test.go:52`) | ✅ pinned |
| Wire failure surfaces as typed `Failure(Wire, …)`, retryable=true | `TestIntegrationStub_FailureTypedOnReceiverDown` (`core/recovery/integration_stub_test.go:298`) — replica conn closed early | ✅ pinned |
| Ctx cancel surfaces as typed `Failure(Cancelled, AwaitClose)`, retryable=false | `TestIntegrationStub_FailureTypedOnCancellation` (`core/recovery/integration_stub_test.go:352`) | ✅ pinned |
| Sender's `defer` always seals + EndSessions, regardless of error path | `TestIntegrationStub_ReceiverFailsEarly_SenderUnblocks` (`core/recovery/integration_stub_test.go:163`) — post-error PushLiveWrite errors; coord back to Idle<br>`TestIntegrationStub_CtxCancelUnblocksSender` (`core/recovery/integration_stub_test.go:230`) — same on ctx cancel<br>`TestE2E_PushLiveWriteAtomicSeal` (`core/recovery/e2e_test.go:365`) — 8 goroutines × 5 pushes; every accepted push present on replica | ✅ pinned (3 angles) |

---

## Forward-direction INVs (declared in `doc.go`, deliberately not yet tested)

| INV | Why deferred | Target milestone |
|---|---|---|
| `INV-PIN-ADVANCES-ONLY-ON-REPLICA-ACK` | Requires wire ack frame from receiver → primary to drive `coord.SetPinFloor`. | #3 (BaseBatchAck → pin_floor) |
| `INV-PIN-COMPATIBLE-WITH-RETENTION` | Requires real substrate's `Boundaries().S` cross-check at session start. | #3 |
| `INV-BASE-SPARSE-REQUIRES-SUBSTRATE-CLOSURE` _(or alternate `INV-BASE-FULL-SCAN-NO-SPARSE-ASSUMPTION`)_ | POC chose dense base; sparse omit + substrate basement-clearing is a separate INV pair, one of two must hold. | Sparse base milestone |
| `INV-REPL-OVERLAP-HISTORY-NO-REGRESS` | Bitmap-as-WAL-claim refinement — when stale/duplicate LSN arrives that substrate apply gate skips, bitmap may still be marked to prevent base-lane historical refill over newer replica state. Code currently marks bitmap only after successful ApplyEntry. | After apply-gate redesign |
| `INV-BACKLOG-LIVE-INTERLEAVE-LSN-SERIALIZED` _(candidate name)_ | Spec §3.2 #3 promise: single ordered queue mixing recover-tagged + post-target traffic by LSN. POC ships sequential (backlog → seal → live). | #2 (single-queue real-time interleave) |
| `INV-PRIMARY-SHIP-PHASE` | Operator-visible "primary did not declare InSync on steady-live alone" check; needs engine-level integration to surface. | Wiring PR |

---

## Test sweep summary

```
core/recovery/                       36 tests, all PASS  (+3 from g7-redo/pin-floor)
core/storage/memorywal/              14 tests, all PASS
core/storage/contract_test.go        12 tests × 3 impls = 36 sub-test runs, all PASS
                                     ────
                                     86 test results pinning the above INVs
```

Race-detector status: not run on Windows host (CGO_ENABLED required).
Linux CI must run `go test -race ./core/recovery/... ./core/storage/...`
to validate `INV-BITMAP-NO-INDEPENDENT-LOCK` under stress.

---

## How to keep this current

When a new INV lands in `core/recovery/doc.go` (or in the spec):

1. Add a row to the corresponding section.
2. Cite the test function + file:line at branch HEAD.
3. If forward-only (no test yet), put it in the bottom section with the target milestone.
4. Update the "Test sweep summary" counts.

When a test is renamed or moved, update the line number column. CI
gating (future): a small linter could parse this file and assert
each cited test actually exists and contains the INV name in a
comment — left for a later milestone.
