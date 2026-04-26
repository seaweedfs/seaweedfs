# T4d-3 — G-1 V2 Read (pre-code diff-footprint review with explicit V2/V3 split)

**Date**: 2026-04-25 (v0.2 — addresses QA v0.1 ADDITIONS 1+2 + nit)
**Owner**: sw (porter)
**Reviewer**: QA (Gate G-1 sign)
**Mini-plan ref**: `v3-phase-15-t4d-mini-plan.md` v0.3 §2.3 T4d-3 / §7.1 Gate G-1
**Discipline**: pre-code (§0-B Stability-Locality rule); same as T4a-2 / T4a-4 / T4b-4 / T4c-2 G-1
**Scope binding** (kickoff §9.5): explicit PORT vs V3-native split required. T4d-3 ports the bounded catch-up muscle, NOT the V2 `WALShipper` object model
**Predecessors**: T4c batch closed (`c910464a9`); T4d kickoff §1–9 RATIFIED; T4d mini-plan v0.3 (architect-revised); BlockStore walHead pre-T4d hotfix (`f6084ee`); T4d-1 + T4d-2 may run in parallel — this G-1 gates T4d-3 code only

---

## §1 Scope

T4d-3 has TWO production deliverables that share one G-1:

1. **R+1 threading** — `StartCatchUp` signature carries `fromLSN`; executor scans `R+1..target`, not `1..target`
2. **Engine-driven recovery wiring** — `ReplicationVolume↔adapter` plumbed end-to-end; engine retry loop runs through real production code path; `WithEngineDrivenRecovery` framework primitive becomes real

The G-1 is required because (1) touches the V2 catch-up muscle boundary (`runCatchUpTo(replicaFlushedLSN, targetLSN)`). (2) is V3-native end-to-end (no V2 source).

| Code | In G-1? | Reason |
|---|---|---|
| `core/transport/catchup_sender.go` `doCatchUp` body — replace `ScanLBAs(1, ...)` with `ScanLBAs(fromLSN, ...)` | **YES** | direct V2-faithful start-boundary port; §0-B Stability-Locality binds |
| `core/transport/executor.go` `BlockExecutor.StartCatchUp` signature gains `fromLSN uint64` | **YES** (signature change boundary) | propagates V2's two-arg shape into V3 |
| `core/adapter/executor.go` `CommandExecutor.StartCatchUp` interface signature update | no | mechanical signature ripple |
| `core/engine/commands.go` `engine.StartCatchUp` command struct gains `FromLSN uint64` | no | mechanical signature ripple |
| `core/engine/apply.go` populates `FromLSN` from `Recovery.R` at emit + retry-loop re-emit | no | V3-native engine-emit; spec is "thread R from probe state" |
| `core/replication/volume.go` `ReplicationVolume↔adapter` wiring | no | V3-native; engine retry loop already exists at unit scope |
| `core/replication/component/cluster.go` `WithEngineDrivenRecovery()` becomes real | no | framework primitive; V3-native |

**Anchor**: T4c-2 already ported the inner-loop muscle (callback-return-nil-continues, lastSent-monotonic, deadline-per-call-scope, target-not-reached-vs-recycled-distinguished, completion-from-barrier-achievedLSN). T4d-3 only changes the **start LSN** of that muscle from `1` to `R+1`. The 6 T4c-2 invariants must forward-carry green.

---

## §2 V2 reference excerpts — verbatim

### §2.1 `runCatchUpTo` body — `wal_shipper.go:845-911`

```go
func (s *WALShipper) runCatchUpTo(fromLSN uint64, targetLSN uint64) (uint64, error) {
	s.state.Store(uint32(ReplicaCatchingUp))

	// Set a deadline for the entire catch-up operation.
	s.mu.Lock()
	if s.dataConn != nil {
		s.dataConn.SetDeadline(time.Now().Add(catchupTimeout))
	}
	conn := s.dataConn
	s.mu.Unlock()

	if conn == nil {
		return 0, fmt.Errorf("catch-up: no data connection")
	}

	// Stream entries from WAL.
	var lastSent uint64
	err := s.wal.StreamEntries(fromLSN+1, func(entry *WALEntry) error {
		if targetLSN > 0 && entry.LSN > targetLSN {
			return nil
		}
		encoded, encErr := entry.Encode()
		if encErr != nil {
			return encErr
		}
		if wErr := WriteFrame(conn, MsgWALEntry, encoded); wErr != nil {
			return wErr
		}
		lastSent = entry.LSN
		return nil
	})

	if err != nil {
		if errors.Is(err, ErrWALRecycled) {
			s.state.Store(uint32(ReplicaNeedsRebuild))
			return lastSent, fmt.Errorf("catch-up: WAL recycled: %w", err)
		}
		return lastSent, fmt.Errorf("catch-up: stream error: %w", err)
	}

	// Send CatchupDone marker.
	doneLSN := lastSent
	if doneLSN == 0 {
		doneLSN = fromLSN
	}
	if err := WriteFrame(conn, MsgCatchupDone, EncodeCatchupDone(doneLSN)); err != nil {
		return lastSent, fmt.Errorf("catch-up: send done: %w", err)
	}

	// Clear deadline.
	s.mu.Lock()
	if s.dataConn != nil {
		s.dataConn.SetDeadline(time.Time{})
	}
	s.mu.Unlock()

	effectiveLast := lastSent
	if effectiveLast == 0 {
		effectiveLast = fromLSN
	}
	if targetLSN > 0 && effectiveLast < targetLSN {
		return lastSent, fmt.Errorf("catch-up: target %d not reached (last=%d)", targetLSN, lastSent)
	}
	log.Printf("wal_shipper: catch-up complete %s: from=%d target=%d last=%d",
		s.dataAddr, fromLSN+1, targetLSN, effectiveLast)
	return lastSent, nil
}
```

### §2.2 Calling frame — `wal_shipper.go:258-298` `CatchUpTo`

```go
func (s *WALShipper) CatchUpTo(targetLSN uint64) (uint64, error) {
    // ... handshake to obtain replicaFlushedLSN ...
    case ReplicaCatchingUp:
        achievedLSN, catchErr := s.runCatchUpTo(replicaFlushedLSN, targetLSN)
        if catchErr != nil {
            s.catchupFailures++
            if s.catchupFailures >= maxCatchupRetries {  // line 152: const = 3
                s.state.Store(uint32(ReplicaNeedsRebuild))
                return achievedLSN, fmt.Errorf("catch-up failed %d times: %w", s.catchupFailures, catchErr)
            }
            s.markDegraded()
            return achievedLSN, ErrReplicaDegraded
        }
        // ...
```

### §2.3 Pre-bounded-catch-up call site — `wal_shipper.go:204-210`

```go
// Fresh or late-attached replicas must consume the retained backlog before
// receiving a live-tail entry. This closes the LSN-gap path where the first
// post-attach live write would otherwise arrive before the retained prefix.
if st == ReplicaDisconnected && s.wal != nil && entry.LSN > 1 {
    if _, err := s.CatchUpTo(entry.LSN - 1); err != nil {
        log.Printf("wal_shipper: bounded catch-up before live ship failed ...")
        return nil
    }
}
```

This is V2's late-attach pattern: fresh replica needing backlog before live tail. V3 covers the same case differently — the engine emits `StartCatchUp` from probe results; this V2 inline call is **NOT** ported. Document explicitly so reviewer doesn't expect to find it.

---

## §3 PORT items (5 — kickoff §9.5)

Each row: V2 concern → V3 location → diff against current V3 (T4c-2 baseline) → invariant pin.

### Item 1 — `runCatchUpTo(replicaFlushedLSN, targetLSN)` start boundary

| Aspect | V2 | V3 today (T4c-2) | V3 after T4d-3 |
|---|---|---|---|
| Scan start arg | `fromLSN+1` (line 862) where `fromLSN = replicaFlushedLSN` | `ScanLBAs(1, ...)` hardcoded (`catchup_sender.go` post-T4c-3 minor fix) | `ScanLBAs(fromLSN, ...)` where `fromLSN = command.FromLSN` |
| Source of `replicaFlushedLSN` | V2 `reconnectWithHandshake` returns it | V3: probe handshake reports `Recovery.R`; engine populates `StartCatchUp.FromLSN = Recovery.R + 1` at command emit | same as v0.3 spec |

**V2/V3 diff (explicit):**
- V2 derives `fromLSN` from a reconnect-handshake call **inside** `CatchUpTo` (`replicaFlushedLSN, err := s.reconnectWithHandshake()`). The handshake is shipper-internal.
- V3 derives `FromLSN` from `Recovery.R` **inside the engine** at command emit time. Probe (T4c-1) is the discovery mechanism; engine state is the source of truth. **Reason for the relocation**: engine-owned recovery decisions per Q1 (kickoff §9.3 #1). Transport doesn't re-discover R; engine tells transport what to ship.
- Off-by-one: V2 calls `StreamEntries(fromLSN+1, ...)` (skip applied LSN); V3 must do the same — engine should populate `FromLSN = Recovery.R + 1` directly, OR sender does the `+1` adjustment locally. **Decision needed in §6** below.

**Pin**: `INV-REPL-CATCHUP-WITHIN-RETENTION-001` (un-pinned at T4c, ported at T4d-3).

### Item 2 — Per-call catch-up deadline

| Aspect | V2 | V3 today (T4c-2) | V3 after T4d-3 |
|---|---|---|---|
| Deadline source | `catchupTimeout` const (V2 package-level) | `recoveryConnTimeout` const (T4c-2 sender) | unchanged in T4d-3; T4c-pre-B `RecoveryRuntimePolicy.Timeout` migration is T4e/G5 work |
| Deadline scope | per-call: set at line 851, cleared at line 897 | per-call: SetDeadline at entry, deferred SetDeadline(zero) at exit | preserved; T4d-3 must NOT regress |

**V2/V3 diff (explicit):**
- V2 sets the deadline ONLY when there's an existing `s.dataConn`; if conn is nil, returns `"no data connection"` error immediately. V3 doesn't have the same nil-check shape — V3 dials lazily inside `doCatchUp`. Both are correct; V2's nil-check is implementation-specific to its conn-pooled `WALShipper`. **NOT ported** (different conn-lifecycle model).
- V2 clears deadline AFTER `MsgCatchupDone` write, BEFORE final log line. V3's deferred `SetDeadline(time.Time{})` clears at function exit. Functionally equivalent; V3 deferred is cleaner.

**Pin**: `INV-REPL-CATCHUP-DEADLINE-PER-CALL-SCOPE` (T4c-2; forward-carry).

### Item 3 — Last-sent / achieved-LSN monotonicity

| Aspect | V2 | V3 today (T4c-2) | V3 after T4d-3 |
|---|---|---|---|
| `lastSent` advance | line 873 — AFTER successful `WriteFrame` | T4c-2: `lastSent = entry.LSN` AFTER successful WriteMsg | preserved |
| Final completion source | V2: returns `lastSent` from `runCatchUpTo`; CALLER (`CatchUpTo`) returns it | V3: returns `BarrierResponse.AchievedLSN` from barrier round-trip (T4c-2 §4.2 binding) | preserved (V3 collapses V2's `MsgCatchupDone` into the existing barrier; INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN) |

**V2/V3 diff (explicit):**
- V2 emits `MsgCatchupDone(doneLSN)` as a separate wire frame (line 890). V3 collapsed this into the existing barrier per T4c-2 §4.2 architect Option B (`INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN`). **NOT a regression** — completion information content is preserved (`BarrierResponse.AchievedLSN`); the wire is simpler.
- V2's `effectiveLast = lastSent; if zero, fromLSN` fallback (lines 901-904) is for the empty-window case (replica was already caught up). V3's barrier returns `AchievedLSN` from replica-side `Sync()` which already encodes that semantic — **NOT explicitly ported**, but the equivalent state is reached.

**Pin**: `INV-REPL-CATCHUP-LASTSENT-MONOTONIC` (T4c-2; forward-carry under R+1 — test must verify lastSent advances normally when starting from R+1, not 1).

### Item 4 — Catch-up retry budget (semantic port, V3 location)

| Aspect | V2 | V3 today | V3 after T4d-3 |
|---|---|---|---|
| Retry budget value | `maxCatchupRetries = 3` (V2 line 152) | `RecoveryRuntimePolicy.MaxRetries = 3` for `wal_delta` (T4c-pre-B) | preserved value |
| Retry counter location | `s.catchupFailures` on `WALShipper` instance (line 285) | `RecoveryTruth.Attempts` on engine state (T4c-3) | unchanged |
| Retry escalation | `if catchupFailures >= max → state = NeedsRebuild` (line 287) | engine `applySessionFailed` increments Attempts; if exhausted → escalate via `recovery.Decision = Unknown` + `PublishDegraded` (T4c-3) | wired end-to-end via T4d-3 ReplicationVolume↔adapter plumbing |

**V2/V3 diff (explicit):**
- V2 puts retry counter on the `WALShipper` per-instance (transport-side bookkeeping). V3 puts it on engine state per Q1 (kickoff §9.3 #1). **Architect-locked relocation** — engine owns recovery decisions including retry.
- V2 ESCALATION: `s.state.Store(NeedsRebuild)` directly. V3: engine sets `Decision = Rebuild` via the structured-kind path (T4d-1 work — `RecoveryFailureWALRecycled` triggers it). **Different mechanism, equivalent semantic.**
- V2's catch-up failure on non-recycle errors: returns `ErrReplicaDegraded` (line 291) — caller handles. V3: engine retry loop re-emits StartCatchUp until budget exhausted, then publishes Degraded. **More structured.**
- V2 `start_timeout` analog: V2 has no equivalent of V3's watchdog start-timeout; V3 added it for adapter-level executor stalls (T4c-3). Bypasses retry per T4c-3 design. **NOT a V2 port; V3-native robustness add.**

**Pin**: V2 `maxCatchupRetries = 3` value preserved; mechanism is V3-native engine retry loop.

### Item 5 — Retention-miss escalation (semantic port, V3 location)

| Aspect | V2 | V3 today (T4c-2) | V3 after T4d-3 |
|---|---|---|---|
| Substrate signal | `errors.Is(err, ErrWALRecycled)` (line 878) | `storage.ErrWALRecycled` sentinel via `ScanLBAs` (T4c-2) | T4d-1 wraps as `storage.RecoveryFailure{Kind: StorageRecoveryFailureWALRecycled}` |
| Sender response | wraps `"catch-up: WAL recycled: %w"` (line 880) + sets `NeedsRebuild` | wraps `"catch-up: WAL recycled: %w"` + returns to caller; engine maps via substring (T4c-2) | sender extracts kind via `errors.As`, maps to `engine.RecoveryFailureWALRecycled`, populates `SessionCloseResult.FailureKind` |
| Engine response | (V2: state set in sender) | engine `applySessionFailed` substring-match → `Decision = Rebuild` (T4c-2) | T4d-1 typed branch: `if e.FailureKind == RecoveryFailureWALRecycled → Decision = Rebuild`; substring removed |

**V2/V3 diff (explicit):**
- V2 sets state DIRECTLY in sender (`s.state.Store(NeedsRebuild)` line 879). V3 separates: sender reports failure kind, engine decides state transition. **Q1 architect-locked separation.**
- V2 leaks substrate sentinel name into transport error message ("WAL recycled"). V3 v0.3 contract: storage owns `StorageRecoveryFailureKind`; transport maps to engine kind; engine never imports storage. **Cleaner boundary; v0.3 dependency lock.**
- V2 retention boundary IS the WAL writer's `checkpointLSN`. V3 retention boundary is the substrate's own (walstore: `checkpointLSN`; smartwal: `oldestPreserved = head - capacity`). **Same semantic, substrate-specific implementation** — and T4d-3 doesn't add any new substrate work; T4c-2 already covers.

**Pin**: `INV-REPL-CATCHUP-RECYCLE-ESCALATES` (T4c-2; pinning method changes from substring to typed `FailureKind` at T4d-1).

---

## §4 V3-NATIVE items (5 — kickoff §9.5)

These items have NO V2 source. The G-1 reviewer must NOT expect a V2 mapping for them. Each row is described to make scope explicit.

### Item A — Engine-owned retry loop (Q1)

**V3-native because**: V2 owned retry on the WALShipper instance (transport-side). V3 architect-locked at Q1: retry is engine-owned. Transport stays byte-movement-only. T4c-3 already implemented this at engine unit scope. T4d-3 wires it through ReplicationVolume so it runs end-to-end at integration scope.

**Implementation today (T4c-3)**: `engine/apply.go applySessionFailed` increments `Recovery.Attempts`; while ≤ `MaxRetries`, re-emits `StartCatchUp`/`StartRebuild`; on exhaustion, resets + emits `PublishDegraded`.

**T4d-3 work**: wire `ReplicationVolume↔adapter` so the engine's re-emitted commands actually drive a fresh executor session (not just unit-test scope). Component framework's `WithEngineDrivenRecovery()` swaps from stub to real.

**No V2 source.** Do NOT look for one.

### Item B — Unified recovery command model (`StartRecovery` / `RecoveryContentKind` / runtime policy)

**V3-native because**: V2 has separate `CatchUpTo` and rebuild paths (different code, different commands). V3 unified at T4c-pre-B per memo §7a (architect rounds 29-30): one `StartRecovery` command + `RecoveryContentKind` + per-kind `RuntimePolicy`. V2 has no analog.

**T4d-3 work**: nothing direct — T4c-pre-B already shipped the unified command. T4d-3 just continues to dispatch through the existing `StartRecoverySession` bridge. The legacy `StartCatchUp` path retained for engine emit; engine emit migration to `StartRecovery` is post-G5.

**No V2 source.**

### Item C — Lane-aware apply gate integration

**V3-native because**: T4d-2 builds the gate from round-43/44 architect text; no V2 source. T4d-3's transport call into the gate is the integration point.

**T4d-3 work**: catch-up sender's per-entry callback no longer calls `substrate.ApplyEntry` directly on the replica (it doesn't today either — it ships via wire and the replica handler applies). The change is at the **replica handler side** in T4d-2: `replica.go MsgShipEntry` handler routes via apply gate based on handler context (recovery vs live). T4d-3 ensures the catch-up handler route is wired correctly post-R+1 threading. Sender does NOT query substrate per-LBA freshness (`INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY` — Q3).

**No V2 source.** V2 has no gate; replica blindly applies.

### Item D — `BarrierResponse.AchievedLSN` as completion truth

**V3-native because**: V2 uses `MsgCatchupDone(doneLSN)` as a separate wire frame. V3 collapsed at T4c-2 §4.2 (architect Option B) — `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN`. T4d-3 does NOT add MsgCatchupDone back.

**T4d-3 work**: nothing direct — T4c-2 already established. Forward-carry test must verify barrier still completes correctly when scan starts at R+1 (not 1).

**No V2 source for the collapse.** Original wire shape IS V2; the collapse is V3-native.

### Item E — Package-boundary discipline (`INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY`)

**V3-native because**: V2 monolithic `weed/storage/blockvol/` package — no equivalent layered separation. V3 layered architecture has clean boundaries (Q3 architect-locked at kickoff §9.3 #3): `core/transport` may depend on `core/storage` recovery contracts only, never substrate internals; `core/recovery` package move deferred post-G5/T4e.

**T4d-3 work**: ensure the new R+1 threading + adapter wiring does NOT introduce new substrate-internal imports. Sender-side import audit in PR. **V3-native discipline; no V2 source for the rule.**

**Pin**: `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY` inscribed at T4d-3.

---

## §5 Hidden-invariant audit

T4d-3 is largely a delta-on-T4c-2 (start LSN change + adapter wiring); most invariants are forward-carries from T4c-2. Anticipated 2 NEW hidden invariants:

| # | Name | Statement | Inscribed at |
|---|---|---|---|
| 1 | `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` | Engine emits `StartCatchUp.FromLSN = Recovery.R + 1`. Sender does NOT add `+1` (avoids double-add). The "+1 to skip already-applied LSN" semantic lives at the engine, not transport. | T4d-3 |
| 2 | `INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE` | Engine populates `StartCatchUp.FromLSN` from its **own** `Recovery.R` state (the single source of truth). Probe results are ingested as facts that update engine state; the command emit path reads the updated state, never the raw probe payload directly. This pins the race window: probe → recovery decision → command emit always goes through engine state, no shortcut. (v0.2: positive-form rename per QA nit; was `INV-REPL-CATCHUP-FROMLSN-NOT-FROM-PROBE-DIRECTLY` in v0.1.) | T4d-3 |

These pin the V3-native engine-emit policy decisions. Both relate to the V2/V3 diff Item 1 (start boundary derivation is V3-native; V2's reconnect-handshake-inside-shipper has no V3 analog).

---

## §6 Open placement decisions (QA must sign before code)

### §6.1 `+1` location: engine vs sender

V2 calls `StreamEntries(fromLSN+1, ...)` — the `+1` lives at the consumer (V2 sender).

V3 has two places for `+1`:

| Option | Where | Pros | Cons |
|---|---|---|---|
| **A** — engine adds `+1` | `apply.go` populates `StartCatchUp.FromLSN = Recovery.R + 1` | engine owns the policy ("skip applied LSN"); sender stays substrate-mechanical; matches Q1 engine-owned-decisions philosophy | engine has to encode the off-by-one rule |
| **B** — sender adds `+1` | `catchup_sender.go` calls `ScanLBAs(command.FromLSN + 1, ...)` | sender semantics matches V2 (`StreamEntries(fromLSN+1, ...)`) | engine emits a "raw R" that the sender modifies; spreads the off-by-one rule |

**sw recommendation**: **Option A** (engine adds `+1`). Reasons:
1. Engine owns recovery decisions per Q1; "skip applied LSN" is a recovery decision
2. Sender stays mechanical: scan from whatever the command says, no policy
3. Pinning becomes simpler: `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` lives at one site (engine emit)
4. V2's "+1 in sender" was an artifact of V2's monolithic shipper — V3's separation lets the policy move to its right home

QA decision required pre-code.

### §6.2 `Recovery.R` source for retry re-emit

When engine retries (T4c-3 retry loop), it re-emits `StartCatchUp` with a fresh sessionID. Question: does the re-emit use `Recovery.R + 1` (the value from the original probe) or re-query somehow?

V2 has no analog (V2 reconnects + handshakes per attempt — gets fresh `replicaFlushedLSN`).

V3 options:

| Option | Behavior | Notes |
|---|---|---|
| **A** — re-emit uses original `Recovery.R + 1` | Engine reuses the probe-derived value across retries within the same Decision window | Simpler; if previous attempt advanced replica's frontier, the next attempt will harmlessly re-ship the gap (replica's apply-gate handles via per-LBA stale-skip per T4d-2) |
| **B** — engine re-probes between retries | Inserts a probe between failure and re-emit | More accurate fromLSN; more wire traffic; engine state machine more complex |

**sw recommendation**: **Option A** for T4d-3. Reasons:
1. Apply gate (T4d-2) is the correctness boundary: re-shipping the gap is safe
2. V2's per-attempt reconnect-handshake was for connection establishment, not specifically for fresh-fromLSN
3. Adding a probe between retries complicates the engine retry loop without correctness benefit
4. If the apply gate finds replica advanced, scan-time waste is limited (replica's per-LBA tracking causes immediate skip)

QA decision required pre-code.

### §6.3 Test rename for T4c-3 trailing chore

T4d kickoff §5 carries: rename or split `TestT4c3_Catchup_ShortDisconnect_DeltaOnly` per architect's PM/lease round-1 finding ("test name overclaims today; once R+1 lands, either rename to reflect the now-correct delta semantic or split").

| Option | Action |
|---|---|
| **A** — rename | `TestT4d3_Catchup_RPlus1ShortGap_DeltaOnly` reflects the now-actually-delta semantic |
| **B** — split | Keep the original as a "ship-from-genesis" test; add `TestT4d3_Catchup_RPlus1ShortGap_DeltaOnly` as the R+1-specific test |

**sw recommendation**: **Option B** (split). Both shapes are useful — the genesis-ship variant exercises the wrap-pattern (no replica state); the R+1 variant exercises the actual production path. Splitting documents the difference in the test names.

QA decision required pre-code.

---

## §7 Test parity matrix

V2 test references: V2 has no formal R+1 unit test (the behavior is exercised through `TestRebuildWALCatchUpToReplica` and full integration scenarios). V3 will pin explicitly.

| Test | Purpose | Pins which invariant |
|---|---|---|
| `TestT4d3_CatchUp_ScansFromReplicaR_NotGenesis` | Replica at R=50, primary at H=100; ScanLBAs called with `fromLSN=51`, NOT `1` | `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` |
| `TestT4d3_CatchUp_NonEmptyReplica_ShortGap_BandwidthBounded_Walstore` | Walstore substrate; use `NewObservedScanWrap`; emit count reflects R+1..target only, not 1..target. (v0.2 ADDITION 2: substrate-bound name per QA — walstore honors fromLSN; BlockStore synthesis is fromLSN-agnostic per §9 caveat) | `INV-REPL-CATCHUP-WITHIN-RETENTION-001` (T4c un-pin → T4d-3 PORTED) |
| `TestT4d3_CatchUp_NonEmptyReplica_ShortGap_BandwidthBounded_Smartwal` | Smartwal substrate; same shape as walstore variant; both honor fromLSN-driven retention | same |
| `TestT4d3_CatchUp_NonEmptyReplica_BlockStoreOverShipsExpected` | BlockStore variant — asserts the OPPOSITE: emit count = ALL stored LBAs regardless of fromLSN. Documents the test-scaffolding limitation per §9; surfaces if BlockStore synthesis ever changes (which would silently break the bandwidth narrative for production substrates) | (regression fence on §9 caveat) |
| `TestT4d3_RetryAfterReplicaAdvanced_OverScansHandledByApplyGate` | (v0.2 ADDITION 1 — pins §6.2 Option A integration assumption explicitly) Setup: catch-up attempt #1 ships entries `[R+1..K]` partially, then fails non-recycle; replica's actual R has advanced from R to K via the partial. Engine retry re-emits with **original** `Recovery.R+1` (not K+1). Attempt #2 over-ships `[R+1..K]` again. Apply gate (T4d-2) MUST per-LBA stale-skip the duplicates while still advancing `recoveryCovered`. Assert: (a) replica converges byte-exact, (b) per-LBA data NOT regressed at any LBA in `[R+1..K]`, (c) `recoveryCovered` includes `[R+1..K] ∪ [K+1..target]`. **Without this test, §6.2 Option A's "apply gate handles re-shipped gap" is asserted but not verified.** | `INV-REPL-NO-PER-LBA-DATA-REGRESSION` (T4d-2) + `INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP` (T4d-2) under retry path |
| `TestT4d3_CatchUp_Engine_RPlus1_FromOwnState_NotProbeDirectly` | Probe reports R=50; engine state updated; second probe reports R=60; engine emit uses CURRENT state R=60+1, not stale 50+1 | `INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE` |
| `TestT4d3_CatchUp_StartCatchUpSignature_FromLSNRequired` | Sender rejects calls without explicit `fromLSN` (compile fence) | signature contract |
| `TestT4d3_EngineRetryLoop_E2E_RetryUntilBudget` | Engine retry loop end-to-end through real ReplicationVolume; budget exhaustion → escalate observable | engine retry-loop wiring |
| `TestT4d3_EngineRetryLoop_E2E_RecycleEscalatesImmediate` | ErrWALRecycled bypasses retry, escalates to rebuild via real path | T4d-1 + T4d-3 chain |
| `TestComponent_WithEngineDrivenRecovery_NotStubAnymore` | Framework primitive emits real engine commands | framework primitive |
| `TestT4d3_Catchup_TransportNeverImportsSubstrateInternals` | grep `core/transport/catchup_sender.go` for `core/storage/walstore` or `core/storage/smartwal` imports; fails if present | `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY` (Q3) |
| (forward-carry from T4c-2) `TestCatchUpSender_HappyPath_WALReplay` | re-runs against R+1; must still pass | `INV-REPL-CATCHUP-CALLBACK-RETURN-NIL-CONTINUES`, lastSent-monotonic, deadline-per-call-scope |
| L2 matrix re-pass | 3 existing T4c L2 scenarios (ShortDisconnect, GapExceedsRetention, RecoveryModeLabelSurfaced) re-pass with engine-driven flow | T4c forward-carries |

---

## §8 LOC heuristic + V3 vs V2 expansion budget

Per `feedback_g1_pre_code_review.md` LOC asymmetry note: only V3 < 40-60% V2 is the red-flag direction.

| Surface | V2 LOC | V3 expected LOC (T4d-3) | Notes |
|---|---|---|---|
| Inner-loop body (`runCatchUpTo` ~67 lines) | 67 | T4c-2 already at ~120 (callback shape adds boilerplate; QA-accepted at round-37) | T4d-3 changes ~3 lines (start LSN); no body rewrite |
| Calling frame (`CatchUpTo` ~40 lines retry+escalation) | 40 | engine apply.go applySessionFailed retry block (~30 lines today) + new `+1` site at emit (~3 lines) | engine handles retry; transport doesn't grow |
| Signature ripple | n/a (V2 method on shipper) | ~5 file touches: `engine/commands.go` + `apply.go` + `adapter/executor.go` + `transport/executor.go` + test stubs (mockExecutor / closure / policy / restart / Healthy / noop / ops) | ~50 LOC mechanical |
| ReplicationVolume↔adapter wiring | n/a (V2 monolithic) | ~80-100 LOC in `volume.go` to bind adapter into the live volume's command path | V3-native; no V2 source |
| `WithEngineDrivenRecovery()` real binding | n/a | ~50 LOC in component framework | V3-native |

**Estimated total**: ~250 production + ~150 tests (matches mini-plan §2.3). No red-flag direction expected.

---

## §9 Per-substrate behavior under R+1

T4d-3 only changes the start LSN of `ScanLBAs`. Each substrate's `ScanLBAs` already exists from T4c-2. Verify no substrate-specific edge case opens up:

| Substrate | Behavior at `ScanLBAs(R+1, ...)` where R+1 > 1 | Concern? |
|---|---|---|
| **walstore** | Walks WAL entries in LSN order, filters `entry.LSN >= R+1`. Same code path as `ScanLBAs(1, ...)`; just emits fewer entries | None |
| **smartwal** | `oldestPreserved = head - capacity` if head > capacity, else 1. If `R+1 < oldestPreserved` → ErrWALRecycled (correct: gap exceeds retention). If `R+1 >= oldestPreserved` → scans normally | None — substrate handles cleanly |
| **BlockStore** | In-memory; no WAL. T4c-2 synthesized with `walHead` as scan-time LSN. Behavior at `ScanLBAs(51, ...)` when walHead=100: emits all stored LBAs at LSN=100. fromLSN=51 doesn't filter (synthesis emits at frontier-LSN, not per-LBA write-LSN) | T4d-3-specific concern: BlockStore's synthesis ALWAYS emits all LBAs regardless of fromLSN. **Test scenarios that use BlockStore + R+1 will not see bandwidth reduction** — they'll over-ship. This is a test-scaffolding limitation; production uses walstore/smartwal which honor fromLSN correctly. Document in test comments |

**Action** (v0.2 — explicit per QA ADDITION 2):
- `TestT4d3_CatchUp_NonEmptyReplica_ShortGap_BandwidthBounded_Walstore` — asserts emit count is bounded
- `TestT4d3_CatchUp_NonEmptyReplica_ShortGap_BandwidthBounded_Smartwal` — asserts emit count is bounded
- `TestT4d3_CatchUp_NonEmptyReplica_BlockStoreOverShipsExpected` — asserts emit count is UNBOUNDED on BlockStore (regression fence: documents the synthesis limitation; surfaces immediately if BlockStore behavior ever changes)

The substrate-bound naming prevents the silent-failure mode where a future "make tests run faster" refactor swaps walstore→BlockStore in the matrix and the bandwidth assertion silently passes a no-op test.

---

## §10 Predicates (must be true before sw codes T4d-3)

| Predicate | Source / receipt |
|---|---|
| T4c batch closed | `c910464a9` (architect 2026-04-25) |
| T4d kickoff §1–9 RATIFIED | architect sign 2026-04-25 |
| T4d mini-plan v0.3 ratified | architect 2026-04-25 |
| T4d-1 typed `RecoveryFailureKind` contract LANDED | (gates T4d-3 #5 retention-miss escalation port) — sw owns sequencing |
| T4d-2 apply gate LANDED | (gates T4d-3 Item C — sender-into-gate integration) — sw owns sequencing |
| QA single-sign on this G-1 | _________ pending |

**Sequencing**: T4d-3 cannot land before T4d-1 + T4d-2 because items #5 (typed kind) and Item C (apply gate) depend on them. This G-1 documents the dependency; sw schedules accordingly.

---

## §11 Sign

| Role | Signer | Date | Decision |
|---|---|---|---|
| QA Owner | Claude (QA agent) | _________ | ⏸ pending review of §3 PORT items + §4 V3-native classification + §6 placement decisions |

sw will not write T4d-3 production code until QA signs this G-1. Per mini-plan v0.3 §7.1 procedural binding: every T4d-3 PR commit references this G-1 commit hash via `Refs G-1 sign: <sha>`.

---

## §12 Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-25 | Initial G-1 V2 read v0.1 for T4d-3 (R+1 threading + engine-driven recovery wiring). Explicit V2/V3 diff per kickoff §9.5 binding: 5 PORT items (start boundary, deadline, lastSent-monotonic, retry budget, retention-miss escalation) each with V2/V3-today/V3-after table + explicit diff narration; 5 V3-native items (engine-owned retry, unified command model, lane-aware apply gate integration, BarrierResponse.AchievedLSN as completion truth, package-boundary discipline) each with "no V2 source — do NOT look for one" disclaimer. 2 NEW hidden invariants surfaced for inscription at T4d-3 close: `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` + `INV-REPL-CATCHUP-FROMLSN-NOT-FROM-PROBE-DIRECTLY` (renamed in v0.2 per nit). 3 placement decisions raised for QA pre-code sign: §6.1 `+1` location (engine vs sender; sw recommends engine), §6.2 `Recovery.R` source for retry re-emit (original vs re-probe; sw recommends original), §6.3 T4c trailing test rename/split (sw recommends split). LOC heuristic: ~250 prod expected, no red-flag direction. Test parity matrix: 8 R+1 + engine wiring tests, 1 import-discipline fence (Q3), forward-carry of T4c-2 muscle invariants under R+1. Per-substrate audit: walstore + smartwal honor fromLSN cleanly; BlockStore synthesis is fromLSN-agnostic (test scaffolding limitation, documented). Predicates: T4d-1 + T4d-2 must land before T4d-3 (dependency ordering). | sw |
| 2026-04-25 (v0.2) | QA v0.1 review absorbed: ⏸ APPROVE WITH 2 ADDITIONS + 1 nit. **§6 placement decisions all RATIFIED** (Option A engine adds +1; Option A reuse original Recovery.R+1 conditional on Addition 1; Option B split T4c trailing test). **ADDITION 1 (medium)**: §6.2 Option A's "apply gate handles re-shipped gap via per-LBA stale-skip" was asserted-not-verified. Added `TestT4d3_RetryAfterReplicaAdvanced_OverScansHandledByApplyGate` to §7 — explicit setup: catch-up #1 ships `[R+1..K]` partially then fails non-recycle; replica's actual R advanced to K; engine retry re-emits with **original** `Recovery.R+1`; attempt #2 over-ships; apply gate per-LBA stale-skips while advancing recoveryCovered; assertions cover (a) byte-exact convergence, (b) no per-LBA data regression, (c) coverage union completeness. Pins `INV-REPL-NO-PER-LBA-DATA-REGRESSION` + `INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP` under retry path. **ADDITION 2 (low)**: §7 bandwidth test split into substrate-bound names: `_Walstore` + `_Smartwal` (assert bounded) + `_BlockStoreOverShipsExpected` (regression fence asserting unbounded — documents §9 synthesis limitation; surfaces if BlockStore behavior changes). Substrate-bound naming prevents the silent-failure mode where a future test refactor swaps walstore→BlockStore and the bandwidth assertion becomes a no-op. **Nit**: `INV-REPL-CATCHUP-FROMLSN-NOT-FROM-PROBE-DIRECTLY` → `INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE` (positive form per catalogue convention; same semantic). §9 action list updated with the 3 substrate-bound test names explicitly. Test count grows from 8 to 11. | sw |
