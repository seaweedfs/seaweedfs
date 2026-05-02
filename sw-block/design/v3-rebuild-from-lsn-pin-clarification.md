# V3 Rebuild `fromLSN` Pin — Sentinel Translation Clarification

**Status**: clarification memo, hardware-validated 2026-05-02
**Anchors**: `v3-recovery-algorithm-consensus.md` §I P7 / §6.9, `recover-semantics-adjustment-plan.md` §1
**Hardware evidence**: `seaweed_block@bc4286e` g7-dual-lane on m01/M02; logs `g7-20260502T010018Z`
**Scope**: pins the rebuild path's `fromLSN` sentinel semantic; does NOT introduce new wire fields, predicates, or invariants.

---

## §1 — The gap this memo closes

The consensus is clear on `recover(a,b)` band semantics, on `fromLSN`/`pinLSN` parity (§I P7 — "transport silently overwriting `fromLSN := 0` violates parity with decision logic"), and on `Y=targetLSN` as enumerator post-G0. What it does NOT yet pin:

> **For a rebuild session, when does the engine publish `fromLSN`, and what does the transport-layer hardcode `fromLSN=0` MEAN in the absence of an engine-published value?**

Pre-fix: `transport.BlockExecutor.StartRebuild(...)` signature carries only `targetLSN`. `startRebuildDualLane` hardcoded `fromLSN=0` to the bridge. On a primary with `checkpointLSN > 0` (i.e., any production primary), `WalShipper.DrainBacklog` immediately hit the recycle gate (`fromLSN(0) <= floor(checkpointLSN)`), the wal goroutine errored before barrier emission, hardware silently stalled. **This is the exact P7 parity violation §I P7 calls out, in production.**

## §2 — Sentinel semantic (pinned)

For the current `StartRebuild(replicaID, sessionID, epoch, EV, targetLSN)` signature — engine surfaces only `targetLSN`, not `fromLSN`:

| Caller intent | Engine-published `fromLSN` | Transport translation | Receiver-visible `fromLSN` |
|---|---|---|---|
| Full rebuild (replica empty / too far behind retention) | `0` (sentinel — "primary picks the pin") | `sessionFromLSN := targetLSN` | `targetLSN` |
| Catch-up from `replicaLSN` (future — engine surfaces `fromLSN := replicaLSN`) | `replicaLSN` | passthrough — no translation | `replicaLSN` |

**Sentinel rule**: the value `0` from the engine on the `StartRebuild` path means "**rebuild — primary, pick the pin**". The pin is `targetLSN` (the frozen frontier the replica must reach; equal to primary's `H` at probe time). The base lane covers the snapshot through the pin via `WriteExtentDirect`; the WAL drain ships nothing at the pin (cursor==head), live writes during the session flow via `NotifyAppend`.

**Why `targetLSN` (not `checkpointLSN+1` or `NextLSN`)**:
- Coord constraint `fromLSN <= targetLSN` forbids `fromLSN > targetLSN`.
- `targetLSN` is the engine's frozen frontier — the receiver's `RebuildSession.targetLSN` already references this; reusing it as the pin keeps one source of truth per session.
- `targetLSN >= checkpointLSN` (engine probes after WAL is durable through `checkpointLSN`), so the recycle-gate boundary case (`fromLSN == checkpointLSN`) is covered by the `cursor >= head` shortcut in `WalShipper.DrainBacklog` — `ScanLBAs` is never called in that case, so the gate's `<=` strictness is preserved untouched.

This rule does NOT extend to catch-up. Catch-up requires the engine to surface a real `fromLSN := replicaLSN` (out of scope of this clarification; sketched in §5).

## §3 — Required transport mechanics (hardware-validated)

Three coordinated implementations satisfy the sentinel rule without violating any consensus invariant:

1. **`startRebuildDualLane`**: translate `0`-sentinel → `sessionFromLSN := targetLSN`. Pass to `bridge.StartRebuildSessionWithSink` and on to `WalShipper.StartSession`.
2. **`WalShipper.DrainBacklog`**: cursor-caught-up shortcut. If `cursor >= head` at iter start, skip `ScanLBAs` and try R1 transition directly. Hits cleanly when `cursor == fromLSN == targetLSN == primary head` (the rebuild bootstrap case); the recycle gate's `<=` strictness is preserved.
3. **`RebuildSession.SeedWalApplied(fromLSN)`**: receiver seeds `walApplied := fromLSN` at `frameSessionStart` so a base-only rebuild satisfies `TryComplete`'s `walApplied >= targetLSN` conjunct after `MarkBaseComplete` + `WitnessBarrier` — without it, base-only rebuilds (where WAL drain ships nothing) can never close. This is consistent with §6.10 frontier reconciliation: `MarkBaseComplete` advances the substrate frontier to `targetLSN`; the session's `walApplied` should reflect the same boundary the substrate just adopted.

## §4 — Known seams (out of scope; tracked as separate work)

**T4 monotonic vs. live writes during session**:
The seeded `walApplied := fromLSN` and the receiver's `appliedWalLSN := fromLSN` (T4) both equal `fromLSN`. T4 expects next live WAL frame to have `lsn == appliedWalLSN+1 == fromLSN+1`. But primary's next live write gets LSN `== primary.NextLSN()` which equals `fromLSN` (because `fromLSN == targetLSN == primary's H` at probe time, and `NextLSN := H+1`, so the FIRST new live write has `lsn == fromLSN+1` which T4 accepts ✓).

This works for a quiet primary at probe time, but if `H` advances between probe and `WalShipper.StartSession` (live writes during the dispatch race window), live writes get `lsn ∈ [fromLSN+1, ...]` — first new write still `fromLSN+1`, T4 happy. **What actually breaks** is if multiple primary writes happen DURING the session and the shipper hasn't drained them yet at `StartSession`: those queued writes get LSN `> fromLSN+1` but the shipper's cursor is at `fromLSN`, so `drive()` CASE A runs (substrate scan from cursor) — same path as the cursor-caught-up shortcut, just with non-zero scan. T4 sees the entries in LSN order, contiguous from `fromLSN+1`. ✓

The architectural seam: **wire-fromLSN** (T4 expectation; what the receiver thinks "I already have through here") and **shipper-anchor** (DrainBacklog cursor; where primary's WAL drain starts) are conflated in the current `WalShipper.StartSession(fromLSN)` API. They happen to coincide for the rebuild bootstrap case but the conflation is fragile. Splitting them is future work, gated on a real catch-up integration test.

**Hardware harness scenario #5 (concurrent writes during rebuild)** never reached this seam in the 2026-05-02 run — it failed earlier at `wait_until_rebuild_dispatched`, an unrelated probe-loop / engine retry issue (the second wipe didn't trigger a second `StartRebuild` dispatch).

## §5 — Future: engine publishes `fromLSN` for catch-up

When the engine starts surfacing `fromLSN` per the architect's catch-up semantic ("if `replicaLSN` is in primary's WAL retention, catch-up from `replicaLSN`; else rebuild"):

1. Extend `BlockExecutor.StartRebuild` signature: add `fromLSN uint64` param.
2. Engine publishes `fromLSN := replicaLSN` for catch-up, `0` (sentinel) for rebuild.
3. `startRebuildDualLane` reads the sentinel: passes `targetLSN` for `0`, passthrough otherwise.
4. The wire-fromLSN / shipper-anchor split (§4 seam) becomes load-bearing — catch-up has live writes during the session by definition.

This is a coherent next step on the same code path; the current commit pins the rebuild branch only.

## §6 — Anti-discipline notes

- This memo does NOT introduce a new completion predicate, wire field, or invariant. It clarifies the meaning of an existing transport-layer constant (`0` to bridge) so it stops violating §I P7 parity.
- The sentinel rule is BOUND TO the current `StartRebuild` signature. When the signature gets `fromLSN`, this memo's §2 table changes — the sentinel becomes a real engine choice, not a transport hardcode.
- The `cursor >= head` shortcut in `WalShipper.DrainBacklog` is NOT a relaxation of the recycle gate. The gate's `<=` strictness is preserved verbatim. The shortcut just observes that when there's nothing to scan (cursor at head), you don't need to ask the substrate at all.
- `SeedWalApplied` is NOT a new conjunct in `TryComplete`. The conjunct stays `baseDone ∧ walApplied >= targetLSN ∧ barrierWitnessed` (per A-class wave). The seed reflects "the receiver starts the session already at fromLSN by virtue of base-lane coverage" — same meaning the consensus already gave `fromLSN`.

## §7 — Receipt & cross-references

- Implementation: `seaweed_block@bc4286e fix(recovery,transport): dual-lane rebuild WAL-recycle + completion marker`
- Diagnostic instrumentation (precursor): `seaweed_block@0e3023d chore(recovery,transport): g7-debug instrumentation`
- A-class predicate replacement (predecessor; this memo's `walApplied >= targetLSN` reference): `seaweed_block@a7eb135`
- Hardware evidence: `g5-test/logs/g7-20260502T010018Z.log` (G7 #2 PASS: dispatch=1s, complete=1s, total=2s, 1000 LBAs byte-equal)
- Anchors: `v3-recovery-algorithm-consensus.md` §I P7, §6.9, §6.10; `recover-semantics-adjustment-plan.md` §1
