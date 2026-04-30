# Dual-lane recovery canonical — runbook

**Status**: paper artifact. Live execution waits for the G7-redo train (#11..#15) to merge to trunk (`main` or `phase-15`, whichever the team uses as the integration target). Architect 2026-04-29: "实测 在 主干含 train 之后". This runbook is the "你们定义的 canonical" — it ships ahead so the run is one-script-away the moment trunk includes the train.

**Companion YAML**: [`v3-recovery-dual-lane-canonical.yaml`](v3-recovery-dual-lane-canonical.yaml) (same directory)

## 1. What is "canonical"?

Architect-defined canonical = **one green end-to-end run that exercises every PR in the G7-redo train against a real binary**.

Concretely:

| PR | Behavior surfaced by canonical |
|---|---|
| **#11** (POC + memorywal) | Sender/Receiver session lifecycle — `frameSessionStart` / `frameBaseDone` / `frameBarrierResp` round-trip on the dual-lane port |
| **#12** (BaseBatchAck + pin_floor) | `frame 0x07 frameBaseBatchAck` flows from replica; `coord.SetPinFloor` advances on primary |
| **#13** (transport+cmd wiring) | `--recovery-mode=dual-lane` reaches the binary; per-replica dual-lane listener bound on `ctrl-port + 1` |
| **#14** (recycle gate consumes MinPin) | Under active session, `walstore.persistCheckpoint` clamps at `min(pin_floor_active_sessions)` — checkpoint does NOT advance past pinned LSNs |
| **#15** (PinUnderRetention bypass) | Engine retry path observably skips when `FailureKind=PinUnderRetention`; `Attempts` counter does NOT increment; `PublishDegraded` emitted instead of Start* re-emit |

One green run on **rf2-localhost** = engineering-side trust closure. One green run on **rf2-rdma** (m01 + M02 with 25 Gbps RoCE link, per memory) = production-side trust closure. **rf3-localhost** is an optional nice-to-have for multi-pin coverage.

## 2. Pre-conditions before triggering the canonical

| Check | Required state |
|---|---|
| Trunk includes #11..#15 | `git log trunk --oneline` shows commit shas of all 5 train PRs (or their squash-merge equivalents) |
| Binary rebuilt from trunk | `cmd/blockvolume --version` reports a build sha matching trunk tip; otherwise behavior under test is stale |
| `--recovery-mode=dual-lane` honored | startup log emits `"G7-redo recovery-mode=dual-lane"` and `"G7-redo-2.5 recycle gate installed"` |
| Test infra clear of prior state | no leftover `blockmaster` / `blockvolume` PIDs; `${artifact_dir}` empty so log scraping is unambiguous |
| (rf2-rdma only) RDMA link healthy | `ibstat` reports both endpoints active; `iperf` between m01 (10.0.0.1) and M02 (10.0.0.3) ≥ 20 Gbps line rate |

If any pre-condition fails: do NOT run the canonical. Fix the pre-condition first; the canonical's value is its trust signal, and a flaky pre-condition makes the signal noise instead of trust.

## 3. Topology — three variants

### 3.1 rf2-localhost

Single-host smoke. No network — both replicas on `127.0.0.1`. Useful for CI or local dev validation.

```
Host: any (developer workstation; CI runner)
Master:    127.0.0.1:7000
Primary:   data=127.0.0.1:7041  ctrl=127.0.0.1:7141  dual-lane=7142
Replica:   data=127.0.0.1:7042  ctrl=127.0.0.1:7143  dual-lane=7144
```

### 3.2 rf2-rdma — production trust variant

Two-host RDMA setup per `MEMORY.md` test infrastructure.

```
Host m01: 192.168.1.181 (RDMA 10.0.0.1, NIC rocep1s0f0)
Host M02: 192.168.1.184 (RDMA 10.0.0.3, NIC mlx5_0)
SSH user: testdev
Remote:   /opt/work/sra (existing rsync target)

Master   on m01: 192.168.1.181:7000
Primary  on m01: data=10.0.0.1:7041  ctrl=10.0.0.1:7141  dual-lane=7142
Replica  on M02: data=10.0.0.3:7042  ctrl=10.0.0.3:7143  dual-lane=7144
```

Note: dual-lane port = `ctrl-addr port + 1` per architect Option A separation. The binary's `replica_barrier.go` handles binding both `ctrl` and `ctrl+1`.

### 3.3 rf3-localhost — optional multi-pin coverage

Three replicas, single host. Useful only to demonstrate that `MinPinAcrossActiveSessions()` reports the lowest of multiple concurrent sessions' pins, NOT that recycle-gate works (which is already the rf2 demonstration).

## 4. Run procedure

For each variant in `variant_matrix`:

```bash
# 1. Build trunk binary
cd ~/work/seaweed_block
go build -o ${ARTIFACT_DIR}/bin/blockmaster ./cmd/blockmaster
go build -o ${ARTIFACT_DIR}/bin/blockvolume ./cmd/blockvolume

# 2. (rf2-rdma only) deploy to m01 + M02
ssh testdev@m01 "mkdir -p /opt/work/sra/seaweed_block/bin"
ssh testdev@M02 "mkdir -p /opt/work/sra/seaweed_block/bin"
scp ${ARTIFACT_DIR}/bin/blockmaster testdev@m01:/opt/work/sra/seaweed_block/bin/
scp ${ARTIFACT_DIR}/bin/blockvolume testdev@m01:/opt/work/sra/seaweed_block/bin/
scp ${ARTIFACT_DIR}/bin/blockvolume testdev@M02:/opt/work/sra/seaweed_block/bin/

# 3. Start packet capture on dual-lane port (audit artifact)
sudo tcpdump -i any -w ${ARTIFACT_DIR}/dual_lane.pcap port 7144 &
TCPDUMP_PID=$!

# 4. Run scenario
sw-test-runner run v3-recovery-dual-lane-canonical.yaml \
    --variant=${VARIANT} \
    --artifact-dir=${ARTIFACT_DIR} \
    --topology=${TOPOLOGY_FILE}

# 5. Stop capture, collect artifacts
sudo kill -TERM $TCPDUMP_PID
ls -la ${ARTIFACT_DIR}
```

**Capture window**: tcpdump runs across the full scenario so `dual_lane.pcap` shows the complete frame sequence — `frameSessionStart` (1) → `frameBaseBlock` (2) → `frameBaseDone` (3) → `frameWALEntry`/Kind=Backlog (4/1) → `frameBaseBatchAck` (7) interleaved → `frameWALEntry`/Kind=SessionLive (4/2) → `frameBarrierReq` (5) → `frameBarrierResp` (6). The pcap is the audit trail when the test report is the question.

## 5. Pass / fail decision tree

```
All YAML phases PASS (excluding optional bypass phase)?
├─ YES: variant green for the run
│       ├─ rf2-localhost green only:
│       │  → engineering trust signal CLOSED. Architect notification: "canonical green on rf2-localhost; rf2-rdma scheduled."
│       ├─ rf2-localhost + rf2-rdma green:
│       │  → production trust signal CLOSED. Architect notification: "G7-redo train trust closure complete."
│       └─ + rf3-localhost green:
│          → multi-pin coverage demonstrated. Optional artifact for §3.2 #3 kickoff context.
└─ NO: variant FAILED
        ├─ Investigate per phase that failed (logs + pcap + metrics timeline)
        ├─ DO NOT cherry-pick "the bug" to a fix branch and re-run on the same trunk —
        │  the canonical is the trust signal; flake-on-bug-fix-on-flake spirals undermine it.
        ├─ File a bug PR targeting trunk (or one of the train PRs if pre-merge).
        └─ Re-run canonical clean on the bug-fixed trunk.
```

**The canonical is binary** — green or not green. There is no "mostly green". A flake on phase 4 (`observe_pin_advancement`) is a real signal; do not silently rerun.

## 6. Optional bypass phase substitution

The phase `observe_retry_bypass_pin_under_retention` requires triggering a `PinUnderRetention` mid-session, which naturally requires retention to advance past a still-active pin. Two options:

### 6.a Live-injection path (preferred when fault injection exists)

Add a fault-injection flag to `cmd/blockvolume`: `--inject-recycle-bypass-pin-under-retention=true`. The flag patches `walstore.persistCheckpoint` to skip its gate check exactly once during a session, forcing the next `BaseBatchAck` to land below the (advanced) S boundary, generating a typed `recovery.FailurePinUnderRetention`. The canonical asserts the engine bypasses retry as documented.

This requires a small **dev-only** wiring patch in the binary (gated behind a flag that defaults to off; never compiled into release binaries). Acceptable for canonical exercise; not acceptable as a long-term injection point.

### 6.b Unit-test substitution (zero binary patching)

Skip the live phase and pin the bypass behavior to the unit tests:

| Test | Pinning |
|---|---|
| `TestT4d_PinUnderRetention_BypassesRetryBudget` (`core/engine/recovery_pin_retention_test.go`) | Engine retry path: Attempts unchanged, no Start* re-emit, PublishDegraded emitted. |
| `TestT4d_PinUnderRetention_AtBudgetEdge_DoesNotEscalate` (same) | Counter-pin against round-47 path. |
| `TestClassifier_RecoveryPinUnderRetention_MapsToEngineKind` (`core/transport/classifier_pin_under_retention_test.go`) | Boundary mapping closes silent fall-through. |

The architect's "trust signal" framing ratifies this substitution: canonical = the train's behavior demonstrably correct on real binary; the bypass phase is one node in that train and its mechanism is fully unit-pinned.

**Decision**: rf2-localhost runs the bypass phase via 6.a if the fault-injection flag landed; otherwise substitutes 6.b. rf2-rdma always substitutes 6.b (no fault injection on the trust-signal infra). rf3-localhost is identical to rf2-localhost.

## 7. After the canonical lands green

| Trigger | Action |
|---|---|
| rf2-localhost green | Notify architect; flip the architect priority list status — train trust signal CLOSED at engineering side. |
| rf2-rdma green | Notify architect; production trust signal CLOSED. Eligible to flip `--recovery-mode` default from `legacy` to `dual-lane` per Open Question 3 of the wiring plan (separate one-line PR). |
| Default flip merged | After 1 release cycle (per architect Open Question 4), legacy code path eligible for removal (`g7-redo/legacy-removal` branch). |
| Both variants green + default flipped | Engineering & ops both satisfied; G7-redo train fully delivered. §3.2 #3 implementation kickoff is the next binding action. |

## 8. What this runbook does NOT cover

- **§3.2 #3 (single-queue / sliding-window) coverage** — the canonical exercises the *current* dual-lane shape (sequential phases with Backlog→SessionLive transition gated by closeCh). After §3.2 #3 lands the gate is gone but **wire format is byte-identical**; the canonical re-runs without modification. Per [`v3-recovery-unified-wal-stream-kickoff.md`](v3-recovery-unified-wal-stream-kickoff.md) §4: only the kind byte's transition rule changes, not the framing.
- **Option B retry policy** — Option A (this train's PR #15) covers PinUnderRetention only. Option B (per-failure-kind budget + lossless recovery↔engine mapping + observability + #3 backoff) is a separate PR with its own canonical-prep cycle.
- **Long-running soak** — canonical is one full session. Long-running stability is operator-territory, scheduled separately on the production trust infrastructure.
- **Multi-volume / topology stress** — canonical runs against a single volume `v1`. Multi-volume stress is a separate hardening item.

## 9. Open questions for architect

| Q | Status |
|---|---|
| Is the architect's "你们定义的 canonical" satisfied by this scope, or should the bypass phase be live-only (no 6.b substitution)? | **Open** — assumes 6.b acceptable per architect's "测试一次, in 二进制" lower-bound framing |
| Does fault-injection-via-binary-flag (6.a) need a separate kickoff before adding to `cmd/blockvolume`? | **Open** — flag is dev-only / off-by-default; minimal blast radius. Architect ratification needed before binary patch lands. |
| Does rf3-localhost run as part of the canonical-green gate, or always optional? | **Provisionally optional** — runbook §1 treats it as nice-to-have; architect can promote to gate if multi-pin behavior needs hard pin |
