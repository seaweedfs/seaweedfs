# V3 Phase 15 — G5-5 (m01 Hardware First-Light + L3 Integration) Mini-Plan

**Date**: 2026-04-26 (v0.3 — §2 acceptance criteria rewritten per architect REVISE round 51-followup) / **§close appended 2026-04-27 — awaiting architect single-sign**
**Status**: §close submitted; awaiting architect single-sign per `v3-batch-process.md §5` + §8C.2 (round 14 close decision: 3 of 4 verify steps GREEN on m01 hardware; #4 → G5-5C carry-forward, architect-bound 2026-04-27)
**Owner**: sw (script + Go driver); QA (m01 verification + scenario authoring)
**Process**: First trial of compressed `v3-batch-process.md` (one doc, §close appended at batch close)
**Predecessors**: G5-4 closed (`seaweed_block@c820e17` + `seaweedfs@36ba7b44e`); 5 INV-BIN-WIRING-* invariants ACTIVE in ledger; `--expected-slots-per-volume` flag landed (`f5de7c5`)

---

## §1 Scope

G5-5 promotes G5-4's L1 (binary composition) result to L3 (Replicated IO) on real hardware via m01+M02. This is the first batch where a real frontend write moves through the production binary code path, lands on a remote replica via TCP, and is verified byte-equal.

### Architecture touchpoints

- `v3-architecture.md §6.1` Write Path — frontend write → projection check → local durable append → replication fan-out → host ack
- `v3-architecture.md §6.3` Replication Path — primary local write → `ReplicationVolume.Observe` → `ReplicaPeer` → `BlockExecutor` dials peer DataAddr → `ReplicaListener` accepts → apply gate dispatches → substrate applies
- `v3-architecture.md §7` Recovery Architecture — short disconnect → engine-driven catch-up within retention
- `v3-architecture.md §13` Product Completion Ladder — G5-4 reached L1; G5-5 targets L3 (and edges into L4 for the disconnect scenario)

### What G5-5 delivers

| # | Item | Verifier |
|---|---|---|
| 1 | `scripts/iterate-m01-replicated-write.sh` — orchestration script that builds binaries on m01, deploys via SMB share, starts blockmaster + 2× blockvolume cluster, drives a kernel iSCSI write workload from m01, verifies replica's underlying storage matches primary | Manual run on m01 + log artifacts |
| 2 | Real frontend write byte-equal verification — primary's iSCSI target accepts a kernel write from m01; the same bytes land in replica's walstore on M02; verified by opening the replica's `storage.LogicalStorage` via the same `storage/walstore` package the daemon uses and calling `Read(lba)` on the targeted LBA range. **Architect REVISE binding round 51**: raw `.extent` byte peek is REJECTED — walstore on-disk layout includes WAL frames + checkpoint metadata + sparse regions + potentially-stale-but-valid blocks; only the storage abstraction's `Read(lba)` returns the authoritative current value for a given LBA. The verifier loads the same package as the daemon, opens the substrate (read-only), invokes `Read(lba)`, and SHA-256-compares against the kernel write payload. | Step in #1 script + Go assertion helper using `storage/walstore.OpenReadOnly` (or equivalent — sw confirms exact API at G-implementation time and adds it if missing) |
| 3 | Mid-stream **network disconnect** + catch-up scenario — iptables drops primary↔replica WAL data port mid-write; replica falls behind; iptables restored; verify replica catches up via T4d engine-driven recovery (catch-up path, NOT rebuild — gap stays within walstore retention). Proves: live TCP interruption + recovery without process restart. **R/H observation source (architect REVISE binding round 51)**: the existing `/status?volume=v1` endpoint returns `frontend.Projection` (VolumeID, ReplicaID, Epoch, EndpointVersion, Healthy) — NO R/S/H/Mode/Decision. G5-5 adds a NEW endpoint `/status/recovery?volume=v1` that returns `engine.ReplicaProjection` (Mode, RecoveryDecision, R, S, H, Reason, etc.). The endpoint is gated by a NEW `--status-recovery` daemon flag (default off; production binaries do not enable it; m01 script passes it). Verifier polls primary's `/status/recovery` for `H` and replica's `/status/recovery` for `R` until `R == primaryH` within deadline. **Instrumentation change scope**: ~30 LOC in `core/host/volume/status_server.go` + 1 CLI flag in `cmd/blockvolume/main.go`. NOT a production behavior change — the new endpoint is opt-in and the engine projection it surfaces already exists (just wasn't exposed via HTTP). | Step in #1 script + new `/status/recovery` endpoint |
| 4 | **Replica process stop/restart** + catch-up scenario — `SIGTERM` the replica's `blockvolume` process mid-write (clean shutdown); restart the same binary against the same `--durable-root`; verify replica reopens durable storage, resubscribes to master, and catches up via T4d engine-driven recovery. Proves: durable reopen + master resubscribe + recovery reconstruction (architect REVISE binding round 51 — distinct from #3 network-only path). | Step in #1 script |
| 5 | m01 `-race ×10` verification of G5-4's integration test (`TestG54_BinaryWiring_RoleSplit_2NodeSmoke`) — Windows can't run `-race` (CGO/gcc); m01 is the verification environment | Step in #1 script |

### What G5-5 does NOT deliver (explicit non-claims)

- **No rebuild path on m01.** Catch-up within retention only (#3). Rebuild scenario (replica beyond retention → full extent transfer) defers to a separate batch — needs scrub state + WAL truncation timing that adds scope.
- **No NVMe target verification.** iSCSI is the kernel-target choice for #2 (existing m01 tooling per memory). NVMe-over-TCP target exists in V3 but kernel client setup on m01 isn't verified; defers to a follow-up batch.
- **No durability mode coverage.** ReplicationVolume defaults to `BestEffort`; this batch verifies the WIRE PATH, not the durability semantics. `sync_all` / `sync_quorum` mode coverage defers to G5-2 / G5-6.
- **No failover.** Primary stays primary throughout. Failover (kill primary → replica promotes) is G8 scope.
- **No failure-injection at backend layer.** All faults are network-layer (iptables) — backend-injection (disk full, walstore corruption) defers to fault-scenario batches.

### Files

| File | Change | LOC |
|---|---|---|
| `scripts/iterate-m01-replicated-write.sh` (new, in `seaweed_block`) | Orchestration script: build/deploy/start cluster/drive workload/verify-byte-equal/disconnect-network/verify-network-catchup/restart-replica/verify-restart-catchup/race-stress/cleanup | ~280 |
| `cmd/blockvolume/m01_verify_helper.go` (new, build-tag `m01verify`) | Go helper for storage-aware byte-equal: opens replica's walstore via `core/storage/walstore.OpenReadOnly` (or equivalent — sw confirms exact API at impl time and adds it if missing), invokes `Read(lba)` for each LBA in the verified range, SHA-256 vs primary's known payload, prints `OK` / `MISMATCH lba=N exp=... got=...` to stdout for the script to grep. NOT raw extent peek. | ~100 |
| `core/host/volume/status_server.go` + `cmd/blockvolume/main.go` | Add `/status/recovery?volume=v1` endpoint returning `engine.ReplicaProjection` (Mode, R, S, H, RecoveryDecision); gated by new `--status-recovery` flag (default off). Required for §2 #3 named R/H observation source. | ~30 prod + ~30 unit test |
| `sw-block/design/v3-phase-15-g5-5-mini-plan.md` (this doc) | §close appended at batch close | ~280 + §close |

### Architecture truth-domain check (v3-architecture.md §4)

No new truth-domain owner. G5-5 verifies the existing truth-domain map (§4) at L3 scope — same owners as G5-4, just exercised end-to-end through real hardware.

---

## §2 Acceptance criteria

| # | Criterion | Verifier |
|---|---|---|
| 1 | `iterate-m01-replicated-write.sh` runs end-to-end on m01 cleanly: blockmaster + 2× blockvolume start, both reach role-appropriate ready state | Script step `verify_cluster_ready` exits 0 |
| 2 | Kernel iSCSI write from m01 → primary's iSCSI target → replication fan-out → byte-equal on replica's storage. Verifier opens replica's walstore via `core/storage/walstore.OpenReadOnly` (or equivalent — sw confirms exact API at impl time and adds it if missing) and calls `storage.LogicalStorage.Read(lba)` over the target LBA range; SHA-256-compares per-LBA bytes against primary's known write payload. **No raw `.extent` peek; storage abstraction is the only authoritative read path.** | Script step `verify_byte_equal` invokes the `m01verify` Go helper which uses `walstore.OpenReadOnly` + `Read(lba)` per LBA + SHA-256 compare; helper exits 0 / prints `OK` on equality, exits 1 / prints `MISMATCH lba=N exp=... got=...` on diff |
| 3 | iptables drops primary→replica WAL data port during a write burst → replica lags → iptables restored → replica catches up to primary's H within 30s via T4d engine-driven recovery (catch-up path within walstore retention; NOT rebuild). **R/H observation source: NEW `/status/recovery?volume=v1` endpoint** returning `engine.ReplicaProjection` (Mode, RecoveryDecision, R, S, H, Reason); gated by NEW `--status-recovery` daemon flag (default off; m01 script enables on both nodes). | Script step `verify_network_catchup` polls primary's `/status/recovery?volume=v1` for `H` and replica's `/status/recovery?volume=v1` for `R` until `R >= primary.H` within 30s deadline; asserts `RecoveryDecision == "catch_up"` (not "rebuild") at the catch-up trigger |
| 4 | Replica process stop/restart + catch-up — `SIGTERM` the replica's `blockvolume` process mid-write (clean shutdown); restart the SAME binary against the SAME `--durable-root`; verify replica reopens durable storage, resubscribes to master, catches up via T4d engine-driven recovery. Proves: durable reopen + master resubscribe + recovery reconstruction. **R/H observation source: same `/status/recovery?volume=v1` endpoint as #3.** | Script step `verify_restart_catchup` SIGTERMs replica process, waits for clean shutdown (process exit), restarts the binary with identical CLI flags + same `--durable-root`, then polls `/status/recovery?volume=v1` for `R >= primary.H` within 30s deadline; asserts replica's status returns `Healthy=false` at restart (not yet caught up) and `RecoveryDecision == "catch_up"` (not "rebuild") |
| 5 | `TestG54_BinaryWiring_RoleSplit_2NodeSmoke` runs 10× under `-race` on m01 with no flake | Script step `verify_race_stress` runs `CGO_ENABLED=1 go test -race -count=10 -run TestG54_BinaryWiring ./cmd/blockvolume/` |
| 6 | No regressions in V3 suite | Script step `verify_full_suite` runs `go test ./... -count=1 -timeout 600s` from m01; all packages green |
| 7 | `v3-dev-roadmap.md` updated to reflect G5-5 close + L3 reached per `v3-architecture.md §13` | sw + QA at §close (per `v3-batch-process.md §8` — gate-close trigger) |

### Architect review checklist (§12) coverage

| Check | This batch's answer |
|---|---|
| Scope truth | Done: end-to-end byte-equal write + short-disconnect catch-up at L3 on m01 hardware. Not done: rebuild scenario, NVMe target, durability modes, failover. Product risk: catch-up within retention is verified; gap-beyond-retention rebuild path stays unverified at L3 (verified at L1 component scope only). |
| V2/new-build decision | New build — script + Go helper + opt-in instrumentation endpoint. NO V2 muscle PORT (orchestration is shell + Go test). G-1 NOT required (per `v3-batch-process.md §6.1`). |
| Engine/adapter impact | Zero engine/adapter logic change. G5-5 surfaces existing `engine.ReplicaProjection` (R/S/H/Mode/Decision — already computed by engine, just not exposed via HTTP) through a new opt-in `/status/recovery` endpoint. If implementation discovers an engine/adapter change is needed (e.g., projection field is missing), STOP and re-ratify per architect binding round 51. |
| Product usability level | Operator can run a real iSCSI volume across 2 nodes; writes survive a transient network blip via catch-up. Reaches L3 (Replicated IO) per `v3-architecture.md §13`. Falls short of L4 (rebuild path) and L5 (CSI/API operator surface). |

---

## §3 Invariants to inscribe at close

G5-5 verifies existing invariants on real hardware; it does NOT inscribe new INVs (verification batches don't add invariants — they upgrade existing ones from "component-only" to "Integration-verified" status in the ledger).

**Invariants whose ledger row updated at G5-5 §close (landed 2026-04-27):**

| INV ID | Ledger row update | Status |
|---|---|---|
| `INV-BIN-WIRING-ROLE-FROM-ASSIGNMENT` | `Last verified` → 2026-04-27 (G5-5 §close — Tier 2 m01 cross-node hardware Integration backstop: `seaweed_block@5c4718f` rounds 1-14) | ✅ ledger updated |
| `INV-BIN-WIRING-PEER-SET-FROM-ASSIGNMENT-FACT` | Same | ✅ ledger updated |
| `INV-BIN-WIRING-LISTENER-LIFECYCLE-LIFO` | Same | ✅ ledger updated |
| `INV-BIN-WIRING-ASSIGNMENT-DRIVES-MEMBERPRESENT` | Same | ✅ ledger updated |
| `INV-BIN-WIRING-SESSIONID-VIA-ADAPTER` | Same | ✅ ledger updated |
| `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` | m01 #3 network-disconnect catch-up exercises path; ledger pointer addition deferred to G5-5C close (where the matching test rerun lands as Integration evidence) | ⏳ G5-5C |
| `INV-REPL-LSN-ORDER-FANOUT-001` (T4a-4) | m01 #2 byte-equal write exercises path; ledger pointer addition deferred to G5-5C close to package both #2 + #4 evidence as one Integration row update | ⏳ G5-5C |

**New INV considered + rejected:** none — verification batches don't introduce new claims. G5-5 surfaces a real product finding (peer-recovery trigger after replica restart) which becomes G5-5C scope; the corresponding INV (engine-driven peer recovery re-trigger) is authored at G5-5C close, not here.

---

## §4 G-1 V2 read

**N/A.** Per `v3-batch-process.md §6.1`: G-1 required only for V2 muscle PORT batches. G5-5 is verification + bash + Go helper — no V2 muscle source. Skipping G-1.

---

## §5 Forward-carry consumed (from G5-4 §close)

G5-4 explicitly deferred to G5-5:

| G5-4 criterion | G5-5 absorption |
|---|---|
| #3 Byte-equal primary→replica via real frontend write | G5-5 §2 #2 (storage-aware verifier per architect REVISE binding round 51) |
| #4 Stop-restart catch-up convergence | **Split per architect REVISE binding round 51**: network disconnect proves live TCP interruption + recovery (G5-5 §2 #3 — **GREEN on m01 hardware** at round 14, `seaweed_block@5c4718f`); process restart proves durable reopen + master resubscribe + recovery reconstruction (G5-5 §2 #4 — **FAILED on m01 hardware**: replica's LBA[2] does not converge in 30s after restart; primary's `gate-degraded` rejects ships without retry, no runtime trigger re-establishes the peer). **Partially consumed**: network-disconnect path absorbed by G5-5 #3; process-restart path **carries forward to G5-5C** per architect ruling 2026-04-27 (real recovery-path finding, not test flaw). |
| #6 10× `-race` stress on G5-4 integration test | G5-5 §2 #5 |

Plus the broader G5-4 wiring claim — that the binary-level T4 replication stack composes — gets m01 hardware verification at G5-5 §2 #1 (cluster reaches role-appropriate ready state on real hardware, not subprocess-only).

---

## §6 Risks + mitigations

| Risk | Mitigation |
|---|---|
| iSCSI kernel target setup on m01 is fragile (existing memory: `--iscsi-listen` + open-iscsi initiator on m01) | Reuse m01's existing iSCSI tooling per memory; `iterate-m01-replicated-write.sh` documents the exact `iscsiadm` invocations. If setup fails, script exits with named error; doesn't continue silently. |
| iptables disconnect window timing is racy (too short → no observable lag; too long → exceeds walstore retention → escalates to rebuild instead of catch-up) | Hard-coded conservative window (3s drop with 256-block walstore + low-rate write should keep gap within retention). Script asserts catch-up (NOT rebuild) via engine projection field. If rebuild emits, script fails fast with clear message — surfaces as a script-tuning fix, not a bug claim. |
| SMB share binary deployment race (already-running binary holds .exe open; rebuild fails) | Script kills processes BEFORE rebuild. Idempotent: re-run from any state. |
| `-race` on m01 needs `CGO_ENABLED=1` + `gcc` installed | Memory: m01 has gcc per RDMA build chain. Script asserts `which gcc` succeeds before -race step. |
| Test-only `m01_verify_helper.go` accidentally compiles into production binary | Guarded by `//go:build m01verify` build tag; default builds exclude it. Production binary unchanged. |
| `--status-recovery` endpoint accidentally enabled in production | Default OFF; flag must be explicitly passed. Endpoint is loopback-only (same `isLoopbackRemote` guard the existing `/status` uses). Documented in mini-plan as test-only. Architect-binding scope: instrumentation surface, not engine/adapter change. |
| Replica restart scenario races against in-flight writes (replica down before primary's write commits → primary times out vs. continues; recovery test is non-deterministic) | Script throttles writes between restart events; each phase has a clear quiesce step before assertions. If non-determinism appears, script fails fast with named diagnostic, not silently continues. |
| Storage-aware verifier needs `walstore.OpenReadOnly` (or equivalent) which may not exist | sw confirms exact API at impl time; if missing, ADD it with godoc + 1 unit test as part of this batch (small scope expansion contained in `core/storage/walstore`). NOT a substrate semantic change — read-only opener for verification only. |

---

## §7 Sign table

| Stage | Signer | When | Status |
|---|---|---|---|
| §1-§6 ratification | architect (pingqiu) + QA review (§12 checklist) | v0.3 RATIFIED 2026-04-26 (round 51, G-1 ceremony dropped) | ✅ |
| Code start (G5-5.1-5) | sw | After §1-§6 ratification | ✅ landed at `seaweed_block@2745cf4` and follow-up commits |
| m01 hardware verification | sw self-iteration (architect-approved 2026-04-27 trial mode) | rounds 1-14, surfaced 14 bugs + 1 product finding | ✅ rounds complete; #1+#2+#3 GREEN |
| §close append | sw drafts; QA verifies evidence | This commit | ✅ submitted |
| §close architect single-sign | architect (pingqiu) | After this submission | ⏳ pending |
| Code start (script + Go helper) | sw | After §1-§6 ratification | ⏳ blocked on ratification |
| m01 hardware verification run | QA | After sw lands script + helper | ⏳ blocked |
| §close append + close sign | sw drafts §close; QA verifies evidence; architect single-sign per `v3-batch-process.md §5` | After m01 verification | ⏳ blocked |

---

## §close

**Date**: 2026-04-27
**Status**: ⏳ awaiting architect single-sign per `v3-batch-process.md §5` / §8C.2
**Result**: 3 of 4 verify steps GREEN on m01 hardware; 1 surfaces a real recovery-path finding deferred to a named follow-up batch.

```
Done:
  - #1 verify_cluster_ready (role split confirmed cross-node)
  - #2 verify_byte_equal — live iSCSI write replicated end-to-end
    on real m01+M02 cross-host TCP, byte-equal verified by
    storage-aware m01verify (LBA[0]=0xab matches on replica
    walstore via storage.OpenReadOnly + LogicalStorage.Read)
  - #3 verify_network_catchup — iptables OUTPUT drop + write
    during partition + heal + replica converges to byte-equal
    LBA[1]=0xcd in 8s via engine-driven catch-up
  - 14 distinct bugs surfaced and fixed across rounds 1-14 (full
    ledger in §close.evidence.bugs)
  - 5 INV-BIN-WIRING-* invariants in v3-invariant-ledger.md from
    G5-4 close still load-bearing; G5-5 hardware run is the
    Integration evidence backstop for those rows

Not done:
  - #4 verify_restart_catchup — kill replica + write while down
    + restart same --durable-root: replica's LBA[2]=0xef does
    NOT converge within 30s. Per architect ruling 2 (round 13):
    'run #4 naively; if it does not converge, that is a real
    recovery-path finding to surface, not a test flaw.'
    Surfaces as carry-forward G5-5C (architect ratification
    2026-04-27).
  - #5 verify_race_stress + #6 verify_full_suite — depend on
    cluster up across multi-step; gated on #4 fix or test
    sequencing rework.

Product level reached:
  - L3 (Replicated IO) per v3-architecture.md §13 — proven by
    #2 + #3 (live replicated write + network-blip catch-up
    convergence on real cross-host hardware).
  - Falls short of full L4 (Failure/recovery under IO) — the
    process-restart recovery path (peer-recovery-after-restart
    trigger) is the gap, scoped as G5-5C.

Next gate that makes it usable:
  - G5-5C Peer Recovery Trigger After Replica Restart — fix
    the engine-driven catch-up re-trigger so a degraded peer
    that becomes reachable again (replica restart) replays
    missed LSNs without manual intervention. After G5-5C
    closes: re-run #4 + #5 + #6 in this same harness; full
    L4 is reached.
```

### §close.deltas vs §1-§6

- §1 file map: ALL planned files landed plus 4 unplanned
  instrumentation passes (rounds 7-9-10-12 added INFO logs at
  iSCSI handler + StorageBackend + observer dispatch +
  ReplicationVolume.OnLocalWrite + ReplicaPeer.ShipEntry/Barrier
  + transport.Ship lazy-dial). Beneficial side-effect of the
  diagnostic iteration; production-acceptable verbosity.
- §1.5 listener bind addr corrected: mini-plan v0.4 said
  `--ctrl-addr` reuse; round-12 hardware found `--data-addr` is
  the correct binding (transport executor dials peer.DataAddr).
  Fixed in code; documented in PR. Not a scope change — same
  semantic surface, corrected addr field.
- §2 acceptance #2 verifier wording: m01verify reads via
  walstore.OpenReadOnly per architect REVISE round 51 — landed
  exactly as specified.
- §2 acceptance #4: status changed mid-batch from "GREEN expected"
  to "real recovery-path finding". Architect ruling 2026-04-27
  ratifies as carry-forward, not failure.
- §3 invariants: no new INV inscribed for G5-5 (verification
  batch); existing INV-BIN-WIRING-* rows updated with
  Last verified=2026-04-27 (Tier 2 m01 cross-node).
- §6 risks: pre-flight iptables cleanup added to
  start_cluster() (test-infra hardening, architect-approved
  2026-04-27); not product scope expansion.

### §close.evidence

**Commits (chronological):**

| Commit | Round | Content |
|---|---|---|
| `f5de7c5` | pre-G5-5 | blockmaster `--expected-slots-per-volume` flag |
| `c820e17` | G5-4 | Binary T4 replication wiring (foundation for G5-5) |
| `36ba7b44e` (seaweedfs) | G5-4 | 5 INV-BIN-WIRING-* in ledger |
| `2745cf4` | G5-5.1-5 initial | /status/recovery + walstore.OpenReadOnly + m01verify + script |
| `64c7899` | round 52 | walstore_readonly no-goroutine fix + script fail-closed |
| `b9eb82c` | round 53 | LOCAL_MODE Tier 1 |
| `1f7f472` | round 53 | SSH `-T -n` + timeout (Windows Git Bash hang fix) |
| `52565ff` | round 53 | PREBUILT_BIN_DIR + SRC_DIR safety guard |
| `f078096` | round 53 | bash -s heredoc launch (line-collapse fix) |
| `fb30b90` | round 53 | drop -n from launch ssh (bash -s stdin conflict) |
| `7233cb2` | round 54 | bind+advertise routable IP |
| `c995d74` | G5-5A | master collectPeers observation fallback |
| `ba8abf0` | round 54 follow-ups | freshness gate + lineage rule + comment |
| `beedcc2` | round 54 | INFO logs on UpdateReplicaSet success |
| `c2a2418` | round 53 | ship/dial/barrier instrumentation |
| `1277a10` | round 54 | upstream observer + OnLocalWrite + gate-close instrumentation |
| `003daa2` | round 54 | SCSI handler + StorageBackend.Write entry instrumentation |
| `2b76afc` | round 54 | StorageBackend.SetIdentity round 1 (all-fields-zero check) |
| `7d19cbe` | round 11 | SetIdentity v2 — latch on Epoch=0 (round 1 was no-op) |
| `5c66466` | round 12 | drop H-advance preflight (engine semantic mismatch) |
| `8f7b9cc` | round 12B | walstore filename `v1.bin` not `v1.walstore` |
| `7e3e6d5` | round 12C | dd 0xab fill via python3 (tr lacks `\xHH` escape) — **#2 GREEN** |
| `530af8d` | round 13 | rewrite #3/#4 to byte-equal pattern + helpers |
| `5c4718f` | round 14 | pre-flight stale-iptables cleanup — **#3 GREEN** + **#4 surfaces real finding** |

**m01 hardware artifacts (V:/share/g5-test/logs/):**
- `iterate-20260427T084120Z.log` — #2 GREEN evidence (round 12 part C)
- `iterate-20260427T091245Z.log` — #1+#2+#3 GREEN, #4 surfaces finding (round 14)
- `artifacts-20260427T091245Z/{master,primary,replica}-fail.log` — diagnostic dump

**Production code change footprint** (excluding script + tests):
- `core/host/master/services.go`: G5-5A peer-set construction (path 1 + path 2 fallback to ObservationStore.SlotFact, subscriber-stamped Epoch/EV)
- `core/authority/observation_store.go`: SlotFact accessor with FreshnessConfig gate
- `core/frontend/durable/storage_adapter.go`: SetIdentity(latch-from-Epoch=0) + observer-dispatch INFO logs
- `core/frontend/durable/provider.go`: dp.Open auto-latches Identity post-waitHealthy
- `core/host/volume/host.go`: UpdateReplicaSet success + adopted-as-PRIMARY INFO logs
- `core/host/volume/status_server.go`: /status/recovery endpoint (opt-in)
- `core/host/volume/projection_bridge.go`: AdapterProjectionView.EngineProjection accessor
- `core/storage/walstore_readonly.go`: NEW — OpenReadOnly + WALStoreReader
- `cmd/blockmaster/main.go`: `--expected-slots-per-volume` flag (G5-5 prereq)
- `cmd/blockvolume/main.go`: `--status-recovery` flag + dp.EnsureStorage + ReplicationVolume + ReplicaListener wiring
- `cmd/m01verify/main.go`: NEW — standalone byte-equal verifier (build-tag m01verify)
- `core/replication/peer.go`: ShipEntry + Barrier instrumentation
- `core/transport/ship_sender.go`: lazy-dial INFO log
- `core/frontend/iscsi/scsi.go`: SCSI WRITE/SYNCHRONIZE_CACHE handler INFO logs

### §close.evidence.bugs (round-by-round)

14 bugs surfaced + fixed during m01 self-iteration:

1. (round 6) Replica role-split design verification (G5-4 expected behavior)
2. (round 53) SSH hang from Windows Git Bash — `-T -n` flags
3. (round 53) iSCSI 0.0.0.0 listen rejection (loopback-only enforcement)
4. (round 53) Script remote bash silent failure (`set -euo pipefail`)
5. (round 53) tar root filesystem hazard (SRC_DIR guard)
6. (round 53) Backslash-newline collapse in SSH heredoc
7. (round 53) `-n` vs `bash -s` stdin conflict (split SSH_OPTS)
8. (round 54) 0.0.0.0 bind/advertise vs routable IP
9. (round 54) Master collectPeers misses observation-only replicas (G5-5A)
10. (round 11) SetIdentity all-zero check too strict (latch never fired)
11. (round 12) walstore filename mismatch (`.walstore` vs `.bin`)
12. (round 12) `tr "\0" "\xab"` doesn't produce 0xab (tr escape gap)
13. (round 12) H-advance preflight false positive (engine state semantic)
14. (round 14) Stale iptables INPUT DROP rule poisoning all subsequent runs

### §close.evidence.findings

1 real product finding (architectural, NOT script-level):

**G5-5C carry: peer recovery trigger after replica restart.** Engine-driven catch-up primitives exist (T4d-4) but no runtime trigger fires when a degraded peer becomes reachable again (e.g., after replica restart). Primary's `gate-degraded` rejects ships without retry; barrier acks at stale `achievedLSN`; replica never receives missed LSNs. Carry-forward to a dedicated mini-plan per architect ruling 2026-04-27.

### §close.forward-carries

To **G5-5C** (named follow-up, architect-bound 2026-04-27):
- Reuse existing engine-driven recovery primitives (T4d-4); do not invent ad-hoc re-ship from replication layer.
- Define the trigger source first: observation reappearance, periodic probe loop, or stream/transport reconnect signal.
- Pass criterion: exactly the failed hardware case from G5-5 #4 — kill replica, write while down, restart same `--durable-root`, wait, verify LBA byte-equal.
- Seed evidence: `seaweed_block@5c4718f` primary-fail.log shows the gate-degraded + stale-barrier-ack pattern.

To **future hardening** (no specific gate, opportunistic):
- Component-scope test for `EnsureStorage → assignment-arrives → first-Open` Identity-latch path (would have caught the round-10/11 bug pre-m01)
- Test-infra discipline: `start_cluster()` pre-flight cleanup pattern (iptables, residual files, leftover sessions) generalized across future hardware harnesses

To **G5-6** (G5-DECISION-001 close):
- Engine-state serializability (TestG5Decision001_ReplicaState_RoundTripJSON pinned in T4d) survives G5-5; Path A vs Path B decision still open at G5-6 close.

### §close.architect-review-checklist (`v3-batch-process.md §12`)

| Check | Answer |
|---|---|
| Scope truth | Done: #1+#2+#3 GREEN on real hardware. Not done: #4 (carry G5-5C) + #5+#6 (gated on #4 or sequencing rework). Product risk: replica-restart recovery is L3-/L4-blocking until G5-5C lands. |
| V2 / new-build decision | Pure verification + new instrumentation + small architectural fixes (G5-5A, SetIdentity latch). No V2 muscle PORT involved. G-1 correctly N/A. |
| Engine / adapter impact | Zero engine/adapter logic change. master/observation peer-set construction (G5-5A) + frontend backend Identity latch (G5-5) — both covered by architect rulings 2026-04-26 (G5-5A round 54) and implicitly by SetIdentity test pin. |
| Product usability level | Operator can run a 2-node cluster, write via iSCSI, get the data on the replica, survive a network blip, and observe convergence. Cannot yet survive a replica process restart with auto-recovery. L3 reached; full L4 awaits G5-5C. |

---

## Next actions (post-§close)

| Agent | Action |
|---|---|
| **architect** | Single-sign §close per `v3-batch-process.md §5` + §8C.2. After sign: ratify G5-5C kickoff/mini-plan when sw drafts (architect bindings already supplied 2026-04-27: reuse engine-driven primitives; define trigger source first; pass criterion = G5-5 #4 failed hardware case). |
| **sw** | After architect §close sign: (1) update `v3-dev-roadmap.md` for the gate-close per `v3-batch-process.md §8` (G5-5 → CLOSED with G5-5C carry-forward; mark G5-5C as next batch); (2) draft G5-5C mini-plan; (3) optional opportunistic unit test for `EnsureStorage → assignment-arrives → first-Open` Identity-latch round-trip (would have caught round 10/11 bug pre-m01). |
| **QA** | Optional clean Tier 2 evidence run on `seaweed_block@5c4718f` for §close artifacts; package `g5-artifacts/` as §close evidence record. Standing by for G5-5C mini-plan review when sw drafts. |
