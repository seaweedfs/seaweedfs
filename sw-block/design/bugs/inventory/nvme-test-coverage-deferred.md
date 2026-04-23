# NVMe Test Coverage — Deferred Inventory (B-tier)

**Date**: 2026-04-22
**Status**: LIVING — items move OUT when landed, marked CLOSED when fully in ACTIVE tracks
**Purpose**: enumerate NVMe test coverage deliberately deferred past T2 closed, so nothing rots silently. Each item has (a) V2 correspondent where applicable, (b) assigned downstream track, (c) activation trigger.

**Governance**: adding to this file does NOT require Discovery Bridge — it's a downgrade (from "should do" to "will do later"). Removing an item (i.e., landing it) is governed by the track it's assigned to.

---

## L1-B — Go subprocess component tests deferred

| ID | Test | V2 correspondent | Track | Activation trigger |
|---|---|---|---|---|
| ~~L1B-1~~ | ~~`TestT2Process_NVMe_ReconnectLoop` — attach/write/disconnect × 50 cycles on one workstation~~ | ~~TestComponent_FastReconnect~~ | ~~T3 perf~~ | **LANDED 2026-04-22 as `TestT2V2Port_NVMe_Process_ReconnectLoop50` (QA parallel capacity, 14.9s PASS, goroutine-leak guard included). Ledger row `PCDD-NVME-SESSION-RECONNECT-LOOP-001` queued ACTIVE.** |
| L1B-2 | `TestT2Process_NVMe_FailoverMidWrite` — kill primary mid-in-flight-write, verify replica takeover | TestComponent_FailoverPromote | G8/T6 Failover Data Continuity | master-side "kill + promote" RPC exposed |
| L1B-3 | `TestT2Process_NVMe_CrashRecovery` — write/sync/crash/restart/read-consistent | TestCP13_SyncAll_ReplicaRestart_Rejoin | T3 durability or T5 | durable backend (beyond memback) available |
| ~~L1B-4~~ | ~~`TestT2Process_NVMe_DisconnectMidR2T` — kernel-realistic cancel path~~ | ~~n/a (new)~~ | ~~T3~~ | **LANDED 2026-04-22 as `TestT2V2Port_NVMe_IO_DisconnectMidH2CDataStream` (unit-scope, 0.6s PASS). Pins recvH2CData mid-loop EOF unwind. Ledger row `PCDD-NVME-SESSION-DISCONNECT-MID-R2T-001` queued ACTIVE.** |

### T3 deferred scenarios (post-sw-commit additions; G4 canonical not blocked)

| ID | Scenario | V2 parity | Defer target |
|---|---|---|---|
| T3-DEF-1 | `t3c-durable-crash-during-sync` — kill mid-Sync (before Sync returns); pre-Sync acked all present, post-Sync-initiation may vanish | V2 CP13 crash variants | Post-G4 hardening (G21 or follow-up batch) |
| T3-DEF-2 | `t3c-durable-restart-loop` — 10-cycle attach/write-distinct-pattern/crash/restart/verify with pattern integrity | V2 t0-hosting-smoke 20-cycle | Post-G4 hardening (same) |

### Latent-gap findings (from Tier A V2/V3 Contract Bridge Catalogue retrofill 2026-04-22)

| ID | Statement | Surfaced by | Defer target |
|---|---|---|---|
| ~~T3-DEF-5~~ | ~~iSCSI VPD Model / Vendor / Firmware-Rev — V2-hardcoded constants; PORT-AS-IS pending multi-tenant trigger~~ | `v2-v3-contract-bridge-catalogue.md` §2.2.13 Tier A retrofill | **CATALOGUED 2026-04-22 as row C4-ISCSI-VPD-STATIC-FIELDS-HARDCODED (PORT-AS-IS LOCKED explicit). Closes T3-DEF-5 as discipline row; no regression needed** |
| ~~T3-DEF-6~~ | ~~NVMe Session cleanup-on-close contract only implicit (goroutine leak) → add explicit L1 addendum~~ | same | **LANDED 2026-04-22 as `t3_qa_session_cleanup_addendum_test.go` (`TestT3_NVMe_Session_CleanupOnClose_NoLeaksAcrossCycles` + `_CNTLIDAllocator`, both PASS 0.78s). Pins C5-NVME-SESSION-STATE-CLEANUP-ON-CLOSE.** |
| ~~T3-DEF-7~~ | ~~NVMe KATO timer currently store-only; m01 long-keepalive-RTT warning exposed today~~ | same | **FILED 2026-04-22 as `bugs/006_nvme_kato_timer_not_enforced.md` (Medium, target G21 / post-T3). C1-NVME-SESSION-KATO reclassified VIOLATED with BUG-006 anchor. Closes T3-DEF-7 as inventory row → BUG queue** |

Rationale: proposed post-sw-commit; shipped 4-scenario set covers canonical G4 pass-gate (`INV-DURABLE-001`) + 3 adjacent invariants. Not regressions; V2-parity hardening adjuncts.
| L1B-5 | `TestT2Process_NVMe_MultiVolumeConcurrent` — 3 volumes × 4 queues each | TestComponent_MultiReplica (adapted) | T3 perf | n/a |

---

## L2-B — Scenario YAML + Go replay deferred

| ID | Scenario | V2 correspondent | Track | Activation trigger |
|---|---|---|---|---|
| L2B-1 | `t2-nvme-crash-recovery.yaml` | crash (1 of 11) | T3 durability | L1B-3 lands first |
| L2B-2 | `t2-nvme-ha-failover.yaml` | HA (4 of 11, collapsed) | G8/T6 | L1B-2 lands first |
| L2B-3 | `t2-nvme-fault-disk-fill.yaml` | fault (1 of 3) | T3 | backend exposes "fail next N writes" hook |
| L2B-4 | `t2-nvme-fault-network-drop.yaml` | fault (1 of 3) | T3 | fault-injection infrastructure |
| L2B-5 | `t2-nvme-consistency-fsync.yaml` | consistency (1 of 2) | T3 or G22 final gate | L1B-3 or durable backend |

---

## L3-B — m01 real-kernel runs deferred

| ID | Run | Track | Activation trigger |
|---|---|---|---|
| L3B-1 | `iterate-m01-nvme-reconnect.sh` — attach/detach loop 1 hr, watch for connection leak | T3 perf | after L3-A proven green in CI |
| L3B-2 | `iterate-m01-nvme-fio.sh` — mixed read/write IOPS/throughput/latency measurement via fio | T3 perf | perf baseline established for iSCSI side first |
| L3B-3 | `iterate-m01-nvme-24h-soak.sh` — 24 hr continuous mkfs/mount/I/O/umount loop | post-T2 closed (monitoring) | T2B-NVMe-product-ready signed |

---

## L4-B — Long-duration / endurance

| ID | Run | Track | Activation trigger |
|---|---|---|---|
| L4B-1 | 7-day endurance soak (bonnie++ style) | G22 Final Gate | all prior tracks green |
| L4B-2 | Chaos mesh — network partition + process crash + disk fill random mix | G22 Final Gate | fault-injection infra available |

---

## Activation checklist (when item moves from B to ACTIVE)

Filing a new entry from B to implementation requires:

1. Assigned owner (not "TBD")
2. Concrete activation trigger satisfied (not "when convenient")
3. Acceptance criteria copied from the B-row or expanded inline
4. Ledger row provisional → queued for ACTIVE
5. Updated here: row marked `→ [link to impl commit or test file]`, strike-through the text

Items that never satisfy their trigger during the V3 P15 lifecycle carry forward to the next phase's inventory (P16 or beyond).

---

## Rotation audit (quarterly)

Once per quarter (or per phase gate), reviewer walks this file and asks for each row:

- Is the track still alive?
- Is the activation trigger still relevant?
- Has V3 architecture drifted such that the test is no longer meaningful?

Drop rows that fail any of the three; add rationale line in §Change log.

---

## Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial file with 5 L1-B + 5 L2-B + 3 L3-B + 2 L4-B | QA Owner (Batch 11c sign prep) |
| 2026-04-22 | L1B-1 ReconnectLoop landed early: file `t2_v2port_nvme_reconnect_loop_test.go`, 50 cycles PASS 14.9s, no goroutine leak | QA Owner |
| 2026-04-22 | L1B-4 DisconnectMidH2CDataStream landed early: file `t2_v2port_nvme_disconnect_mid_r2t_test.go`, unit-scope 0.6s PASS; server log confirms recvH2CData EOF unwind path exercised | QA Owner |
| 2026-04-22 | T3-DEF-5/6/7 processed during T3-end retrospective: DEF-5 catalogued (C4-ISCSI-VPD-STATIC-FIELDS-HARDCODED), DEF-6 test landed (`t3_qa_session_cleanup_addendum_test.go`), DEF-7 filed as BUG-006 (`006_nvme_kato_timer_not_enforced.md`). All three struck through above | QA Owner |
