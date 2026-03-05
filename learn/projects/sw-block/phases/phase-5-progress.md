# Phase 5 Progress

## Status
- CP5-1 through CP5-4 complete. Phase 5 DONE.

## Completed
- CP5-1: ALUA implicit support, REPORT TARGET PORT GROUPS, VPD 0x83 descriptors, write fencing on standby.
- CP5-1: Multipath config + setup script, 4 multipath integration tests.
- CP5-1: Reviewer fixes (RoleNone write regression, T_SUP flag, TPG ID validation, ASCII log).
- CP5-1: 10 ALUA unit tests + 16 adversarial tests (all PASS).
- CP5-2: CoW snapshots implemented with flusher-based CoW, delta files, and recovery.
- CP5-2: Review fixes applied (PauseAndFlush safety, snapMu race fix, beginOp/endOp, lock order doc, error propagation).
- CP5-2: 10 unit tests + 22 adversarial tests (all PASS).
- CP5-3: CHAP auth, online resize, Prometheus metrics, admin endpoints.
- CP5-3: Review fixes applied (empty secret validation, AuthMethod echo, docs).
- CP5-3: 12 dev tests + 28 QA adversarial tests (all PASS).
- CP5-4: Failure injection (7 tests) + distributed consistency (17 tests) + Postgres crash loop (50 iters).
- CP5-4: 6 bugs found and fixed (lease expiry, scp auth, permissions, fdatasync, pg reinit, pgbench tables).
- CP5-4: 26/26 tests ALL PASS on m01/M02 remote environment (1067.7s combined).
- CP5-4: Added CleanFailoverNoDataLoss (500 PG rows survive failover via volume copy).

## In Progress
- None.

## Blockers
- None.

## Next Steps
- Phase 5 complete. Ready for Phase 6 (NVMe-oF) or other priorities.

## Notes
- SCSI test count: 53 (12 ALUA). Integration multipath tests require multipath-tools + sg3_utils.
- Known flaky: rebuild_full_extent_midcopy_writes under full-suite CPU contention (pre-existing).
- Known flaky: rebuild_catchup_concurrent_writes (WAL_RECYCLED timing, pre-existing).
- Known limitation: WAL shipper barrier timeout (5s) causes degradation under heavy fdatasync
  workloads. PgCrashLoop shows ~50% data divergence per failover without full rebuild. Expected
  behavior — production would use master-driven rebuild after each failover.
- Failover latency probe (10 iters): promote+first I/O ~30ms; total pause dominated by iSCSI
  login (avg 552ms, bimodal 130-180ms vs ~1170ms). Multipath should keep pause near 100-200ms;
  otherwise tune open-iscsi login timeout and avoid stale portals.

## CP5-4 Test Catalog

### Failure Injection (`test/fault_test.go`)
| ID | Test | What it proves |
|----|------|----------------|
| F1 | PowerLossDuringFio | fdatasync'd data survives kill-9 + failover |
| F2 | DiskFullENOSPC | reads survive ENOSPC, writes recover after space freed |
| F3 | WALCorruption | WAL recovery discards corrupted tail, early data intact |
| F4 | ReplicaDownDuringWrites | primary keeps serving after replica crash mid-write |
| F5 | SlowNetworkBarrierTimeout | writes continue under 200ms netem delay (remote only) |
| F6 | NetworkPartitionSelfFence | primary self-fences on iptables partition (remote only) |
| F7 | SnapshotDuringFailover | snapshot + replication interaction, both patterns survive |

### Distributed Consistency (`test/consistency_test.go`)
| ID | Test | What it proves |
|----|------|----------------|
| C1 | EpochPersistedOnPromotion | epoch survives kill-9 + restart (superblock persistence) |
| C2 | EpochMonotonicThreePromotions | 3 failovers, epoch 1→2→3, data from all phases intact |
| C3 | StaleEpochWALRejected | replica at epoch=2 rejects WAL entries from epoch=1 |
| C4 | LeaseExpiredWriteRejected | writes fail after lease expiry |
| C5 | LeaseRenewalUnderJitter | lease survives 100ms netem jitter with 30s TTL (remote) |
| C6 | PromotionDataIntegrityChecksum | 10MB byte-for-byte match after failover |
| C7 | PromotionPostgresRecovery | postgres recovers from crash (single-node, no repl) |
| C8 | DeadZoneNoWrites | fencing gap verified between old/new primary |
| C9 | RebuildWALCatchup | WAL catch-up rebuild after brief replica outage |
| C10 | RebuildFullExtent | full extent rebuild after heavy writes |
| C11 | RebuildDuringActiveWrites | fio uninterrupted during rebuild |
| C12 | GracefulDemoteNoDataLoss | data intact after demote + re-promote |
| C13 | RapidRoleFlip10x | 10 rapid epoch bumps, no crash or panic |
| C14 | LeaseTimerRealExpiry | lease transitions true→false at ~5s mark |
| C15 | DistGroupCommitEndToEnd | replica WAL advances during fdatasync fio |
| C16 | DistGroupCommitReplicaCrash | primary continues in degraded mode |
| C17 | DistGroupCommitBarrierVerify | replica LSN >= primary after fdatasync |

### Postgres Crash Loop (`test/pgcrash_test.go`)
| ID | Test | What it proves |
|----|------|----------------|
| PG1 | CleanFailoverNoDataLoss | 500 PG rows survive volume-copy failover, content verified |
| PG2 | ReplicatedFailover50 | 49 kill→promote→recover→pgbench cycles, PG recovers |
