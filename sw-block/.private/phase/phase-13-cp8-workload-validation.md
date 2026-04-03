# CP13-8 Real-Workload Validation — Envelope + Contract

Date: 2026-04-03

## Workload Envelope

| Parameter | Value |
|-----------|-------|
| Topology | RF=2, sync_all, cross-machine (m01 ↔ M02) |
| Transport | iSCSI (primary frontend) |
| Filesystem workload | ext4: 200 files, write + sync + failover + fsck + checksum verify |
| Application workload | PostgreSQL pgbench TPC-B (scale=1, c=1, 10s) on promoted replica |
| Disturbance | One bounded failover: kill primary, promote replica (epoch 1→2) |
| **NOT included** | NVMe-TCP, RF>2, hours/days soak, degraded-mode perf, mode normalization |

## Scenario

`weed/storage/blockvol/testrunner/scenarios/internal/cp13-8-real-workload-validation.yaml`

### Phase flow

1. **Setup**: RF=2 sync_all pair (primary on M02, replica on m01), standalone `iscsi-target` binary
2. **ext4 write**: iSCSI login → mkfs ext4 → write 200 files → md5sum → sync → umount → wait replication
3. **Failover**: kill primary → promote replica to primary (epoch 2)
4. **ext4 verify**: iSCSI login to promoted replica → fsck (filesystem integrity) → mount → file count == 200 → md5sum diff == MATCH
5. **pgbench**: iSCSI login → pgbench_init (ext4, scale=1) → TPC-B run (c=1, 10s) → TPS reported
6. **Cleanup**: always-run phase

### Pass criteria

| Proof | Assertion | What it validates |
|-------|-----------|-------------------|
| ext4 integrity | `fsck_ext4` passes | Replicated writes are filesystem-consistent after failover |
| ext4 completeness | `file_count == 200` | No files lost during replication + failover |
| ext4 correctness | `md5sum diff == MATCH` | File content identical to pre-failover (no corruption) |
| pgbench durability | `pgbench_run` completes with TPS > 0 | Database transactions are durable on sync_all promoted replica |

## Relation to CP13-1..7

| Accepted checkpoint | What this workload validates |
|---------------------|------------------------------|
| CP13-2 (address truth) | Cross-machine iSCSI replication uses canonical addresses |
| CP13-3 (durable progress) | ext4 data survives failover because barrier guarantees flushed durability |
| CP13-4 (state eligibility) | Only InSync replica was eligible for barrier during replication |
| CP13-5 (reconnect/catch-up) | Replication completed before failover (all writes reached replica) |
| CP13-6 (retention) | WAL retained long enough for replication to complete |
| CP13-7 (rebuild fallback) | Not directly exercised — failover is clean (no WAL gap). Support-only. |

## Existing infrastructure reused

| Existing scenario | Relation |
|-------------------|----------|
| `cp85-db-ext4-fsck.yaml` | CP13-8 extends this pattern with checksums, pgbench, and explicit envelope |
| `benchmark-pgbench.yaml` | CP13-8 pgbench phase uses same `pgbench_init` + `pgbench_run` actions |

## Run instructions

```bash
# From m01 (client node):
sw-test-runner run cp13-8-real-workload-validation.yaml

# Or from Windows dev machine (testrunner SSH):
cd C:/work/seaweedfs
go run ./weed/storage/blockvol/testrunner/cmd/sw-test-runner run \
  weed/storage/blockvol/testrunner/scenarios/internal/cp13-8-real-workload-validation.yaml
```

## What CP13-8 Does NOT Close

- Mode normalization (CP13-9)
- Broad launch approval
- Performance floor (see Phase 12 P4)
- Degraded-mode validation
- NVMe-TCP transport validation
- Hours/days soak under sustained load
