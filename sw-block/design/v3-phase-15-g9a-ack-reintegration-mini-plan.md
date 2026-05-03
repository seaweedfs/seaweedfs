# V3 Phase 15 — G9A ACK + Reintegration Policy Mini-Plan

**Date**: 2026-05-02
**Status**: CLOSED 2026-05-02; first ACK/reintegration policy slice pushed on `p15-g9a/ack-reintegration-policy`
**Predecessor**: G8 CLOSED 2026-05-02
**Code repo**: `seaweed_block`

---

## 0. Product Sentence

The system exposes an explicit replication ACK profile and does not silently return full-sync success while the required replica is lagging, recovering, or unable to acknowledge. Returned replicas re-enter through candidate/sync/rebuild states before they are treated as ready.

---

## 1. Scope

### 1.1 In scope for first G9A slices

1. Name the ACK profile at product-daemon level.
2. Keep `best-effort` as the beta default.
3. Add a strict write-ACK seam: missing observer, observer error, or recovering peer cannot be counted as full-sync success.
4. Preserve recovery: lagging peers must still catch up or rebuild; best-effort does not disable recovery.
5. Prepare the returned-replica reintegration path vocabulary.

### 1.2 Out of scope

1. RF>=3 placement/quorum policy.
2. Rack/AZ placement.
3. Transparent OS initiator failover.
4. Full flow-control enforcement.
5. CSI user-visible policy knobs.

---

## 2. ACK Profiles

| Profile | Write success condition | Recovery implication |
|---|---|---|
| `best-effort` | Primary local write succeeds; replication errors are logged and drive recovery/degrade policy. | Lagging replicas still catch up or rebuild. |
| `sync-quorum` | Primary local write plus enough sync-eligible peers to satisfy quorum. RF=2 requires the only secondary. | Replica in recovery is not sync-ack eligible. |
| `sync-all` | Primary local write plus every configured peer is sync-eligible and accepts the write. | Any recovering/down peer fails foreground write ACK. |

Rule: recovery progress is not a substitute for synchronous ACK eligibility.

---

## 3. TDD Plan

### 3.1 Landed first slice

| Commit | Evidence |
|---|---|
| `8ba4884` | durable `WriteAckPolicy`: default best-effort preserves observer-error ACK; strict mode fails without observer or on observer error. |
| `1571212` | replication `OnLocalWrite`: `sync_all` / RF=2 `sync_quorum` fail when peer is recovering/non-Healthy; best-effort still retains. |
| `5232e3a` | `cmd/blockvolume --replication-ack=best-effort|sync-quorum|sync-all` wires daemon mode to replication durability and durable write ACK policy. |
| `520b25b` | `/status` append-only G9A vocabulary: returned old primary is `AuthorityRole=superseded`, `FrontendPrimaryReady=false`, `ReplicationRole=not_ready`; authority movement does not imply replica readiness. |
| `8362d42` | subprocess L2 strict ACK oracle: real blockmaster + 2x blockvolume + iSCSI, `--replication-ack=sync-quorum`, secondary down => foreground WRITE returns non-GOOD. |
| `154bd96` | subprocess L2 best-effort oracle: same RF=2 daemon/iSCSI shape, secondary down => foreground WRITE still returns GOOD. |
| `da8a321` | authority reintegration oracle: returned/high-evidence replica with `ReadyForPrimary=false` is skipped as failover target until a progress-ready fact exists. |
| `6d4a0e7` | `/status` maps engine `ModeRecovering` to `ReplicationRole=recovering`, distinct from `not_ready` and `replica_ready` (not yet emitted). |
| `f1117ce` | component oracle: best-effort foreground writes succeed while replica is down, then production probe/recovery path catches the lagging replica back up to byte-equal. |
| `cfffc7d` | RF>2 ACK oracle: `sync-quorum` RF=3 tolerates one recovering peer but fails when both secondaries are recovering. |

### 3.2 Forward-carry tests

1. `TestG9A_SyncQuorumWriteFailsWhenPeerInRecovery_Process`
   - Follow-up L2 variant where peer is explicitly in recovery (not merely down) and the daemon path still fails foreground ACK.
   - Requires a product-safe way to force/observe peer `ReplicaCatchingUp` in subprocess harness; do not add a test-only production RPC just for this.

---

## 4. Non-Claims

G9A first slices do not claim:

1. strict RF=2 no-loss under all crash timings;
2. transparent kernel initiator failover;
3. returned replica has completed reintegration;
4. automatic operator policy selection between best-effort and sync modes;
5. flow-control enforcement under primary flush pressure.

---

## 5. Close-Ready Posture

G9A first slice can close on current evidence if architect accepts subprocess-secondary-down as the L2 strict ACK oracle and component-level catching-up as the explicit recovery-state oracle.

Non-claims to carry forward:

1. Product-daemon L2 "peer explicitly in recovery" strict ACK variant.
2. `replica_ready` publication after completed reintegration.
3. RF>=3 quorum/placement policy.

---

## 6. Close Evidence

### 6.1 Closure claim

G9A establishes the MVP ACK/reintegration contract:

1. `best-effort` remains the default and does not wait for secondary ACK.
2. `sync-quorum` / `sync-all` do not silently return foreground success when the required replica is unavailable or not sync-eligible.
3. Returned replicas are not treated as ready from heartbeat/authority observation alone; they remain `not_ready` / `recovering` until progress/reintegration evidence exists.
4. Best-effort write success does not disable recovery; lagging replicas still enter the feeder/probe/recovery path and can converge byte-equal.

### 6.2 Evidence commands

Canonical scoped regression:

```powershell
go test ./core/authority ./cmd/blockvolume ./core/frontend/durable ./core/replication ./core/replication/component ./core/engine ./core/host/volume -count=1
```

Last verified by sw on 2026-05-02: green.

### 6.3 Evidence commits

| Commit | Role |
|---|---|
| `8ba4884` | durable write ACK policy seam |
| `1571212` | replication strict ACK mode semantics |
| `5232e3a` | product daemon `--replication-ack` flag |
| `520b25b` | returned old primary status vocabulary |
| `8362d42` | L2 subprocess strict ACK failure oracle |
| `154bd96` | L2 subprocess best-effort success oracle |
| `da8a321` | authority returned-replica progress-ready oracle |
| `6d4a0e7` | status `recovering` role |
| `f1117ce` | component best-effort still recovers lagging peer |
| `cfffc7d` | RF=3 sync-quorum compatibility |

### 6.4 Forward-carry register

1. L2 product-daemon test for explicit `ReplicaCatchingUp` strict ACK failure. Requires a safe observable/control seam; do not add a test-only production RPC.
2. Publish `ReplicationRole=replica_ready` only after completed reintegration evidence exists.
3. RF>=3 quorum and placement policy.
4. Flow-control enforcement under primary flush/pin pressure.
