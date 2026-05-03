# V3 Phase 15 — G15a CSI Static MVP Close Report

**Date**: 2026-05-03
**Status**: CLOSED — static/pre-provisioned CSI iSCSI path verified
**Code branch**: `p15-g15a/csi-static-mvp`
**Close anchor**: `seaweed_block@ac49adb`
**Docs/test evidence**:
- `sw-block/design/test/v3-phase-15-g15a-qa-test-instruction.md`
- `sw-block/design/test/v3-phase-15-g15a-privileged-qa-test-instruction.md`
- `V:\share\g15a-priv\runs\20260503T153623Z\`

---

## §1 Close Claim

G15a proves the static/pre-provisioned V3 CSI iSCSI path:

```
cluster-spec / product-loop assignment
  -> blockmaster authority assignment
  -> blockvolume iSCSI frontend target fact
  -> blockcsi ControllerPublish publish_context
  -> blockcsi NodeStage real Linux iSCSI login + mkfs + mount
  -> blockcsi NodePublish bind mount
  -> filesystem write/read byte-equal
  -> clean unpublish/unstage/logout
```

CSI remains a consumer of V3 product facts. It does not mint authority, create volumes, own storage, or derive target identity when a frontend target fact exists.

---

## §2 Code Delivered

| Commit | Summary |
|---|---|
| `7b10413` | Added `core/csi` skeleton, CSI Identity/Controller/Node services, boundary guards, and iSCSI NodeStage/Publish mechanism. |
| `4e150ca` | Added frontend target fact propagation: `blockvolume` heartbeat -> master observation/status -> CSI `ControlStatusLookup`. |
| `94ff9cf` | Added `cmd/blockcsi` static driver binary with Identity/Controller/Node registration. |
| `ac49adb` | Added non-privileged L2 smoke proving `ControllerPublish` returns real `blockvolume` frontend facts through `blockcsi`. |

---

## §3 Evidence

### §3.1 Non-Privileged L2

Tree: `p15-g15a/csi-static-mvp@ac49adb`

Commands:

```powershell
go test ./cmd/blockcsi -run TestG15a_BlockCSIControllerPublishUsesMasterFrontendFact -count=1 -v
go test ./core/csi ./cmd/blockcsi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Result: PASS.

What this proves:
- `cmd/blockcsi` starts and serves CSI Identity.
- `blockmaster`, `blockvolume`, and `blockcsi` run as real subprocesses.
- `blockvolume` reports the iSCSI frontend target fact.
- `blockmaster QueryVolumeStatus` exposes the assigned replica's frontend fact.
- `ControllerPublishVolume` returns `publish_context["iscsiAddr"]` and `["iqn"]` matching the real `blockvolume` target.

### §3.2 Privileged M01 L3

Run ID: `20260503T153623Z`

Artifact directory:
- Windows: `V:\share\g15a-priv\runs\20260503T153623Z\`
- m01: `/mnt/smb/work/share/g15a-priv/runs/20260503T153623Z/`

Key files:
- `test-stdout.log`
- `blockmaster.log`
- `blockvolume-primary.log`
- `blockvolume-replica.log`
- `topology.yaml`

Result: PASS.

The privileged test verified six phases:

| Phase | Verified operation |
|---|---|
| 1 | `ControllerPublishVolume` returned expected `iscsiAddr` and `iqn`. |
| 2 | `NodeStageVolume` completed real `iscsiadm` discovery/login, `mkfs.ext4`, and mount; staging path was a mountpoint. |
| 3 | `NodePublishVolume` bind-mounted staging to target; target path was a mountpoint. |
| 4 | 4096 random bytes wrote and read back byte-equal through the CSI-mounted iSCSI device. |
| 5 | `NodeUnpublishVolume` and `NodeUnstageVolume` completed cleanly. |
| 6 | No dangling iSCSI session remained for the test IQN. |

Additional log evidence from `blockvolume-primary.log`:
- Real Linux kernel initiator reached iSCSI `FullFeature` for target `iqn.2026-05.io.seaweedfs:g15a-priv-v1`.
- `mkfs.ext4` generated real SCSI `WRITE` and `SYNCHRONIZE_CACHE` traffic.
- The final payload write path included `OnLocalWrite`, `ship ok peer=r2`, and replication barrier acknowledgements.

Known artifact note: `blockcsi.log` was not persisted in this run because the harness helper wrote it to an ephemeral temp directory. This is a harness artifact gap, not a product-path gap; `test-stdout.log` and `blockvolume-primary.log` contain the relevant externally visible CSI and iSCSI/data-plane evidence.

---

## §4 Boundary Guards Preserved

G15a preserves the V3 control-plane boundaries:

- CSI package does not import `core/authority` or `core/adapter`.
- CSI package does not construct `AssignmentAsk`, `AssignmentFact`, or `adapter.AssignmentInfo`.
- CSI `ControllerPublish` reads frontend target facts through a narrow `PublishTargetLookup`.
- `blockmaster` status exposes frontend target facts as read-only observation/status data.
- `blockvolume` remains the owner of iSCSI/NVMe target identity and serving.

This matches the G15a unification rule: CSI is attach/mount orchestration, not storage ownership or authority.

---

## §5 Non-Claims

G15a does not claim:

- Kubernetes PV/PVC/Pod integration.
- Dynamic CSI `CreateVolume`.
- CSI snapshot, clone, or expansion.
- NVMe CSI attach path.
- Multi-node RWO enforcement.
- CHAP, mutual auth, TLS, or routable frontend security.
- Pod remount after primary kill.
- Failover while a filesystem is mounted.
- Plugin restart cleanup after crash.
- Performance, throughput, or soak behavior.

---

## §6 Forward-Carry

| Item | Target |
|---|---|
| Persist `blockcsi.log` into privileged artifact dirs. | Harness polish; not a G15a blocker. |
| Kubernetes static PV/PVC + pod write/read using `cmd/blockcsi`. | G15b. |
| Plugin restart + stale-volume `NodeUnstage` cleanup. | G15b/G15c follow-up. |
| Multi-node attach and RWO enforcement. | G15c. |
| Failover under live mounted filesystem. | G15c/G16, after G8 failover semantics are exercised through CSI. |
| NVMe NodeStage/NodePublish path. | G15d. |
| Dynamic `CreateVolume` from CSI into desired-volume / placement allocation. | G15e or later, after product API ratification. |

---

## §7 Close Decision

G15a is closed on `seaweed_block@ac49adb`.

The static CSI iSCSI path is proven at both:

- non-privileged L2 control-plane fidelity, and
- privileged m01 Linux data-plane fidelity.

Next gate should be G15b: Kubernetes static PV/PVC/pod integration using the same `cmd/blockcsi` binary and the same static/pre-provisioned volume model.
