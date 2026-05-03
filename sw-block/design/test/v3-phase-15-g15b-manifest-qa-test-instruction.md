# V3 Phase 15 G15b Manifest QA Test Instruction

**Date**: 2026-05-03
**Status**: manifest-layer QA for `p15-g15b/k8s-static-pv@62325c9`
**Scope**: Kubernetes static PV manifest shape only; no Kubernetes cluster required.

---

## Headline

At `seaweed_block@62325c9`, G15b-1 proves the V3 Kubernetes static PV deployment shape is pinned before cluster execution:

```text
CSIDriver attachRequired=true
  -> controller uses external-attacher, not provisioner
  -> static PV does not embed iSCSI/NVMe target identity
  -> node plugin is privileged/hostNetwork with kubelet/dev/iscsi mounts
  -> PV uses block.csi.seaweedfs.com + volumeHandle=v1
```

This is a manifest guard layer. It does not prove pod write/read; that belongs to the G15b Kubernetes lab run.

---

## Commands

From the code repo:

```powershell
cd C:\work\seaweed_block_g9c
git checkout p15-g15b/k8s-static-pv
go test ./cmd/blockcsi -run TestG15b_Manifest -count=1 -v
go test ./core/csi ./cmd/blockcsi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Expected result:

- focused manifest tests: PASS
- 7-package CSI/product-loop regression: PASS

---

## Scenario Map

| Scenario | Backing test |
|---|---|
| CSIDriver requires attach so Kubernetes invokes `ControllerPublish`. | `cmd/blockcsi/g15b_manifest_test.go::TestG15b_Manifest_CSIDriverRequiresAttach` |
| Controller sidecar shape uses external-attacher and not external-provisioner. | `cmd/blockcsi/g15b_manifest_test.go::TestG15b_Manifest_ControllerUsesAttacherNotProvisioner` |
| Static PV close path does not embed `iscsiAddr`, `iqn`, `nqn`, epoch, or endpoint version. | `cmd/blockcsi/g15b_manifest_test.go::TestG15b_Manifest_StaticPVDoesNotEmbedTargetFacts` |
| Node plugin has the privileged Linux shape needed for `iscsiadm`, `/dev`, and kubelet bind mounts. | `cmd/blockcsi/g15b_manifest_test.go::TestG15b_Manifest_NodePluginPrivilegedShape` |
| Static PV uses the V3 CSI driver name and a volume handle, not an endpoint. | `cmd/blockcsi/g15b_manifest_test.go::TestG15b_Manifest_StaticPVUsesBlockCSIDriver` |
| Manifests do not carry authority-shaped fields. | `cmd/blockcsi/g15b_manifest_test.go::TestG15b_Manifest_NoAuthorityShapedFields` |

---

## Non-Claims

G15b-1 does not claim:

- Kubernetes pod starts successfully.
- kubelet calls `NodeStageVolume`.
- external-attacher reaches `ControllerPublishVolume`.
- real iSCSI login/mount happens under Kubernetes.
- pod write/read byte-equal.
- cleanup removes all iSCSI sessions.
- dynamic provisioning, snapshots, expansion, NVMe, failover under mount, multi-node RWO, or performance.

---

## Next Evidence Layer

G15b-2 should add the Kubernetes lab harness:

```text
build images
  -> apply blockmaster/blockvolume/product-loop setup
  -> apply CSI manifests
  -> apply static PV/PVC/pod
  -> pod checksum write/read
  -> cleanup + no dangling iSCSI session
```

The G15b-1 manifest tests are the pre-flight guard for that run.
