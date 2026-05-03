# V3 Phase 15 G15b Kubernetes Static PV QA Test Instruction

**Date**: 2026-05-03
**Status**: K8s lab instruction for `p15-g15b/k8s-static-pv@eb13105`; M02 re-run pending
**Scope**: single-node Kubernetes static PV/PVC/pod smoke through real V3 daemons and CSI.

---

## Headline

At `seaweed_block@eb13105`, the G15b lab harness, image build inputs, and M02 DNS/log-preservation fixes are staged to prove:

```text
blockmaster + product-loop + r1/r2 blockvolume
  -> CSI external-attacher calls ControllerPublish
  -> blockcsi reads frontend target fact from blockmaster
  -> kubelet NodeStage performs real iSCSI attach/mount
  -> pod writes/reads checksum through PVC
  -> cleanup leaves no dangling test iSCSI session
```

This is the first Kubernetes form of the G15a privileged m01 proof. The first run is single-node and preserves loopback-only frontend exposure.

---

## Preconditions

Required:

- Kubernetes cluster API reachable by `kubectl`.
- Single Linux node with iSCSI initiator support.
- `iscsiadm`, mount, and kubelet CSI mount paths available on the node.
- Local images preloaded into the cluster:
  - `sw-block:local` containing `/usr/local/bin/blockmaster` and `/usr/local/bin/blockvolume`
  - `sw-block-csi:local` containing `/usr/local/bin/blockcsi`
- The cluster must allow privileged CSI node pods.

Known current local limitation:

- On the current dev workstation, `kubectl` context `rancher-desktop` exists but API server is not reachable. This instruction needs QA or a running K8s lab.

M02 first-run blocker fixed:

- `5375add` failed because `hostNetwork: true` blockvolume pods inherited host DNS and could not resolve `blockmaster.kube-system.svc.cluster.local`.
- `eb13105` adds `dnsPolicy: ClusterFirstWithHostNet` to both blockvolume pods.
- `eb13105` also collects daemon logs on every exit before cleanup, so failure evidence is preserved.

---

## Commands

Pre-flight from the code repo:

```powershell
cd C:\work\seaweed_block_g9c
git checkout p15-g15b/k8s-static-pv
go test ./cmd/blockcsi -run TestG15b_Manifest -count=1 -v
go test ./core/csi ./cmd/blockcsi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Build local images:

```bash
bash scripts/build-g15b-images.sh "$PWD"
```

For kind clusters, build and load into the cluster:

```bash
G15B_KIND_CLUSTER=<kind-cluster-name> bash scripts/build-g15b-images.sh "$PWD"
```

Local image build result already verified at `5375add`: PASS, images `sw-block:local` and `sw-block-csi:local` built.

After pulling `eb13105`, rebuild images before rerun:

```bash
bash scripts/build-g15b-images.sh "$PWD"
```

Kubernetes lab run from Linux or WSL with `kubectl` configured:

```bash
cd /path/to/seaweed_block
G15B_ARTIFACT_DIR=/mnt/smb/work/share/g15b-k8s/runs/$(date -u +%Y%m%dT%H%M%SZ) \
  bash scripts/run-g15b-k8s-static.sh "$PWD"
```

Expected result:

- script exits 0;
- pod `default/sw-block-static-smoke` reaches `Succeeded`;
- artifact dir contains pod logs and product daemon logs;
- cleanup deletes the test PV/PVC/pod and product/CSI manifests.

---

## Scenario Map

| Scenario | Evidence / backing test |
|---|---|
| Manifest attach path requires `ControllerPublish`. | `TestG15b_Manifest_CSIDriverRequiresAttach` |
| Controller deploy uses external-attacher, not provisioner. | `TestG15b_Manifest_ControllerUsesAttacherNotProvisioner` |
| Product stack uses G9G cluster-spec/product-loop and RF=2 r1/r2. | `TestG15b_Manifest_ProductStackSingleNodeLoopbackShape` |
| Static PV does not carry target endpoint truth. | `TestG15b_Manifest_StaticPVDoesNotEmbedTargetFacts` |
| Node plugin has privileged host mount shape. | `TestG15b_Manifest_NodePluginPrivilegedShape` |
| Pod write/read checksum path. | `scripts/run-g15b-k8s-static.sh` pod phase + `pod.log` |
| Product logs captured for debug. | `blockmaster.log`, `blockvolume-r1.log`, `blockvolume-r2.log`, `blockcsi-controller.log` |

---

## Artifact Expectations

The run should write:

- `run.log`
- `kubectl-version.txt`
- `nodes.before.txt`
- `apply-*.log`
- `pod.log`
- `pod.describe.txt`
- `blockmaster.log`
- `blockvolume-r1.log`
- `blockvolume-r2.log`
- `blockcsi-controller.log`
- `kube-system-pods.txt`
- `app-pv-pvc-pod.txt`
- `cleanup.log`

If the run fails, preserve the entire artifact directory.

---

## Non-Claims

G15b first lab does not claim:

- multi-node Kubernetes;
- routable iSCSI target exposure;
- dynamic CSI provisioning;
- snapshot/clone/expand;
- NVMe CSI;
- pod remount after failover;
- primary kill while mounted;
- plugin restart cleanup;
- performance or soak.

---

## Follow-Up If First Run Fails

Triage by first failing layer:

1. Image pull / binary missing
   - Fix image build/load pipeline; product code not implicated.

2. external-attacher does not call `ControllerPublish`
   - Inspect `CSIDriver`, `VolumeAttachment`, and attacher logs.

3. `ControllerPublish` returns no publish_context
   - Inspect `blockmaster.log`, `blockvolume-r1.log`, and master status facts.

4. `NodeStage` fails before iSCSI login
   - Inspect node plugin logs and CSI request context.

5. `iscsiadm` login/mount fails
   - Compare with G15a privileged m01 evidence; verify hostNetwork/loopback and `/etc/iscsi` mounts.

6. Pod checksum fails
   - Inspect `blockvolume-r1.log` for SCSI writes and replication barriers.
