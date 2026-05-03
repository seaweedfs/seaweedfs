# V3 Phase 15 — G15b Kubernetes Static PV Mini-Plan

**Date**: 2026-05-03
**Status**: G15b-1 manifests implemented at `62325c9`; G15b-2 lab harness staged at `32b3a13`; image build inputs added at `5375add`; M02 first run found DNS/harness blockers fixed at `eb13105`; M02 second run found `iscsi_tcp` + primary/listener alignment blockers fixed at `95b7217`; Kubernetes re-run pending
**Branch**: `p15-g15b/k8s-static-pv` from `ac49adb`
**Goal**: prove a Kubernetes pod can consume a pre-provisioned V3 block volume through `cmd/blockcsi`, using real Kubernetes CSI control flow and real Linux iSCSI staging.

---

## §1 Scope

G15b is the Kubernetes form of the G15a static/pre-provisioned CSI path.

The intended product chain is:

```text
cluster-spec / product-loop assignment
  -> blockmaster publishes authority + frontend target facts
  -> Kubernetes external-attacher calls blockcsi ControllerPublish
  -> blockcsi reads frontend facts from blockmaster
  -> Kubernetes kubelet calls NodeStage with publish_context
  -> blockcsi NodeStage performs real iscsiadm login + mkfs/mount
  -> kubelet mounts PVC into pod
  -> pod writes/reads byte-equal data
  -> pod/PVC cleanup unstages/logs out cleanly
```

Close claim:

> A static Kubernetes PV/PVC/pod can use an already-provisioned V3 volume through the V3 CSI driver without embedding stale target identity in the PV and without CSI minting authority.

---

## §2 Critical Design Decision: Attach Required

G15b must not copy V2's first deployment shape blindly.

The V2 `CSIDriver` used:

```yaml
attachRequired: false
```

That is not the right first-close shape for V3 G15b if the claim is "target facts come from blockmaster through `ControllerPublish`."

With `attachRequired=false`, Kubernetes may bypass `ControllerPublish`. In that mode, `NodeStageVolume` receives only PV `volumeAttributes` / `VolumeContext`, so a static PV would need to embed `iscsiAddr` and `iqn`. That is acceptable as an emergency/debug fallback, but it is not the G15b product path because it duplicates frontend target truth outside blockmaster.

G15b therefore uses:

```yaml
attachRequired: true
```

and deploys the CSI external-attacher. The external-attacher invokes `ControllerPublishVolume`, receives `publish_context`, and kubelet passes that context into `NodeStageVolume`.

Allowed fallback:

- `NodeStageVolume` may continue supporting `VolumeContext` target fields because G15a already uses this as a low-level mechanism fallback.

G15b non-claim:

- static PV target-address fallback is not the close path and must not be used as the primary Kubernetes evidence.

---

## §3 V2 Deploy Port Discipline

V2 source directory:

```text
weed/storage/blockvol/csi/deploy/
```

Port decisions:

| V2 file | G15b decision | Reason |
|---|---|---|
| `csi-driver.yaml` | PORT-REBIND, but change `attachRequired` to `true` | V3 needs ControllerPublish to carry master frontend facts. |
| `csi-node.yaml` | PORT-AS-IS / light rebind | Privileged node plugin, hostNetwork, kubelet/dev/iscsi mounts are mechanism. |
| `rbac.yaml` | PORT-AS-IS / trim to sidecars used | Kubernetes RBAC mechanism. |
| `csi-controller.yaml` | PORT-REBIND | Use `blockcsi` + `csi-attacher`; do not include `csi-provisioner` in G15b. |
| `storageclass.yaml` | SKIP | G15b is static PV, no dynamic provisioning. |
| `example-pvc.yaml` | REWRITE-TINY | Replace dynamic PVC with static PV+PVC+pod example. |

Boundary rule:

- Manifests may wire product binaries and sidecars.
- Manifests must not encode authority epoch, endpoint version, primary role, or replica readiness.
- Static PV must not embed `iscsiAddr` or `iqn` in the close-path example.

---

## §4 Red Tests / Guards

Land these before or with the first manifest commit:

1. `TestG15b_Manifest_CSIDriverRequiresAttach`
   - Asserts `CSIDriver.spec.attachRequired == true`.

2. `TestG15b_Manifest_ControllerUsesAttacherNotProvisioner`
   - Asserts controller manifest contains `csi-attacher`.
   - Asserts controller manifest does not contain `csi-provisioner`.

3. `TestG15b_Manifest_StaticPVDoesNotEmbedTargetFacts`
   - Asserts static PV example does not contain `iscsiAddr`, `iqn`, `nqn`, or endpoint/version fields.

4. `TestG15b_Manifest_NodePluginPrivilegedShape`
   - Asserts node DaemonSet is privileged, `hostNetwork: true`, and mounts `/var/lib/kubelet`, `/dev`, and `/etc/iscsi`.

5. `TestG15b_Manifest_StaticPVUsesBlockCSIDriver`
   - Asserts PV driver is `block.csi.seaweedfs.com`.
   - Asserts `volumeHandle` is a V3 volume ID, not an endpoint.

6. `TestG15b_Manifest_NoAuthorityShapedFields`
   - Scans deploy examples for `epoch`, `endpointVersion`, `primary`, `healthy`, `ready` outside comments.

These tests are not a substitute for Kubernetes. They prevent the highest-risk configuration drift before the privileged lab run.

---

## §5 Slices

### G15b-1 — K8s Manifest Skeleton + Static Guards

Status: **implemented** at `seaweed_block@62325c9`.

Code:

- Add V3 Kubernetes manifests, likely under:

```text
deploy/k8s/g15b/
```

Files:

- `csi-driver.yaml`
- `rbac.yaml`
- `csi-controller.yaml`
- `csi-node.yaml`
- `static-pv-pvc-pod.yaml`

Tests:

```powershell
go test ./cmd/blockcsi -run TestG15b_Manifest -count=1 -v
```

Pass:

- All §4 manifest guards green.
- No product behavior change.

Verification:

```powershell
go test ./cmd/blockcsi -run TestG15b_Manifest -count=1 -v
go test ./core/csi ./cmd/blockcsi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Result: PASS on `62325c9`.

### G15b-2 — K8s Lab Harness

Status: **harness staged** at `seaweed_block@32b3a13`; image build inputs added at `seaweed_block@5375add`; DNS/logging fixes at `seaweed_block@eb13105`; iSCSI TCP module loading + deterministic attachable-primary bridge fix at `seaweed_block@95b7217`; real Kubernetes re-run pending.

Artifacts:

- `scripts/run-g15b-k8s-static.sh`
- `sw-block/design/test/v3-phase-15-g15b-k8s-qa-test-instruction.md`
- `scripts/build-g15b-images.sh`

Additional manifests:

- `deploy/k8s/g15b/block-stack.yaml`
  - `sw-block-cluster-spec` ConfigMap
  - `sw-blockmaster` Deployment + Service
  - `sw-blockvolume-r1` Deployment
  - `sw-blockvolume-r2` Deployment
- `deploy/k8s/g15b/Dockerfile.sw-block`
- `deploy/k8s/g15b/Dockerfile.blockcsi`

First topology:

- single-node Kubernetes;
- `blockvolume` pods use `hostNetwork: true`;
- iSCSI remains `127.0.0.1:3260`;
- this intentionally preserves the G15a loopback-only frontend guard.

M02 first-run findings:

- `blockvolume` pods use `hostNetwork: true`.
- Without `dnsPolicy: ClusterFirstWithHostNet`, they inherited host DNS and could not resolve `blockmaster.kube-system.svc.cluster.local`.
- Result: no heartbeat, no frontend fact, `ControllerPublish` returned NotFound, and the pod stayed Pending.
- The harness also collected daemon logs only after success, so failure-path evidence was lost unless captured manually.

Fix at `eb13105`:

- adds `dnsPolicy: ClusterFirstWithHostNet` to both `sw-blockvolume-r1` and `sw-blockvolume-r2`;
- changes `scripts/run-g15b-k8s-static.sh` so daemon logs are collected from the EXIT trap before cleanup;
- adds `.gitattributes` to keep `*.sh` as LF on future checkouts.

M02 second-run findings:

- M02 did not have `iscsi_tcp` loaded, so kubelet/CSI `NodeStage` failed with `iSCSI driver tcp is not loaded`;
- after manual `modprobe iscsi_tcp`, Kubernetes reached `ControllerPublish` and iSCSI login, but the volume still did not materialize because r2 won authority while only r1 exposed the static loopback iSCSI target;
- root cause: the G9F-2 product bridge mapped every RF=2 verified placement slot to a competing `Bind` ask for the same volume; authority's queue keeps latest desired ask per volume, so r2 could overwrite r1 nondeterministically.

Fix at `95b7217`:

- adds a privileged CSI node init container that runs `modprobe iscsi_tcp || true` and mounts host `/lib/modules`;
- adds `kmod` to the `sw-block-csi:local` image;
- changes the G9F-2 verified-placement bridge so RF>1 slots produce one deterministic frontend-primary `Bind` ask (first verified slot) instead of multiple competing `Bind` asks;
- pins the G15b static manifest assumption that the attachable iSCSI replica (`r1/s1`) is the first placement slot and that r2 does not expose a competing static iSCSI target.

Harness responsibilities:

1. Build V3 binaries/images for `blockmaster`, `blockvolume`, and `blockcsi`.
2. Load images into the test cluster.
3. Apply cluster-spec/product-loop setup for one RF=2 volume.
4. Apply CSI driver/controller/node manifests.
5. Apply static PV/PVC/pod.
6. Wait for pod ready.
7. Exec into pod and perform write/read checksum.
8. Delete pod/PVC/PV and assert node plugin cleanup.
9. Collect logs and relevant Kubernetes events.

Pass:

- Pod sees mounted filesystem.
- Pod writes and reads byte-equal data.
- No dangling iSCSI session for the test IQN after cleanup.

Pre-flight verification green at `95b7217`:

```powershell
go test ./cmd/blockcsi -run TestG15b_Manifest -count=1 -v
go test ./core/host/master -run 'TestG9F2|TestG15b_ProductLoop' -count=1 -v
go test ./internal/testops ./core/host/master ./cmd/blockcsi -count=1
go test ./core/csi ./cmd/blockcsi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Image build verification added at `5375add`:

```bash
bash scripts/build-g15b-images.sh .
```

Result: PASS; built `sw-block:local` and `sw-block-csi:local`.

Not yet proven:

- Kubernetes API server availability;
- image load path into the target cluster after rebuilding at `95b7217`;
- external-attacher calling `ControllerPublish`;
- kubelet calling `NodeStage` / `NodePublish`;
- pod checksum write/read.

### G15b-3 — First Kubernetes Close Run

Evidence target:

- Real Kubernetes control plane.
- Real CSI external-attacher.
- Real kubelet calling NodeStage/NodePublish.
- Real iSCSI login/mkfs/mount on the node.
- Pod-level byte-equal oracle.

Close anchor:

- Pin commit, image digest, cluster node(s), kernel, Kubernetes version, and artifact directory.

---

## §6 Non-Claims

G15b does not claim:

- dynamic CSI `CreateVolume`;
- snapshots, clones, or expansion;
- NVMe CSI path;
- multi-node RWO enforcement;
- pod remount after primary kill;
- failover under live mounted filesystem;
- network-partition behavior;
- security for routable iSCSI/NVMe target exposure;
- performance or soak.

---

## §7 Open Questions

Q1. First lab topology:

Recommended default: single-node Kubernetes on m01 first, because current frontend loopback guard is intentional and G15a privileged evidence already proves the Linux node path on m01.

Q2. Should `cmd/blockcsi` grow `--mode=controller|node|all` before G15b?

Recommended default: not required for first close. Running all CSI services in both controller and node pods is acceptable if sidecars call only the relevant service. Add `--mode` only if sidecar behavior or logs become confusing.

Q3. Should static PV include target fallback fields?

Recommended default: no for the close-path PV. Keep fallback only in code/tests for debug and plugin-restart recovery.

Q4. Should G15b include V2 dynamic provisioning sidecar?

Recommended default: no. Dynamic provisioning belongs after the product API can create desired volumes and placement safely.

---

## §8 QA Command Targets

Before K8s lab:

```powershell
go test ./cmd/blockcsi -run TestG15b_Manifest -count=1 -v
go test ./core/csi ./cmd/blockcsi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

K8s lab command will be added in `v3-phase-15-g15b-k8s-qa-test-instruction.md` once the harness exists.

---

## §9 Start Decision

Start with G15b-1 manifest skeleton + manifest red tests.

Do not start by applying YAML to Kubernetes. The attach semantics and manifest shape are the failure-prone boundary; pin them first, then run the cluster.
