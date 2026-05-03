# V3 Phase 15 G15d QA Test Instruction

Date: 2026-05-03
Status: QA-verified on `p15-g15d/blockvolume-launcher@a3d1e6a`
Scope: Kubernetes dynamic PVC smoke through launcher-generated blockvolume workload

## Headline

At `p15-g15d/blockvolume-launcher@a3d1e6a`, G15d proves a dynamic
PVC can create a V3 lifecycle volume intent, trigger blockmaster's launcher to
generate a blockvolume Deployment, apply that generated workload, and complete
a pod filesystem checksum write/read through CSI + iSCSI.

This is the first no-precreated-PV / no-hand-started-blockvolume Kubernetes
path. It is still a single-node lab smoke, not a production operator.

## Preconditions

- M02 or equivalent k3s node is available.
- Images are built and loaded as `sw-block:local` and `sw-block-csi:local`.
- `kubectl` points at the test cluster.
- iSCSI TCP module is loadable on the host. The CSI node init container runs
  `modprobe iscsi_tcp || true`.

## Command

From the seaweed_block checkout at `a3d1e6a`:

```bash
bash scripts/run-g15d-k8s-dynamic.sh "$PWD"
```

Optional artifact directory override:

```bash
G15D_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/runs/<run-id> \
  bash scripts/run-g15d-k8s-dynamic.sh "$PWD"
```

## Expected PASS

The script should print:

```text
[g15d] PASS: dynamic PVC pod completed checksum write/read
```

The pod log should contain:

```text
/data/payload.bin: OK
```

The generated manifest artifact should exist:

```text
generated-blockvolume.yaml
```

It must contain a Deployment named like:

```text
sw-blockvolume-<dynamic-volume-id>-r1
```

It must include a writable blockvolume state directory:

```text
mountPath: /var/lib/sw-block
emptyDir: {}
```

## Scenarios

1. Dynamic PVC has no pre-created PV.

Backing files:
`deploy/k8s/g15d/dynamic-pvc-pod.yaml`,
`cmd/blockcsi/g15d_manifest_test.go::TestG15d_K8sDynamicPVC_UsesStorageClassNoPrecreatedPV`

2. External provisioner is present.

Backing files:
`deploy/k8s/g15d/csi-controller.yaml`,
`cmd/blockcsi/g15d_manifest_test.go::TestG15d_K8sCSIController_IncludesExternalProvisioner`

3. Product stack does not pre-create blockvolume workloads.

Backing files:
`deploy/k8s/g15d/block-stack.yaml`,
`cmd/blockcsi/g15d_manifest_test.go::TestG15d_K8sBlockStack_HasLauncherButNoPrecreatedBlockvolume`

4. Launcher-generated manifest is applied by harness after CreateVolume.

Backing files:
`scripts/run-g15d-k8s-dynamic.sh`,
`cmd/blockcsi/g15d_manifest_test.go::TestG15d_K8sRunner_AppliesLauncherGeneratedManifest`

5. Pod writes 4 KiB, syncs, and verifies checksum through mounted PVC.

Backing file:
`deploy/k8s/g15d/dynamic-pvc-pod.yaml`

## Artifacts

The script preserves:

- `run.log`
- `block-stack.rendered.yaml`
- `generated-blockvolume.yaml`
- `blockmaster.log`
- `blockcsi-controller.log`
- `csi-provisioner.log`
- `csi-attacher.log`
- `blockvolume-generated.log`
- `pod.log`
- `pod.describe.txt`
- `app-storage.txt`
- `kube-system-pods-deploys.txt`
- `cleanup.log`

## Non-Claims

- No multi-node Kubernetes claim.
- No operator/controller reconciliation claim.
- No automatic deletion of dynamically-created blockvolume workloads beyond
  this harness cleanup path.
- No failover during mounted dynamic PVC.
- No snapshot, resize, block-mode, or topology-aware scheduling claim.
- No performance claim.

## QA Sign

QA verified: 2026-05-03 on M02/k3s.

Evidence:

- Tree: `p15-g15d/blockvolume-launcher@a3d1e6a`
- Run ID: `20260503T185756Z-a3d1e6a`
- Artifact directory: `V:\share\g15d-k8s\runs\20260503T185756Z-a3d1e6a\`
- Result: PASS; dynamic PVC pod completed checksum write/read.
- Pod oracle: `/data/payload.bin: OK`
- Cleanup: no active iSCSI sessions; no `sw-block` pod/PVC leak from this run.

Follow-up note: cleanup emits benign `command terminated with exit code 2`
lines from best-effort post-delete log collection after resources are gone. It
does not affect the PASS oracle or cleanup result.
