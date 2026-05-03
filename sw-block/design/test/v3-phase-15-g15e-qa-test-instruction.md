# V3 Phase 15 G15e QA Test Instruction

Date: 2026-05-03
Status: QA-verified on `p15-g15e/dynamic-delete-cleanup@ddec28c`
Scope: Kubernetes dynamic PVC create/delete cleanup through CSI, lifecycle, launcher manifest sync, and iSCSI teardown.

## Headline

At `p15-g15e/dynamic-delete-cleanup@ddec28c`, G15e proves a dynamic PVC can:

1. Create a lifecycle volume intent through CSI `CreateVolume`.
2. Trigger blockmaster launcher manifest generation.
3. Apply the generated blockvolume Deployment.
4. Complete pod checksum write/read through CSI + iSCSI.
5. Delete the pod and PVC.
6. Trigger CSI `DeleteVolume`, removing lifecycle volume intent and placement intent.
7. Let launcher sync remove the generated manifest.
8. Let the harness delete the generated blockvolume Deployment.
9. Leave no active sw-block iSCSI session or G15e-created K8s resources.

This is still a single-node lab smoke. It does not claim a production operator;
the harness is the temporary operator that applies and deletes generated
blockvolume manifests.

## Command

From the seaweed_block checkout at `ddec28c`:

```bash
bash scripts/run-g15d-k8s-dynamic.sh "$PWD"
```

Optional artifact directory override:

```bash
G15D_ARTIFACT_DIR=/mnt/smb/work/share/g15e-k8s/runs/<run-id> \
  bash scripts/run-g15d-k8s-dynamic.sh "$PWD"
```

## Expected PASS

The script should print:

```text
[g15d] PASS: dynamic PVC create/delete completed checksum write/read and cleanup
```

The pod log must contain:

```text
/data/payload.bin: OK
```

Post-delete iSCSI evidence must contain:

```text
iscsiadm: No active sessions.
```

## Scenarios

1. `DeleteVolume` removes desired volume intent and placement intent.

Backing tests:
`core/host/master/lifecycle_service_test.go::TestG15e_LifecycleService_DeleteVolumeRemovesPlacementIntent`

2. Launcher manifest sync removes stale generated manifests.

Backing tests:
`core/launcher/manifest_writer_test.go::TestG15e_ManifestWriter_SyncRemovesStaleRenderedFiles`
`cmd/blockmaster/launcher_loop_test.go::TestG15e_BlockmasterLauncherTickRemovesManifestAfterVolumeDelete`

3. Dynamic PVC harness performs create, write/read, delete, manifest cleanup, generated Deployment cleanup, and session check.

Backing tests:
`cmd/blockcsi/g15d_manifest_test.go::TestG15d_K8sRunner_AppliesLauncherGeneratedManifest`

4. TestOps can locate the G15e shell scenario and expected artifact set.

Backing test:
`internal/testops/registration_test.go::TestG15eK8sDynamicCleanupRegistrationBuildsShellDriver`

## Canonical Evidence

- Tree: `p15-g15e/dynamic-delete-cleanup@ddec28c`
- Run ID: `20260503T212500Z-ddec28c`
- Artifact directory: `V:\share\g15e-k8s\runs\20260503T212500Z-ddec28c\`
- Result: PASS
- Pod oracle: `/data/payload.bin: OK`
- Post-delete oracle: `iscsiadm: No active sessions.`
- Cluster check after run: no G15e-created `sw-block` pods, Deployments, PVCs, or active iSCSI sessions remained.

Known unrelated residual on M02:

- `default/sw-block-test` PVC and its PV are 60-day-old V2 artifacts, not created by G15e.

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
- `delete-pod.log`
- `delete-pvc.log`
- `delete-generated-blockvolume.log`
- `app-storage.after-delete.txt`
- `kube-system-pods-deploys.after-delete.txt`
- `iscsi-sessions.after-delete.txt`
- `cleanup.log`

## Non-Claims

- No multi-node Kubernetes claim.
- No production operator/controller reconciliation claim.
- No backend data shredding claim.
- No failover during mounted dynamic PVC.
- No snapshot, resize, clone, block-mode, topology spread, or NVMe claim.
- No performance or soak claim.

## QA Sign

QA verified: 2026-05-03 on M02/k3s.
