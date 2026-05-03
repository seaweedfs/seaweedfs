# V3 Phase 15 G15c Mini-Plan — CSI Dynamic Provisioning MVP

Date: 2026-05-03
Status: G15c-A/B/C first slice landed at `seaweed_block@20e6d3b`; G15c-D K8s dynamic PVC remains pending
Code base: `seaweed_block` after G15b close, branch `p15-g15c/csi-dynamic-provisioning`

## 0. Product Sentence

G15c makes V3 usable by a normal Kubernetes `StorageClass` + PVC flow: the CSI controller accepts `CreateVolume`, records desired volume intent in blockmaster, lets the existing lifecycle/product-loop/publisher chain produce assignment, and then the already-proven G15b attach path mounts the volume.

## 1. Scope

IN:
- CSI `ControllerCreateVolume` / `ControllerDeleteVolume` minimal MVP.
- A master-side lifecycle RPC that writes desired volume records only.
- K8s manifests for external-provisioner + `StorageClass` + dynamic PVC smoke.
- L2 proof: CSI CreateVolume through real blockmaster creates lifecycle intent and later publish target becomes available.
- L3/M02 proof: dynamic PVC pod writes + checksum reads through real K8s CSI+iSCSI path.

OUT:
- Snapshot, clone, resize, topology-aware scheduling, multi-node K8s, online failover while mounted.
- CSI directly minting authority, assignment, epoch, endpoint_version, placement, or frontend facts.
- Replacing G15b static PV; it stays as the stable fallback and regression scenario.

## 2. Algorithm

1. Kubernetes external-provisioner calls `ControllerCreateVolume(name, capacity, parameters)`.
2. `blockcsi` validates a filesystem/block capability shape and maps the request to a product-level desired volume spec.
3. `blockcsi` sends `LifecycleService.CreateVolume` to blockmaster.
4. blockmaster writes `core/lifecycle.VolumeSpec` into its lifecycle store. This is desired product state only.
5. Existing lifecycle product loop reconciles volume + node inventory into placement intent, verifies observations, bridges to `AssignmentAsk`, and publisher mints authority.
6. `ControllerPublishVolume` remains read-only against `EvidenceService.QueryVolumeStatus`; it succeeds only after publisher + frontend observation expose a current publish target.
7. `ControllerDeleteVolume` removes desired volume intent. First MVP is best-effort cleanup of lifecycle intent only; data shredding and authority revoke are follow-up.

Important boundary: CSI owns user intent submission; master owns lifecycle persistence and authority publication; publisher remains the only authority minter.

## 3. TDD Gates

G15c-A — API and shape:
- `ControllerGetCapabilities` advertises `CREATE_DELETE_VOLUME`.
- `ControllerCreateVolume` rejects empty name, zero capacity, and unsupported capabilities.
- `ControllerCreateVolume` returns a CSI `Volume` whose `volume_id` is stable and whose context has no authority-shaped fields.

G15c-B — Master lifecycle RPC:
- `LifecycleService.CreateVolume` persists `VolumeSpec`.
- Repeated same request is idempotent.
- Conflicting size/RF returns AlreadyExists/FailedPrecondition-style error.
- Request/response proto has no epoch, endpoint_version, assignment, primary, healthy, or ready fields.

G15c-C — CSI to master:
- `blockcsi` uses the lifecycle RPC for Create/Delete and does not import `core/authority`.
- L2 subprocess: real blockcsi + real blockmaster, `ControllerCreateVolume` creates a lifecycle record visible in master snapshot/status helper.

G15c-D — K8s dynamic PVC:
- Manifest includes external-provisioner sidecar and `StorageClass`.
- M02: PVC without pre-created PV binds, pod writes 4 KiB, fsyncs, checksum reads back OK.
- Static G15b remains green.

## 4. Safety Checks

- No CSI package import of `core/authority`, `core/adapter`, or publisher types.
- No lifecycle RPC carries authority-shaped fields.
- Dynamic provisioning must use the same attach path as G15b after volume creation.
- Delete is explicitly not a data-plane deletion claim in first MVP.

## 5. QA Instructions To Produce

When G15c-C lands, write `sw-block/design/test/v3-phase-15-g15c-qa-test-instruction.md` with:
- Focused L2 command.
- Full package regression command.
- M02 dynamic PVC command after G15c-D.
- Scenario table with backing test names.
- Non-claims and artifact paths.

## 6. Close Criteria

G15c can close when:
- All G15c-A/B/C tests pass locally.
- M02 G15c-D dynamic PVC smoke passes once with artifacts.
- G15b static K8s scenario remains registered and green or explicitly re-run as regression.
- The close doc states dynamic provisioning is minimal and does not claim production-grade deletion, resize, snapshot, topology, or failover-under-mount.

## 7. First Slice Evidence

Commit: `20e6d3b G15c: add CSI dynamic volume intent RPC`

Implemented:
- `LifecycleService.CreateVolume/DeleteVolume` in control proto and generated bindings.
- blockmaster service handlers that write `core/lifecycle.VolumeSpec` only.
- blockcsi `CreateVolume/DeleteVolume` backed by master lifecycle RPC.
- product loop now reconciles desired volumes into placement intent before verification/publication.

Verification run:

```powershell
go test ./core/csi ./core/host/master ./cmd/blockcsi -count=1
go test ./core/lifecycle ./core/host/master ./core/authority ./core/csi ./cmd/blockmaster ./cmd/blockcsi ./cmd/blockvolume ./internal/testops -count=1
```

Both passed on 2026-05-03 before commit. The full package run took ~153s and is dominated by `cmd/blockvolume`.
