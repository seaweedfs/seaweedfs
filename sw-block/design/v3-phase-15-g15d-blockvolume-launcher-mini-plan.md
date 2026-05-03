# V3 Phase 15 G15d Mini-Plan — BlockVolume Launcher MVP

Date: 2026-05-03
Status: first pure-planner slice landed on `p15-g15d/blockvolume-launcher`
Depends on: G15c `CreateVolume -> lifecycle desired intent`

## 0. Product Sentence

G15d turns dynamic volume intent into runnable blockvolume workloads. It keeps the current safe runtime model — one blockvolume process per volume replica — and adds the missing orchestration layer that makes Kubernetes dynamic provisioning real.

## 1. Why This Gate Exists

G15c can record a desired volume from CSI `CreateVolume`, but no component yet starts the blockvolume daemon(s) that serve that volume. Without G15d, a dynamic PVC can create intent but cannot become mountable.

Do not solve this by making `blockvolume` multi-volume in one step. That is a larger runtime refactor. The MVP path is:

```text
PVC
  -> CSI CreateVolume
  -> master lifecycle desired volume
  -> launcher creates blockvolume workload(s)
  -> blockvolume heartbeat
  -> product loop verifies placement
  -> publisher assignment
  -> CSI publish/stage/mount
```

## 2. Scope

IN:
- Pure workload planner: desired volume + placement + node inventory -> blockvolume workload intent.
- Anti-authority shape tests: workload plans cannot carry epoch, endpoint_version, primary, ready, or healthy.
- Idempotency tests.
- Later slice: K8s manifest renderer / apply loop.
- Later slice: M02 dynamic PVC smoke.

OUT:
- Multi-volume blockvolume daemon.
- K8s operator framework.
- Scheduling policy beyond existing lifecycle placement.
- Authority minting from launcher.
- Replica readiness or primary selection.

## 3. First Slice Evidence

Commit: `56a1047 G15d: add blockvolume workload planner`

Implemented:
- `core/lifecycle.BlockVolumeWorkloadPlan`
- `PlanBlockVolumeWorkloads`
- Tests:
  - blank-pool RF=2 creates deterministic replica workload intents
  - existing replica keeps replica ID
  - same inputs are idempotent
  - workload plan structs are not authority-shaped

Verification:

```powershell
go test ./core/lifecycle -count=1
```

## 4. Next Slices

G15d-B — K8s spec renderer:
- Convert `BlockVolumeWorkloadPlan` to deterministic K8s Deployment/StatefulSet-like spec data.
- Keep renderer pure; no cluster client yet.
- Test generated args include `--volume-id`, `--replica-id`, `--data-addr`, `--ctrl-addr`, durable root, and optional iSCSI listen.

G15d-C — Apply loop:
- Add a narrow launcher runner that applies generated workload specs.
- First implementation may shell `kubectl apply -f` or write manifests for QA to apply.
- It must be idempotent.

G15d-D — M02 dynamic PVC:
- `StorageClass` + PVC without hand-written PV.
- external-provisioner calls CSI CreateVolume.
- launcher starts blockvolume workload.
- pod writes 4 KiB + sync + checksum read-back OK.

## 5. Close Criteria

G15d closes only when M02 proves dynamic PVC smoke without a pre-created PV and without hand-started blockvolume for that PVC.
