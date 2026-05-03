# V3 Phase 15 G15d Mini-Plan — BlockVolume Launcher MVP

Date: 2026-05-03
Status: planner + renderer + node inventory + replica-id materialization + manifest writer loop landed on `p15-g15d/blockvolume-launcher`
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

Commits:
- `56a1047 G15d: add blockvolume workload planner`
- `f4d695f G15d: render blockvolume Kubernetes workloads`
- `ae2cd1a G15d: import cluster spec node inventory`
- `1848099 G15d: materialize launcher replica identities`
- `109edd8 G15d: write launcher blockvolume manifests`

Implemented:
- `core/lifecycle.BlockVolumeWorkloadPlan`
- `PlanBlockVolumeWorkloads`
- Tests:
  - blank-pool RF=2 creates deterministic replica workload intents
  - existing replica keeps replica ID
  - same inputs are idempotent
- workload plan structs are not authority-shaped
- pure K8s Deployment renderer for blockvolume workloads
- renderer output pins hostNetwork DNS policy, deterministic names, args, ports, and no authority-shaped words
- `--cluster-spec` can now carry node/pool inventory and blockmaster imports it into lifecycle node registration before the product loop runs
- master-side workload planning tick converts blank-pool placements into concrete replica workload identities and writes those replica IDs back as existing-replica placement intent
- dynamic-volume assignment subscriptions can fall back to lifecycle placement slots when static accepted topology does not yet contain the volume
- `blockmaster` can run an optional launcher loop that writes rendered blockvolume Deployment YAMLs to `--launcher-manifest-dir`; it does not call `kubectl apply`

Verification:

```powershell
go test ./core/lifecycle -count=1
go test ./core/lifecycle ./core/launcher -count=1
go test ./cmd/blockmaster ./core/lifecycle ./core/host/master ./core/launcher -count=1
```

## 4. Next Slices

G15d-B — K8s spec renderer:
- Convert `BlockVolumeWorkloadPlan` to deterministic K8s Deployment/StatefulSet-like spec data.
- Keep renderer pure; no cluster client yet.
- Test generated args include `--volume-id`, `--replica-id`, `--data-addr`, `--ctrl-addr`, durable root, and optional iSCSI listen.

G15d-C — Apply loop:
- DONE for write-manifest mode: `blockmaster --launcher-loop-interval=<d> --launcher-manifest-dir=<dir>` renders workload manifests idempotently.
- Still pending for K8s automation: a shell/script stage that `kubectl apply -f <dir>` after the manifests are written.
- The writer intentionally stops before apply so QA can inspect exact Deployment YAML before the cluster mutates.

G15d-D — M02 dynamic PVC:
- `StorageClass` + PVC without hand-written PV.
- external-provisioner calls CSI CreateVolume.
- launcher starts blockvolume workload.
- pod writes 4 KiB + sync + checksum read-back OK.

## 5. Close Criteria

G15d closes only when M02 proves dynamic PVC smoke without a pre-created PV and without hand-started blockvolume for that PVC.
