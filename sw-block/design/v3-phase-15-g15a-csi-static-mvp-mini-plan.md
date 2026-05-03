# V3 Phase 15 — G15a CSI Static MVP Mini-Plan

**Date**: 2026-05-03
**Status**: G15a-1/2/3 implemented; G15a-4 non-privileged ControllerPublish L2 implemented; G15a-4 privileged NodeStage L2 and G15a-5 pending
**Branch**: `p15-g15a/csi-static-mvp` off `origin/phase-15`
**Goal**: make Kubernetes CSI consume an already-provisioned V3 block assignment and mount it through the Linux node path.

---

## §1 Scope

G15a is not dynamic provisioning. It is the first CSI product loop:

```
cluster-spec / product-loop assignment
  -> blockmaster publishes target facts
  -> CSI ControllerPublish returns publish_context
  -> CSI NodeStage logs in / connects and mounts
  -> CSI NodePublish bind-mounts into pod path
```

The claim is: **a pre-provisioned V3 volume can be consumed by CSI without CSI minting authority or owning storage**.

---

## §2 V2 Port Discipline

Port V2 execution code where it is mechanism:

| V2 source | Decision | Reason |
|---|---|---|
| `csi/identity.go` | PORT-AS-IS | CSI spec mechanism. |
| `csi/server.go` endpoint/service registration | PORT-REBIND | Keep gRPC/endpoint shape; replace V2 local manager/backend wiring. |
| `csi/node.go` NodeStage/Publish/Unstage core | PORT-REBIND, mostly complete | Linux attach/mount mechanism is valuable and already QA-hardened. Remove local `VolumeManager` fallback for G15a. |
| `csi/iscsi_util.go` | PORT-AS-IS | OS initiator + mount mechanism; injectable for tests. |
| `csi/nvme_util.go` | PORT-AS-IS but optional in G15a | Keep shape; G15a may default to iSCSI-first if NVMe hardware confidence is lower. |
| `csi/controller.go` ControllerPublish/Unpublish/Capabilities/Validate only | PORT-REBIND | Keep CSI request validation and response shape; backend is V3 assignment/status lookup. |
| `csi/volume_backend.go` interface concept | REWRITE-TINY | V3 backend should expose lookup/publish facts from blockmaster, not V2 master RPC. |
| `csi/volume_manager.go` | SKIP | V2 storage/target ownership; violates V3 boundary. |
| snapshots / expand / CreateVolume | SKIP for G15a | Dynamic lifecycle belongs after product API/placement allocation is ratified. |

---

## §3 iSCSI / NVMe / CSI Unification Rule

CSI must not invent target identity. The V3 iSCSI/NVMe frontends already own protocol identity and target serving:

- `cmd/blockvolume --iscsi-listen --iscsi-iqn` starts `core/frontend/iscsi.Target`.
- `cmd/blockvolume --nvme-listen --nvme-subsysnqn` starts `core/frontend/nvme.Target`.
- iSCSI and NVMe target packages own wire/session semantics, VPD/Identify identity, capacity, and backend opening.

CSI is only the Kubernetes attach/mount layer. It consumes a **published frontend target fact**:

```
volume_id
replica_id
authority epoch / endpoint_version
protocol = iscsi | nvme
target address
iqn or subsys_nqn
lun or nsid
capacity / block size (optional read-only evidence)
```

Current V3 gap: `AssignmentFact` carries data/ctrl addresses, and `cmd/blockvolume` prints `iscsi-listening` / `nvme-listening` lines for tests, but the master status API does not yet expose frontend target facts to a CSI controller. G15a must either:

1. add a read-only observation/status path for frontend target facts, or
2. constrain the first L2 CSI smoke to a test backend that receives the same facts from subprocess ready lines.

The product direction is option 1. The test-only bridge in option 2 is allowed only as a temporary pre-product slice and must be named as such.

Implementation note (2026-05-03):
- `7b10413` added the CSI static skeleton + Node iSCSI path.
- `4e150ca` added the read-only frontend target fact path:
  `blockvolume frontend -> heartbeat observation -> master QueryVolumeStatus -> CSI ControlStatusLookup`.
  This closes option 1 for the first iSCSI attach backend.
- `94ff9cf` added `cmd/blockcsi`: CSI Identity/Controller/Node services, endpoint binding, and read-only master status lookup wiring.
- `ac49adb` added the non-privileged ControllerPublish L2:
  real `blockmaster + 2x blockvolume + blockcsi`, with `ControllerPublish` returning the iSCSI frontend fact reported by blockvolume.

Boundary rules:

- CSI package may import CSI spec, OS attach helpers, and a narrow V3 publish-target lookup interface.
- CSI package must not import `core/authority`.
- CSI package must not construct `AssignmentAsk`, `AssignmentFact`, `adapter.AssignmentInfo`, Epoch, or EndpointVersion.
- CSI package must not derive IQN/NQN from volume ID when a publish target fact is available.
- IQN/NQN derivation, if needed for defaults, belongs in one shared helper used by `cmd/blockvolume` and CSI tests, not duplicated independently.

Loopback note:

Current `cmd/blockvolume` rejects non-loopback iSCSI/NVMe binds because frontends are unauthenticated. G15a has two valid modes:

- same-node/static MVP: CSI node plugin connects to a loopback target on the same node;
- real multi-node Kubernetes: requires a separate security/bind ratification before exposing iSCSI/NVMe on routable addresses.

Do not silently turn off the loopback guard inside CSI.

---

## §4 Red Tests

The first code slice must land tests before or with implementation:

1. `ControllerPublish` fails closed when the V3 assignment/status backend has no verified frontend target for `volume_id`.
2. `ControllerPublish` returns `iscsiAddr` + `iqn` from V3 assignment/status facts, not from cluster-spec directly.
3. CSI package does not import `core/authority` and does not call `Publisher` / mint assignment facts.
4. `NodeStageVolume` uses `PublishContext` before `VolumeContext`.
5. `NodeStageVolume` is idempotent when the staging path is already mounted.
6. `NodeStageVolume` cleans up login/connect if format/mount fails.
7. `NodeUnstageVolume` preserves staged state on unmount/logout failure for retry.
8. `.transport` restart recovery picks the correct disconnect path and rejects garbage transport values.
9. `NodePublishVolume` bind-mounts staging to target and is idempotent.
10. `CreateVolume`, snapshots, and expand return `Unimplemented` or are not advertised in G15a capabilities.
11. CSI publish target lookup uses the same IQN/NQN as `cmd/blockvolume` frontend flags / frontend target facts.
12. CSI attach must fail closed if the current authority target has no frontend target fact yet.
13. CSI must not bypass the current authority line by using a stale endpoint from `VolumeContext`.

---

## §5 Slices

### G15a-1 — CSI package skeleton + boundary guards

Status: **implemented** at `7b10413`.

Code:
- New V3 CSI package, likely `core/csi` or `core/frontend/csi`.
- Identity service.
- Endpoint parser and service registration.
- Controller capability surface with dynamic operations omitted.

Tests:
- Identity nil request behavior.
- endpoint parse tests.
- no authority import / no publisher call guard.
- Controller capabilities advertise only G15a-supported operations.

### G15a-2 — Node iSCSI attach/mount mechanism

Status: **implemented** at `7b10413` for iSCSI. NVMe node attach remains forward-carry.

Code:
- Port V2 `node.go` iSCSI path and `iscsi_util.go` / mount helpers.
- Remove local V2 manager fallback.
- Keep `PublishContext` > `VolumeContext`.
- Keep staged map, `.transport`, cleanup, idempotency.

Tests:
- Port V2 node tests that do not rely on `VolumeManager`.
- Port V2 adversarial cleanup tests.

### G15a-3 — ControllerPublish V3 backend

Status: **implemented** at `4e150ca`.

Code:
- Define V3 CSI backend interface: `LookupPublishTarget(ctx, volumeID, nodeID)`.
- Implement read-only backend against current blockmaster status / assignment surface plus frontend target facts.
- Return `publish_context` with iSCSI first; NVMe optional.

Tests:
- found/missing target.
- no direct cluster-spec shortcut.
- no authority minting.
- target fact must match the frontend fact reported by blockvolume.
- L2 subprocess: real `blockmaster` + two `blockvolume` processes, primary iSCSI target enabled, master `QueryVolumeStatus` exposes the assigned replica's iSCSI frontend fact.

### G15a-4 — L2 subprocess CSI smoke

Status: **partially implemented** at `94ff9cf` and `ac49adb`.

Code:
- `cmd/blockcsi` binary.
- Flags: endpoint, mode, node-id, master/status address, default transport.

Test:
- implemented: `cmd/blockcsi` subprocess starts and serves CSI Identity over gRPC.
- implemented: `blockmaster + blockvolume + blockcsi` subprocess chain; CSI `ControllerPublish` gets target facts through the real master status lookup.
- implemented by G15a-3: blockmaster + blockvolume L2 proves master status exposes frontend target facts.
- pending: NodeStage/Publish uses mockable or real privileged path depending environment.
- Non-privileged CI can stop at fake `ISCSIUtil` / `MountUtil`; M01/k8s executes real path.

### G15a-5 — Kubernetes YAML skeleton

Code/docs:
- Port V2 deploy YAML with V3 image/flags/driver name.
- Static PV/PVC example for pre-provisioned volume.

Test:
- QA instruction only unless k8s lab is available.

---

## §6 Non-Claims

G15a does not claim:
- dynamic CSI provisioning (`CreateVolume`);
- snapshot or expansion support;
- replica auto-repair under Kubernetes faults;
- pod remount after primary kill;
- multi-node network partition behavior;
- NVMe as the default attach path.

Those belong to G15b/G15c after the static CSI path is stable.

---

## §7 QA Instructions

Default CI:

```powershell
go test ./core/csi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Default G15a CI after `cmd/blockcsi`:

```powershell
go test ./core/csi ./cmd/blockcsi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Focused G15a-3 L2:

```powershell
go test ./cmd/blockvolume -run TestG15a_BlockvolumeReportsFrontendTargetsToMasterStatus -count=1 -v
```

Focused G15a-4 binary smoke:

```powershell
go test ./cmd/blockcsi -run TestBlockCSI_BinaryStartsAndServesIdentity -count=1 -v
```

Focused G15a-4 ControllerPublish L2:

```powershell
go test ./cmd/blockcsi -run TestG15a_BlockCSIControllerPublishUsesMasterFrontendFact -count=1 -v
```

Known wider-suite note: `go test ./core/...` currently includes pre-existing `core/calibration` and `core/conformance` failures unrelated to G15a frontend target facts. Do not use `./core/...` as the G15a gate until those tracks are reconciled.

M01 / privileged Linux:

1. Start blockmaster with cluster-spec.
2. Start blockvolume primary with iSCSI enabled.
3. Start CSI controller/node.
4. Run CSI ControllerPublish for the existing volume.
5. Run NodeStage/NodePublish.
6. Write/read through mounted filesystem.
7. NodeUnpublish/NodeUnstage must clean up mount and session.

---

## §8 Forward-Carry

- G15b: dynamic `CreateVolume` backed by desired-volume + placement allocation.
- G15c: Kubernetes node down/up, pod reschedule, primary kill, replica catch-up/rebuild, and data continuity.
- G15d: NVMe attach path as first-class CSI transport.
- G15e: snapshots/expand once V3 product APIs exist.
