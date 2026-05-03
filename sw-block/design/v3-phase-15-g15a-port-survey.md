# V3 Phase 15 — G15a (CSI MVP) port survey

**Date**: 2026-04-28; code-read addendum 2026-05-03
**Status**: Layer 1 topology + 2026-05-03 V2 code-read addendum complete; G15a scope not yet ratified
**Owner**: QA (read-only port-survey assignment per architect 2026-04-28 ratification)
**Scope**: pre-ratification scoping only; not a code commitment; per `feedback_porting_discipline.md`

**Methodology — top-down**:
- Sources: directory listings, V2 design doc `v3-csi-iscsi-nvme-block-overview.md`, repo memory, k8s CSI v1.5 spec
- Goal: identify high-level modules + dependency graph, classify port disposition at module granularity
- Anti-pattern guard: Layer 1 did not open V2 `.go` files. The 2026-05-03 addendum below does open selected V2 `.go` files and records actual port disposition.

---

## Layer 1 — V2 CSI module topology

### V2 code locations (inventory only)

| Path | Contents |
|---|---|
| `weed/storage/blockvol/csi/` | 9 `.go` source files + 9 `_test.go` files + `cmd/` + `deploy/` |
| `weed/storage/blockvol/csi/cmd/block-csi/` | Driver binary entry point |
| `weed/storage/blockvol/csi/deploy/` | 6 k8s YAMLs (controller, node, driver, rbac, storageclass, example-pvc) |
| `weed/storage/blockvol/testrunner/` | Test runner platform: engine, agent, coordinator, baseline; `actions/` (k8s, iscsi, block, io, metrics, fault); `infra/` (target, ha_target, iscsi_client, node, artifacts, fault) |
| `weed/storage/blockvol/testrunner/scenarios/` | 16+ YAML scenarios incl. `coord-smoke-iscsi`, `coord-ha-failover`, `cp103-nvme-*`, `cp83-snapshot-expand`, `cp84-soak-4h`, `cp85-chaos-disk-full` |

### Top-level modules (Layer 1)

Seven distinct modules, classified by their primary coupling:

| # | Module | What it does (1-line) | Primary coupling |
|---|---|---|---|
| 1 | **CSI gRPC service layer** (controller.go, node.go, identity.go, server.go) | Implements k8s CSI v1.5 spec gRPC services: Controller / Node / Identity | k8s CSI spec (protobuf) |
| 2 | **Transport adapter layer** (iscsi_util.go, nvme_util.go) | OS-level orchestration of iSCSI initiator (iscsiadm) and NVMe-oF initiator (nvme-cli); device-path resolution; format/mount glue | OS / Linux kernel |
| 3 | **Volume backend bridge** (referenced as `VolumeBackend` in design doc, called from controller.go) | Talks to V2 master to create/delete BlockVol; receives IQN/NQN + addresses to put in PublishContext | **V2 wire** ← THIS IS THE PORT EDGE |
| 4 | **Snapshot ID encoding** (snapshot_ids.go) | Encodes/decodes snapshot identifiers across CSI/backend boundary | Internal Go interface |
| 5 | **Main binary** (cmd/block-csi/) | Wires gRPC server + transport adapters + volume backend; CLI flags | Internal wiring |
| 6 | **K8s deployment artifacts** (deploy/) | k8s YAMLs: csi-controller, csi-node, csi-driver, rbac, storageclass, example-pvc | k8s API |
| 7 | **Test corpus** (`*_test.go` + testrunner YAML scenarios) | In-package unit tests (controller, node, identity, snapshot, qa_*) + sw-test-runner end-to-end YAML scenarios driving CSI gRPC + iSCSI/NVMe device flows in a real (or simulated) cluster | Mixed (some k8s-spec, some V2-wire, some OS) |

### Dependency graph

```
                ┌──────────────────┐
                │  cmd/block-csi   │  (entrypoint, wires everything)
                └────────┬─────────┘
                         │
       ┌─────────────────┼─────────────────┐
       ▼                 ▼                 ▼
┌─────────────┐  ┌────────────────┐  ┌──────────────────┐
│ gRPC svc    │  │ Volume backend │  │ Transport        │
│ (Controller │──▶│  bridge        │  │ adapters         │
│  Node       │  │ (★ V2 wire ★)  │  │ (iSCSI / NVMe)   │
│  Identity)  │  └────────┬───────┘  └────────┬─────────┘
└──────┬──────┘           │                   │
       │                  ▼                   ▼
       ▼          ┌──────────────┐    ┌──────────────┐
  k8s CSI spec    │ V2 master /  │    │ OS / kernel  │
  (protobuf)      │ volume gRPC  │    │ (iscsiadm,   │
                  └──────────────┘    │  nvme cli,   │
                                      │  mount)      │
                                      └──────────────┘
       (deployment artifacts:                    (snapshot IDs:
        deploy/*.yaml,                           pure Go,
        k8s API only)                            no external coupling)
```

**Key observations:**
- Module 3 (Volume backend bridge) is the **only** module with hard V2 wire coupling. Everything else couples to k8s CSI spec or OS — both stable across V2/V3.
- Modules 1, 2, 4 are protocol/spec-bound and don't know V2 vs V3 exists.
- Module 6 (deployment YAMLs) couples only to driver name + image name — trivial edits.
- Module 7 (tests) splits along the same lines as the modules they exercise: protocol-level tests (most) are V2/V3-agnostic; wire-level QA tests (`qa_cp62`, `qa_cp83`) need rewire matching new backend.

### Layer 1 disposition (preliminary — Layer 4 will refine)

| # | Module | Preliminary disposition | Why |
|---|---|---|---|
| 1 | CSI gRPC service layer | **port-as-is** | k8s CSI v1.5 spec is stable; gRPC handlers shape doesn't change |
| 2 | Transport adapter layer | **port-as-is** | OS/kernel interface unchanged; iscsiadm + nvme-cli identical between V2/V3 |
| 3 | Volume backend bridge | **port-with-rewire** | Same shape (CreateVolume → backend, DeleteVolume → backend), but the backend interface implementation must be replaced to call V3 master/replication APIs instead of V2 master |
| 4 | Snapshot ID encoding | **port-as-is** | Pure Go encoding, no external deps |
| 5 | Main binary | **port-with-light-rewrite** | Wiring code: same module composition, new flag set possibly; minor edits |
| 6 | K8s deployment artifacts | **port-with-light-rewrite** | Update image name, driver name, possibly RBAC for V3-specific service accounts; YAML structure unchanged |
| 7 | Test corpus | **mixed** — split per Layer 2 | Most tests follow their module's disposition; QA wire-level tests need rewire |

### Layer 1 estimated split (rough)

- **port-as-is**: ~50-60% of LOC (modules 1, 2, 4 + most of 7)
- **port-with-light-rewrite**: ~10-15% (modules 5, 6 + a few tests)
- **port-with-rewire**: ~25-35% (module 3 + wire-level QA tests in 7)
- **rewrite-required**: should be near 0% — would surface in Layer 4 if found

### Layer 1 cross-checks (sanity)

| Cross-check | Result |
|---|---|
| **Architect's discipline** ("port from V2 is heavy reuse, not rewrite") | ✅ consistent — Layer 1 estimates ~75% port-as-is or light-rewrite |
| **NVMe support preservation** (user's concern, 2026-04-28) | ✅ Module 2 includes `nvme_util.go`; transport adapter layer is multi-protocol by design |
| **CSI v1.5 spec stability across V2/V3** | ✅ k8s CSI spec is independent of any storage backend |
| **Existing k8s e2e scenarios reusable** | ✅ 16+ YAML scenarios in testrunner/scenarios/ exercise full CSI flows; most should port-as-is |

### Layer 1 open questions (deferred to Layer 2-5)

1. **Volume backend interface** (Module 3): how thin is the V2-wire surface? If it's ≤10 method calls into V2 master, port is mechanical. If it's deeply intertwined with V2 master state shapes, may need redesign of the interface.
2. **NodeStageVolume flow**: how is "prefer NVMe over iSCSI" implemented today? Is it config or auto-detect? V3 needs the same flexibility.
3. **Snapshot ID semantics**: Module 4 ID encoding may assume V2 snapshot model; V3 snapshot lifecycle may differ when G10/G15b lands.
4. **Test corpus split**: of the 9 `_test.go` files, how many touch V2 wire vs are pure protocol/encoding tests? Affects port effort estimate.

---

## 2026-05-03 code-read addendum — actual V2 CSI port disposition

Selected V2 files read:

| V2 file | Finding | Disposition for V3 |
|---|---|---|
| `csi/controller.go` | CSI Controller service shape is spec-bound: create/delete/publish/unpublish/capability/expand/snapshot handlers translate CSI requests into a `VolumeBackend` interface. | **M / port selective**. Keep CSI request validation, idempotency, `VolumeContext` / `PublishContext` shape. Do not initially port snapshots/expand into G15a static MVP. |
| `csi/node.go` | NodeStage/Publish/Unstage uses `PublishContext` / `VolumeContext`, chooses NVMe if available, otherwise iSCSI, runs OS initiator commands, formats/mounts, persists `.transport`, and is idempotent on already-mounted paths. | **M / port complete for Node path**. This is the first useful CSI MVP surface because it tests real Linux attach/mount/read/write. Rebind imports and keep OS utilities injectable for tests. |
| `csi/iscsi_util.go` | Pure OS adapter: `iscsiadm`, device discovery under `/dev/disk/by-path`, mount/mkfs/resize helpers, mock utilities for unit tests. | **M / port complete**. This is directly reusable for Linux node plugin. |
| `csi/nvme_util.go` | Pure OS adapter: `nvme connect/disconnect/list-subsys/ns-rescan`, JSON parsing, mock utilities. | **M / port complete but G15a may default iSCSI-first**. NVMe is reusable after frontend/NVMe m01 confidence is high. |
| `csi/identity.go` | CSI Identity service constants and readiness. | **M / port complete**. Minimal and safe. |
| `csi/server.go` | Wires CSI Identity/Controller/Node services and local V2 `VolumeManager` / master client modes. | **M* / port shell only**. Keep endpoint parsing and service registration; replace local manager/master backend with V3-specific backends. |
| `csi/volume_backend.go` | Abstract `VolumeBackend`, local V2 `VolumeManager` backend, and V2 master gRPC client. | **M* + E split**. Keep the interface shape concept; do not port V2 master client. V3 backend must read from cluster-spec/product-loop assignment facts or a future blockmaster volume API. |
| `csi/volume_manager.go` | Embeds V2 `BlockVol` and V2 iSCSI `TargetServer`; creates local `.blk` files and exposes them. | **E / do not port** for V3 product mode. V3 blockvolume already owns frontend/export and durable storage. |
| `csi/cmd/block-csi/main.go` | CLI entry point for CSI driver. | **M* / rewrite-thin**. Keep flag style and signal handling; wire V3 backend and no local V2 manager. |
| `csi/deploy/*.yaml` | Kubernetes CSI controller/node/driver/rbac/storageclass/example PVC. | **M* / port with image/name/flag edits**. Useful as G15a deployment skeleton after binary exists. |

### Key architecture conclusion

V2 CSI should not be ported as "create volume + embedded BlockVol + local target" for V3. That path would bypass the P15 product loop we just built.

For V3 the first CSI gate should be:

```
cluster-spec / existing placement
  -> blockmaster product loop
  -> authority assignment
  -> blockvolume exposes iSCSI/NVMe
  -> CSI ControllerPublish returns publish_context
  -> CSI NodeStage attaches OS initiator and mounts
  -> pod writes filesystem
```

This tests the real V3 control/data boundary without reintroducing V2 storage ownership into the CSI driver.

### Recommended G15a split

| Slice | Claim | Code scope | Test scope |
|---|---|---|---|
| G15a-0 CSI service skeleton | CSI binary starts; Identity works; Controller/Node capability shape correct; no authority minting. | New `cmd/blockcsi` or `cmd/block-csi`; `core/csi` package; Identity; endpoint parsing; mocks. | Unit tests only. |
| G15a-1 static/pre-provisioned iSCSI Node path | Given publish_context from an existing V3 blockvolume assignment, CSI NodeStage/Publish can attach, format, mount, bind, unpublish, unstage. | Port V2 `node.go`, `iscsi_util.go`, mount helpers; no CreateVolume. | Mock unit tests + optional privileged M01 test. |
| G15a-2 ControllerPublish from blockmaster assignment | CSI ControllerPublish/Validate can look up existing volume target info from V3 blockmaster/status and return `iscsiAddr` + `iqn`. | V3 `VolumeBackend` backed by blockmaster query/status, not V2 master RPC. | L2 subprocess with real blockmaster + blockvolume; no Kubernetes yet. |
| G15a-3 Kubernetes smoke | K8s static PV/PVC + CSI node plugin can mount a V3 iSCSI volume and read/write. | Deploy YAMLs; container/image flags; node privileged mounts. | M01 or k8s lab only. |
| G15b dynamic provisioning | CSI CreateVolume creates desired volume + placement intent, waits for assignment readiness, then returns volume. | New product API for desired volume creation and placement allocation. | Requires G9/G10 product-loop maturity; not G15a. |
| G15c fault-domain validation | Node down/up, network partition, replica repair, pod remount/recover. | Mostly harness + policy, not CSI-only. | Hardware/k8s failure matrix. |

### Red-test anchors for G15a

1. `ControllerPublish` must fail closed if blockmaster has no verified assignment for the requested volume.
2. `ControllerPublish` must return publish_context from authority/product assignment facts, not from cluster-spec directly.
3. `NodeStageVolume` must be idempotent when the staging path is already mounted.
4. `NodeStageVolume` must clean up iSCSI/NVMe sessions if format/mount fails.
5. `NodeUnstageVolume` must recover transport identity from `.transport` after CSI plugin restart.
6. `NodePublishVolume` must bind-mount staging to pod target and be idempotent.
7. CSI package must not import `core/authority` or mint assignments.
8. Dynamic `CreateVolume` must be explicitly unimplemented in G15a unless a V3 product API is ratified.

### What this means for "real domain" testing

CSI is the right next user-facing integration layer, but it should not be asked to prove every recovery property at once.

- **G15a proves**: Kubernetes/node attach path can consume existing V3 assignment and mount a real block device.
- **G15c/G16 prove**: node down/up, network interruption, replica auto-repair, remount behavior, and pod-level data continuity.
- **G9/G10 continue to own**: placement, readiness, recovery decisions, and authority minting. CSI consumes those facts; it does not decide them.

## Layer 2-5 — superseded by 2026-05-03 addendum; original next-step notes retained

Per `v3-batch-process.md` discipline + architect 2026-04-28 ratification: Layer 1 (this section) is **read-only scoping** allowed without commitment. Layers 2-5 require the dogfood spine ratification before further work, since the "should we port at all + when" question depends on the schedule architect picks.

If/when ratified, Layer 2-5 plan:
- **Layer 2** (sub-modules + call graph within each Layer 1 module): 1 hour
- **Layer 3** (interface inventory per sub-module): 1 hour
- **Layer 4** (per-interface disposition: port-as-is / port-with-rewire / port-with-light-rewrite / rewrite-required / n/a): 30 min
- **Layer 5** (open V2 code — first time — and verify Layer 1-4 against actual implementation): 1-2 hours
- **Total** Layer 2-5: ~3-4 hours QA work

### Methodology guard for Layer 2-5

Per `feedback_porting_discipline.md`:
- No "顺便重构" (incidental refactor) during port
- No rewrite based on "this code looks unfamiliar"
- Rewrite-required disposition (Layer 4) requires explicit architect sign-off, not QA judgment alone

---

## Survey output for sw + architect at G15a kickoff

When G15a §1.A scope is being bound, this survey provides:

1. **Module count + dependency graph** (Layer 1 — already in this doc)
2. **Volume backend interface surface** (Layer 3 — TBD)
3. **Per-interface port disposition** (Layer 4 — TBD)
4. **LOC + test count estimate** (Layer 5 — TBD)
5. **NVMe + iSCSI dual-transport coverage assertion** (Layer 4)
6. **Reusable test scenarios from testrunner/scenarios/** (Layer 4)

These six artifacts let architect bind G15a scope without sw guessing port effort.
