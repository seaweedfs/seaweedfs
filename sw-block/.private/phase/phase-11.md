# Phase 11

Date: 2026-04-02
Status: complete
Purpose: bind selected product-facing surfaces onto the accepted V2-backed chosen path without reopening accepted backend execution or control-plane closure

## Why This Phase Exists

`Phase 09` accepted production-grade execution closure on the chosen path.
`Phase 10` accepted bounded master-driven control-plane closure on that same path.

What remains is no longer:

1. whether the chosen backend path executes correctly
2. whether accepted control truth can reach the live volume-server path coherently

It is now:

1. whether selected product-facing surfaces can be rebound onto that accepted path without semantic drift
2. whether reuse of older V1-facing adapters reintroduces V1 recovery truth implicitly
3. whether the first product-facing surface can be proven in a bounded way before broader surface expansion

## Phase Goal

Move from accepted backend/control closure on one bounded chosen path to the first bounded product-surface rebinding proof.

Execution note:

1. treat `P0` as real planning work, not placeholder prose
2. use `phase-11-log.md` as the technical pack for:
   - step breakdown
   - hard indicators
   - reject shapes
   - assignment text for `sw` and `tester`

## Scope

### In scope

1. one bounded first product-surface slice
2. explicit no-overclaim around what that first surface proves and does not prove
3. reuse of existing implementation only where V2 truth still owns placement, recovery, and correctness claims
4. focused integration tests and contract checks for the chosen first surface

### Out of scope

1. reopening accepted `Phase 09` execution semantics
2. reopening accepted `Phase 10` control-plane closure
3. broad multi-surface product completion in one slice
4. `RF>2`, new durability modes, or broad cluster hardening
5. full production readiness / soak / rollout gates

## Phase 11 Items

### P0: First Surface Selection

Goal:

- choose the first product-facing surface that gives real product completion movement without turning the phase into a multi-system rewrite

Accepted decision:

1. the first bounded slice is `snapshot product path`
2. `CSI` is deferred to a later `Phase 11` slice because it pulls controller/node lifecycle, staging/publish, and broader cluster contract surface
3. `NVMe` / `iSCSI` rebinding are also deferred because they are transport/front-end adapters whose useful proof should come after one simpler product surface is already closed

Why this first:

1. snapshot is closest to already accepted backend truth
2. it exercises a real product-facing contract without immediately absorbing node/attach orchestration
3. it keeps the first `Phase 11` slice bounded to metadata/visibility/restore-contract correctness rather than transport and lifecycle breadth

Status:

- accepted

### P1: Snapshot Product-Path Rebinding

Goal:

- prove that the snapshot product path can be rebound onto the accepted V2-backed chosen path without semantic drift between snapshot-visible behavior and the accepted backend snapshot truth

Execution steps:

1. Step 1: contract freeze
   - define exactly what the first slice claims:
     - snapshot create
     - snapshot list
     - snapshot delete
   - explicitly exclude clone/restore unless a later slice accepts them
2. Step 2: implementation binding
   - bind product-visible snapshot operations onto the accepted backend snapshot path
   - keep master/volume-server state and visible metadata coherent
3. Step 3: proof package
   - prove create/list/delete on the chosen path
   - prove fail-closed behavior for unsupported/invalid inputs
   - prove no-overclaim around broader snapshot workflows

Required scope:

1. snapshot create/list/delete product-visible behavior on the chosen path
2. proof that snapshot metadata and visible snapshot set reflect the same accepted backend truth
3. proof that snapshot claims do not exceed the accepted V2 snapshot contract
4. explicit boundedness around restore/clone if they are not part of the first slice

Must prove:

1. snapshot creation on the product path maps to the accepted backend snapshot boundary rather than an implicit V1 truth
2. listing and deletion reflect the real volume-server/master state coherently
3. fail-closed behavior is preserved when snapshot prerequisites are missing or the volume is not eligible
4. the slice does not silently imply clone/restore/product workflow support that is not yet proven

Reuse discipline:

1. V1/master-facing snapshot RPC surface may be reused only as a product wrapper:
   - `CreateBlockSnapshot`
   - `DeleteBlockSnapshot`
   - `ListBlockSnapshots`
2. V1/volume-server-facing snapshot surface may be reused only as the bounded execution adapter:
   - `SnapshotBlockVol`
   - `DeleteBlockSnapshot`
   - `ListBlockSnapshots`
3. underlying `blockvol` snapshot implementation may be reused as execution reality, not as product truth ownership
4. every reused V1 surface must be called out explicitly in `phase-11-log.md` with one of:
   - `update in place`
   - `reference only`
   - `reuse as bounded adapter`
5. no reused V1 surface may silently redefine snapshot semantics, placement truth, or product support claims

Verification mechanism:

1. focused integration tests for create/list/delete on the chosen path
2. contract checks that visible snapshot metadata matches the accepted backend snapshot truth
3. no-overclaim review on what user-visible snapshot behavior is actually supported after the slice

Hard indicators:

1. one accepted create proof:
   - product-visible create succeeds on the chosen path
   - created snapshot is observable through list/readback metadata
2. one accepted delete proof:
   - deleted snapshot disappears from the visible snapshot set
   - repeated delete is either idempotent-success or explicitly fail-closed as designed
3. one accepted list coherence proof:
   - listed snapshot IDs/metadata match the real backend snapshot state
4. one accepted fail-closed proof:
   - invalid volume / missing snapshot / unsupported preconditions do not imply false success
5. one accepted boundedness proof:
   - docs/tests do not imply clone/restore/full snapshot workflow readiness unless separately proven
6. one accepted reuse-boundary proof:
   - all V1 reuse surfaces touched by the slice are explicitly listed and their role is bounded

Reject if:

1. the slice proves only local helper behavior rather than product-visible snapshot behavior
2. visible snapshot metadata can drift from backend truth
3. the first slice quietly absorbs clone/restore or broader workflow work
4. the slice claims product readiness beyond create/list/delete on the chosen path
5. reuse of V1 surfaces is implicit or lets V1 semantics become the source of truth

Status:

- accepted

Carry-forward from `P1`:

1. bounded snapshot create/list/delete product rebinding is now accepted on the chosen path
2. `P1` does not claim restore/clone/full snapshot workflow readiness
3. `CSI` rebinding is now the next active `Phase 11` slice

### Later candidate slices inside `Phase 11`

1. `P2`: `CSI` rebinding after snapshot product-path closure
2. `P3`: `NVMe` / `iSCSI` front-end rebinding after one simpler product-visible surface is already accepted
3. `P4`: broader snapshot workflow closure (`restore` / `clone`) or other residual product workflow work only after earlier slices are bounded and proven

### P2: CSI Rebinding

Goal:

- bind the accepted V2-backed chosen path to the `CSI` controller/node product surface without reintroducing V1 recovery truth

Execution steps:

1. Step 1: contract freeze
   - define the first bounded `CSI` surface claims:
     - `CreateVolume`
     - `DeleteVolume`
     - `ControllerPublishVolume`
     - `NodeStageVolume`
     - `NodePublishVolume`
     - `NodeUnpublishVolume`
     - `NodeUnstageVolume`
   - explicitly exclude CSI snapshot, expand, and NVMe-specific transport work unless a later slice accepts them
2. Step 2: backend rebinding
   - bind CSI controller operations to the accepted master-backed chosen-path volume surface
   - bind CSI node operations to the accepted chosen-path access contract for remote attach/stage/publish
3. Step 3: proof package
   - prove bounded create/publish/stage/use/delete lifecycle on the chosen path
   - prove fail-closed behavior for unsupported or invalid cases
   - prove no-overclaim around broader CSI/product workflow breadth

Required scope:

1. bounded CSI controller/node lifecycle on the chosen path
2. explicit separation between accepted backend/control truth and CSI orchestration wrappers
3. remote target publication/staging behavior for the chosen path
4. no-overclaim around snapshots via CSI, expand, NVMe transport preference, multi-node topology breadth, or broad K8s readiness

Must prove:

1. CSI controller create/delete/publish map to the accepted master-backed chosen-path truth rather than a local V1 shortcut
2. CSI node stage/publish/unstage/unpublish consume the same chosen-path access truth without redefining recovery semantics
3. product-visible CSI lifecycle behavior is coherent across controller and node surfaces
4. fail-closed behavior is preserved when required publish/volume context or target information is missing

Reuse discipline:

1. V1/CSI-facing controller and node RPC surfaces may be reused only as bounded product adapters:
   - `controller.go`
   - `node.go`
   - `server.go`
2. `volume_backend.go` may be reused only as the bounded bridge between CSI and accepted master/local surfaces
3. `volume_manager.go` may be reused only as bounded local execution reality where the slice explicitly proves that local manager behavior does not become semantic owner
4. accepted master block RPC surfaces may be reused only as bounded control/product adapters underneath the CSI backend bridge:
   - `CreateBlockVolume`
   - `DeleteBlockVolume`
   - `LookupBlockVolume`
5. every reused V1 surface must be called out explicitly in `phase-11-log.md` with one of:
   - `update in place`
   - `reference only`
   - `reuse as bounded adapter`
   - `reuse as bounded bridge`
   - `reuse as execution reality only`
6. no reused V1 surface may silently redefine lifecycle semantics, placement truth, or product support claims

Verification mechanism:

1. focused CSI controller/node integration tests on the chosen path
2. contract checks that controller-visible and node-visible truth match accepted backend/control truth
3. no-overclaim review on what CSI behavior is actually supported after the slice

Hard indicators:

1. one accepted controller create/publish proof:
   - CSI create returns coherent volume/publish context on the chosen path
2. one accepted node stage/publish proof:
   - node consumes the published target info and stages/publishes coherently on the chosen path
3. one accepted unpublish/unstage/delete proof:
   - teardown/deletion complete without leaving false-visible ownership
4. one accepted fail-closed proof:
   - missing or partial transport/context information does not imply false success
5. one accepted reuse-boundary proof:
   - all CSI/V1 reuse surfaces touched by the slice are explicitly listed and bounded
6. one accepted boundedness proof:
   - docs/tests do not imply CSI snapshot, expand, NVMe transport preference, or broad K8s/product readiness unless separately proven

Reject if:

1. the slice proves only CSI wrapper-local behavior without chosen-path backend/control coherence
2. controller truth and node truth can drift from accepted master-backed volume truth
3. the first CSI slice quietly absorbs snapshot, expand, NVMe, or broad multi-node/K8s readiness work
4. reuse of V1 surfaces is implicit or lets V1 semantics become the source of truth

Status:

- accepted

Carry-forward from `P2`:

1. bounded CSI controller/node lifecycle rebinding is now accepted on the chosen path
2. accepted proof uses the real master-backed create/lookup/delete path plus `mgr=nil` node consumption of published target truth
3. `P2` does not claim CSI snapshot, CSI expand, NVMe preference/failover closure, or broad Kubernetes readiness

### P3: NVMe / iSCSI Front-End Rebinding

Goal:

- bind transport/front-end publication surfaces onto the accepted V2-backed chosen path so the product-visible access path matches accepted backend/control truth

Execution steps:

1. Step 1: contract freeze
   - define the first bounded front-end publication claims:
     - create returns coherent front-end publication data
     - lookup returns coherent front-end publication data
     - heartbeat refresh preserves and updates publication truth
     - failover switches publication truth to the new primary coherently
   - explicitly exclude broad transport-performance claims, real initiator benchmarking, and broad cluster rollout readiness
2. Step 2: publication rebinding
   - bind `iSCSI` and `NVMe` publication fields onto the accepted master-backed chosen-path truth
   - keep registry-visible, lookup-visible, and CSI-visible publication truth coherent
3. Step 3: proof package
   - prove bounded create/lookup/failover/restart publication truth on the chosen path
   - prove fallback behavior is explicit where `NVMe` is absent
   - prove no-overclaim around full transport runtime/performance closure

Required scope:

1. publication/address/naming truth for front-end adapters on the chosen path
2. bounded integration proof that master-visible and product-visible access metadata stay coherent
3. `NVMe` primary publication and `iSCSI` fallback publication where supported by the chosen path
4. explicit boundedness around real initiator behavior, transport performance, and broad cluster hardening

Must prove:

1. create/lookup publication fields map to accepted chosen-path truth rather than ad hoc wrapper-local construction
2. heartbeat refresh and failover preserve or update front-end publication truth coherently
3. `NVMe` and `iSCSI` publication fields do not drift between registry, lookup, and product-facing responses
4. mixed-capability or fallback behavior is explicit rather than silently overclaimed

Reuse discipline:

1. master-facing product/control publication surfaces may be reused only as bounded adapters:
   - `CreateBlockVolume`
   - `LookupBlockVolume`
2. registry publication fields may be reused only as bounded truth carriers, not independent semantic owners:
   - `ISCSIAddr`
   - `IQN`
   - `NvmeAddr`
   - `NQN`
3. volume-server allocation/publication surfaces may be reused only as bounded front-end publication sources:
   - `AllocateBlockVolume`
   - block heartbeat publication of `NvmeAddr` / `NQN`
4. existing `CSI` controller consumption of publication fields may be reused only as a bounded downstream consumer, not as the source of truth for `P3`
5. every reused V1 surface must be called out explicitly in `phase-11-log.md` with one of:
   - `update in place`
   - `reference only`
   - `reuse as bounded adapter`
   - `reuse as bounded truth carrier`
   - `reuse as publication source only`
6. no reused V1 surface may silently redefine publication truth, failover truth, or supported transport claims

Verification mechanism:

1. focused integration tests for create/lookup publication truth on the chosen path
2. contract checks that registry-visible, lookup-visible, and consumer-visible publication fields match
3. failover/restart checks that front-end publication truth is reconstructed or updated coherently
4. no-overclaim review on what transport/front-end behavior is actually supported after the slice

Hard indicators:

1. one accepted create/lookup publication proof:
   - create returns coherent front-end publication fields
   - lookup returns the same chosen-path publication truth
2. one accepted failover publication proof:
   - front-end publication fields move to the new primary coherently after failover
3. one accepted restart/heartbeat reconstruction proof:
   - publication fields can be reconstructed or refreshed from accepted heartbeat truth
4. one accepted fallback proof:
   - `iSCSI` fallback or mixed-capability behavior is explicit and coherent when `NVMe` is absent
5. one accepted reuse-boundary proof:
   - all front-end publication surfaces touched by the slice are explicitly listed and bounded
6. one accepted boundedness proof:
   - docs/tests do not imply real transport runtime, performance leadership, or broad production readiness unless separately proven

Reject if:

1. the slice proves only field plumbing without chosen-path publication coherence
2. publication truth can drift across create, lookup, heartbeat, or failover
3. the slice quietly absorbs full transport runtime or performance claims
4. reuse of V1/publication surfaces is implicit or lets wrappers become the truth owner

Status:

- accepted

Carry-forward from `P3`:

1. bounded front-end publication/address truth rebinding is now accepted on the chosen path
2. accepted proof closes create/lookup coherence, failover publication switch, heartbeat reconstruction, and no-`NVMe` fallback
3. `P3` does not claim full initiator/runtime transport proof, performance claims, or broad production readiness

### P4: Broader Product Workflow Closure

Goal:

- close the remaining bounded snapshot product workflow gaps downstream of accepted `P1` / `P2` / `P3` without reopening earlier accepted truth

Execution steps:

1. Step 1: contract freeze
   - define the first bounded `P4` workflow claim as snapshot `restore`
   - explicitly defer `clone` unless and until a real product-facing clone surface exists and is accepted into scope
2. Step 2: workflow rebinding
   - bind product-visible restore behavior onto the accepted snapshot and chosen-path execution truth
   - keep restore-visible state coherent across master-visible and volume-server/backend-visible truth
3. Step 3: proof package
   - prove bounded restore success, destructive semantics, and post-restore visible truth
   - prove fail-closed behavior for missing snapshot or unsupported conditions
   - prove no-overclaim around clone or broader workflow productization

Required scope:

1. bounded snapshot restore product workflow on the chosen path
2. explicit proof that restore uses accepted snapshot/backend truth rather than reopening new execution ownership
3. explicit post-restore visible truth checks
4. explicit boundedness around `clone` and any broader workflow work

Must prove:

1. product-visible restore maps to accepted backend restore execution truth on the chosen path
2. restore-visible outcome matches the selected snapshot truth after the operation completes
3. destructive restore semantics are explicit rather than hidden
4. fail-closed behavior is preserved for missing snapshot, missing volume, or unsupported preconditions

Reuse discipline:

1. accepted master-facing snapshot RPC surfaces may be reused only as bounded product adapters for restore if a restore entry surface exists
2. accepted volume-server-facing snapshot/restore surfaces may be reused only as bounded execution adapters
3. underlying `blockvol.RestoreSnapshot` may be reused only as execution reality, not as product-truth ownership
4. `clone` must stay explicitly deferred unless a real product-facing surface is brought into scope and written into `phase-11-log.md`
5. every reused V1 surface must be called out explicitly in `phase-11-log.md` with one of:
   - `update in place`
   - `reference only`
   - `reuse as bounded adapter`
   - `reuse as execution reality only`
6. no reused V1 surface may silently redefine restore semantics, workflow readiness, or clone claims

Verification mechanism:

1. focused restore integration tests on the chosen path
2. contract checks that post-restore visible truth matches selected snapshot truth
3. fail-closed checks for invalid or unsupported restore conditions
4. no-overclaim review on what restore/clone workflow behavior is actually supported after the slice

Hard indicators:

1. one accepted restore success proof:
   - product-visible restore succeeds on the chosen path
   - visible post-restore state matches the selected snapshot truth
2. one accepted destructive-semantics proof:
   - writes after the snapshot are lost as designed and this is explicitly verified
3. one accepted fail-closed proof:
   - missing snapshot / missing volume / unsupported conditions do not imply false success
4. one accepted post-restore coherence proof:
   - list/readback/visible workflow state are coherent after restore
5. one accepted reuse-boundary proof:
   - all restore-facing V1 surfaces touched by the slice are explicitly listed and bounded
6. one accepted boundedness proof:
   - docs/tests do not imply clone or broad snapshot workflow readiness unless separately proven

Reject if:

1. the slice proves only backend-local restore mechanics without product-visible restore behavior
2. post-restore visible truth is not asserted
3. destructive semantics are left implicit
4. the slice quietly absorbs `clone` or broader workflow readiness work
5. reuse of V1 surfaces is implicit or lets V1 semantics become the source of truth

Status:

- accepted

Carry-forward from `P4`:

1. bounded snapshot restore workflow closure is now accepted on the chosen path
2. accepted proof closes restore success, destructive semantics, post-restore visible truth, and fail-closed behavior
3. `clone` remains explicitly deferred because no real product-facing clone surface is yet accepted into scope

## Phase 11 Completion Judgment

`Phase 11` is complete because:

1. `P1` accepted bounded snapshot create/list/delete product rebinding
2. `P2` accepted bounded `CSI` controller/node lifecycle rebinding
3. `P3` accepted bounded `NVMe` / `iSCSI` publication/address truth rebinding
4. `P4` accepted bounded snapshot restore workflow closure
5. the chosen-path product surface rebinding goal is now closed without reopening accepted `Phase 09` / `Phase 10` semantics
6. remaining work is no longer product-surface rebinding inside `Phase 11`, but production hardening in `Phase 12`

## Assignment For `sw`

Current next tasks:

1. `Phase 11` is closed
2. move next to `Phase 12 P0` production-hardening planning
3. do not reopen accepted `P1` / `P2` / `P3` / `P4` semantics casually during hardening planning
4. keep `clone` deferred unless separately re-scoped in a future phase

## Assignment For `tester`

Current next tasks:

1. `Phase 11` is closed
2. validate `Phase 12 P0` as real planning work rather than placeholder prose
3. keep no-overclaim active around accepted `P1` / `P2` / `P3` / `P4` closure
4. treat `clone` or any other future workflow work as separate re-scoping work, not implicit `Phase 11` residue
