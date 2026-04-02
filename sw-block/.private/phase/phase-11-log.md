# Phase 11 Log

Date: 2026-04-02
Status: active
Purpose: record the technical pack and design notes for the first bounded product-surface rebinding slice after accepted `Phase 10` control-plane closure

---

### P0 Technical Pack

Date: 2026-04-02
Goal: convert `Phase 11` from a broad roadmap label into one bounded first product-surface package with explicit steps, hard indicators, and reject shapes

#### Step breakdown

##### Step 1: First-surface contract freeze

Goal:

- decide the first surface and freeze what the first slice claims

Decision:

1. the first surface is `snapshot product path`
2. the first slice is limited to:
   - create
   - list
   - delete
3. the first slice does not claim:
   - restore
   - clone
   - broad snapshot workflow readiness
   - `CSI` / `NVMe` / `iSCSI` rebinding

Hard indicators:

1. one explicit first-slice statement exists in `phase-11.md`
2. excluded surfaces/workflows are written down explicitly
3. no active planning text still says the first slice is ambiguous

Reject if:

1. the first slice still includes multiple product surfaces
2. clone/restore are implied without an explicit proof plan
3. the package is broad enough that assignment cannot stay bounded

##### Step 2: Product-truth mapping

Goal:

- define exactly how product-visible snapshot behavior maps to accepted backend snapshot truth

Must make explicit:

1. what product-visible create means on the chosen path
2. what product-visible list must reflect
3. what delete removes and how visibility changes afterward
4. which backend surfaces remain authoritative for snapshot truth
5. which unsupported cases must fail closed

Hard indicators:

1. one authoritative truth chain is written:
   - product request
   - master/volume-server action
   - backend snapshot truth
   - visible product metadata/result
2. one explicit no-drift statement exists:
   - visible snapshot state must match backend snapshot state
3. one explicit unsupported set exists:
   - restore/clone or any deferred workflow

Reject if:

1. product-visible semantics are described only in backend terms
2. visible metadata ownership is ambiguous between master and volume server
3. unsupported cases are left implicit

##### Step 3: Proof package definition

Goal:

- define the minimum test/evidence ladder that can accept the first slice

Required proofs:

1. create proof
   - product-visible snapshot create succeeds on the chosen path
   - created snapshot is visible through list/readback metadata
2. delete proof
   - deleted snapshot disappears from the visible set
3. list coherence proof
   - visible snapshot metadata matches backend truth
4. fail-closed proof
   - invalid or unsupported cases do not imply false success
5. boundedness proof
   - the slice does not overclaim restore/clone/full workflow support

Hard indicators:

1. at least one focused create/list/delete integration package is identified
2. visible-state assertions exist, not only RPC success assertions
3. no-overclaim wording is part of the acceptance text

Reject if:

1. tests prove only local helper logic
2. metadata/reporting truth is not asserted
3. broader workflow claims appear without evidence

#### Recommended first-cut implementation boundary

Prefer touching only the smallest surfaces that define product-visible snapshot truth:

1. master snapshot RPC path
2. volume-server snapshot/list/delete path
3. focused snapshot integration tests

Reference only unless the proof exposes a real mismatch:

1. accepted V2 executor/backend snapshot machinery
2. accepted control-loop closure from `Phase 10`
3. unrelated front-end surfaces (`CSI`, `NVMe`, `iSCSI`)

#### Assignment For `sw`

1. Goal
   - deliver `P1` snapshot product-path rebinding on the chosen path

2. Required outputs
   - one bounded contract statement for create/list/delete
   - one create/list/delete proof package
   - one fail-closed package for invalid/unsupported cases
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09` backend execution semantics
   - do not reopen accepted `Phase 10` control-plane closure
   - do not absorb `CSI`, `NVMe`, `iSCSI`, restore, or clone into the first slice
   - keep claims bounded to snapshot create/list/delete on the chosen path

#### Assignment For `tester`

1. Goal
   - validate that `P1` proves product-visible snapshot rebinding rather than backend-only snapshot behavior

2. Validate
   - create/list/delete visible behavior
   - metadata coherence with backend truth
   - fail-closed unsupported behavior
   - no-overclaim around deferred workflows

3. Reject if
   - evidence is backend-only
   - visible metadata truth is not checked
   - clone/restore/full workflow claims appear without proof

#### Short judgment

`P0` is complete when:

1. the first surface is fixed
2. the first slice steps are explicit
3. hard acceptance indicators are written down
4. `sw` and `tester` can start `P1` without re-deciding scope

---

### P1 Technical Pack

Date: 2026-04-02
Goal: deliver bounded snapshot product-path rebinding on the chosen path so product-visible snapshot create/list/delete behavior is backed by accepted V2 execution/control truth without semantic drift or overclaim

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted real snapshot execution on the chosen path.
`Phase 10` accepted bounded master-driven control-loop closure on that same path.

What is still not accepted is not backend snapshot ability itself.
It is whether the product-visible snapshot path can reuse the existing master/volume-server surfaces without letting older V1-facing wrappers silently become the source of truth.

The first `Phase 11` slice should close that gap for one bounded snapshot surface:

1. create
2. list
3. delete

It should not turn into:

1. restore
2. clone
3. full snapshot workflow productization
4. `CSI` / `NVMe` / `iSCSI` rebinding

##### System reality

Current reality already includes these surfaces:

1. master-facing snapshot RPC wrappers:
   - `MasterServer.CreateBlockSnapshot()`
   - `MasterServer.DeleteBlockSnapshot()`
   - `MasterServer.ListBlockSnapshots()`
2. volume-server execution wrappers:
   - `BlockService.SnapshotBlockVol()`
   - `BlockService.DeleteBlockSnapshot()`
   - `BlockService.ListBlockSnapshots()`
3. backend execution reality under `blockvol`:
   - `CreateSnapshot`
   - `DeleteSnapshot`
   - `ListSnapshots`

The phase question is therefore:

1. which surfaces are reused
2. which surfaces are updated
3. how visible product truth is kept aligned with accepted backend truth
4. how no-overclaim is enforced around unsupported workflow breadth

##### High-level algorithm

Use this rebinding model:

1. product-visible snapshot request enters through the master snapshot RPC surface
2. master resolves the authoritative chosen-path volume entry and delegates to the owning volume server
3. volume server executes the snapshot operation on the accepted block volume path
4. product-visible response/listing reflect the same backend snapshot truth
5. unsupported or invalid cases fail closed rather than implying broader snapshot readiness

##### Pseudo code

```text
on snapshot create/list/delete request:
    validate request at product-facing RPC boundary
    resolve authoritative volume entry from master registry
    call the owning volume-server snapshot surface
    execute against accepted blockvol snapshot machinery
    return only the product-visible metadata supported by the bounded slice

after operation:
    visible snapshot set matches backend snapshot truth
    repeated delete is bounded by explicit idempotent or fail-closed contract
    no restore/clone/full-workflow support is implied
```

##### State / contract

`P1` must make these truths explicit:

1. product-visible snapshot create/list/delete are now part of the accepted chosen-path product proof
2. V1-facing snapshot wrappers may be reused, but they are bounded adapters, not semantic owners
3. accepted backend snapshot truth remains the correctness source underneath the product surface
4. visible snapshot metadata must match backend truth for the chosen path
5. restore/clone/full workflow readiness remain deferred unless a later slice accepts them explicitly

##### Reject shapes

Reject before implementation if the slice:

1. proves only backend snapshot mechanics without product-visible create/list/delete behavior
2. reuses V1 wrappers without explicitly bounding their role
3. silently turns create/list/delete into broader snapshot workflow claims
4. makes master-visible and volume-server-visible snapshot truth diverge
5. absorbs `CSI`, `NVMe`, `iSCSI`, restore, or clone work

#### Layer 2: Execution Core

##### Current gaps `P1` must close

1. current snapshot tests mostly prove master wrapper behavior and volume-server local behavior separately
2. there is not yet one accepted bounded statement that these surfaces together form the first product-visible snapshot slice
3. V1 surface reuse is present in code but not yet explicitly bounded in phase accounting
4. no-overclaim around restore/clone/full workflow support is not yet part of the first-slice acceptance package

##### Reuse / update instructions

1. `weed/server/master_grpc_server_block.go`
   - `update in place`
   - this is the primary product-facing snapshot RPC surface for the bounded slice
   - keep it as a wrapper/resolution layer; do not let it redefine snapshot semantics
   - V1 reuse role:
     - `reuse as bounded adapter`

2. `weed/server/volume_server_block.go`
   - `update in place`
   - this is the primary volume-server execution surface for create/list/delete
   - keep it as the execution adapter over accepted `blockvol` snapshot reality
   - V1 reuse role:
     - `reuse as bounded adapter`

3. `weed/server/master_grpc_server_block_test.go`
   - `update in place`
   - extend/reshape tests so the first slice proves bounded product-visible RPC behavior rather than wrapper-only success

4. `weed/server/volume_grpc_block_test.go`
   - `update in place`
   - keep focused create/list/delete execution checks on the volume-server side

5. `weed/server/*snapshot*test.go`
   - `update in place` if a dedicated bounded integration package is cleaner than overloading existing wrapper tests
   - prefer one focused create/list/delete product-path package rather than many scattered small tests

6. `weed/storage/blockvol/*`
   - `reference only` unless the first product proof exposes a real snapshot-execution mismatch
   - accepted snapshot execution reality should not be casually reopened
   - V1 reuse role:
     - `reuse as execution reality only`

7. `weed/storage/blockvol/v2bridge/*`
   - `reference only`
   - no new bridge semantics should be introduced for this first snapshot slice unless a real mismatch is exposed

8. copy guidance
   - prefer `update in place`
   - no parallel snapshot path
   - every reused V1 surface must be named explicitly in delivery notes

##### Validation focus

Required proofs:

1. create proof
   - product-visible create succeeds on the chosen path
   - response metadata is coherent with the created backend snapshot

2. list coherence proof
   - listed snapshot IDs/metadata match backend truth after create and after delete

3. delete proof
   - delete removes the snapshot from the visible set
   - repeated delete behavior is explicit and bounded

4. fail-closed proof
   - nonexistent volume, missing snapshot, or unsupported preconditions do not imply false success

5. reuse-boundary proof
   - delivery notes identify every reused V1-facing surface and how it is bounded

6. no-overclaim proof
   - acceptance wording does not imply restore/clone/full workflow readiness

Reject if:

1. evidence remains split into unrelated wrapper-local tests with no bounded product statement
2. visible metadata truth is not asserted after create/delete/list
3. slice claims broader snapshot workflow support than it proves
4. reuse of V1 surfaces is left implicit

##### Suggested first cut

1. freeze the bounded product contract for create/list/delete
2. identify the smallest product-visible create/list/delete proof path through:
   - master RPC
   - volume-server execution
   - backend snapshot truth
3. add or reshape tests so metadata truth is checked after create/list/delete
4. write explicit no-overclaim wording for restore/clone/full workflow deferral

##### Assignment For `sw`

1. Goal
   - deliver `P1` snapshot product-path rebinding on the chosen path

2. Required outputs
   - one bounded contract statement for snapshot create/list/delete
   - one focused create/list/delete proof package
   - one fail-closed package for invalid/unsupported cases
   - one explicit V1 reuse note listing:
     - master-facing reused surfaces
     - volume-server reused surfaces
     - backend execution surfaces referenced only
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09` snapshot execution semantics casually
   - do not reopen accepted `Phase 10` control-plane closure
   - do not absorb restore, clone, `CSI`, `NVMe`, or `iSCSI` into the first slice
   - keep all V1 reuse explicit and bounded

##### Assignment For `tester`

1. Goal
   - validate that `P1` proves bounded product-visible snapshot rebinding rather than wrapper-only or backend-only behavior

2. Validate
   - create/list/delete visible behavior
   - metadata coherence with backend truth
   - fail-closed unsupported behavior
   - explicit V1 reuse boundaries in delivery notes
   - no-overclaim around restore/clone/full workflow support

3. Reject if
   - evidence is backend-only or wrapper-only
   - visible metadata truth is not checked
   - V1 reuse is not explicitly bounded
   - restore/clone/full workflow claims appear without proof

#### Short judgment

`P1` is acceptable when:

1. product-visible snapshot create/list/delete are proven on the chosen path
2. visible metadata matches backend truth
3. fail-closed cases are explicit
4. all V1 reuse surfaces are named and bounded
5. broader snapshot workflow support is still explicitly deferred

---

### P1 Completion Record

Date: 2026-04-02
Status: accepted

Accepted contract:

1. `P1` proves bounded snapshot product-path rebinding for:
   - create
   - list
   - delete
2. accepted proof is product-visible and routed through the real master and volume-server adapter chain
3. `P1` does not claim:
   - restore
   - clone
   - full snapshot workflow readiness
   - `CSI`/transport rebinding

Accepted evidence:

1. `weed/server/qa_block_snapshot_product_test.go`
   - `TestP11P1_SnapshotCreate`
   - `TestP11P1_SnapshotListCoherence`
   - `TestP11P1_SnapshotDelete`
   - `TestP11P1_SnapshotFailClosed`
2. the accepted chain includes:
   - master snapshot RPC entry
   - real block registry resolution
   - real volume-server snapshot adapter methods
   - accepted `blockvol` snapshot execution
3. metadata coherence is explicitly checked between create response and list/readback truth

Reuse note:

1. `weed/server/master_grpc_server_block.go`
   - `reuse as bounded adapter`
2. `weed/server/volume_server_block.go`
   - `reuse as bounded adapter`
3. `weed/storage/blockvol/*`
   - `reuse as execution reality only`
4. `weed/storage/blockvol/v2bridge/*`
   - `reference only`

Residual notes:

1. `P1` closure does not reduce the need for `P2`; it only proves snapshot product rebinding
2. `CSI` snapshot, expand, and transport-specific publish behavior remain outside accepted `P1` scope

---

### P2 Technical Pack

Date: 2026-04-02
Goal: deliver bounded `CSI` controller/node rebinding on the chosen path so product-visible CSI volume lifecycle behavior is backed by accepted V2 execution/control truth without letting older CSI/V1 wrappers silently become semantic owners

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted execution truth on the chosen path.
`Phase 10` accepted bounded control-plane closure on that same path.
`P1` accepted a first product-visible rebinding slice for snapshots.

What is still not accepted is whether the `CSI` product surface for the main volume lifecycle is coherently bound to that same chosen path.

The first `P2` slice should close that gap for one bounded `CSI` surface:

1. `CreateVolume`
2. `DeleteVolume`
3. `ControllerPublishVolume`
4. `NodeStageVolume`
5. `NodePublishVolume`
6. `NodeUnpublishVolume`
7. `NodeUnstageVolume`

It should not turn into:

1. `CSI` snapshot closure
2. `CSI` expand closure
3. `NVMe` transport preference or failover closure
4. broad Kubernetes readiness/productization
5. multi-node/topology breadth beyond the chosen bounded path

##### System reality

Current reality already includes these surfaces:

1. CSI controller wrapper surface:
   - `weed/storage/blockvol/csi/controller.go`
2. CSI node wrapper surface:
   - `weed/storage/blockvol/csi/node.go`
3. CSI driver/server assembly:
   - `weed/storage/blockvol/csi/server.go`
4. CSI backend bridge:
   - `weed/storage/blockvol/csi/volume_backend.go`
5. local execution helper:
   - `weed/storage/blockvol/csi/volume_manager.go`
6. accepted master-backed block product/control surfaces underneath the backend bridge:
   - `CreateBlockVolume`
   - `DeleteBlockVolume`
   - `LookupBlockVolume`

The phase question is therefore:

1. which of these CSI/V1 surfaces are only wrappers
2. which of them are allowed to remain bridges/adapters
3. how controller-visible truth and node-visible truth are kept aligned with accepted chosen-path volume truth
4. how no-overclaim is enforced around unproven CSI breadth

##### High-level algorithm

Use this rebinding model:

1. CSI controller receives create/delete/publish requests
2. controller delegates through the backend bridge to the accepted master-backed chosen-path volume surface
3. master returns authoritative target information for the chosen path
4. CSI node consumes only that published access truth and performs bounded stage/publish work
5. teardown uses the same bounded truth and removes staged/published state coherently
6. unsupported or incomplete publish/volume context fails closed rather than implying readiness

##### Pseudo code

```text
on CSI controller create/delete/publish:
    validate bounded CSI request contract
    call backend bridge
    backend bridge calls accepted master-backed volume surface
    return only the target and metadata truth proven by this slice

on CSI node stage/publish/unstage/unpublish:
    consume publish_context or volume_context from the bounded controller truth
    resolve transport/device only for the chosen bounded path
    stage and publish coherently
    teardown coherently and preserve retry semantics on failure

after operation:
    controller-visible truth and node-visible truth match accepted chosen-path volume truth
    missing or partial target info fails closed
    no snapshot/expand/NVMe/K8s-breadth claim is implied
```

##### State / contract

`P2` must make these truths explicit:

1. CSI controller/node lifecycle is now the active product-surface rebinding target
2. CSI wrappers may be reused, but only as bounded adapters over accepted chosen-path truth
3. `volume_backend.go` is a bounded bridge, not a semantic owner
4. local `volume_manager.go` behavior may be used as execution reality only where the chosen proof explicitly allows it
5. node-visible staging/publishing must not drift from controller/master-provided target truth
6. CSI snapshot, expand, NVMe preference, and broader cluster readiness remain deferred unless a later slice accepts them explicitly

##### Reject shapes

Reject before implementation if the slice:

1. proves only controller-local or node-local helper behavior with no chosen-path coherence statement
2. lets local manager shortcuts replace accepted master-backed truth for the active slice
3. silently turns bounded CSI lifecycle proof into snapshot/expand/NVMe/K8s readiness claims
4. leaves publish-context ownership or fail-closed behavior implicit

#### Layer 2: Execution Core

##### Current gaps `P2` must close

1. current CSI tests are broad and mixed across controller, node, local mode, NVMe, snapshot, and adversarial cases
2. there is not yet one accepted bounded statement that the main CSI controller/node lifecycle is rebound to the accepted chosen path
3. reuse of CSI/V1 surfaces is present in code but not yet explicitly bounded in phase accounting
4. no-overclaim around snapshot/expand/NVMe/K8s breadth is not yet part of an accepted `P2` package

##### Reuse / update instructions

1. `weed/storage/blockvol/csi/controller.go`
   - `update in place`
   - this is the primary CSI controller product surface for the bounded slice
   - keep it as a wrapper over the backend bridge; do not let it define backend truth
   - V1 reuse role:
     - `reuse as bounded adapter`

2. `weed/storage/blockvol/csi/node.go`
   - `update in place`
   - this is the primary CSI node product surface for stage/publish lifecycle proof
   - keep retry/fail-closed behavior explicit; do not let local fallback silently replace chosen-path truth
   - V1 reuse role:
     - `reuse as bounded adapter`

3. `weed/storage/blockvol/csi/server.go`
   - `update in place`
   - driver assembly and mode wiring may be reused
   - do not let mode convenience become product-scope inflation
   - V1 reuse role:
     - `reuse as bounded adapter`

4. `weed/storage/blockvol/csi/volume_backend.go`
   - `update in place`
   - this is the key backend bridge from CSI to accepted master-backed volume truth
   - keep the bridge explicit; do not let it become a second semantic owner
   - V1 reuse role:
     - `reuse as bounded bridge`

5. `weed/storage/blockvol/csi/volume_manager.go`
   - `reference only` unless the bounded proof exposes a real execution mismatch
   - local manager behavior may support tests or local execution reality, but should not redefine the active chosen-path proof
   - V1 reuse role:
     - `reuse as execution reality only`

6. `weed/server/master_grpc_server_block.go`
   - `reference only` unless the bounded CSI proof exposes a real mismatch
   - accepted master-facing volume truth already exists underneath the CSI bridge
   - V1 reuse role:
     - `reuse as bounded adapter`

7. `weed/server/volume_server_block.go`
   - `reference only` unless a real mismatch is exposed through CSI publication or chosen-path target truth
   - do not reopen accepted control/execution surfaces casually
   - V1 reuse role:
     - `reuse as bounded adapter`

8. `weed/storage/blockvol/csi/*test.go`
   - `update in place`
   - prefer one focused bounded lifecycle proof package rather than scattering acceptance across many unrelated tests

9. copy guidance
   - prefer `update in place`
   - no parallel CSI path
   - every reused V1 surface must be named explicitly in delivery notes

##### Validation focus

Required proofs:

1. controller create/publish proof
   - controller create succeeds on the chosen path
   - publish context reflects accepted master-backed target truth

2. node stage/publish proof
   - node stages and publishes using that controller/master-provided truth
   - chosen-path access contract is coherent end-to-end

3. teardown proof
   - unpublish/unstage/delete complete coherently
   - retry semantics remain bounded and explicit on failure

4. fail-closed proof
   - missing or partial publish/volume context does not imply false success
   - invalid or missing target information remains explicit

5. reuse-boundary proof
   - delivery notes identify every reused CSI/V1-facing surface and how it is bounded

6. no-overclaim proof
   - acceptance wording does not imply CSI snapshot, expand, NVMe preference, or broad K8s readiness

Reject if:

1. evidence remains split into unrelated unit tests with no bounded product statement
2. controller-visible truth and node-visible truth are not asserted against the same chosen-path source
3. local-manager convenience silently becomes the active proof path
4. slice claims broader CSI readiness than it proves

##### Suggested first cut

1. freeze the bounded CSI lifecycle contract for create/delete/publish/stage/publish/unstage/unpublish
2. identify the smallest chosen-path proof chain through:
   - CSI controller
   - backend bridge
   - accepted master-backed target truth
   - CSI node
3. add or reshape tests so controller-visible target info and node-consumed target info are asserted together
4. write explicit no-overclaim wording for CSI snapshot/expand/NVMe/K8s deferral

##### Assignment For `sw`

1. Goal
   - deliver bounded `P2` CSI controller/node rebinding on the chosen path

2. Required outputs
   - one bounded contract statement for CSI lifecycle in scope
   - one focused controller/node lifecycle proof package
   - one fail-closed package for missing/partial target information
   - one explicit V1 reuse note listing:
     - CSI wrapper surfaces
     - backend bridge surfaces
     - execution-reality-only surfaces
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09`, `Phase 10`, or accepted `P1` semantics casually
   - do not absorb CSI snapshot, expand, `NVMe`, or broad K8s/productization work into the first CSI slice
   - keep all V1/CSI reuse explicit and bounded

##### Assignment For `tester`

1. Goal
   - validate that `P2` proves bounded product-visible CSI rebinding rather than local-manager-only or wrapper-only behavior

2. Validate
   - controller/node lifecycle coherence
   - publish-context coherence with accepted chosen-path truth
   - fail-closed handling for missing/partial target information
   - explicit V1 reuse boundaries in delivery notes
   - no-overclaim around CSI snapshot, expand, NVMe, and broader K8s readiness

3. Reject if
   - evidence is local-only or wrapper-only
   - controller truth and node truth are not checked together
   - V1/CSI reuse is not explicitly bounded
   - broader CSI readiness claims appear without proof

#### Short judgment

`P2` is acceptable when:

1. controller create/publish and node stage/publish/teardown are proven on the chosen path
2. controller-visible target truth and node-consumed target truth match
3. fail-closed cases are explicit
4. all reused CSI/V1 surfaces are named and bounded
5. broader CSI snapshot/expand/NVMe/K8s readiness is still explicitly deferred

---

### P2 Completion Record

Date: 2026-04-02
Status: accepted

Accepted contract:

1. `P2` proves bounded CSI controller/node lifecycle rebinding for:
   - `CreateVolume`
   - `DeleteVolume`
   - `ControllerPublishVolume`
   - `NodeStageVolume`
   - `NodePublishVolume`
   - `NodeUnpublishVolume`
   - `NodeUnstageVolume`
2. accepted proof uses the real master-backed create/lookup/delete path for the controller side
3. accepted proof uses `mgr=nil` on the node side so published target truth is the only viable source
4. `P2` does not claim:
   - CSI snapshot closure
   - CSI expand closure
   - NVMe preference/failover closure
   - broad Kubernetes or product readiness

Accepted evidence:

1. `weed/server/qa_block_csi_lifecycle_test.go`
   - `TestP11P2_CSI_MasterProduced_PublishContext`
   - `TestP11P2_CSI_FullLifecycle`
   - `TestP11P2_CSI_FailClosed_NoContext`
   - `TestP11P2_CSI_FailClosed_MissingVolume`
2. accepted chain includes:
   - CSI controller wrapper entry
   - backend bridge calling real `MasterServer.CreateBlockVolume()`
   - real `MasterServer.LookupBlockVolume()`
   - real `MasterServer.DeleteBlockVolume()`
   - real block registry and allocation path
   - node-side consumption of published target truth with `mgr=nil`
3. publish-context proof explicitly asserts:
   - master-produced `iscsiAddr`
   - master-produced `iqn`
4. lifecycle proof explicitly asserts:
   - `isLocal=false`
   - staged address matches published address
   - teardown errors are checked
   - real `.blk` removal after delete
5. fail-closed proof explicitly asserts:
   - `FailedPrecondition` with missing context on `mgr=nil` node
   - `NotFound` on missing volume publish
   - idempotent delete on missing volume

Reuse note:

1. `weed/storage/blockvol/csi/controller.go`
   - `reuse as bounded adapter`
2. `weed/storage/blockvol/csi/node.go`
   - `reuse as bounded adapter`
3. `weed/storage/blockvol/csi/volume_backend.go`
   - `reuse as bounded bridge`
4. `weed/server/master_grpc_server_block.go`
   - `reuse as bounded adapter`
5. `weed/storage/blockvol/csi/volume_manager.go`
   - `reference only` for the accepted proof path
6. `weed/storage/blockvol/csi/export_test_helpers.go`
   - test-only support surface

Residual notes:

1. `P2` closure does not imply CSI snapshot or expand readiness
2. `P2` closure does not imply NVMe publication/failover truth closure
3. `P3` remains necessary to close front-end publication/address/naming truth

---

### P3 Technical Pack

Date: 2026-04-02
Goal: deliver bounded `NVMe` / `iSCSI` front-end publication rebinding on the chosen path so product-visible access metadata matches accepted backend/control truth without letting wrapper-local field plumbing become the semantic owner

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted execution truth on the chosen path.
`Phase 10` accepted bounded control-plane closure on that same path.
`P1` accepted snapshot product rebinding.
`P2` accepted bounded CSI controller/node lifecycle rebinding.

What is still not accepted is whether front-end publication truth itself is coherent for the chosen path:

1. are `iSCSI` publication fields coherent at create and lookup
2. are `NVMe` publication fields coherent at create and lookup
3. do heartbeat refresh and failover preserve or update those fields correctly
4. does fallback behavior remain explicit when `NVMe` is absent

The first `P3` slice should close that gap for bounded front-end publication truth:

1. `CreateBlockVolume` publication fields
2. `LookupBlockVolume` publication fields
3. heartbeat-driven publication refresh/reconstruction
4. failover publication truth update

It should not turn into:

1. full runtime initiator behavior proof
2. end-to-end transport performance proof
3. broad NVMe superiority/product positioning claims
4. broad cluster hardening or soak readiness

##### System reality

Current reality already includes these surfaces:

1. master-facing publication surfaces:
   - `weed/server/master_grpc_server_block.go`
2. registry publication truth carriers:
   - `weed/server/master_block_registry.go`
3. volume-server publication source:
   - `weed/server/volume_grpc_block.go`
4. block-service publication helpers:
   - `weed/server/volume_server_block.go`
5. existing publication-focused test surfaces:
   - `weed/server/qa_block_nvme_publication_test.go`

The phase question is therefore:

1. which fields are authoritative publication truth on the chosen path
2. how publication truth flows from volume server to registry to create/lookup responses
3. how failover and restart change or reconstruct that truth
4. how fallback is kept explicit when front-end capability differs by server

##### High-level algorithm

Use this rebinding model:

1. volume server allocates or heartbeats with front-end publication fields
2. master registry stores or refreshes those fields as chosen-path truth
3. master create/lookup responses publish the same front-end truth
4. failover moves publication truth to the new primary and clears stale ownership
5. mixed-capability or fallback behavior remains explicit in published metadata

##### Pseudo code

```text
on create/lookup:
    resolve chosen-path primary in master registry
    return the current publication truth for that primary
    include fallback publication fields where supported

on heartbeat refresh or master restart:
    rebuild or refresh publication fields from accepted volume-server truth

on failover:
    promote new primary
    switch publication fields to the new primary's front-end truth
    clear stale publication ownership from the old primary

after operation:
    create-visible, lookup-visible, and registry-visible publication truth match
    fallback behavior is explicit
    no runtime/performance closure is implied
```

##### State / contract

`P3` must make these truths explicit:

1. front-end publication truth is now the active product-surface rebinding target
2. registry publication fields may carry truth, but only as bounded carriers under accepted chosen-path semantics
3. volume-server allocation/heartbeat publication fields are bounded publication sources, not independent semantic owners
4. create-visible and lookup-visible publication truth must remain coherent through failover and restart reconstruction
5. absence of `NVMe` must produce explicit fallback behavior rather than false capability claims

##### Reject shapes

Reject before implementation if the slice:

1. proves only field presence without create/lookup/failover coherence
2. lets stale publication truth survive failover or restart reconstruction
3. silently turns publication-field proof into runtime transport/performance claims
4. leaves fallback semantics implicit

#### Layer 2: Execution Core

##### Current gaps `P3` must close

1. publication fields exist in code and tests, but have not yet been accepted as one bounded chosen-path closure package
2. `NVMe` and `iSCSI` publication truth still risks being treated as scattered field plumbing rather than one coherent product surface
3. failover/restart reconstruction proof exists directionally, but not yet as the active accepted slice
4. no-overclaim around full transport runtime/performance still needs explicit phase accounting

##### Reuse / update instructions

1. `weed/server/master_grpc_server_block.go`
   - `update in place`
   - this is the primary master-facing publication surface for create/lookup truth
   - keep it as a bounded adapter over registry truth
   - V1 reuse role:
     - `reuse as bounded adapter`

2. `weed/server/master_block_registry.go`
   - `update in place`
   - this is the bounded truth carrier for publication fields across heartbeat and failover
   - do not let registry field caching become an unexamined semantic shortcut
   - V1 reuse role:
     - `reuse as bounded truth carrier`

3. `weed/server/volume_grpc_block.go`
   - `update in place`
   - this is the primary publication source for allocate responses
   - keep publication derivation explicit and canonical
   - V1 reuse role:
     - `reuse as publication source only`

4. `weed/server/volume_server_block.go`
   - `reference only` unless a real publication mismatch is exposed
   - heartbeat publication helpers may be reused as bounded publication sources
   - V1 reuse role:
     - `reuse as publication source only`

5. `weed/server/qa_block_nvme_publication_test.go`
   - `update in place`
   - prefer one bounded create/lookup/failover/restart publication package rather than scattered field tests

6. `weed/storage/blockvol/csi/controller.go`
   - `reference only`
   - CSI remains a downstream consumer of the accepted publication truth in `P3`

7. copy guidance
   - prefer `update in place`
   - no parallel publication path
   - every reused V1/publication surface must be named explicitly in delivery notes

##### Validation focus

Required proofs:

1. create/lookup proof
   - create publishes coherent `iSCSI` and, where enabled, `NVMe` publication fields
   - lookup returns the same chosen-path publication truth

2. failover proof
   - publication truth moves to the new primary after failover
   - stale publication truth from the old primary does not survive

3. heartbeat/restart reconstruction proof
   - publication truth can be refreshed or rebuilt from accepted heartbeat data

4. fallback/mixed-capability proof
   - clusters with and without `NVMe` capability expose explicit coherent fallback behavior

5. reuse-boundary proof
   - delivery notes identify every publication surface touched and how it is bounded

6. no-overclaim proof
   - acceptance wording does not imply real initiator runtime, performance, or broad production readiness

Reject if:

1. evidence remains field-local and does not prove create/lookup/failover/restart coherence
2. publication truth is not checked across at least two user-visible surfaces
3. fallback behavior is implicit
4. slice claims transport runtime or performance closure without proof

##### Suggested first cut

1. freeze the bounded publication contract for create/lookup/failover/restart
2. identify the smallest proof chain through:
   - volume-server publication source
   - registry truth carrier
   - master create/lookup responses
3. extend tests so `iSCSI` and `NVMe` fields are checked together where relevant
4. write explicit no-overclaim wording for runtime transport/performance deferral

##### Assignment For `sw`

1. Goal
   - deliver bounded `P3` front-end publication rebinding on the chosen path

2. Required outputs
   - one bounded contract statement for publication/address/naming truth
   - one focused create/lookup/failover/restart proof package
   - one explicit fallback package for mixed-capability or no-`NVMe` behavior
   - one explicit V1 reuse note listing:
     - master publication surfaces
     - registry truth carriers
     - volume-server publication sources
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09`, `Phase 10`, `P1`, or `P2` semantics casually
   - do not absorb full runtime transport proof, performance claims, or broad production hardening into the first publication slice
   - keep all V1/publication reuse explicit and bounded

##### Assignment For `tester`

1. Goal
   - validate that `P3` proves bounded publication rebinding rather than scattered field plumbing

2. Validate
   - create/lookup publication coherence
   - failover publication update coherence
   - heartbeat/restart reconstruction coherence
   - explicit fallback behavior when `NVMe` is absent
   - explicit V1/publication reuse boundaries in delivery notes
   - no-overclaim around runtime transport/performance/product readiness

3. Reject if
   - evidence is field-local only
   - stale publication truth can survive failover or restart
   - fallback behavior is not explicit
   - broader transport/runtime claims appear without proof

#### Short judgment

`P3` is acceptable when:

1. create-visible and lookup-visible publication truth are proven coherent on the chosen path
2. failover and restart reconstruction update publication truth correctly
3. fallback behavior is explicit
4. all reused publication/V1 surfaces are named and bounded
5. broader runtime/performance readiness is still explicitly deferred

---

### P3 Completion Record

Date: 2026-04-02
Status: accepted

Accepted contract:

1. `P3` proves bounded front-end publication rebinding for:
   - create/lookup publication coherence
   - failover publication switch
   - heartbeat-driven publication reconstruction
   - no-`NVMe` fallback behavior
2. accepted proof closes publication/address truth on the chosen path without claiming runtime transport closure
3. `P3` does not claim:
   - full initiator/runtime transport proof
   - performance claims
   - broad production readiness
   - any workflow closure beyond bounded publication truth

Accepted evidence:

1. `weed/server/qa_block_publication_test.go`
   - `TestP11P3_CreateLookup_PublicationCoherence`
   - `TestP11P3_Failover_PublicationSwitches`
   - `TestP11P3_HeartbeatReconstruction`
   - `TestP11P3_NoNVMe_Fallback`
2. accepted chain includes:
   - volume-server allocation publication fields
   - master registry publication truth
   - `CreateBlockVolume` publication response
   - `LookupBlockVolume` publication response
   - failover-driven publication switch
   - `UpdateFullHeartbeat()`-driven reconstruction
3. fallback proof explicitly checks `iSCSI` remains present while `NVMe` fields remain empty when disabled

Reuse note:

1. `weed/server/master_grpc_server_block.go`
   - `reuse as bounded adapter`
2. `weed/server/master_block_registry.go`
   - `reuse as bounded truth carrier`
3. `weed/server/volume_grpc_block.go`
   - `reuse as publication source only`
4. `weed/server/volume_server_block.go`
   - `reference only`
5. `weed/storage/blockvol/csi/controller.go`
   - `reference only` as downstream consumer

Residual notes:

1. `P3` closure does not imply runtime initiator correctness or transport-performance leadership
2. `P3` closure does not close restore/clone workflow behavior
3. `P4` remains necessary for bounded restore workflow closure

---

### P4 Technical Pack

Date: 2026-04-02
Goal: deliver bounded snapshot restore workflow closure on the chosen path so product-visible restore behavior is backed by accepted snapshot/execution/control truth without silently inflating scope to clone or broad workflow productization

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted snapshot execution truth.
`Phase 10` accepted bounded control-plane closure.
`P1` accepted snapshot create/list/delete product rebinding.
`P2` accepted CSI controller/node lifecycle rebinding.
`P3` accepted front-end publication truth rebinding.

What is still not accepted is whether the next bounded snapshot workflow step, `restore`, is coherently rebound onto the same chosen path.

Current system reality shows:

1. real backend restore execution exists:
   - `blockvol.RestoreSnapshot`
2. accepted product-facing snapshot create/list/delete surfaces already exist
3. a visible product-facing `clone` workflow surface is not yet evident in the current implementation

Therefore the first `P4` slice should be:

1. snapshot `restore`

and should explicitly not turn into:

1. snapshot `clone`
2. broad snapshot workflow productization
3. restarting accepted snapshot/backend/control semantics from scratch

##### System reality

Current reality already includes these surfaces:

1. product-facing snapshot surfaces:
   - `weed/server/master_grpc_server_block.go`
   - `weed/server/volume_server_block.go`
2. backend restore execution:
   - `weed/storage/blockvol/blockvol.go`
   - `RestoreSnapshot`
3. accepted snapshot product-path test package:
   - `weed/server/qa_block_snapshot_product_test.go`

The phase question is therefore:

1. what the product-visible restore entry surface is
2. how restore maps onto accepted backend restore execution
3. how post-restore visible truth is checked
4. how `clone` remains explicitly deferred

##### High-level algorithm

Use this rebinding model:

1. product-visible restore request enters through the chosen restore entry surface
2. master resolves the authoritative volume and selected snapshot truth
3. volume server executes restore against accepted `blockvol.RestoreSnapshot`
4. post-restore visible state is checked against the selected snapshot truth
5. unsupported or invalid restore conditions fail closed rather than implying broader workflow readiness

##### Pseudo code

```text
on restore request:
    validate volume + snapshot selection
    resolve authoritative chosen-path volume entry
    call bounded restore adapter surface
    execute accepted blockvol restore
    verify post-restore visible truth against selected snapshot

after restore:
    writes after the selected snapshot are lost as designed
    visible snapshot/workflow state is explicit
    no clone/full-workflow support is implied
```

##### State / contract

`P4` must make these truths explicit:

1. restore is now the active bounded workflow target
2. backend `RestoreSnapshot` remains execution reality, not product-truth ownership
3. product-visible restore must make destructive semantics explicit
4. post-restore visible truth must match the selected snapshot truth
5. `clone` remains deferred unless a real product-facing clone surface is separately accepted into scope

##### Reject shapes

Reject before implementation if the slice:

1. proves only backend-local restore mechanics with no product-visible restore behavior
2. hides destructive semantics
3. does not assert post-restore visible truth
4. quietly absorbs clone or broad workflow product readiness

#### Layer 2: Execution Core

##### Current gaps `P4` must close

1. restore execution exists in backend code, but has not yet been accepted as a bounded product/workflow slice
2. `P1` closed create/list/delete only; restore remains downstream workflow work
3. the product-facing restore entry surface and post-restore visibility contract are not yet frozen in phase accounting
4. clone is still better treated as explicitly deferred because no real product-facing clone surface is evident in the current code

##### Reuse / update instructions

1. `weed/server/master_grpc_server_block.go`
   - `update in place` if this file becomes the product-facing restore entry surface
   - keep it as a bounded adapter/resolution layer
   - V1 reuse role:
     - `reuse as bounded adapter`

2. `weed/server/volume_server_block.go`
   - `update in place` if this file becomes the volume-server restore adapter
   - keep it as a bounded execution adapter over accepted backend restore truth
   - V1 reuse role:
     - `reuse as bounded adapter`

3. `weed/server/*snapshot*test.go`
   - `update in place`
   - prefer one focused restore workflow proof package rather than scattering restore checks across backend tests

4. `weed/storage/blockvol/blockvol.go`
   - `reference only` unless the product-visible restore proof exposes a real restore-execution mismatch
   - backend restore remains accepted execution reality
   - V1 reuse role:
     - `reuse as execution reality only`

5. `weed/storage/blockvol/*restore*` and snapshot backend tests
   - `reference only`
   - use them to understand existing restore guarantees, not as product closure evidence

6. clone-related work
   - `reference only`
   - do not create clone acceptance text unless a real product-facing clone surface is added to scope

7. copy guidance
   - prefer `update in place`
   - no parallel restore path
   - every reused restore-facing surface must be named explicitly in delivery notes

##### Validation focus

Required proofs:

1. restore success proof
   - product-visible restore succeeds on the chosen path
   - post-restore visible truth matches the selected snapshot

2. destructive-semantics proof
   - data written after the selected snapshot is lost as designed
   - this is stated explicitly in both tests and delivery wording

3. fail-closed proof
   - missing snapshot / missing volume / unsupported conditions do not imply false success

4. post-restore coherence proof
   - visible workflow state after restore is coherent with backend truth

5. reuse-boundary proof
   - delivery notes identify every restore-facing V1 surface and how it is bounded

6. no-overclaim proof
   - acceptance wording does not imply clone or broad snapshot workflow readiness

Reject if:

1. evidence remains backend-only
2. post-restore visible truth is not checked
3. destructive restore semantics are left implicit
4. clone or broader workflow claims appear without proof

##### Suggested first cut

1. freeze the bounded restore contract
2. identify the smallest product-visible restore chain through:
   - product-facing restore entry
   - volume-server restore adapter
   - accepted `blockvol.RestoreSnapshot`
3. add or reshape tests so post-restore state is compared with selected snapshot truth
4. write explicit no-overclaim wording for clone deferral

##### Assignment For `sw`

1. Goal
   - deliver bounded `P4` snapshot restore workflow closure on the chosen path

2. Required outputs
   - one bounded contract statement for restore
   - one focused restore proof package
   - one fail-closed package for missing snapshot/volume or unsupported conditions
   - one explicit V1 reuse note listing:
     - master-facing restore surfaces
     - volume-server restore surfaces
     - backend restore execution surfaces referenced only
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `Phase 09`, `Phase 10`, `P1`, `P2`, or `P3` semantics casually
   - do not absorb clone or broad workflow productization into the first restore slice
   - keep all V1/restore reuse explicit and bounded

##### Assignment For `tester`

1. Goal
   - validate that `P4` proves bounded product-visible restore closure rather than backend-only restore behavior

2. Validate
   - restore-visible truth coherence
   - destructive restore semantics
   - fail-closed behavior for missing snapshot/volume or unsupported conditions
   - explicit V1 reuse boundaries in delivery notes
   - no-overclaim around clone and broader workflow support

3. Reject if
   - evidence is backend-only
   - post-restore visible truth is not checked
   - destructive semantics are not explicit
   - clone/broader workflow claims appear without proof

#### Short judgment

`P4` is acceptable when:

1. product-visible restore is proven on the chosen path
2. post-restore visible truth matches selected snapshot truth
3. destructive restore semantics are explicit
4. all reused restore-facing V1 surfaces are named and bounded
5. clone remains explicitly deferred unless separately proven

---

### P4 Completion Record

Date: 2026-04-02
Status: accepted

Accepted contract:

1. `P4` proves bounded snapshot restore workflow closure for:
   - restore success
   - destructive restore semantics
   - post-restore visible truth
   - fail-closed behavior for missing snapshot/volume
2. accepted proof routes restore through:
   - master restore RPC
   - real volume-server restore adapter
   - accepted backend `blockvol.RestoreSnapshot`
3. `P4` does not claim:
   - clone support
   - broad snapshot workflow readiness
   - reopening accepted `Phase 09` / `Phase 10` semantics

Accepted evidence:

1. `weed/server/qa_block_restore_test.go`
   - `TestP11P4_RestoreSuccess`
   - `TestP11P4_DestructiveSemantics`
   - `TestP11P4_FailClosed_MissingSnapshot`
   - `TestP11P4_FailClosed_MissingVolume`
2. accepted chain includes:
   - `MasterServer.RestoreBlockSnapshot`
   - real registry resolution
   - `BlockService.RestoreBlockSnapshot`
   - `blockvol.RestoreSnapshot`
3. restore success proof explicitly checks:
   - `LBA 0 == 0x00` after restore
   - `LBA 5 == 0x00` after restore
4. destructive-semantics proof explicitly checks:
   - all snapshots removed after restore

Reuse note:

1. `weed/server/master_grpc_server_block.go`
   - `reuse as bounded adapter`
2. `weed/server/volume_server_block.go`
   - `reuse as bounded adapter`
3. `weed/storage/blockvol/blockvol.go`
   - `reuse as execution reality only`
4. `clone`-related work
   - `reference only`
   - explicitly deferred

Residual notes:

1. `clone` remains outside accepted scope because no real product-facing clone surface is yet accepted into scope
2. restore closure completes the bounded workflow work needed inside `Phase 11`

---

### Phase 11 Completion Record

Date: 2026-04-02
Status: accepted and closed

Completion judgment:

`Phase 11` is complete because it closed the selected product-surface rebinding work promised after accepted backend/control closure:

1. `P1` closed bounded snapshot create/list/delete product rebinding
2. `P2` closed bounded `CSI` controller/node lifecycle rebinding
3. `P3` closed bounded `NVMe` / `iSCSI` publication/address truth rebinding
4. `P4` closed bounded snapshot restore workflow rebinding

What `Phase 11` accomplished:

1. product-facing surfaces are now rebound onto the accepted V2-backed chosen path in a bounded, no-overclaim way
2. accepted product closure was achieved without reopening accepted `Phase 09` execution or `Phase 10` control-plane truth
3. residual work has shifted from product rebinding to production hardening

What remains explicitly outside `Phase 11` closure:

1. `clone`
2. full runtime initiator proof
3. soak / restart / repeated disturbance hardening
4. operational diagnosis quality and production blockers accounting

Next phase:

1. start `Phase 12 P0` production-hardening planning
