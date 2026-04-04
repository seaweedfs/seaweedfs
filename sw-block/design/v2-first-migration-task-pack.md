# V2 First Migration Task Pack

Date: 2026-04-04
Status: delivered

## Purpose

This note turns the first separation batch into validate-able engineering tasks.

Each task must name:

1. source
2. destination
3. authority rule
4. adapter boundary
5. acceptance criteria
6. validation proof

The goal is to make separation work parallelizable without letting `V1`
runtime-owner behavior silently leak back in.

## Shared Rules

All tasks in this pack inherit these rules:

1. `sw-block` must not directly import `weed/storage/blockvol`
2. `weed/` may implement ports, but must not redefine semantic truth
3. each task must move one boundary, not redesign the whole runtime
4. compatibility guards may stay, but must not be treated as semantic-authority
   proof

Existing landing zones already exist:

1. `sw-block/bridge/blockvol`
2. `sw-block/bridge/blockvol/control_adapter.go`
3. `sw-block/bridge/blockvol/contract.go`

So Task A does not begin with package creation. It begins with canonical-rule
consolidation into the existing `sw-block` bridge layer.

## Task A: Canonical Assignment Translation

### Goal

Make `sw-block` own the canonical helper rules for:

1. replica identity
2. recovery-target mapping
3. engine replica-assignment packaging

### Source

1. `weed/storage/blockvol/v2bridge/control.go`
2. `weed/server/volume_server_block.go`

### Destination

1. `sw-block/bridge/blockvol/control_adapter.go`

### Authority Rule

This is semantic translation logic, so the canonical rule belongs in
`sw-block`, not in product adapters.

### Adapter Boundary

`weed/` may still:

1. parse `BlockVolumeAssignment`
2. decide which source fields exist on the wire/runtime side

`weed/` must not separately redefine:

1. `ReplicaID = <volume>/<server>`
2. `replica -> catchup`
3. `rebuilding -> rebuild`

### Acceptance

1. `sw-block` exports canonical helpers for identity and recovery-target mapping
2. `weed/storage/blockvol/v2bridge/control.go` and
   `weed/server/volume_server_block.go` both use those helpers
3. no direct address-derived identity logic remains in adapter code

### Validation

1. `go test ./sw-block/bridge/blockvol`
2. `go test ./weed/storage/blockvol/v2bridge -run "TestControl_|TestBridge_"`
3. focused server path still passes:
   - `go test ./weed/server -run "TestBlockService_ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart)"`

### Current proof anchors

1. `TestControlAdapter_StableIdentity`
2. `TestControlAdapter_RebuildRoleMapping`
3. `TestControl_PrimaryAssignment_StableServerID`
4. `TestControl_RebuildAssignment`

## Task B: Reader Port Separation

### Goal

Separate retained-history state reading as a pure execution muscle behind a
stable `sw-block` port.

### Source

1. `weed/storage/blockvol/v2bridge/reader.go`

### Destination

1. contract remains in `sw-block/bridge/blockvol/contract.go`
2. implementation stays thin in `weed/storage/blockvol/v2bridge/reader.go`
3. future code landing zone, if needed:
   - `sw-block/bridge/blockvol/runtime`
   - or equivalent execution package under `sw-block`

### Authority Rule

Reader logic is not semantic authority. It must only read backend facts and
project them into the engine-facing retained-history shape.

### Adapter Boundary

`weed/` may:

1. read `BlockVol.StatusSnapshot()`
2. map backend fields into the contract shape

`weed/` must not:

1. reinterpret durability meaning
2. patch semantic fallbacks into the reader

### Acceptance

1. `BlockVolReader` contract stays complete and stable in `sw-block`
2. `Reader` remains a thin adapter over real `BlockVol`
3. `StorageAdapter.GetRetainedHistory()` depends only on the contract, not on
   weed internals

### Validation

1. `go test ./sw-block/bridge/blockvol`
2. `go test ./weed/storage/blockvol/v2bridge -run "TestReader_"`

### Current proof anchors

1. `TestStorageAdapter_RetainedHistoryFromReader`
2. `TestReader_RealBlockVol_StatusSnapshot`
3. `TestReader_RealBlockVol_HeadAdvancesWithWrites`

## Task C: Pinner Port Separation

### Goal

Separate WAL/snapshot/full-base hold mechanics as execution muscles behind a
stable `sw-block` pinning port.

### Source

1. `weed/storage/blockvol/v2bridge/pinner.go`

### Destination

1. contract remains in `sw-block/bridge/blockvol/contract.go`
2. implementation stays thin in `weed/storage/blockvol/v2bridge/pinner.go`
3. future migration target is a `sw-block`-owned execution-muscle package, with
   weed-side `BlockVol` binding left thin

### Authority Rule

Hold/release mechanics are execution detail. Recovery policy decides *when* to
hold; pinner only decides *how* to pin in the backend.

### Adapter Boundary

`weed/` may:

1. wire retention floor into `BlockVol`
2. validate concrete hold positions against backend state

`weed/` must not:

1. decide recovery target
2. redefine which boundary is authoritative

### Acceptance

1. `BlockVolPinner` is the sole engine-facing pin contract
2. pinner implementation remains backend-thin and side-effect-local
3. pin lifecycle symmetry is covered in `sw-block` contract tests

### Validation

1. `go test ./sw-block/bridge/blockvol`
2. `go test ./weed/storage/blockvol/v2bridge -run "TestPinner_|TestBridge_"`

### Current proof anchors

1. `TestStorageAdapter_WALPinRejectsRecycled`
2. `TestStorageAdapter_SnapshotPinRejectsUntrusted`
3. `TestStorageAdapter_PinReleaseSymmetry`
4. `TestPinner_RealBlockVol_HoldWALRetention`
5. `TestPinner_RealBlockVol_HoldRejectsRecycled`

## Task D: Executor Muscle Separation

### Goal

Separate catch-up / rebuild execution mechanics from `weed/` runtime ownership
so that executor behavior is treated as a reusable muscle behind
`sw-block`-owned ports.

### Source

1. `weed/storage/blockvol/v2bridge/executor.go`
2. related tests in `weed/storage/blockvol/v2bridge/*transfer*`
3. related tests in `weed/storage/blockvol/v2bridge/*snapshot*`
4. related tests in `weed/storage/blockvol/v2bridge/*truncate*`

### Destination

1. contract shape in `sw-block/bridge/blockvol/contract.go`
2. engine-facing use through:
   - `engine.CatchUpIO`
   - `engine.RebuildIO`
3. implementation remains thin in `weed/` until backend-binding interfaces are
   fully extracted

### Authority Rule

Executor code is allowed to:

1. transfer bytes
2. apply WAL entries
3. install snapshots/full base
4. truncate local WAL

Executor code is not allowed to:

1. classify recovery outcome
2. decide whether rebuild vs catch-up is needed
3. own publication or health meaning

### Adapter Boundary

`weed/` may:

1. call real `BlockVol` APIs
2. speak TCP rebuild/catch-up protocol
3. update local backend runtime state during execution

`weed/` must not:

1. redefine engine recovery phases
2. redefine target/achieved boundary meaning

### Acceptance

1. `BlockVolExecutor` aligns exactly with engine execution port expectations
2. engine/executor integration is possible without `sw-block` importing weed
3. executor logic is documented as reusable execution muscle, not semantic
   authority

### Validation

1. `go test ./sw-block/bridge/blockvol`
2. `go test ./weed/storage/blockvol/v2bridge -run "TestExecutor_|TestBridge_"`
3. focused integrated runtime tests remain green:
   - `go test ./weed/server -run "TestBlockService_ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart)"`

### Current proof anchors

1. `TestContract_BlockVolReaderInterface`
2. `TestExecutor_RealBlockVol_StreamWALEntries`
3. `TestExecutor_RealBlockVol_StreamPartialRange`
4. `TestExecutor_ErrorPaths`

## Parallel Execution Recommendation

These four tasks are safe to run in parallel if ownership stays clear:

1. Task A: canonical translation rules
2. Task B: reader port hardening
3. Task C: pinner port hardening
4. Task D: executor contract alignment

Recommended order for merge:

1. Task A
2. Task B
3. Task C
4. Task D

Reason:

1. Task A removes semantic drift first
2. Tasks B/C/D then migrate pure muscles behind that stable rule layer

## Delivery Note

Final outcome:

1. Task A required code change and is now delivered
2. Tasks B/C/D were reviewed and confirmed already at the acceptance bar
3. the next migration frontier is backend-binding extraction, not more contract
   cleanup
