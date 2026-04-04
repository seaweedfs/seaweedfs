# V2 Second Migration Task Pack

Date: 2026-04-04
Status: delivered

## Purpose

This note turns the second separation batch into validate-able engineering
tasks.

The first batch proved that contract ownership and translation authority already
belong in `sw-block`. The second batch now targets the remaining physical
coupling: backend bindings.

## Shared Rules

All tasks in this pack inherit these rules:

1. `sw-block` must not directly import `weed/storage/blockvol`
2. reusable execution-muscle logic should move toward `sw-block`
3. weed-side code should shrink toward thin concrete bindings
4. no task in this pack may redefine engine semantics or recovery policy

## Task E: Reader Backend-Binding Extraction

### Goal

Extract reusable reader logic from direct `BlockVol` coupling so the
`BlockVol`-specific part becomes a thin snapshot binding.

### Source

1. `weed/storage/blockvol/v2bridge/reader.go`
2. `weed/server/block_recovery.go` reader shim

### Destination

1. reusable reader logic in `sw-block/bridge/blockvol/runtime`
2. thin `BlockVol` snapshot binding in `weed/storage/blockvol/v2bridge`

### Authority Rule

The reusable logic that shapes backend snapshot data into
`bridge.BlockVolState` belongs with `sw-block` execution muscles.

The weed-side binder may fetch snapshot fields from real `BlockVol`, but it
must not own the reusable state-shaping layer.

### Adapter Boundary

`weed/` may:

1. call `StatusSnapshot()` on real `BlockVol`
2. expose raw backend snapshot data to the extracted layer

`weed/` must not:

1. keep a second contract-shape mapping layer in `block_recovery.go`
2. reinterpret retained-history meaning

### Acceptance

1. reusable reader logic no longer depends on direct `BlockVol` import
2. `weed/storage/blockvol/v2bridge/reader.go` is reduced to thin binding code
3. `readerShimForRecovery` is removed or reduced to trivial wiring

### Validation

1. `go test ./sw-block/bridge/blockvol`
2. `go test ./weed/storage/blockvol/v2bridge -run "TestReader_"`
3. if the recovery shim changes, run:
   - `go test ./weed/server -run "TestP4_|TestP16B_"`

### Current proof anchors

1. `TestStorageAdapter_RetainedHistoryFromReader`
2. `TestReader_RealBlockVol_StatusSnapshot`
3. `TestReader_RealBlockVol_HeadAdvancesWithWrites`

## Task F: Pinner Backend-Binding Extraction

### Goal

Extract hold bookkeeping and release lifecycle from direct `BlockVol` coupling
so weed-side code only performs concrete retention-floor binding and state
validation.

### Source

1. `weed/storage/blockvol/v2bridge/pinner.go`
2. `weed/server/block_recovery.go` pinner shim

### Destination

1. reusable hold bookkeeping in `sw-block/bridge/blockvol/runtime`
2. thin `BlockVol` retention binding in `weed/storage/blockvol/v2bridge`

### Authority Rule

Hold bookkeeping is reusable execution-muscle logic. Concrete interaction with
the flusher and `StatusSnapshot()` stays in `weed/`, but ID tracking and release
symmetry should not require direct `BlockVol` imports.

### Adapter Boundary

`weed/` may:

1. install retention-floor callbacks on real `BlockVol`
2. validate requested hold positions against live backend snapshot state

`weed/` must not:

1. keep reusable hold lifecycle ownership trapped in `weed/`
2. force recovery policy knowledge into the pinner binding

### Acceptance

1. reusable hold bookkeeping can live in `sw-block` without `BlockVol` imports
2. weed-side pinner code shrinks toward concrete callback/state binding
3. `pinnerShimForRecovery` is removed or reduced to trivial wiring

### Validation

1. `go test ./sw-block/bridge/blockvol`
2. `go test ./weed/storage/blockvol/v2bridge -run "TestPinner_|TestBridge_"`
3. if the recovery shim changes, run:
   - `go test ./weed/server -run "TestP4_|TestP16B_"`

### Current proof anchors

1. `TestStorageAdapter_WALPinRejectsRecycled`
2. `TestStorageAdapter_SnapshotPinRejectsUntrusted`
3. `TestStorageAdapter_PinReleaseSymmetry`
4. `TestPinner_RealBlockVol_HoldWALRetention`
5. `TestPinner_RealBlockVol_HoldRejectsRecycled`

## Task G: Executor Backend-Capability Extraction

### Goal

Split executor logic into:

1. reusable orchestration that belongs with `sw-block` execution muscles
2. concrete backend capabilities and wire operations that remain in `weed/`

### Source

1. `weed/storage/blockvol/v2bridge/executor.go`
2. related tests in:
   - `weed/storage/blockvol/v2bridge/*transfer*`
   - `weed/storage/blockvol/v2bridge/*snapshot*`
   - `weed/storage/blockvol/v2bridge/*truncate*`

### Destination

1. reusable executor orchestration in `sw-block/bridge/blockvol/runtime`
2. thin backend capability bindings in `weed/storage/blockvol/v2bridge`

### Authority Rule

The engine still owns recovery policy. This task does not move policy.

The reusable execution sequence for:

1. bounded WAL replay
2. full-base install plus second catch-up
3. snapshot transfer verification
4. truncate escalation boundary

should no longer be inseparable from direct `BlockVol` imports.

### Adapter Boundary

`weed/` may:

1. implement concrete backend operations on real `BlockVol`
2. own rebuild TCP framing and network transport while it still depends on
   `blockvol` protocol types

`weed/` must not:

1. keep the whole recovery step orchestration trapped behind direct
   `BlockVol` imports when capability interfaces can be extracted
2. redefine engine-visible boundary meaning

### Acceptance

1. executor reusable logic depends on extracted capability interfaces, not
   direct `BlockVol` imports
2. weed-side executor code is reduced to concrete backend/network bindings
3. outcome classification still remains outside the executor layer

### Validation

1. `go test ./sw-block/bridge/blockvol`
2. `go test ./weed/storage/blockvol/v2bridge -run "TestExecutor_|TestBridge_"`
3. focused runtime integration still passes:
   - `go test ./weed/server -run "TestBlockService_ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart)"`

### Current proof anchors

1. `TestContract_BlockVolReaderInterface`
2. `TestExecutor_RealBlockVol_StreamWALEntries`
3. `TestExecutor_RealBlockVol_StreamPartialRange`
4. `TestExecutor_ErrorPaths`

## Recommended Execution Order

Recommended order:

1. Task E
2. Task F
3. Task G

Reason:

1. reader extraction is lowest risk and pure read-path
2. pinner extraction adds lifecycle but still avoids policy
3. executor extraction is the largest surface and should build on the previous
   two cuts

## Delivery Note

Final outcome:

1. Task E was completed by code change
2. Task F was completed by code change
3. Task G was reviewed and confirmed already clean
4. after Batch 2, `weed/server/block_recovery.go` no longer carries
   reader/pinner shim types
