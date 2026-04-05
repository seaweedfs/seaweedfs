# Phase 19 Log

Date: 2026-04-05
Status: complete

## 2026-04-05

### Start Of Phase

Created the initial `Phase 19` control document.

Starting point recorded:

1. `Phase 18` is complete
2. the current runtime-bearing RF2 envelope is explicit
3. the current productionization judgment is explicit:
   - `block expansion`
   - `not pilot-ready`

Initial execution rule:

1. move by major milestone
2. keep the order `M6 -> M7 -> M8 -> M9 -> M10`
3. keep each milestone reviewable with healthy and fail-closed proofs

Current next step:

1. `M6` seam step for live transport-backed runtime queries

### `M6` Delivered

Delivered in this update:

1. one live loopback HTTP evidence transport now exists
2. runtime registration can run on that live transport path
3. healthy and gated transport-backed failover tests now pass over that path

Tests:

1. `TestHTTPTransportEvidenceAdapter_HealthyFailoverFlow`
2. `TestHTTPTransportEvidenceAdapter_GatedFailoverFlow`
3. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

### `M7` Delivered

Delivered in this update:

1. one background Loop 2 service now exists
2. one bounded auto-failover service now exists
3. RF2 outward surfaces can now refresh from continuous runtime activity

Tests:

1. `TestInProcessRuntimeManager_Loop2Service_RefreshesRF2Surface`
2. `TestInProcessRuntimeManager_AutoFailoverService_TriggersOnPrimaryLoss`
3. `TestInProcessRuntimeManager_AutoFailoverService_DoesNotTriggerOnCatchingUpReplica`
4. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

### `M8` Delivered

Delivered in this update:

1. one runtime-managed iSCSI export path now exists
2. one bounded replica repair wrapper now exists
3. the runtime can now rebind service and repair a lagging replica without
   moving truth ownership out of `volumev2`

Tests:

1. `TestInProcessRuntimeManager_ExportVolumeISCSI_BindsFrontendToRuntimeNode`
2. `TestInProcessRuntimeManager_RepairReplicaFromPrimary_ReturnsLoop2ToHealthy`
3. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

### `M9` Delivered

Delivered in this update:

1. one end-to-end RF2 handoff proof now exists with:
   - live transport
   - runtime-managed frontend
   - automatic failover
   - reconnect and continued I/O on the new primary
2. one gated handoff counterproof now stops fail-closed

Tests:

1. `TestInProcessRuntimeManager_EndToEndRF2Handoff_ContinuesIOOnNewPrimary`
2. `TestInProcessRuntimeManager_EndToEndRF2Handoff_GatedReplicaStopsFailClosed`
3. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2`

### `M10` Delivered

Delivered in this update:

1. one bounded HTTP operator surface now exists over runtime-owned views
2. one bounded CSI runtime backend adapter now exists over runtime-owned export
   truth
3. CSI create/lookup/publish can now read from the V2 runtime path on the
   bounded adapter path

Tests:

1. `TestInProcessRuntimeManager_OperatorSurface_ExposesRuntimeOwnedViews`
2. `TestV2RuntimeBackend_CreateLookupAndPublish`
3. `go test ./sw-block/runtime/masterv2 ./sw-block/runtime/volumev2 ./weed/storage/blockvol/csi`

Current interpretation:

1. `Phase 19` is complete as one bounded working RF2 block path
2. this is still a bounded working path on the current runtime harness, not broad
   launch approval
3. the next major work should focus on multi-process / pilot-ready closure
