# V3 Phase 15 G9D/F QA Test Instruction

Date: 2026-05-02
Status: QA-verified on `phase-15@eeef486`
Scope: lifecycle registration, placement planning, verified placement, and authority-boundary guards

## Headline

At `phase-15@eeef486`, G9D/F proves master can discover/register desired volume
and node inventory, compute and verify placement readiness, but still does not
grant authority.

## Environment

- Repo: `seaweed_block`
- Branch: `phase-15`
- Minimum commit: `eeef486`
- Fidelity: component/L2 with real package wiring; not m01/M02 hardware
- Non-claim: no production authority minting, frontend service, or recovery run

## Command

```powershell
go test ./core/lifecycle ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Expected result: all five packages pass. `cmd/blockvolume` can take about two
minutes because it includes the G8 subprocess failover tests.

## Scenario Checklist

1. Desired volume persists.
   Backing test: `core/lifecycle/store_test.go::TestFileStore_CreateVolumePersistsAndIsIdempotent`

2. Node inventory persists.
   Backing test: `core/lifecycle/node_inventory_test.go::TestNodeInventoryStore_RegisterNodePersistsCapacityAndReplicas`

3. Planner produces placement intent.
   Backing test: `core/lifecycle/reconciler_test.go::TestReconcilePlacement_HappyPathWritesPlacementIntent`

4. Insufficient inventory fails closed.
   Backing test: `core/lifecycle/reconciler_test.go::TestReconcilePlacement_InsufficientInventoryReportsButDoesNotPersist`

5. Existing replica conflict fails closed.
   Backing test: `core/lifecycle/reconciler_test.go::TestReconcilePlacement_ExistingReplicaConflictIsReportOnly`

6. Verified placement requires observation.
   Backing tests: `core/lifecycle/verified_placement_test.go::TestG9F_PlacementIntentWithoutObservation_DoesNotVerify`,
   `core/host/master/lifecycle_test.go::TestMasterLifecycleSnapshot_PlacementWithoutNodeObservationIsNotVerified`

7. Fresh observation verifies an existing replica.
   Backing test: `core/host/master/lifecycle_test.go::TestMasterLifecycleSnapshot_ExistingReplicaVerifiesFromObservationStore`

8. Stale observation or missing address fails closed.
   Backing tests: `core/lifecycle/verified_placement_test.go::TestG9F_PlacementIntentWithStaleObservation_DoesNotVerify`,
   `core/lifecycle/verified_placement_test.go::TestG9F_FreshObservationMissingControlAddress_DoesNotVerify`

9. No authority side effect.
   Backing tests: `core/host/master/lifecycle_test.go::TestMasterLifecycleStore_OpensRegistrationStoresWithoutAuthority`,
   `core/host/master/lifecycle_test.go::TestMasterLifecycleSnapshot_ExistingReplicaVerifiesFromObservationStore`,
   `core/host/master/lifecycle_test.go::TestMasterLifecycleSnapshot_PlacementWithoutNodeObservationIsNotVerified`

## Non-Claims

- Does not auto-assign a newly created volume.
- Does not make any blockvolume become primary or replica.
- Does not start iSCSI, NVMe, or frontend service paths.
- Does not start recovery, rebuild, or WAL feeding.
- Does not call the authority publisher or mint epoch / endpoint-version facts.

## Closure Sentence

G9D/F is a verify-only fact-layer checkpoint: product intent and observation can
produce `VerifiedPlacement`, but `VerifiedPlacement` is still not authority.
