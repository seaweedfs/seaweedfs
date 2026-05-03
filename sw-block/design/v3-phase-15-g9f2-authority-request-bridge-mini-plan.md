# V3 Phase 15 G9F-2 - Verified Placement To Authority Request Bridge Mini-Plan

Date: 2026-05-02
Status: slice 1 ask-only bridge QA-signed at `seaweed_block@05f0011`; slice 2 product-loop wiring requires architect §1.A acceptance
Branch target: `p15-g9f2/authority-request-bridge`
Scope: first bounded bridge from `VerifiedPlacement` to `authority.AssignmentAsk`

## 0. Why This Gate Exists

G9D/F closed the verify-only fact layer:

```text
DesiredVolume + NodeInventory + PlacementIntent + Observation -> VerifiedPlacement
```

M01 needs the next edge:

```text
VerifiedPlacement -> AssignmentAsk -> authority.Publisher -> AssignmentFact
```

This is the first authority-adjacent bridge. It must not become a shortcut where placement intent or heartbeat observation directly mints epoch/endpoint-version truth.

## 1.A Binding Status

Slice 1 is an ask-only TDD bridge:

- it produces `authority.AssignmentAsk` values;
- it does not call `Publisher` in production;
- it does not mint `AssignmentFact`, epoch, endpoint-version, or frontend readiness;
- it is QA-signed at `seaweed_block@05f0011` against all five §3 tests plus the cross-package regression command.

Slice 2 is the product-loop wiring:

- it will feed bridge asks into the live blockmaster publisher path;
- it changes product behavior from "verified placement visible" to "verified placement can create assignment";
- it requires architect §1.A acceptance before code.

## 1.B Slice 1 Bindings

Bindings used by the first TDD slice:

1. **Bridge owner**: bridge code lives in `core/host/master` or another controller seam, not in `core/lifecycle`.
2. **Lifecycle purity**: `core/lifecycle` remains authority-free and must not import `core/authority`.
3. **Input condition**: only `VerifiedPlacement{Verified:true}` can produce asks.
4. **Negative condition**: placement-only and observation-only inputs produce no asks.
5. **Publisher seam**: bridge produces `authority.AssignmentAsk`; only `authority.Publisher` mints epoch and endpoint-version.
6. **No frontend shortcut**: bridge output does not imply `frontend_primary_ready`.
7. **Stop rule**: if this requires proto/control API changes, engine mode changes, or direct `AssignmentInfo` construction, stop and split a new gate.

## 2. Algorithm Sketch

For one verified placement:

1. Reject if `Verified == false`.
2. For each verified slot:
   - require `VolumeID`, `DataAddr`, and `CtrlAddr`;
   - existing-replica slot must carry `ReplicaID`;
   - blank-pool slot needs a controller-assigned replica id before it can emit an ask.
3. Emit `authority.AssignmentAsk` with:
   - `VolumeID`
   - `ReplicaID`
   - observed `DataAddr`
   - observed `CtrlAddr`
   - `IntentBind` for first-close scope.
4. Do not call publisher from the bridge helper itself.

First slice can reject blank-pool slots until a replica-id allocator is ratified.

## 3. TDD Plan

Red tests first:

1. `TestG9F2_UnverifiedPlacementProducesNoAssignmentAsk`
2. `TestG9F2_VerifiedExistingReplicaProducesBindAsk`
3. `TestG9F2_AssignmentRequestShapeHasNoEpochOrEndpointVersion`
4. `TestG9F2_PlacementBridgeOutputPublishesThroughAuthorityPublisher`
5. `TestG9F2_LifecyclePackageDoesNotImportAuthority`

## 4. Close Criteria

G9F-2 first slice closes when:

1. tests in §3 are green;
2. no `AssignmentInfo` construction is added outside the existing allowlist;
3. docs state that bridge output is an ask, not authority;
4. G9G product-loop mini-plan can start from a real `AssignmentAsk` producer.

## 5. Non-Claims

- No create-volume API.
- No blockvolume startup loop.
- No frontend readiness claim.
- No recovery/rebuild decision.
- No RF/quorum ACK policy change.
