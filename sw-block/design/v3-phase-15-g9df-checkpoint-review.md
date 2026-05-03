# V3 Phase 15 G9D/G9F Checkpoint Review

Date: 2026-05-02
Branch: `p15-g9d/volume-lifecycle-intent`
Code range: `3263f62..e930390`
Status: checkpoint-ready for review / merge to `phase-15`

## 1. Goal

Build the product registration and discovery path without crossing the authority boundary:

```text
desired volume
  + node registration / inventory
  + planner
  + placement intent
  + verified placement
  != assignment
  != replica_ready
  != frontend_ready
```

The work intentionally provides SeaweedFS-style discovery/inventory shape for block, while preserving the P15 rule that observation facts and placement facts do not substitute for controller/publisher authority or data-plane proof.

## 2. Commit ledger

| Commit | Scope |
| --- | --- |
| `bbb9d9b` | G9D desired volume + node inventory fact stores |
| `5cb3ea8` | G9D placement planner candidates |
| `d96a818` | G9D placement intent store |
| `9f1b3ba` | G9D reconcile desired volumes to placement intents |
| `a95d35a` | G9D wire lifecycle stores into blockmaster behind `--lifecycle-store` |
| `43e3cba` | G9D idempotency/RF-change/read-only shape guards |
| `e07fe7d` | G9F verify placement intent against node registration observation |
| `50df967` | G9F expose verified placement in lifecycle snapshot |
| `246fffa` | G9F split node data/control addresses |
| `e930390` | G9F verify lifecycle placements from real `ObservationStore` |

## 3. Current fact pipeline

Implemented:

```text
VolumeSpec / VolumeRecord
  -> FileStore

NodeRegistration / ReplicaInventory
  -> NodeInventoryStore

VolumeRecord + NodeRegistration
  -> PlanPlacement
  -> PlacementIntentStore
  -> ReconcilePlacement

PlacementIntent + fresh ObservationStore.SlotFact
  -> VerifiedPlacement
  -> LifecycleSnapshot
```

Not implemented:

```text
VerifiedPlacement -> AssignmentAsk
AssignmentAsk -> Publisher
Publisher -> AssignmentFact from placement
Replica create/start commands
Frontend attach workflow
```

## 4. Boundary status

Preserved:

- lifecycle package does not import `core/adapter`;
- lifecycle package does not mention `AssignmentInfo` or `AssignmentFact`;
- `PlacementCandidate`, `PlacementIntent`, `ReconcileResult`, `LifecycleSnapshot`, and `VerifiedPlacement` are structurally non-authority-shaped;
- `blockmaster --lifecycle-store` opens stores but does not publish assignment;
- `LifecycleSnapshot` is read-only and does not mutate publisher state;
- `VerifiedPlacement` requires fresh observation for existing-replica slots;
- blank-pool placement still does not verify through `ObservationStore` because no replica slot exists yet.

Still intentionally absent:

- no CLI/API verb that calls `ReconcilePlacement`;
- no proto/control API extension;
- no publisher wiring;
- no engine/frontend readiness transition.

## 5. Test evidence

Scoped command used repeatedly during the slice:

```powershell
go test ./core/lifecycle ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Key tests:

- `TestLifecyclePackageDoesNotImportAdapterOrAssignmentInfo`
- `TestPlanPlacement_*`
- `TestPlacementIntentStore_*`
- `TestReconcilePlacement_ResultIsNotAuthorityShaped`
- `TestReconcilePlacement_IdempotentSameInputsProduceSameIntent`
- `TestReconcilePlacement_DesiredRFChangeUpdatesExistingIntent`
- `TestMasterLifecycleStore_OpensRegistrationStoresWithoutAuthority`
- `TestMasterLifecycleSnapshot_IsNotAuthorityShaped`
- `TestG9F_PlacementIntentWithoutObservation_DoesNotVerify`
- `TestG9F_VerifiedPlacement_IsNotAuthorityShaped`
- `TestMasterLifecycleSnapshot_ExistingReplicaVerifiesFromObservationStore`

## 6. Product meaning

This closes the discovery/registration gap identified after G9C:

- master can now host product lifecycle registration stores;
- nodes can be represented as inventory providers, including capacity and local replica identities;
- planner/reconciler can produce placement intents;
- read-only snapshot can show verified placement based on real heartbeat/observation facts.

It does not yet start work. That is the correct current boundary.

## 7. Forward carry

Next risky step: `VerifiedPlacement -> authority request`.

Required before code:

1. update `v3-phase-15-g9f-placement-authority-bridge-mini-plan.md` from draft to ratified;
2. decide whether `VerifiedPlacement` feeds existing `TopologyController`, a new placement controller, or a publisher directive adapter;
3. add a negative test: placement-only fails, observation-only fails, placement+fresh-observation may request authority;
4. keep actual epoch/endpoint-version minting inside authority publisher/controller code.

Do not merge a direct `PlacementIntent -> Publisher` path.

## 8. Merge recommendation

Recommend merging this branch to `phase-15` after review.

Reason: the branch is a bounded, tested fact-layer addition. It improves product discovery and registration without changing authority publication or data-plane behavior.

