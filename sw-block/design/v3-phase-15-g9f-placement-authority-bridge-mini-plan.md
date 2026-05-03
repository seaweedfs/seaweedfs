# V3 Phase 15 G9F - Placement Intent to Authority Bridge Mini-Plan

Date: 2026-05-02
Status: §1.A architect-ratified 2026-05-02 for verify-only first slice; implemented and merged at `phase-15@eeef486`
Branch target: `p15-g9d/volume-lifecycle-intent` or successor branch off `phase-15`
Scope: first bridge from placement intent facts toward authority publication

§1.A architect-ratified: 2026-05-02. Binding applies only to the verify-only first slice:
`PlacementIntent + fresh Observation -> VerifiedPlacement`, with no publisher wiring and no
authority minting. Any follow-up that turns `VerifiedPlacement` into an authority request,
assignment ask, publisher apply, epoch, or endpoint-version change requires a separate G9F-2
mini-plan and ratification.

## 0. Why this needs a mini-plan

G9D-A through G9D-E intentionally stayed on the observation/product-intent side:

```text
DesiredVolumeStore
NodeInventoryStore
PlanPlacement
PlacementIntentStore
ReconcilePlacement
LifecycleSnapshot
```

None of those layers publishes authority or readiness.

G9F is the first possible crossing from placement intent to assignment. That is a truth-domain boundary. A placement intent says "these slots look like candidates"; it does not prove that those slots are fresh, reachable, safe to assign, or data-continuity-ready.

Therefore G9F must be a controller bridge with explicit verification, not a direct `PlacementIntent -> Publisher` shortcut.

## 1. Product goal

Allow blockmaster to use product registration/discovery facts as input to future assignment decisions, while preserving the P15 anti-pattern rule:

```text
placement intent alone cannot mint authority
```

## 1.A Architect bindings

Proposed bindings for ratification:

1. **Bridge shape:** G9F implements a verify-only controller bridge that produces `AssignmentCandidate` / `PlacementAuthorityRequest`-style internal decisions. It does not call `Publisher` directly in the first slice.
2. **Required intersection:** every placement slot considered for authority must intersect with fresh observation for the same `server_id` and usable `data_addr` / `ctrl_addr`.
3. **No ready shortcut:** existing `ReplicaInventory` can justify reusing a local durable identity, but cannot imply `replica_ready`.
4. **No frontend shortcut:** desired volume + placement intent cannot make `frontend_primary_ready`.
5. **Single publisher seam:** actual epoch/endpoint-version minting remains in authority publisher/controller code, not `cmd/blockmaster` and not lifecycle stores.
6. **Negative oracle:** a placement intent with no fresh observation must not produce any authority-shaped output.
7. **Scope stop:** if G9F needs proto/control API changes or modifies publisher apply logic, stop and split into G9F-2 after review.

## 2. Inputs and outputs

### Inputs

- `PlacementIntentStore`: desired slot plan from G9D.
- Observation store / heartbeat facts: fresh runtime address facts.
- Existing topology/controller supportability predicates where reusable.

### Output, first slice

An internal non-authority result:

```text
VerifiedPlacement{
  volume_id
  slots[] {
    server_id
    source
    replica_id?       // existing-replica reuse only
    data_addr
    ctrl_addr
    verified_by = observation
  }
}
```

Forbidden fields on first-slice output:

```text
Epoch
EndpointVersion
Assignment
Ready
Healthy
Primary
```

## 3. Algorithm sketch

For each `PlacementIntent`:

1. Validate slot count equals `DesiredRF`.
2. For each slot:
   - locate fresh observation for the intended server / replica slot;
   - require non-empty `DataAddr` and `CtrlAddr`;
   - if source is `existing_replica`, require local inventory identity match;
   - if source is `blank_pool`, mark as create-needed, not ready.
3. If any slot lacks fresh observation, return `NotVerified`.
4. If all slots verify, return `VerifiedPlacement`.
5. Do not mint authority.

## 4. TDD plan

### A. Negative tests first

1. `TestG9F_PlacementIntentWithoutObservation_DoesNotVerify`
   - placement intent exists;
   - no heartbeat/observation;
   - output is `NotVerified`;
   - no authority-shaped fields.

2. `TestG9F_PlacementIntentWithStaleObservation_DoesNotVerify`
   - observation expired by freshness window;
   - no verified placement.

3. `TestG9F_PlacementIntentWithWrongReplicaInventory_DoesNotVerify`
   - existing-replica slot names `r2`;
   - inventory says different store/size/state conflict;
   - no verified placement.

4. `TestG9F_VerifiedPlacement_IsNotAuthorityShaped`
   - structural guard forbidding `Epoch`, `EndpointVersion`, `Assignment`, `Ready`, `Healthy`, `Primary`.

### B. Positive tests

5. `TestG9F_BlankPoolIntentWithFreshObservation_VerifiesCreateNeededSlot`
   - blank capacity selected by G9D;
   - fresh node observation provides address;
   - verified output marks slot as create-needed, not ready.

6. `TestG9F_ExistingReplicaIntentWithFreshObservation_VerifiesReuseCandidate`
   - existing replica inventory and observation agree;
   - verified output carries data/ctrl addresses and replica id.

### C. Integration guard

7. `TestG9F_VerifiedPlacementDoesNotChangePublisherState`
   - run verifier against a real master host with lifecycle stores;
   - assert `Publisher().VolumeAuthorityLine(volumeID)` remains absent.

## 5. Implementation boundary

Allowed:

- new package or file under `core/lifecycle` or `core/host/master` for verification;
- pure structs and tests;
- read-only use of lifecycle stores and observation facts;
- no proto change.

Forbidden:

- no `Publisher.Apply` call;
- no `AssignmentInfo` construction in lifecycle package;
- no `AssignmentFact` construction in lifecycle package;
- no engine mode update;
- no frontend readiness change;
- no CLI/API verb that triggers authority minting.

## 6. Close criteria

G9F first slice closes when:

- all tests in §4.A and §4.B pass;
- integration guard §4.C passes;
- structural guard appears in code;
- docs state that verified placement is still not authority.

## 7. Follow-up after first slice

Only after the verify-only bridge is green:

1. decide whether verified placement becomes input to existing `TopologyController`, a new placement controller, or a publisher directive adapter;
2. write a separate mini-plan for the authority-minting slice;
3. add a negative test that observation-only and placement-only each fail, while placement+fresh-observation can request authority.
