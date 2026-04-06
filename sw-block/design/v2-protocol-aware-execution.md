# V2 Protocol-Aware Execution

## Purpose
Make host-side execution in `weed/server` and `weed/storage/blockvol` obey the
existing V2 session contract explicitly. The engine remains the semantic source
of truth. Host code owns only:

- execution-state caching derived from sender/session snapshots
- phase gating before data-plane I/O
- observation routing back into core events

## Host-Side Execution State
For each primary volume and replica, the host caches a `replica protocol
execution state` with these fields:

- `ReplicaID`
- `SenderState`
- `SessionID`
- `SessionKind`
- `SessionPhase`
- `StartLSN`
- `TargetLSN`
- `FrozenTargetLSN`
- `RecoveredTo`
- `SessionActive`
- `LiveEligible`
- `Reason`

Rules:

1. State is derived from `v2Orchestrator.Registry` snapshots only.
2. `LiveEligible=false` whenever there is an active recovery session.
3. Data-plane code must consult this cached state before shipping current live
   WAL entries.
4. Heartbeat and publication remain projection-driven; they do not invent local
   session semantics.

## WAL-First Rollout
The first rollout is intentionally narrow:

- cover `keepup` and WAL-based catch-up only
- do not change snapshot/build policy
- do not let fresh late-attached replicas consume current live-tail WAL while a
  bounded catch-up session is active

Current implementation seam:

- `weed/server/block_protocol_state.go`
  - derives host execution state from sender/session snapshots
  - binds a per-volume live-shipping policy back into `BlockVol`
- `weed/storage/blockvol/blockvol.go`
  - carries the host-provided live-shipping policy across shipper-group rebuilds
- `weed/storage/blockvol/wal_shipper.go`
  - checks the policy before any live-tail dial or send

This is intentionally a phase gate, not a second source of truth.

## Observation Seam
Runtime observations should feed back through one server-side seam:

- sender/session snapshots -> `syncProtocolExecutionState()`
- host event application -> `applyCoreEvent()`
- assignment processing -> `ApplyAssignments()`

The rule is:

1. engine chooses the protocol phase
2. host derives execution state from engine snapshots
3. data path obeys that state
4. host emits observed facts back through `applyCoreEvent()`

## Fast Test Roster
The first fast-test roster for protocol-aware execution is:

- `unit`: `TestWALShipper_LiveShippingPolicyBlocksBeforeDial`
  - proves phase gate happens before any transport dial
- `unit`: `TestWALShipper_LiveShippingPolicyAllowsShip`
  - proves the gate does not block normal live shipping after eligibility
- `component`: `TestBlockService_ProtocolExecutionState_ActiveCatchUpBlocksLiveShipping`
  - proves sender/session snapshots become host execution state and block live
    shipping during active catch-up
- `component`: `TestBlockService_ProtocolExecutionState_InSyncSenderAllowsLiveShipping`
  - proves the host reopens live shipping after the recovery session is gone

Next fast tests to add in later waves:

- late attach with backlog must stay bounded until target reached
- transport contact before barrier durability must not imply publish healthy
- timeout with valid retention pin may replan WAL catch-up
- timeout after retention loss must escalate to build
