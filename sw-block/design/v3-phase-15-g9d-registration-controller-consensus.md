# V3 Phase 15 G9D - Registration / Controller Boundary Consensus

Date: 2026-05-02
Status: architect consensus for G9D implementation
Scope: blockmaster registration, heartbeat, inventory, placement, assignment, recovery readiness

## 1. Core rule

Registration and heartbeat are observation facts. They do not grant authority, do not make a replica ready, and do not close recovery.

The controller is the only layer that may turn desired product state plus runtime observations into placement decisions, recovery decisions, and assignment publication requests.

Publisher emits authority only from controller decisions. Frontends consume readiness projections only after the data-plane facts prove serviceability.

## 2. Role split

| Layer | Owns | Must not do |
| --- | --- | --- |
| Desired volume registry | User/product intent: volume exists, size, RF, attach intent | Mint primary, mark replica ready, infer placement from heartbeat |
| Node registration / heartbeat | Node presence, addresses, capacity, labels, local replica inventory | Claim primary, claim replica_ready, close recovery |
| Placement controller | Select candidate nodes/slots from desired state and inventory | Treat observation as data continuity proof |
| Assignment controller / publisher | Publish epoch/endpoint-version authority facts | Bypass recovery/ready facts |
| Recovery feeder | Feed base/WAL and maintain one WAL egress owner per peer | Decide product placement |
| Durable ack / probe | Report progress facts | Create authority directly |
| Frontend | Serve only after frontend_primary_ready | Infer readiness from heartbeat |

## 3. Fact vocabulary

### Node registration

A node registration says:

- this server exists;
- these addresses and labels are visible;
- these storage pools have capacity;
- these local replica records already exist.

It is inventory. It is not authority.

### Desired volume registration

A desired volume record says:

- this volume should exist;
- this is the requested size and replication factor.

It is product intent. It is not placement.

### Replica inventory

A replica inventory record says:

- this node reports a local replica identity;
- the replica has a local durable identity and progress facts;
- the replica may be existing, blank, corrupt, unknown, recovering, or ready-candidate.

It is evidence for planner/controller. It is not replica_ready.

### Assignment

Assignment is epoch/endpoint-version authority published after controller decision. Assignment movement alone does not prove data continuity.

### Replica ready

Replica readiness requires recovery/feed/progress facts, including closed recovery window and post-close durable acknowledgement when the replica returned through recovery.

## 4. Anti-patterns

The following are design stops:

1. Heartbeat directly mints primary assignment.
2. Existing local replica inventory directly becomes replica_ready.
3. Blank capacity directly becomes primary.
4. Authority movement alone claims data continuity.
5. Desired volume creation directly starts frontend service.
6. One peer has multiple independent WAL feeders.
7. Delete intent directly drops data-path state without drain/teardown phase.

## 5. G9D implementation boundary

G9D first slice may add durable stores for:

- desired volume intent;
- node registration;
- local replica inventory.

G9D first slice must not:

- import `core/adapter`;
- mention `AssignmentInfo`;
- construct `AssignmentFact`;
- publish assignment;
- update engine mode;
- claim replica_ready.

Tests must pin these boundaries so future controller work consumes inventory through explicit planner seams instead of reintroducing heartbeat-as-authority.

## 6. Future controller path

The intended future flow is:

```text
CreateVolume(volume_id, size, RF)
  -> DesiredVolumeStore

NodeHeartbeat(server_id, pools, local replicas)
  -> NodeInventoryStore

DesiredVolume + NodeInventory + ReplicaInventory
  -> PlacementController
  -> ReplicaCreate / ReplicaStart / Recover decisions
  -> AssignmentController / Publisher
  -> blockvolume observes AssignmentFact
  -> feeder/recovery proves data-plane facts
  -> replica_ready / frontend_ready projections
```

This keeps SeaweedFS-style automatic discovery while preserving the V3 anti-pattern boundary: observation facts do not substitute for authority or data-plane proof.
