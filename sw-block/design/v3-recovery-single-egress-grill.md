# V3 Recovery Single-Egress Grill

Status: active implementation checklist

## Ownership Rule

For each peer, exactly one primary-side egress owner decides how a committed
WAL fact leaves the primary. Outer layers may report facts and health hints;
they must not choose between steady `Ship`, recovery `PushLiveWrite`, and
post-recovery handoff themselves.

## Entity Map

| Entity | Owns | Does not own |
| --- | --- | --- |
| `ReplicationVolume` | local committed-write fanout order | socket choice, recovery-vs-steady route |
| `ReplicaPeer` | peer health state, target identity, live lineage handle | WAL egress route, recovery completion truth |
| `BlockExecutor` / per-peer feeder seam | live WAL egress decision for one peer | quorum/membership policy |
| `WalShipper` | cursor, emit context, backlog/live serializer | probe policy, rebuild decision |
| `Recovery Sender` | recovery phase driver and frame sequencing under feeder ownership | independent live WAL pump |
| `Receiver` | idempotent apply, durable witness | primary health/membership decision |
| Probe/RSH | capability evidence | data-plane closure |

## First Slice Contract

This slice does not rewrite every recovery component. It creates a single live
write ingress in `BlockExecutor` and moves routing decisions behind it.

Required behavior:

- Active recovery session has first refusal for every live write.
- A degraded peer without an active session must not make the committed WAL
  fact disappear; the fact remains in primary WAL for later recovery.
- Post-recovery steady writes must not reuse pre-recovery session/conn lineage.
- `ReplicaPeer` may pass a health hint, but it must not directly choose steady
  vs recovery emit.

## Grill Questions Per Patch

1. Which state does this entity own?
2. Which old decision was removed from this entity?
3. What is the unique ingress for committed WAL facts?
4. Can any old path still bypass the ingress?
5. What happens when peer is degraded?
6. What happens during active recovery?
7. What happens after recovery close?
8. What test would fail if a second feeder appears?

## Current Slice Tests

- Degraded peer without active session: `ShipEntry` returns nil and retains
  state as degraded; it does not attempt legacy steady emit.
- Degraded peer with active session: live write routes to session lane.
- Post-recovery refresh: steady session rotates past recovery session ID.
- Receiver lineage: old live session is rejected after recovery; newer live
  session is accepted.

