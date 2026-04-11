# Protocol Anti-Patterns in Distributed Storage Systems

Date: 2026-04-09
Status: reference document

## Purpose

This document catalogs recurring protocol anti-patterns found in distributed
storage systems. Each anti-pattern is defined, then illustrated with concrete
cases from real codebases: SeaweedFS (upstream object store), SeaweedFS sw-block
(block storage extension), Ceph RADOS, and DRBD.

The goal is to provide a reusable reference for protocol review and design
decisions. When a new protocol change is proposed, check it against this list.

---

## A1: Heartbeat Timing Defines Recovery Semantics

**Definition**: A timer or heartbeat interval directly determines a semantic
outcome (node death, volume reassignment, recovery type) without collecting
independent facts.

**Why it is wrong**: Heartbeat is transport-layer liveness observation. It
measures "when did I last hear from you," not "what is your data state." Using
it as the sole input for semantic decisions conflates observation latency with
data truth.

**Correct principle**: Timers trigger observation. Facts determine semantics.

### Case: SeaweedFS upstream — dead node detection

`topology/volume_location_list.go:69-86`

When `LastSeen < freshThreshHold` (hardcoded 60 seconds), the node is removed
from volume location lists. When the heartbeat gRPC stream drops
(`master_grpc_server.go:67-101`), the defer block immediately unregisters the
node and marks all volumes unavailable.

A 60-second network hiccup causes immediate volume unavailability. No probe,
no corroboration, no fact collection. One missed heartbeat cycle = topology
change.

### Case: SeaweedFS sw-block — fast replica rejoin missed

When a replica restarts faster than the master's heartbeat detection (~5
seconds), the master never detects the disconnect. No `failoverBlockVolumes`
fires, no `recoverBlockVolumes` fires, no primary refresh is sent. The
primary's shipper holds a dead TCP connection and the volume stays degraded
indefinitely.

The system's recovery depends on the *timing* of the restart relative to the
heartbeat cycle, not on the *fact* of the restart.

### Case: Ceph — OSD flapping storms

Early Ceph used OSD heartbeat timeout (20s default) as the trigger for PG
peering. Network congestion caused heartbeat delays without actual OSD failure,
leading to "flapping" — OSDs bouncing between up and down, triggering
unnecessary recovery storms that made congestion worse.

Fix: Ceph separated heartbeat (liveness signal) from PG peering (data-plane
fact collection). The monitor marks a node down, but PG peering independently
collects `pg_info` from all OSDs before deciding recovery. The
`osd_heartbeat_grace` and `mon_osd_min_down_reporters` tuning knobs are
artifacts of this evolution.

### Case: DRBD — ping timeout triggers resync

DRBD's `ping-timeout` and `connect-int` timers originally drove state
transitions directly. A slow network caused DRBD to declare the peer dead and
start resync — but the peer was alive, just slow.

Fix: DRBD 8.4+ added fencing — you need an external fencing agent to confirm
death before split-brain resolution proceeds. The timer triggers the check;
the fencing agent provides the fact.

### Fix pattern

```
Timer fires → trigger probe/observation
Probe collects bounded facts (R, S, H, epoch, endpoint)
Facts determine semantic action (no-op, catchup, rebuild, promote)
```

---

## A2: Transport Failure Directly Defines Recovery Type

**Definition**: A single transport-layer error (TCP reset, write timeout, dial
failure) directly triggers a semantic state change (rebuild required, volume
degraded, write failed) without distinguishing transient from permanent failure.

**Why it is wrong**: Transport failure and data-plane recovery are different
truth domains. A TCP reset means "I cannot reach you right now," not "your data
is divergent and you need a full rebuild."

**Correct principle**: Transport failure changes reachability truth. Recovery
type requires bounded data-plane facts.

### Case: SeaweedFS upstream — one replica failure fails entire write

`topology/store_replicate.go:73-137`

`DistributedOperation` fans out writes to all replicas in parallel. If ANY
replica returns an error, the entire write fails and returns error to the
client. No distinction between:
- Transient network hiccup (retry would succeed)
- Replica disk full (permanent, needs different handling)
- Replica truly unreachable (degrade and continue)

A single TCP error from one replica out of three fails the write for the
client.

### Case: SeaweedFS sw-block — Ship() 3s deadline causes NeedsRebuild

The WAL shipper's `Ship()` sets a 3-second write deadline on the TCP
connection. Under sustained qd=16 writes, the replica couldn't drain the TCP
buffer fast enough. The deadline fired, `markDegraded()` was called, and
subsequent WAL recycling made the gap unrecoverable — escalating to
NeedsRebuild.

A 3-second TCP write timeout caused a full 1GB rebuild. The transport timeout
was over-interpreted as a semantic failure.

### Case: DRBD — TCP drop triggers full resync

In DRBD 8.x, a single TCP connection failure could trigger a full resync. The
activity log bitmap was supposed to track only changed blocks, but connection
drops during heavy I/O could corrupt the bitmap tracking, causing a full resync
of the entire device.

Fix: DRBD 9 introduced quorum — transport failure alone doesn't trigger resync.
You need quorum loss (confirmed by multiple paths) before the system decides
the peer needs rebuilding.

### Case: Ceph — transport loss vs acting set change

Ceph's PG peering distinguishes transport loss from acting set changes.
A transport failure moves the OSD out of the acting set, but the PG doesn't
start recovery until peering collects actual data-plane facts (pg_info
exchange). Transport loss changes reachability; recovery requires facts.

### Fix pattern

```
Transport error → mark reachability lost
Keep probing last known endpoint (harmless retries)
When reachable again → collect facts (R, S, H)
Facts determine: no recovery, catchup, or rebuild
```

---

## A3: Ack Arrival Defines Terminal Success

**Definition**: An acknowledgment message (ack, barrier response, completion
signal) is treated as terminal success before the full protocol round-trip
closes. The system announces success, then discovers the operation is not
actually complete.

**Why it is wrong**: Observed completion is not the same as closed recovery
authority. An ack means "the remote side received and processed your request."
It does not mean "all side effects are complete, all cleanup is done, and the
system is in a consistent terminal state."

**Correct principle**: Terminal success is emitted only after executor/session
closure completes, not on ack arrival.

### Case: SeaweedFS sw-block — double event on rebuild completion

The remote rebuild ack callback emitted `SessionCompleted` immediately on
receiving `SessionAckCompleted` from the replica. The V2 core transitioned to
`publish_healthy`. Then the executor's cleanup path encountered a "sender
stopped" error and emitted `SessionFailed`, knocking the mode back to
`degraded`. Two contradictory terminal events from the same operation.

Root cause: ack arrival was treated as terminal truth, but the executor was
still unwinding.

Fix: Ack updates observed facts only. Terminal success is emitted after
executor/session closure succeeds.

### Case: SeaweedFS upstream — local write before replica ack

`topology/store_replicate.go:49-64`

The primary writes locally first, then replicates to remote locations. If local
write succeeds but remote replication fails, the primary has data the replicas
don't. There is no reconciliation mechanism. The write is partially committed.

If the client connection drops after local write but before replication error
is returned, the client may see success (from a previous response) while the
replicas don't have the data.

### Case: Ceph — committed before fsync

Ceph had a bug where the primary OSD declared a write "committed" after
receiving replica acks, but the replica crashed before fsyncing. The fix was
`min_size` enforcement — the write isn't committed until the required number
of replicas confirm durable persistence via journal/WAL fsync.

### Fix pattern

```
Ack arrives → update observed facts (progress, achieved LSN)
Do NOT emit terminal success yet
Executor completes cleanup (sender close, session close)
Then emit terminal success
```

---

## A4: Event Ordering Determines Semantics

**Definition**: The same set of facts produces different semantic outcomes
depending on which event arrives first. Two orderings of the same events lead
to different recovery paths or different final states.

**Why it is wrong**: If the system has the same facts, it should make the same
decision. Event order should affect convergence latency, not semantic result.

**Correct principle**: Same facts, same decision, regardless of arrival order.

### Case: SeaweedFS sw-block — NeedsRebuildObserved removes sender before rebuild starts

`handleReplicaProbeResult` calls `applyCoreEvent(NeedsRebuildObserved)` which
emits `InvalidateSessionCommand`. The command removes the sender's session
from the orchestrator registry. Then `installSession` tries to re-create it,
and `RebuildStarted` fires. But by the time the first ack arrives, a
registry reconciliation (triggered by `syncProtocolExecutionState` inside
`applyCoreEvent`) has removed the sender again.

The sender survives or dies depending on the exact ordering of:
1. InvalidateSessionCommand dispatch
2. installSession ProcessAssignment
3. syncProtocolExecutionState reconciliation
4. First ack arrival

Different timings of these four events produce different outcomes (rebuild
succeeds or fails with "sender not found").

### Case: SeaweedFS upstream — volume growth race

`topology/volume_growth.go:288-349`

Concurrent `Assign` requests race on volume slot reservation. Thread A
reserves node1+node2+node3. Thread B sees node3 full (A got it), fails, and
releases its reservations. Under high concurrency, different request ordering
produces different capacity decisions.

### Case: Ceph — PG peering collects all facts first

Ceph's solution to this anti-pattern: the PG peering state machine has
`GetInfo` and `GetLog` states that wait for responses from ALL peers before
advancing to `Active`. Event order within the collection phase doesn't matter
because no decision is made until all facts are present.

### Case: Raft — log position, not arrival order

The Raft consensus algorithm is the canonical solution. All decisions are based
on log position (term + index), not arrival order. If two messages carry the
same log position information, the same decision is made regardless of which
arrives first.

### Fix pattern

```
Collect all required facts before making decisions
Use monotonic identifiers (epoch, LSN, sessionID) to order facts
Decision function: f(facts) → action (deterministic, no ordering dependency)
R >= H → no recovery
R >= S → catchup
R < S → rebuild
```

---

## A5: Projection Drives Control Truth

**Definition**: A derived status field (mode, health, state label) is read
back as input to control decisions. The projection is supposed to be a report
of truth, but it becomes a hidden control input that creates feedback loops.

**Why it is wrong**: Projection must be derived only. When a derived field
is used as input, the system creates a circular dependency: state determines
projection, projection determines state.

**Correct principle**: Projection reports truth. It must never create truth.

### Case: SeaweedFS upstream — ReadOnly status drives assignment

`topology/volume_layout.go:267-271`

Volume `ReadOnly` status (self-reported by the volume server) directly controls
whether the master adds the volume to the writable list and whether it is
eligible for new write assignments. The master has no independent verification.

If a volume server incorrectly marks itself readonly (bug, misconfiguration),
the master stops all writes to that volume. The volume server's self-reported
status becomes authoritative for the master's assignment decisions.

### Case: SeaweedFS upstream — volume compacting status

`topology/topology_vacuum.go:347-349`

The master skips vacuum for volumes marked readonly — but readonly is itself
derived from the volume server's report. A volume server can prevent vacuuming
by claiming readonly, even if the volume should be vacuumed.

### Case: Kubernetes — Pod phase feedback loop

Pod `status.phase` (Running, Pending, Failed) is supposed to be a derived
report. But many controllers use `status.phase` as input for reconciliation
loops, creating feedback cycles. The Kubernetes community has been migrating
toward `conditions` (explicit fact-based fields) instead of `phase` (derived
summary).

### Case: Ceph — PG state as recovery scheduler input

Ceph's PG `state` (active+clean, active+degraded) was used internally to
prioritize recovery scheduling. But `degraded` was itself derived from the
recovery state — creating a loop. Fix: the recovery scheduler works from
`pg_missing` (actual missing objects), not the PG state label.

### Fix pattern

```
Projection = f(facts) — pure derivation, no side effects
Control decisions read facts directly, not projections
If you find a control path reading a projection, refactor to read the
underlying fact instead
```

---

## A6: Timing Workaround States Accumulate in the Engine

**Definition**: New states, flags, or enum values are added to the semantic
engine to survive a specific race condition or timing issue. Over time, the
engine becomes a patch history rather than a semantic model.

**Why it is wrong**: Each workaround state increases the state machine's
surface area without adding semantic meaning. The state exists for timing
reasons, not because the problem space has a new distinction. Future developers
cannot tell which states are essential and which are workarounds.

**Correct principle**: New state enters the engine only if it expresses a new
semantic distinction that cannot be represented by existing dimensions.

### Case: SeaweedFS upstream — phantom volume server Counter

`data_node.go:24`

```go
Counter int // in race condition, the previous dataNode was not dead
```

When a VS reconnects before the old heartbeat stream's defer runs, two streams
exist for the same node. The `Counter` field tracks active streams and only
unregisters when it reaches 0. This is a timing workaround for the race
between stream closure and new stream registration. The comment explicitly
says "in race condition."

### Case: SeaweedFS upstream — reservation timeout

`topology/node.go:294`

```go
const reservationTimeout = 5 * time.Minute // TODO: make this configurable
```

Reservations are cleaned up after an arbitrary 5-minute timeout. This is a
workaround for failed volume creation that doesn't release its reservation.
The timeout exists because there is no proper completion/failure signal for
the reservation lifecycle.

### Case: Ceph — PG peering state explosion

Ceph's PG peering state machine grew from ~8 states in early versions to ~20+
states. Many intermediate states (`Stray`, `GetMissing`, `WaitUpThru`,
`Incomplete`) were introduced for specific race conditions between OSD restart
timing and monitor OSDMap propagation.

`WaitUpThru` exists solely because the monitor's OSDMap epoch update can
arrive before or after PG peering completes — it is a timing workaround state
that has persisted for over a decade.

### Case: DRBD — connection state proliferation

DRBD's original 4 connection states (StandAlone, Connecting, Connected,
Disconnecting) grew to include `NetworkFailure`, `Unconnected`, `Timeout`,
`BrokenPipe`, `TearDown` — many of which handle specific TCP failure modes
rather than semantic differences.

### Fix pattern

```
Before adding a new state, ask:
1. Does this express a new semantic distinction? (keep)
2. Or does it exist to survive a specific timing race? (reject)

If the answer is (2), fix the timing issue in the runtime layer
(retry, epoch comparison, idempotent processing) rather than adding
a new state to the semantic engine.
```

---

## A7: Transport Mechanics Leak Into the Semantic Engine

**Definition**: TCP connection management, retry logic, goroutine scheduling,
or wire protocol details appear inside the semantic state machine instead of
being handled by runtime adapters.

**Why it is wrong**: The semantic engine should process facts and produce
decisions. It should not know or care about TCP, gRPC, goroutines, or wire
formats. When transport mechanics leak in, the engine becomes coupled to a
specific runtime implementation and cannot be tested or reasoned about
independently.

**Correct principle**: The engine processes facts. Runtime adapters handle
transport.

### Case: SeaweedFS sw-block — sender registry as transport gate

The V2 engine's sender registry (`Orchestrator.Registry`) serves dual purpose:
semantic session tracking AND transport connection management. When
`InvalidateSessionCommand` fires, it nulls the sender's session (semantic)
and the same sender is used by the ack observation path to validate incoming
TCP frames (transport). The semantic invalidation kills the transport path.

This coupling caused the "sender not found" rebuild failure: the engine's
semantic state change (invalidate session) removed the transport path needed
by the rebuild coordinator.

### Case: Ceph — messenger abstraction

Ceph explicitly separates the semantic layer (PG peering state machine) from
the transport layer (msgr2 messenger). The PG state machine never directly
touches TCP connections. It produces messages that the messenger delivers.
Connection failures are reported to the state machine as facts, not as
transport errors.

### Fix pattern

```
Engine interface:
  Input: facts (epoch, R, S, H, ack kind, session ID)
  Output: decisions (no-op, catchup, rebuild, promote)

Runtime adapter:
  Translates TCP events → facts
  Translates decisions → TCP actions
  Handles retry, timeout, connection lifecycle

The engine never imports net, io, or sync packages.
```

---

## Summary Table

| Anti-Pattern | SeaweedFS upstream | sw-block V2 | Ceph | DRBD |
|---|---|---|---|---|
| A1: Timer defines semantics | Dead node detection (60s) | Fast rejoin missed | OSD flapping storms | Ping timeout resync |
| A2: Transport = recovery | One replica fails all | Ship() 3s deadline | — | TCP drop = full resync |
| A3: Ack = terminal success | Local write before ack | Double SessionCompleted/Failed | Committed before fsync | — |
| A4: Ordering determines result | Volume growth race | Sender removed before rebuild | Solved: collect all facts first | — |
| A5: Projection as control | ReadOnly drives assignment | — | PG state as scheduler input | — |
| A6: Workaround states | Counter field, reservation timeout | — | WaitUpThru, 20+ PG states | 9+ connection states |
| A7: Transport in engine | — | Sender registry dual-purpose | Solved: messenger abstraction | — |

---

## Using This Document

When reviewing a protocol change:

1. Check if the change matches any anti-pattern above
2. If it does, ask: can the same goal be achieved without the anti-pattern?
3. Apply the "fix pattern" suggested for that anti-pattern
4. If no clean alternative exists, document why the exception is necessary

When designing a new protocol:

1. Separate identity truth (who), reachability truth (can I reach), recovery
   truth (what data action), and execution (transport mechanics)
2. Ensure each truth domain has a single authority
3. Ensure timers trigger observations, never semantic decisions
4. Ensure same facts produce same decisions regardless of event order
5. Keep transport mechanics in runtime adapters, not the semantic engine

---

*This document is a living reference. Add new cases as they are discovered.*
