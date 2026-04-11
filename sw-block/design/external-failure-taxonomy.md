# External Failure Taxonomy

Date: 2026-04-11
Status: reference
Purpose: Catalog real production bugs from Ceph/DRBD/Mayastor/Longhorn,
extract semantic lessons, and map to V2 exposure and V3 prevention rules.

## How to read this document

Each entry answers:
1. What happened (the failure)
2. Why it happened (the root semantic mistake)
3. Did V2 hit this? (or could it)
4. What V3 rule prevents it

The value is not the bugfix — it is the semantic lesson.

---

## Category 1: Membership / Liveness Misjudgment

### 1.1 Ceph: Asymmetric heartbeat flapping
**Source**: [Ceph #14181](https://tracker.ceph.com/issues/14181)

**What**: OSDs flapped up/down continuously. Monitors saw them as dead
(cluster network heartbeat failed), but OSDs saw themselves as alive
(public network to monitor worked). Flap loop: mark down → mark up → repeat.

**Root semantic mistake**: Heartbeat and control-plane used different
network paths. A partial network failure created contradictory liveness
views. Timer-based death declaration with no corroboration.

**V2 exposed?**: Yes. Our fast-rejoin test showed: master didn't detect
3-second replica disconnect because heartbeat cycle was 5 seconds.
Recovery depended on timing, not facts.

**V3 rule**: P1 (timers trigger, facts decide). Liveness observation
on one path must not contradict identity truth on another. Primary
probes independently of master heartbeat.

---

### 1.2 Ceph: Stale epoch checked before sync
**Source**: [Ceph #22673](https://tracker.ceph.com/issues/22673)

**What**: After OSD destroy + recreate, OSD refused to start — checked
DESTROYED flag against locally-cached stale OSDMap epoch instead of
syncing to current epoch first.

**Root semantic mistake**: Membership decision made against stale local
state. The check ran before the sync.

**V2 exposed?**: Similar pattern. Our stale-cleanup race: the master's
heartbeat handler checked the block registry (stale) before the VS had
time to report its volumes. Entry deleted prematurely.

**V3 rule**: P5 (monotonic facts win). Never validate identity against
stale local epoch. Sync to authoritative source first.

---

### 1.3 DRBD: Split brain from asymmetric UUID rules
**Source**: [DRBD 9.0.20-1 release notes](https://github.com/LINBIT/drbd/releases/tag/drbd-9.0.20-1)

**What**: Split brain detected despite quorum, after resize operation.
UUID propagation differed between diskless and disk-backed nodes.

**Root semantic mistake**: Generation identifier (UUID) rules were
asymmetric across node types. Same logical operation produced different
identity artifacts depending on node role.

**V2 exposed?**: Yes. Our replicaID format mismatch: shipper used
`serverID`, engine used `path/serverID`, orchestrator used yet another
format. Same identity, three representations.

**V3 rule**: Single canonical identity format. `MakeReplicaID(path, serverID)`
is the only constructor. No format conversion in hot paths.

---

### 1.4 Longhorn: Instance manager restart blindspot
**Source**: [Longhorn #5809](https://github.com/longhorn/longhorn/issues/5809)

**What**: After instance manager pod restart, Longhorn was unaware
replica processes were dead for 60 seconds. Replicas appeared alive
but were actually killed.

**Root semantic mistake**: The liveness monitor died with the component
it was monitoring. No watchdog survived the restart.

**V2 exposed?**: Yes. When the primary's shipper held a dead TCP
connection after replica restart, it didn't detect the failure until
the next barrier attempt (~30s later).

**V3 rule**: Liveness detection must be independent of the transport
being monitored. The watchdog recheck fires on a timer regardless of
whether Ship/Barrier calls are happening.

---

## Category 2: Recovery Decision Error

### 2.1 DRBD: Full resync when partial would suffice
**Source**: [DRBD 9.1.19 ChangeLog](https://github.com/LINBIT/drbd/blob/drbd-9.1/ChangeLog)

**What**: After brief disconnect + reconnect, DRBD performed full
resync of all data instead of bitmap-tracked partial resync. Multiple
root causes across versions: bitmap loss, UUID mismatch during resync,
UUID history slot corruption.

**Root semantic mistake**: The UUID comparison that decides partial vs
full resync was fragile against transient disconnects. A UUID change
during active resync was misinterpreted as data divergence.

**V2 exposed?**: Yes. Our "no WAL retention" caused every disconnect
(even 3 seconds) to force full 1GB rebuild instead of catchup. The
decision boundary (R < S → rebuild) was too coarse.

**V3 rule**: P4 (deterministic recovery choice). Recovery decision
from bounded facts only: `R >= S → catchup, R < S → rebuild`.
SmartWAL + LBA dirty map eliminates the S boundary entirely — recovery
is always "send dirty blocks from extent."

---

### 2.2 Longhorn: Rebuild sent to wrong replica (address reuse)
**Source**: [Longhorn #5709](https://github.com/longhorn/longhorn/issues/5709)

**What**: After instance-manager restart, rebuild command was sent to
wrong replica. A new volume's replica received the same IP:port as the
old failed replica. Rebuild targeted wrong volume.

**Root semantic mistake**: Recovery command routed by network address,
not by stable replica identity. Address reuse after restart caused
identity collision.

**V2 exposed?**: Yes. Our nil-interface panic in `senderByID` was
caused by a sender lookup returning nil `*Sender` wrapped in a non-nil
interface. The registry keyed by `replicaID` strings, and format
mismatch caused lookup failures.

**V3 rule**: P2 (single authority per truth domain). Replica identity
must use stable, unique IDs (epoch + path + serverID). Never derive
identity from reusable network addresses.

---

### 2.3 Ceph: Backfill cancellation crashes target OSD
**Source**: [Ceph #21613](https://tracker.ceph.com/issues/21613)

**What**: Primary sent REJECT message to backfill target to cancel
backfill. Target crashed — REJECT is a message the target sends to the
primary, not the other way around.

**Root semantic mistake**: Protocol message repurposed in wrong
direction. The state machine on the receiving end never expected this
message from that sender.

**V2 exposed?**: Similar pattern. Our `InvalidateSessionCommand`
dispatch path invalidated the sender's session (semantic cleanup) which
killed the transport path needed by the rebuild coordinator. The
command was semantically valid but operationally destructive in context.

**V3 rule**: Protocol messages have fixed sender/receiver roles.
Recovery commands flow from primary to replica. Acknowledgments flow
from replica to primary. Never reverse the direction.

---

## Category 3: Completion / Durability Illusion

### 3.1 Longhorn: Corrupted replica kept, healthy one discarded
**Source**: [Longhorn #7425](https://github.com/longhorn/longhorn/issues/7425)

**What**: After rebuild timeout + node failure, corrupted replica was
kept and healthy replica was discarded. Autosalvage selected replicas
by `healthyAt` timestamp — corrupted replica had a valid timestamp from
before the rebuild started.

**Root semantic mistake**: Health metadata was not invalidated when
rebuild started. The metadata said "healthy" but the data was corrupt
from a partial rebuild.

**V2 exposed?**: Yes. Our double-event bug: `SessionCompleted` emitted
on ack arrival, then `SessionFailed("sender stopped")` emitted during
executor cleanup. The system briefly declared success then reverted to
failure.

**V3 rule**: A3 (ack arrival ≠ terminal success). Health metadata must
be invalidated at rebuild START, not just at rebuild FAILURE. Terminal
success is emitted only after executor/session closure completes.

---

### 3.2 Longhorn: Healthy replica deleted due to address collision
**Source**: [Longhorn #9216](https://github.com/longhorn/longhorn/issues/9216)

**What**: In a 3-replica degraded volume, the last healthy replica was
deleted. A stopped replica was assigned the same port as a previously
deleted one. The engine saw two replicas at the same address and
deleted the "redundant" one — which was the only healthy copy.

**Root semantic mistake**: Replica identity derived from network address.
Port reuse after restart created identity collision. Two physically
different replicas appeared as the same entity.

**V2 exposed?**: Yes. The `Counter` field in SeaweedFS upstream's
`data_node.go` is a workaround for the same problem — two heartbeat
streams for the same IP:port after fast restart.

**V3 rule**: Replica identity = `(epoch, volumePath, serverID)`.
Immutable, never derived from ports or transport addresses.

---

### 3.3 Ceph: Rolling restarts orphan objects
**Source**: [Ceph #52385](https://tracker.ceph.com/issues/52385)

**What**: After restarting all nodes sequentially without waiting for
`HEALTH_OK`, PGs entered `recovery_unfound` with permanently lost
objects. Objects existed on stray OSDs excluded from the acting set.

**Root semantic mistake**: Peering completion was not waited for before
proceeding with the next restart. Partial membership view during
peering excluded OSDs that had the data.

**V2 exposed?**: Similar risk. If the master's primary refresh isn't
delivered before the next heartbeat cycle, the primary operates with a
partial roster. The rebuild may target the wrong set of replicas.

**V3 rule**: Recovery operations must wait for complete membership view.
Never start recovery with a partial roster.

---

## Category 4: Ordering / Race Conditions

### 4.1 DRBD: Write races with very short resync
**Source**: [DRBD 9.2.14 ChangeLog](https://github.com/LINBIT/drbd/blob/drbd-9.1/ChangeLog)

**What**: A write during a very short resync was not resynced, leaving
replicas divergent. The resync marked the region "clean" while the write
was in-flight.

**Root semantic mistake**: Region bitmap clear was not ordered after
concurrent write drain. The bitmap said "synced" but an in-flight write
had not been applied to the peer.

**V2 exposed?**: Our rebuild bitmap has the same structure. The rule
"WAL-applied wins over base-applied" handles this: the bitmap is SET
by the live lane, and base lane checks before installing. But a race
between bitmap check and base install could produce the same bug if
the lock granularity is wrong.

**V3 rule**: Recovery completion must not mark a region clean while
writes are in-flight to that region. Drain before clear.

---

### 4.2 DRBD: Parallel connection handshake interference
**Source**: [DRBD 9.1.23 ChangeLog](https://github.com/LINBIT/drbd/blob/drbd-9.1/ChangeLog)

**What**: In 3+ node setups, parallel connection establishment left one
connection in `WFBitMapT/Established` — an inconsistent half-syncing
state.

**Root semantic mistake**: Per-connection state transitions were not
isolated. One connection's handshake interfered with another's.

**V2 exposed?**: Our `ShipperGroup` manages multiple shippers. If
`ProbeReconnectAll` probes all replicas in parallel, one probe's state
change could interfere with another's if they share state.

**V3 rule**: Per-replica state must be fully isolated. No shared
mutable state between recovery sessions for different replicas.

---

### 4.3 Longhorn: Zombie engine from rapid attach/detach
**Source**: [Longhorn #11605](https://github.com/longhorn/longhorn/issues/11605)

**What**: Attach + detach within 1 second left an orphaned engine
process. Detach happened before the instance appeared in the CR,
so nothing knew to clean it up.

**Root semantic mistake**: Deletion checked once and gave up.
The target wasn't visible during that single check window because
of CR sync delay.

**V2 exposed?**: Our `cancelAndDrain` with 5-second timeout is the
same pattern. If the catch-up goroutine doesn't exit within 5s, we
abandon it. The zombie goroutine may hold resources indefinitely.

**V3 rule**: Cleanup must retry until confirmed absent. A single
check-miss is not proof of absence.

---

### 4.4 Mayastor: Internal/external state divergence on fault
**Source**: [Mayastor #549](https://github.com/openebs/mayastor/issues/549)

**What**: Faulting a nexus child removed it from internal structure but
NOT from external child list. Destroy caused state machine assertion
failure because internal and external representations disagreed.

**Root semantic mistake**: Partial cleanup. Internal state changed but
external-facing state was not updated atomically.

**V2 exposed?**: Our sender registry vs shipper state divergence.
`InvalidateSessionCommand` nulled the sender's session but the shipper's
transport state was unchanged. The registry said "no session" but the
shipper still had a live TCP connection.

**V3 rule**: State changes must be atomic across all representations.
If you invalidate a session, both the engine state AND the transport
state must reflect it.

---

## Category 5: Background Work Corrupts Semantics

### 5.1 Ceph: Recovery priority starvation
**Source**: [Ceph #62811](https://tracker.ceph.com/issues/62811)

**What**: PGs stuck in `backfilling` indefinitely. Recovery work was
continuously preempted by peering events and sub-op reads from other
PGs. Recovery could never make forward progress.

**Root semantic mistake**: No guaranteed forward-progress for recovery.
Each individual preemption was "correct" but infinite preemption = no
recovery = semantic failure.

**V2 exposed?**: Our recovery goroutine competes with heartbeat
processing on the same thread. If heartbeat processing takes too long
(e.g., many volumes), recovery stalls.

**V3 rule**: Recovery must have guaranteed forward-progress slots that
cannot be infinitely preempted. Rate-limit interruptions.

---

### 5.2 Mayastor: Rebuild thundering herd after node loss
**Source**: [Mayastor #1714](https://github.com/openebs/mayastor/issues/1714)

**What**: When 1 node went down in a 3-node cluster, all volumes
attempted rebuild simultaneously. Connection failures cascaded because
rebuild descriptors couldn't acquire IO channels. Each failed rebuild
faulted its child, and retry created a thundering herd.

**Root semantic mistake**: No admission control for concurrent rebuilds.
Automated remediation without backoff worsened the overload.

**V2 exposed?**: Currently only 1 volume in tests. With 100+ volumes,
simultaneous rebuild after a node failure would create the same
thundering herd on our rebuild server.

**V3 rule**: Rebuild scheduling after node failure must be staggered.
Maximum concurrent rebuilds per node. Exponential backoff on failure.

---

### 5.3 Mayastor: HA feedback loop under shared pool load
**Source**: [Mayastor #1331](https://github.com/openebs/mayastor/issues/1331)

**What**: Shared DiskPool under concurrent IO from 6 pods triggered
timeouts. HA interpreted timeouts as node failure, initiated target
replacement, generating more IO, causing more timeouts. Positive
feedback loop.

**Root semantic mistake**: HA reaction to resource exhaustion did not
include backoff. The "fix" (replace target) consumed more resources
than the failure it was trying to fix.

**V2 exposed?**: If the rebuild server saturates the disk with base
block reads while the primary is also serving live writes, the same
resource exhaustion → timeout → retry loop could occur.

**V3 rule**: HA/rebuild reactions must detect resource exhaustion and
back off. Automated remediation without admission control cascades.

---

## Mapping to V3 Anti-Patterns

| External Lesson | V3 Anti-Pattern | V3 Principle |
|---|---|---|
| Timer-based death without corroboration | A1 | P1: timers trigger, facts decide |
| Stale epoch used for membership decision | A1 | P5: monotonic facts win |
| Transport error → full rebuild | A2 | P6: transport ≠ semantic overreach |
| Ack → success before close | A3 | P2: terminal authority = executor close |
| Address reuse → identity collision | A4 | Stable IDs, never address-derived |
| Health metadata survives rebuild | A5 | P3: projection is derived only |
| Partial cleanup leaves split state | A4 | Atomic state changes across all representations |
| Priority starvation blocks recovery | A6 | Guaranteed forward progress |
| Thundering herd of rebuilds | A6 | Admission control + stagger |

---

## "Would V2 Have This Bug?" Checklist

Use this for self-audit. For each external bug, ask: could our system
hit the same failure class?

| # | Failure Class | V2 Exposed? | Status |
|---|---|---|---|
| 1 | Heartbeat timing = semantic death | Yes (fast rejoin) | Fixed: primary probes independently |
| 2 | Stale local state for membership check | Yes (stale-cleanup race) | Fixed: RegisteredAt grace period |
| 3 | Asymmetric identity rules | Yes (replicaID format mismatch) | Fixed: resolveEngineReplicaID |
| 4 | Liveness monitor dies with target | Yes (shipper holds dead conn) | Partially fixed: watchdog recheck |
| 5 | Transport error → full rebuild | Yes (no WAL retention) | Designed: SmartWAL + LBA dirty map |
| 6 | Address reuse → wrong target | Not yet (single volume tests) | Risk at scale: need stable IDs |
| 7 | Ack = terminal success | Yes (double event) | Fixed: suppression + manual OnRebuildCompleted |
| 8 | Health metadata survives rebuild | Not yet | Risk: need to track |
| 9 | Partial cleanup (sender vs session) | Yes (sender registry) | Fixed: emitTerminal=false |
| 10 | Recovery starvation | Not yet | Risk at scale with many volumes |
| 11 | Rebuild thundering herd | Not yet | Risk at scale: need admission control |

---

*This document is updated as new external cases are discovered.*
