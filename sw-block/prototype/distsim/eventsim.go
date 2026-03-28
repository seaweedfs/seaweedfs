// eventsim.go — timeout events and timer-race infrastructure.
//
// This file implements the eventsim layer within the distsim package.
// The two conceptual layers share the Cluster model but serve different purposes:
//
// distsim (protocol layer — cluster.go, protocol.go):
//   - Protocol correctness: epoch fencing, barrier semantics, commit rules
//   - Reference-state validation: AssertCommittedRecoverable
//   - Recoverability logic: catch-up, rebuild, reservation
//   - Promotion/lineage: candidate eligibility, ranking
//   - Endpoint identity: address versioning, stale endpoint rejection
//   - Control-plane flow: heartbeat → detect → assignment
//
// eventsim (timing/race layer — this file):
//   - Explicit timeout events: barrier, catch-up, reservation
//   - Timer-triggered state transitions
//   - Same-tick race resolution: data events process before timeouts
//   - Timeout cancellation on successful ack/convergence
//
// Boundary rule:
//   - A scenario belongs in distsim tests if the bug is protocol-level
//     (wrong state, wrong commit, wrong rejection).
//   - A scenario belongs in eventsim tests if the bug is timing-level
//     (race between ack and timeout, ordering of concurrent events).
//   - Do not duplicate scenarios across both layers unless
//     timer/event ordering is the actual bug surface.

package distsim

import "fmt"

// TimeoutKind identifies the type of timeout event.
type TimeoutKind string

const (
	TimeoutBarrier     TimeoutKind = "barrier"
	TimeoutCatchup     TimeoutKind = "catchup"
	TimeoutReservation TimeoutKind = "reservation"
)

// PendingTimeout represents a registered timeout that has not yet fired or been cancelled.
type PendingTimeout struct {
	Kind       TimeoutKind
	ReplicaID  string
	LSN        uint64 // for barrier timeouts: which LSN's barrier
	DeadlineAt uint64 // absolute tick when timeout fires
	Cancelled  bool
}

// FiredTimeout records a timeout that actually fired (was not cancelled in time).
type FiredTimeout struct {
	PendingTimeout
	FiredAt uint64
}

// barrierExpiredKey uniquely identifies a timed-out barrier instance.
type barrierExpiredKey struct {
	ReplicaID string
	LSN       uint64
}

// RegisterTimeout adds a pending timeout to the cluster.
func (c *Cluster) RegisterTimeout(kind TimeoutKind, replicaID string, lsn uint64, deadline uint64) {
	c.Timeouts = append(c.Timeouts, PendingTimeout{
		Kind:       kind,
		ReplicaID:  replicaID,
		LSN:        lsn,
		DeadlineAt: deadline,
	})
}

// CancelTimeout cancels a pending timeout matching the given kind, replica, and LSN.
// For catch-up/reservation timeouts, LSN is ignored (matched by kind+replica only).
func (c *Cluster) CancelTimeout(kind TimeoutKind, replicaID string, lsn uint64) {
	for i := range c.Timeouts {
		t := &c.Timeouts[i]
		if t.Cancelled {
			continue
		}
		if t.Kind != kind || t.ReplicaID != replicaID {
			continue
		}
		if kind == TimeoutBarrier && t.LSN != lsn {
			continue
		}
		t.Cancelled = true
		c.logEvent(EventTimeoutCancelled, fmt.Sprintf("%s replica=%s lsn=%d", kind, replicaID, t.LSN))
	}
}

// fireTimeouts checks all pending timeouts against the current tick.
// Called by Tick() AFTER message delivery, so data events (acks) get
// a chance to cancel timeouts before they fire. This is the same-tick
// race resolution rule: data before timers.
//
// State-guard rules (prevent stale timeout from mutating post-success state):
//   - CatchupTimeout only fires if replica is still CatchingUp
//   - ReservationTimeout only fires if replica is still CatchingUp
//   - BarrierTimeout marks the barrier instance as expired (late acks rejected)
func (c *Cluster) fireTimeouts() {
	var remaining []PendingTimeout
	for i := range c.Timeouts {
		t := c.Timeouts[i]
		if t.Cancelled {
			continue
		}
		if c.Now < t.DeadlineAt {
			remaining = append(remaining, t)
			continue
		}
		// Check whether the timeout still has authority to mutate state.
		stale := false
		switch t.Kind {
		case TimeoutBarrier:
			// Barrier timeouts always apply — they mark the instance as expired.
		case TimeoutCatchup, TimeoutReservation:
			// Only valid if replica is still CatchingUp. If already recovered
			// or escalated, the timeout is stale and has no authority.
			if n := c.Nodes[t.ReplicaID]; n == nil || n.ReplicaState != NodeStateCatchingUp {
				stale = true
			}
		}

		if stale {
			c.IgnoredTimeouts = append(c.IgnoredTimeouts, FiredTimeout{
				PendingTimeout: t,
				FiredAt:        c.Now,
			})
			c.logEvent(EventTimeoutIgnored, fmt.Sprintf("%s replica=%s lsn=%d (stale)", t.Kind, t.ReplicaID, t.LSN))
			continue
		}

		// Timeout fires with authority.
		c.FiredTimeouts = append(c.FiredTimeouts, FiredTimeout{
			PendingTimeout: t,
			FiredAt:        c.Now,
		})
		c.logEvent(EventTimeoutFired, fmt.Sprintf("%s replica=%s lsn=%d", t.Kind, t.ReplicaID, t.LSN))
		switch t.Kind {
		case TimeoutBarrier:
			c.removeQueuedBarrier(t.ReplicaID, t.LSN)
			c.ExpiredBarriers[barrierExpiredKey{t.ReplicaID, t.LSN}] = true
		case TimeoutCatchup:
			c.Nodes[t.ReplicaID].ReplicaState = NodeStateNeedsRebuild
		case TimeoutReservation:
			c.Nodes[t.ReplicaID].ReplicaState = NodeStateNeedsRebuild
		}
	}
	c.Timeouts = remaining
}

// removeQueuedBarrier removes a re-queuing barrier from the message queue
// after its timeout fires. Without this, the barrier would re-queue indefinitely.
func (c *Cluster) removeQueuedBarrier(replicaID string, lsn uint64) {
	var kept []inFlightMessage
	for _, item := range c.Queue {
		if item.msg.Kind == MsgBarrier && item.msg.To == replicaID && item.msg.TargetLSN == lsn {
			continue
		}
		kept = append(kept, item)
	}
	c.Queue = kept
}

// cancelRecoveryTimeouts cancels all catch-up and reservation timeouts for a replica.
// Called automatically by CatchUpWithEscalation on convergence or escalation,
// so stale timeouts cannot regress a replica that already recovered or failed.
func (c *Cluster) cancelRecoveryTimeouts(replicaID string) {
	c.CancelTimeout(TimeoutCatchup, replicaID, 0)
	c.CancelTimeout(TimeoutReservation, replicaID, 0)
}

// === Tick event log ===

// TickEventKind identifies the type of event within a tick.
type TickEventKind string

const (
	EventDeliveryAccepted  TickEventKind = "delivery_accepted"
	EventDeliveryRejected  TickEventKind = "delivery_rejected"
	EventTimeoutFired      TickEventKind = "timeout_fired"
	EventTimeoutIgnored    TickEventKind = "timeout_ignored"
	EventTimeoutCancelled  TickEventKind = "timeout_cancelled"
)

// TickEvent records a single event within a tick, in processing order.
type TickEvent struct {
	Tick   uint64
	Kind   TickEventKind
	Detail string
}

// logEvent appends a tick event to the cluster's event log.
func (c *Cluster) logEvent(kind TickEventKind, detail string) {
	c.TickLog = append(c.TickLog, TickEvent{Tick: c.Now, Kind: kind, Detail: detail})
}

// TickEventsAt returns all events recorded at a specific tick.
func (c *Cluster) TickEventsAt(tick uint64) []TickEvent {
	var events []TickEvent
	for _, e := range c.TickLog {
		if e.Tick == tick {
			events = append(events, e)
		}
	}
	return events
}

// === Trace infrastructure ===

// Trace captures a snapshot of cluster state for debugging failed scenarios.
// Reusable across test files and future replay/debug tooling.
type Trace struct {
	Tick            uint64
	CommittedLSN    uint64
	PrimaryID       string
	Epoch           uint64
	NodeStates      map[string]string
	FiredTimeouts   []string
	IgnoredTimeouts []string
	TickEvents      []TickEvent // full ordered event log
	Deliveries      int
	Rejections      int
	QueueDepth      int
}

// BuildTrace captures the current cluster state as a debuggable trace.
func BuildTrace(c *Cluster) Trace {
	tr := Trace{
		Tick:         c.Now,
		CommittedLSN: c.Coordinator.CommittedLSN,
		PrimaryID:    c.Coordinator.PrimaryID,
		Epoch:        c.Coordinator.Epoch,
		NodeStates:   map[string]string{},
		TickEvents:   c.TickLog,
		Deliveries:   len(c.Deliveries),
		Rejections:   len(c.Rejected),
		QueueDepth:   len(c.Queue),
	}
	for id, n := range c.Nodes {
		tr.NodeStates[id] = fmt.Sprintf("role=%s state=%s epoch=%d flushed=%d running=%v",
			n.Role, n.ReplicaState, n.Epoch, n.Storage.FlushedLSN, n.Running)
	}
	for _, ft := range c.FiredTimeouts {
		tr.FiredTimeouts = append(tr.FiredTimeouts,
			fmt.Sprintf("%s replica=%s lsn=%d fired_at=%d", ft.Kind, ft.ReplicaID, ft.LSN, ft.FiredAt))
	}
	for _, it := range c.IgnoredTimeouts {
		tr.IgnoredTimeouts = append(tr.IgnoredTimeouts,
			fmt.Sprintf("%s replica=%s lsn=%d stale_at=%d", it.Kind, it.ReplicaID, it.LSN, it.FiredAt))
	}
	return tr
}

// === Query helpers ===

// FiredTimeoutsByKind returns the count of fired timeouts of a specific kind.
func (c *Cluster) FiredTimeoutsByKind(kind TimeoutKind) int {
	count := 0
	for _, ft := range c.FiredTimeouts {
		if ft.Kind == kind {
			count++
		}
	}
	return count
}
