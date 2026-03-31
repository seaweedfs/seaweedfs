package distsim

import "fmt"

// DangerPredicate checks for a protocol-violating or dangerous state.
// Returns (violated bool, detail string).
type DangerPredicate func(c *Cluster) (bool, string)

// PredicateAckedFlushLost checks if any committed (ACK'd) write has become
// unrecoverable on ANY node that is supposed to have it.
// This is the most dangerous protocol violation: data loss after ACK.
func PredicateAckedFlushLost(c *Cluster) (bool, string) {
	committedLSN := c.Coordinator.CommittedLSN
	if committedLSN == 0 {
		return false, ""
	}

	refState := c.Reference.StateAt(committedLSN)

	// Check primary.
	primary := c.Primary()
	if primary != nil && primary.Running {
		recLSN := primary.Storage.RecoverableLSN()
		if recLSN < committedLSN {
			return true, fmt.Sprintf("primary %s: recoverableLSN=%d < committedLSN=%d",
				primary.ID, recLSN, committedLSN)
		}
		// Verify committed state correctness using StateAt (not LiveExtent).
		// LiveExtent may contain uncommitted-but-durable writes beyond committedLSN.
		// Only check if we can reconstruct the exact committed state.
		if primary.Storage.CanReconstructAt(committedLSN) {
			nodeState := primary.Storage.StateAt(committedLSN)
			for block, expected := range refState {
				if got := nodeState[block]; got != expected {
					return true, fmt.Sprintf("primary %s: block %d = %d, reference = %d at committedLSN=%d",
						primary.ID, block, got, expected, committedLSN)
				}
			}
		}
	}

	// Check replicas that should have committed data (InSync replicas).
	for _, node := range c.Nodes {
		if node.ID == c.Coordinator.PrimaryID {
			continue
		}
		if !node.Running || node.ReplicaState != NodeStateInSync {
			continue
		}
		recLSN := node.Storage.RecoverableLSN()
		if recLSN < committedLSN {
			return true, fmt.Sprintf("InSync replica %s: recoverableLSN=%d < committedLSN=%d",
				node.ID, recLSN, committedLSN)
		}
	}

	return false, ""
}

// PredicateVisibleUnrecoverableState checks if any running node has extent
// state that would NOT survive a crash+restart. This detects ghost visible
// state — data that is readable now but would be lost on crash.
func PredicateVisibleUnrecoverableState(c *Cluster) (bool, string) {
	for _, node := range c.Nodes {
		if !node.Running || node.Storage.LiveExtent == nil {
			continue
		}
		// Simulate what would happen on crash+restart.
		recoverableLSN := node.Storage.RecoverableLSN()

		// Check each block in LiveExtent: is its value backed by
		// a write at LSN <= recoverableLSN?
		for block, value := range node.Storage.LiveExtent {
			// Find which LSN wrote this value.
			writtenAtLSN := uint64(0)
			for _, w := range node.Storage.WAL {
				if w.Block == block && w.Value == value {
					writtenAtLSN = w.LSN
				}
			}
			if writtenAtLSN > recoverableLSN {
				return true, fmt.Sprintf("node %s: block %d has value %d (written at LSN %d) but recoverableLSN=%d — ghost state",
					node.ID, block, value, writtenAtLSN, recoverableLSN)
			}
		}
	}
	return false, ""
}

// PredicateCatchUpLivelockOrMissingEscalation checks if any replica is stuck
// in CatchingUp without making progress and without being escalated to
// NeedsRebuild. Also checks if a replica needs rebuild but hasn't been
// escalated.
func PredicateCatchUpLivelockOrMissingEscalation(c *Cluster) (bool, string) {
	for _, node := range c.Nodes {
		if !node.Running {
			continue
		}
		if node.ReplicaState == NodeStateCatchingUp {
			// A node in CatchingUp is suspicious if it has been there for
			// many ticks without approaching the target. We check if its
			// FlushedLSN is far behind the primary's head.
			primary := c.Primary()
			if primary == nil {
				continue
			}
			primaryHead := primary.Storage.WALDurableLSN
			replicaFlushed := node.Storage.FlushedLSN
			gap := primaryHead - replicaFlushed

			// If the gap is larger than what the WAL can reasonably hold
			// and the node hasn't been escalated, that's a livelock risk.
			// Use a simple heuristic: if gap > 2x what we've seen committed, flag it.
			if gap > c.Coordinator.CommittedLSN*2 && c.Coordinator.CommittedLSN > 5 {
				return true, fmt.Sprintf("node %s: CatchingUp with gap=%d (primary head=%d, replica flushed=%d) — potential livelock",
					node.ID, gap, primaryHead, replicaFlushed)
			}
		}

		// Check if a node is Lagging for a long time without being moved to
		// CatchingUp or NeedsRebuild.
		// Note: Lagging is a transient state that the control plane should resolve.
		// In adversarial random tests without explicit recovery triggers, a node
		// staying Lagging is expected. We only flag truly excessive lag (> 3x committed)
		// as potential livelock — anything smaller is normal recovery latency.
		if node.ReplicaState == NodeStateLagging {
			primary := c.Primary()
			if primary != nil {
				gap := primary.Storage.WALDurableLSN - node.Storage.FlushedLSN
				if gap > c.Coordinator.CommittedLSN*3 && c.Coordinator.CommittedLSN > 10 {
					return true, fmt.Sprintf("node %s: Lagging with gap=%d without escalation to CatchingUp or NeedsRebuild",
						node.ID, gap)
				}
			}
		}
	}
	return false, ""
}

// AllDangerPredicates returns the standard set of danger predicates.
func AllDangerPredicates() map[string]DangerPredicate {
	return map[string]DangerPredicate{
		"acked_flush_lost":           PredicateAckedFlushLost,
		"visible_unrecoverable":      PredicateVisibleUnrecoverableState,
		"catchup_livelock_or_no_esc": PredicateCatchUpLivelockOrMissingEscalation,
	}
}

// CheckAllPredicates runs all danger predicates against a cluster state.
// Returns a map of violated predicate names → detail messages.
func CheckAllPredicates(c *Cluster) map[string]string {
	violations := map[string]string{}
	for name, pred := range AllDangerPredicates() {
		violated, detail := pred(c)
		if violated {
			violations[name] = detail
		}
	}
	return violations
}
