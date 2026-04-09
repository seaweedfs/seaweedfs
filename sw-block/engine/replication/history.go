package replication

import "fmt"

// RetainedHistory represents the primary's WAL retention state as seen
// by the recovery decision path. It answers "why is recovery allowed?"
// with executable proof, not just policy assertions.
//
// This is the engine-level equivalent of the prototype's WALHistory —
// it provides the recoverability inputs that ClassifyRecoveryOutcome
// and rebuild-source selection consume.
type RetainedHistory struct {
	// HeadLSN is the highest LSN written to the primary WAL.
	HeadLSN uint64

	// TailLSN is the oldest retained LSN boundary (exclusive).
	// Entries with LSN > TailLSN are available for catch-up.
	// Entries with LSN <= TailLSN have been recycled.
	TailLSN uint64

	// CommittedLSN is the lineage-safe boundary — the highest LSN
	// acknowledged as durable by the commit protocol.
	CommittedLSN uint64

	// CheckpointLSN is the highest LSN with a durable base image.
	// Used for rebuild-source decision: if CheckpointLSN > 0 and
	// the checkpoint is trusted, snapshot+tail rebuild is possible.
	CheckpointLSN uint64

	// CheckpointTrusted indicates whether the checkpoint base image
	// is known to be consistent and usable for rebuild.
	CheckpointTrusted bool
}

// MakeHandshakeResult generates a HandshakeResult from the primary's
// retained history and a replica's reported flushed LSN.
func (rh *RetainedHistory) MakeHandshakeResult(replicaFlushedLSN uint64) HandshakeResult {
	retentionStart := rh.TailLSN + 1
	if rh.TailLSN == 0 {
		retentionStart = 0
	}
	return HandshakeResult{
		ReplicaFlushedLSN: replicaFlushedLSN,
		CommittedLSN:      rh.CommittedLSN,
		RetentionStartLSN: retentionStart,
	}
}

// IsRecoverable checks whether all entries from startExclusive+1 to
// endInclusive are available in the retained WAL.
func (rh *RetainedHistory) IsRecoverable(startExclusive, endInclusive uint64) bool {
	if startExclusive < rh.TailLSN {
		return false
	}
	if endInclusive > rh.HeadLSN {
		return false
	}
	return true
}

// RebuildSourceDecision determines the optimal rebuild source from
// the current retained history state. Snapshot+tail is only chosen
// when BOTH conditions are met:
//   1. A trusted checkpoint exists
//   2. The WAL tail from CheckpointLSN to CommittedLSN is replayable
//      (i.e., CheckpointLSN >= TailLSN and CommittedLSN <= HeadLSN)
func (rh *RetainedHistory) RebuildSourceDecision() (source RebuildSource, snapshotLSN uint64) {
	// CommittedLSN=0 means no lineage-safe boundary exists (e.g., replica is
	// degraded in sync_all mode). Snapshot-tail requires a valid committed
	// endpoint for the tail-replay range. Without it, fall back to full-base.
	if rh.CommittedLSN == 0 {
		return RebuildFullBase, 0
	}
	if rh.CheckpointTrusted && rh.CheckpointLSN > 0 &&
		rh.IsRecoverable(rh.CheckpointLSN, rh.CommittedLSN) {
		return RebuildSnapshotTail, rh.CheckpointLSN
	}
	return RebuildFullBase, 0
}

// RecoverabilityProof explains why a gap is or is not recoverable.
type RecoverabilityProof struct {
	ReplicaFlushedLSN uint64
	CommittedLSN      uint64
	TailLSN           uint64
	HeadLSN           uint64
	Recoverable       bool
	Reason            string
}

// ProveRecoverability generates an explicit proof for a recovery decision.
func (rh *RetainedHistory) ProveRecoverability(replicaFlushedLSN uint64) RecoverabilityProof {
	proof := RecoverabilityProof{
		ReplicaFlushedLSN: replicaFlushedLSN,
		CommittedLSN:      rh.CommittedLSN,
		TailLSN:           rh.TailLSN,
		HeadLSN:           rh.HeadLSN,
	}

	if replicaFlushedLSN == rh.CommittedLSN {
		proof.Recoverable = true
		proof.Reason = "zero_gap"
		return proof
	}

	if replicaFlushedLSN > rh.CommittedLSN {
		proof.Recoverable = true
		proof.Reason = "replica_ahead_needs_truncation"
		return proof
	}

	if rh.IsRecoverable(replicaFlushedLSN, rh.CommittedLSN) {
		proof.Recoverable = true
		proof.Reason = fmt.Sprintf("gap_within_retention: need LSN %d-%d, tail=%d head=%d",
			replicaFlushedLSN+1, rh.CommittedLSN, rh.TailLSN, rh.HeadLSN)
		return proof
	}

	proof.Recoverable = false
	proof.Reason = fmt.Sprintf("gap_beyond_retention: need LSN %d but tail=%d",
		replicaFlushedLSN+1, rh.TailLSN)
	return proof
}
