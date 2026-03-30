package replication

// HandshakeResult captures what the reconnect handshake reveals about
// a replica's state relative to the primary's lineage-safe boundary.
type HandshakeResult struct {
	ReplicaFlushedLSN uint64 // highest LSN durably persisted on replica
	CommittedLSN      uint64 // lineage-safe recovery target
	RetentionStartLSN uint64 // oldest LSN still available in primary WAL
}

// RecoveryOutcome classifies the gap between replica and primary.
type RecoveryOutcome string

const (
	OutcomeZeroGap      RecoveryOutcome = "zero_gap"
	OutcomeCatchUp      RecoveryOutcome = "catchup"
	OutcomeNeedsRebuild RecoveryOutcome = "needs_rebuild"
)

// ClassifyRecoveryOutcome determines the recovery path from handshake data.
// Zero-gap requires exact equality (FlushedLSN == CommittedLSN).
func ClassifyRecoveryOutcome(result HandshakeResult) RecoveryOutcome {
	if result.ReplicaFlushedLSN == result.CommittedLSN {
		return OutcomeZeroGap
	}
	if result.RetentionStartLSN == 0 || result.ReplicaFlushedLSN+1 >= result.RetentionStartLSN {
		return OutcomeCatchUp
	}
	return OutcomeNeedsRebuild
}
