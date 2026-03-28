package enginev2

// HandshakeResult captures what the reconnect handshake reveals about a
// replica's state relative to the primary's lineage-safe boundary.
type HandshakeResult struct {
	ReplicaFlushedLSN uint64 // highest LSN durably persisted on replica
	CommittedLSN      uint64 // lineage-safe recovery target (committed prefix)
	RetentionStartLSN uint64 // oldest LSN still available in primary WAL
}

// RecoveryOutcome classifies the gap between replica and primary.
type RecoveryOutcome string

const (
	OutcomeZeroGap      RecoveryOutcome = "zero_gap"      // replica has full committed prefix
	OutcomeCatchUp      RecoveryOutcome = "catchup"        // gap within WAL retention
	OutcomeNeedsRebuild RecoveryOutcome = "needs_rebuild"  // gap exceeds retention
)

// ClassifyRecoveryOutcome determines the recovery path from handshake data.
//
// Uses CommittedLSN (not WAL head) as the target boundary. This is the
// lineage-safe recovery point — only acknowledged data counts. A replica
// with FlushedLSN > CommittedLSN has divergent/uncommitted tail that must
// NOT be treated as "already in sync."
//
// Decision matrix (matches CP13-5 gap analysis):
//   - ReplicaFlushedLSN >= CommittedLSN        → zero gap, has full committed prefix
//   - ReplicaFlushedLSN+1 >= RetentionStartLSN → recoverable via WAL catch-up
//   - otherwise                                 → gap too large, needs rebuild
func ClassifyRecoveryOutcome(result HandshakeResult) RecoveryOutcome {
	if result.ReplicaFlushedLSN >= result.CommittedLSN {
		return OutcomeZeroGap
	}
	if result.RetentionStartLSN == 0 || result.ReplicaFlushedLSN+1 >= result.RetentionStartLSN {
		return OutcomeCatchUp
	}
	return OutcomeNeedsRebuild
}
