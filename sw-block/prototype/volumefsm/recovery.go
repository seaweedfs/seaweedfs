package volumefsm

type RecoveryClass string

const (
	RecoveryClassWALInline        RecoveryClass = "wal_inline"
	RecoveryClassExtentReferenced RecoveryClass = "extent_referenced"
)

type RecoveryDisposition string

const (
	RecoveryCatchup     RecoveryDisposition = "catchup"
	RecoveryNeedsRebuild RecoveryDisposition = "needs_rebuild"
)

type RecoveryDecision struct {
	Disposition  RecoveryDisposition
	ReservationID string
	ReservationTTL uint64
	Reason       string
	Classes      []RecoveryClass
}

type RecoveryPlanner interface {
	PlanReconnect(replicaID string, flushedLSN, targetLSN uint64) RecoveryDecision
}

// StaticRecoveryPlanner is the minimal default planner for the prototype.
// If targetLSN >= flushedLSN and the caller provided a real target, reconnect
// is treated as catch-up; otherwise rebuild is required.
type StaticRecoveryPlanner struct{}

func (StaticRecoveryPlanner) PlanReconnect(replicaID string, flushedLSN, targetLSN uint64) RecoveryDecision {
	if targetLSN > flushedLSN {
		return RecoveryDecision{
			Disposition:   RecoveryCatchup,
			ReservationID: replicaID + "-resv",
			ReservationTTL: 100,
			Reason:        "static_recoverable_window",
			Classes:       []RecoveryClass{RecoveryClassWALInline},
		}
	}
	return RecoveryDecision{
		Disposition: RecoveryNeedsRebuild,
		Reason:      "static_no_recoverable_window",
	}
}

// ScriptedRecoveryPlanner returns pre-seeded reconnect decisions in order.
// Once the scripted list is exhausted, the last decision is reused.
type ScriptedRecoveryPlanner struct {
	Decisions []RecoveryDecision
	index     int
}

func (s *ScriptedRecoveryPlanner) PlanReconnect(replicaID string, flushedLSN, targetLSN uint64) RecoveryDecision {
	if len(s.Decisions) == 0 {
		return RecoveryDecision{
			Disposition: RecoveryNeedsRebuild,
			Reason:      "scripted_no_decision",
		}
	}
	if s.index >= len(s.Decisions) {
		return s.Decisions[len(s.Decisions)-1]
	}
	d := s.Decisions[s.index]
	s.index++
	return d
}
