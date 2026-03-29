package enginev2

// CatchUpBudget defines the bounded resource contract for a catch-up session.
// CatchUp is a short-gap, bounded recovery path. When any budget limit is
// exceeded, the session must escalate to NeedsRebuild rather than continuing
// indefinitely.
//
// A zero value for any field means "no limit" for that dimension.
type CatchUpBudget struct {
	// Note: the frozen target is on RecoverySession.FrozenTargetLSN, not here.
	// That field is set unconditionally by BeginCatchUp and enforced by
	// RecordCatchUpProgress regardless of budget presence.

	// MaxDurationTicks is the hard time budget. If the session has not
	// converged within this many ticks, it escalates.
	MaxDurationTicks uint64

	// MaxEntries is the maximum number of WAL entries to replay.
	// Prevents a "short gap" from silently becoming a full rebuild.
	MaxEntries uint64

	// ProgressDeadlineTicks is the stall detection window. If no progress
	// is recorded within this many ticks, the session escalates.
	ProgressDeadlineTicks uint64
}

// BudgetCheck tracks runtime budget consumption for a catch-up session.
type BudgetCheck struct {
	StartTick         uint64 // tick when catch-up began
	EntriesReplayed   uint64 // total entries replayed so far
	LastProgressTick  uint64 // tick of last RecordCatchUpProgress call
}

// BudgetViolation identifies which budget limit was exceeded.
type BudgetViolation string

const (
	BudgetOK              BudgetViolation = ""
	BudgetDurationExceeded BudgetViolation = "duration_exceeded"
	BudgetEntriesExceeded  BudgetViolation = "entries_exceeded"
	BudgetProgressStalled  BudgetViolation = "progress_stalled"
)

// Check evaluates the budget against the current tick. Returns BudgetOK
// if all limits are within bounds, or the specific violation.
func (b *CatchUpBudget) Check(tracker BudgetCheck, currentTick uint64) BudgetViolation {
	if b.MaxDurationTicks > 0 && currentTick-tracker.StartTick > b.MaxDurationTicks {
		return BudgetDurationExceeded
	}
	if b.MaxEntries > 0 && tracker.EntriesReplayed > b.MaxEntries {
		return BudgetEntriesExceeded
	}
	if b.ProgressDeadlineTicks > 0 && tracker.LastProgressTick > 0 &&
		currentTick-tracker.LastProgressTick > b.ProgressDeadlineTicks {
		return BudgetProgressStalled
	}
	return BudgetOK
}
