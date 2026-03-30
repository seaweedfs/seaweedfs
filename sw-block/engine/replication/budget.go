package replication

// CatchUpBudget defines the bounded resource contract for a catch-up session.
// When any limit is exceeded, the session escalates to NeedsRebuild.
// A zero value for any field means "no limit" for that dimension.
//
// Note: the frozen catch-up target is on Session.FrozenTargetLSN, not here.
// FrozenTargetLSN is set unconditionally by BeginCatchUp and enforced by
// RecordCatchUpProgress regardless of budget presence.
type CatchUpBudget struct {
	MaxDurationTicks      uint64 // hard time limit
	MaxEntries            uint64 // max WAL entries to replay
	ProgressDeadlineTicks uint64 // stall detection window
}

// BudgetCheck tracks runtime budget consumption.
type BudgetCheck struct {
	StartTick        uint64
	EntriesReplayed  uint64
	LastProgressTick uint64
}

// BudgetViolation identifies which budget limit was exceeded.
type BudgetViolation string

const (
	BudgetOK               BudgetViolation = ""
	BudgetDurationExceeded BudgetViolation = "duration_exceeded"
	BudgetEntriesExceeded  BudgetViolation = "entries_exceeded"
	BudgetProgressStalled  BudgetViolation = "progress_stalled"
)

// Check evaluates the budget against the current tick.
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
