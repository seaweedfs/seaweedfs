package enginev2

import "fmt"

// RebuildSource identifies the recovery base for a rebuild session.
type RebuildSource string

const (
	// RebuildSnapshotTail uses a trusted base snapshot/checkpoint and
	// replays the retained WAL tail from that point to CommittedLSN.
	// Preferred path — avoids copying the full extent.
	RebuildSnapshotTail RebuildSource = "snapshot_tail"

	// RebuildFullBase copies the full extent from the primary (or a
	// snapshot source) when no acceptable base snapshot exists.
	// Fallback path — expensive but always available.
	RebuildFullBase RebuildSource = "full_base"
)

// RebuildPhase tracks progress within a rebuild session.
type RebuildPhase string

const (
	RebuildPhaseInit         RebuildPhase = "init"
	RebuildPhaseSourceSelect RebuildPhase = "source_select" // choosing snapshot vs full
	RebuildPhaseTransfer     RebuildPhase = "transfer"      // copying base data
	RebuildPhaseTailReplay   RebuildPhase = "tail_replay"   // replaying WAL tail after snapshot
	RebuildPhaseCompleted    RebuildPhase = "completed"
	RebuildPhaseAborted      RebuildPhase = "aborted"
)

// RebuildState tracks the execution state of a rebuild session.
// Owned by the Sender's RecoverySession when Kind == SessionRebuild.
type RebuildState struct {
	Source         RebuildSource
	Phase          RebuildPhase
	AbortReason    string

	// Source selection inputs.
	SnapshotLSN    uint64 // LSN of the best available snapshot (0 = none)
	SnapshotValid  bool   // whether the snapshot is trustworthy

	// Transfer progress.
	TransferredTo  uint64 // highest LSN transferred (base or extent copy)

	// Tail replay progress (snapshot_tail mode only).
	TailStartLSN   uint64 // start of WAL tail replay (= snapshot LSN)
	TailTargetLSN  uint64 // committed boundary
	TailReplayedTo uint64 // highest LSN replayed from tail
}

// NewRebuildState creates a rebuild state in the init phase.
func NewRebuildState() *RebuildState {
	return &RebuildState{Phase: RebuildPhaseInit}
}

// SelectSource chooses the rebuild source based on snapshot availability.
// If a valid snapshot exists, uses snapshot+tail (cheaper).
// Otherwise, uses full base rebuild.
func (rs *RebuildState) SelectSource(snapshotLSN uint64, snapshotValid bool, committedLSN uint64) error {
	if rs.Phase != RebuildPhaseInit {
		return fmt.Errorf("rebuild: source select requires init phase, got %s", rs.Phase)
	}
	rs.SnapshotLSN = snapshotLSN
	rs.SnapshotValid = snapshotValid
	rs.Phase = RebuildPhaseSourceSelect

	if snapshotValid && snapshotLSN > 0 {
		rs.Source = RebuildSnapshotTail
		rs.TailStartLSN = snapshotLSN
		rs.TailTargetLSN = committedLSN
	} else {
		rs.Source = RebuildFullBase
		rs.TailTargetLSN = committedLSN
	}
	return nil
}

// BeginTransfer starts the base data transfer phase.
func (rs *RebuildState) BeginTransfer() error {
	if rs.Phase != RebuildPhaseSourceSelect {
		return fmt.Errorf("rebuild: transfer requires source_select phase, got %s", rs.Phase)
	}
	rs.Phase = RebuildPhaseTransfer
	return nil
}

// RecordTransferProgress records how much base data has been transferred.
func (rs *RebuildState) RecordTransferProgress(transferredTo uint64) error {
	if rs.Phase != RebuildPhaseTransfer {
		return fmt.Errorf("rebuild: progress requires transfer phase, got %s", rs.Phase)
	}
	if transferredTo <= rs.TransferredTo {
		return fmt.Errorf("rebuild: transfer regression: %d <= %d", transferredTo, rs.TransferredTo)
	}
	rs.TransferredTo = transferredTo
	return nil
}

// BeginTailReplay transitions to tail replay after base transfer (snapshot_tail only).
func (rs *RebuildState) BeginTailReplay() error {
	if rs.Phase != RebuildPhaseTransfer {
		return fmt.Errorf("rebuild: tail replay requires transfer phase, got %s", rs.Phase)
	}
	if rs.Source != RebuildSnapshotTail {
		return fmt.Errorf("rebuild: tail replay only valid for snapshot_tail source")
	}
	rs.Phase = RebuildPhaseTailReplay
	return nil
}

// RecordTailReplayProgress records WAL tail replay progress.
func (rs *RebuildState) RecordTailReplayProgress(replayedTo uint64) error {
	if rs.Phase != RebuildPhaseTailReplay {
		return fmt.Errorf("rebuild: tail progress requires tail_replay phase, got %s", rs.Phase)
	}
	if replayedTo <= rs.TailReplayedTo {
		return fmt.Errorf("rebuild: tail regression: %d <= %d", replayedTo, rs.TailReplayedTo)
	}
	rs.TailReplayedTo = replayedTo
	return nil
}

// ReadyToComplete checks whether the rebuild has reached its target.
func (rs *RebuildState) ReadyToComplete() bool {
	switch rs.Source {
	case RebuildSnapshotTail:
		return rs.Phase == RebuildPhaseTailReplay && rs.TailReplayedTo >= rs.TailTargetLSN
	case RebuildFullBase:
		return rs.Phase == RebuildPhaseTransfer && rs.TransferredTo >= rs.TailTargetLSN
	default:
		return false
	}
}

// Complete marks the rebuild as completed.
func (rs *RebuildState) Complete() error {
	if !rs.ReadyToComplete() {
		return fmt.Errorf("rebuild: not ready to complete (source=%s phase=%s)", rs.Source, rs.Phase)
	}
	rs.Phase = RebuildPhaseCompleted
	return nil
}

// Abort marks the rebuild as aborted with a reason.
func (rs *RebuildState) Abort(reason string) {
	if rs.Phase == RebuildPhaseCompleted || rs.Phase == RebuildPhaseAborted {
		return
	}
	rs.Phase = RebuildPhaseAborted
	rs.AbortReason = reason
}
