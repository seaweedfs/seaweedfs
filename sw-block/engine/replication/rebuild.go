package replication

import "fmt"

// RebuildSource identifies the recovery base.
type RebuildSource string

const (
	RebuildSnapshotTail RebuildSource = "snapshot_tail"
	RebuildFullBase     RebuildSource = "full_base"
)

// RebuildPhase tracks rebuild execution progress.
type RebuildPhase string

const (
	RebuildPhaseInit         RebuildPhase = "init"
	RebuildPhaseSourceSelect RebuildPhase = "source_select"
	RebuildPhaseTransfer     RebuildPhase = "transfer"
	RebuildPhaseTailReplay   RebuildPhase = "tail_replay"
	RebuildPhaseCompleted    RebuildPhase = "completed"
	RebuildPhaseAborted      RebuildPhase = "aborted"
)

// RebuildState tracks rebuild execution. Owned by Session.
type RebuildState struct {
	Source        RebuildSource
	Phase         RebuildPhase
	AbortReason   string
	SnapshotLSN   uint64
	SnapshotValid bool
	TransferredTo uint64
	TailStartLSN  uint64
	TailTargetLSN uint64
	TailReplayedTo uint64
}

// NewRebuildState creates a rebuild state in init phase.
func NewRebuildState() *RebuildState {
	return &RebuildState{Phase: RebuildPhaseInit}
}

// SelectSource chooses rebuild source based on snapshot availability.
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

func (rs *RebuildState) BeginTransfer() error {
	if rs.Phase != RebuildPhaseSourceSelect {
		return fmt.Errorf("rebuild: transfer requires source_select, got %s", rs.Phase)
	}
	rs.Phase = RebuildPhaseTransfer
	return nil
}

func (rs *RebuildState) RecordTransferProgress(transferredTo uint64) error {
	if rs.Phase != RebuildPhaseTransfer {
		return fmt.Errorf("rebuild: progress requires transfer, got %s", rs.Phase)
	}
	if transferredTo <= rs.TransferredTo {
		return fmt.Errorf("rebuild: transfer regression")
	}
	rs.TransferredTo = transferredTo
	return nil
}

func (rs *RebuildState) BeginTailReplay() error {
	if rs.Phase != RebuildPhaseTransfer {
		return fmt.Errorf("rebuild: tail replay requires transfer, got %s", rs.Phase)
	}
	if rs.Source != RebuildSnapshotTail {
		return fmt.Errorf("rebuild: tail replay only for snapshot_tail")
	}
	rs.Phase = RebuildPhaseTailReplay
	return nil
}

func (rs *RebuildState) RecordTailReplayProgress(replayedTo uint64) error {
	if rs.Phase != RebuildPhaseTailReplay {
		return fmt.Errorf("rebuild: tail progress requires tail_replay, got %s", rs.Phase)
	}
	if replayedTo <= rs.TailReplayedTo {
		return fmt.Errorf("rebuild: tail regression")
	}
	rs.TailReplayedTo = replayedTo
	return nil
}

func (rs *RebuildState) ReadyToComplete() bool {
	switch rs.Source {
	case RebuildSnapshotTail:
		return rs.Phase == RebuildPhaseTailReplay && rs.TailReplayedTo >= rs.TailTargetLSN
	case RebuildFullBase:
		return rs.Phase == RebuildPhaseTransfer && rs.TransferredTo >= rs.TailTargetLSN
	}
	return false
}

func (rs *RebuildState) Complete() error {
	if !rs.ReadyToComplete() {
		return fmt.Errorf("rebuild: not ready (source=%s phase=%s)", rs.Source, rs.Phase)
	}
	rs.Phase = RebuildPhaseCompleted
	return nil
}

func (rs *RebuildState) Abort(reason string) {
	if rs.Phase == RebuildPhaseCompleted || rs.Phase == RebuildPhaseAborted {
		return
	}
	rs.Phase = RebuildPhaseAborted
	rs.AbortReason = reason
}
