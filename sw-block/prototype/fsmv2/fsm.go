package fsmv2

import "fmt"

type State string

const (
	StateBootstrapping     State = "Bootstrapping"
	StateInSync            State = "InSync"
	StateLagging           State = "Lagging"
	StateCatchingUp        State = "CatchingUp"
	StatePromotionHold     State = "PromotionHold"
	StateNeedsRebuild      State = "NeedsRebuild"
	StateRebuilding        State = "Rebuilding"
	StateCatchUpAfterBuild State = "CatchUpAfterRebuild"
	StateFailed            State = "Failed"
)

type Action string

const (
	ActionNone               Action = "None"
	ActionGrantSyncEligibility Action = "GrantSyncEligibility"
	ActionRevokeSyncEligibility Action = "RevokeSyncEligibility"
	ActionStartCatchup       Action = "StartCatchup"
	ActionEnterPromotionHold Action = "EnterPromotionHold"
	ActionStartRebuild       Action = "StartRebuild"
	ActionAbortRecovery      Action = "AbortRecovery"
	ActionFailReplica        Action = "FailReplica"
)

type FSM struct {
	State State
	Epoch uint64

	ReplicaFlushedLSN   uint64
	CatchupStartLSN     uint64
	CatchupTargetLSN    uint64
	PromotionBarrierLSN uint64
	PromotionHoldUntil  uint64

	SnapshotID    string
	SnapshotCpLSN uint64

	RecoveryReservationID string
	ReservationExpiry     uint64
}

func New(epoch uint64) *FSM {
	return &FSM{State: StateBootstrapping, Epoch: epoch}
}

func (f *FSM) IsSyncEligible() bool {
	return f.State == StateInSync
}

func (f *FSM) clearCatchup() {
	f.CatchupStartLSN = 0
	f.CatchupTargetLSN = 0
	f.PromotionBarrierLSN = 0
	f.PromotionHoldUntil = 0
	f.RecoveryReservationID = ""
	f.ReservationExpiry = 0
}

func (f *FSM) clearRebuild() {
	f.SnapshotID = ""
	f.SnapshotCpLSN = 0
}

func invalid(state State, kind EventKind) error {
	return fmt.Errorf("fsmv2: invalid event %s in state %s", kind, state)
}
