package fsmv2

func (f *FSM) Apply(evt Event) ([]Action, error) {
	switch evt.Kind {
	case EventEpochChanged:
		if evt.Epoch <= f.Epoch {
			return nil, nil
		}
		f.Epoch = evt.Epoch
		switch f.State {
		case StateInSync:
			f.State = StateLagging
			f.clearCatchup()
			f.clearRebuild()
			return []Action{ActionRevokeSyncEligibility}, nil
		case StateCatchingUp, StatePromotionHold, StateRebuilding, StateCatchUpAfterBuild:
			f.State = StateLagging
			f.clearCatchup()
			f.clearRebuild()
			return []Action{ActionAbortRecovery, ActionRevokeSyncEligibility}, nil
		default:
			f.clearCatchup()
			f.clearRebuild()
			return nil, nil
		}
	case EventFatal:
		f.State = StateFailed
		f.clearCatchup()
		f.clearRebuild()
		return []Action{ActionFailReplica, ActionRevokeSyncEligibility}, nil
	}

	switch f.State {
	case StateBootstrapping:
		switch evt.Kind {
		case EventBootstrapComplete:
			f.ReplicaFlushedLSN = evt.ReplicaFlushedLSN
			f.State = StateInSync
			return []Action{ActionGrantSyncEligibility}, nil
		case EventDisconnect:
			f.State = StateLagging
			return nil, nil
		}
	case StateInSync:
		switch evt.Kind {
		case EventDurableProgress:
			if evt.ReplicaFlushedLSN < f.ReplicaFlushedLSN {
				return nil, invalid(f.State, evt.Kind)
			}
			f.ReplicaFlushedLSN = evt.ReplicaFlushedLSN
			return nil, nil
		case EventDisconnect:
			f.State = StateLagging
			return []Action{ActionRevokeSyncEligibility}, nil
		}
	case StateLagging:
		switch evt.Kind {
		case EventReconnectCatchup:
			f.ReplicaFlushedLSN = evt.ReplicaFlushedLSN
			f.CatchupStartLSN = evt.ReplicaFlushedLSN
			f.CatchupTargetLSN = evt.TargetLSN
			f.PromotionBarrierLSN = evt.TargetLSN
			f.RecoveryReservationID = evt.ReservationID
			f.ReservationExpiry = evt.ReservationTTL
			f.State = StateCatchingUp
			return []Action{ActionStartCatchup}, nil
		case EventReconnectRebuild:
			f.State = StateNeedsRebuild
			return []Action{ActionRevokeSyncEligibility}, nil
		}
	case StateCatchingUp:
		switch evt.Kind {
		case EventCatchupProgress:
			if evt.ReplicaFlushedLSN < f.ReplicaFlushedLSN {
				return nil, invalid(f.State, evt.Kind)
			}
			f.ReplicaFlushedLSN = evt.ReplicaFlushedLSN
			if evt.ReplicaFlushedLSN >= f.PromotionBarrierLSN {
				f.State = StatePromotionHold
				f.PromotionHoldUntil = evt.PromotionHoldTill
				return []Action{ActionEnterPromotionHold}, nil
			}
			return nil, nil
		case EventRetentionLost, EventCatchupTimeout:
			f.State = StateNeedsRebuild
			f.clearCatchup()
			return []Action{ActionAbortRecovery}, nil
		case EventDisconnect:
			f.State = StateLagging
			f.clearCatchup()
			return []Action{ActionAbortRecovery}, nil
		}
	case StatePromotionHold:
		switch evt.Kind {
		case EventDurableProgress:
			if evt.ReplicaFlushedLSN < f.ReplicaFlushedLSN {
				return nil, invalid(f.State, evt.Kind)
			}
			f.ReplicaFlushedLSN = evt.ReplicaFlushedLSN
			return nil, nil
		case EventPromotionHealthy:
			if evt.Now < f.PromotionHoldUntil {
				return nil, nil
			}
			f.State = StateInSync
			f.clearCatchup()
			return []Action{ActionGrantSyncEligibility}, nil
		case EventDisconnect:
			f.State = StateLagging
			f.clearCatchup()
			return []Action{ActionRevokeSyncEligibility}, nil
		}
	case StateNeedsRebuild:
		switch evt.Kind {
		case EventStartRebuild:
			f.State = StateRebuilding
			f.SnapshotID = evt.SnapshotID
			f.SnapshotCpLSN = evt.SnapshotCpLSN
			f.RecoveryReservationID = evt.ReservationID
			f.ReservationExpiry = evt.ReservationTTL
			return []Action{ActionStartRebuild}, nil
		}
	case StateRebuilding:
		switch evt.Kind {
		case EventRebuildBaseApplied:
			f.State = StateCatchUpAfterBuild
			f.ReplicaFlushedLSN = f.SnapshotCpLSN
			f.CatchupStartLSN = f.SnapshotCpLSN
			f.CatchupTargetLSN = evt.TargetLSN
			f.PromotionBarrierLSN = evt.TargetLSN
			return []Action{ActionStartCatchup}, nil
		case EventRetentionLost, EventRebuildTooSlow, EventDisconnect:
			f.State = StateNeedsRebuild
			f.clearCatchup()
			f.clearRebuild()
			return []Action{ActionAbortRecovery}, nil
		}
	case StateCatchUpAfterBuild:
		switch evt.Kind {
		case EventCatchupProgress:
			if evt.ReplicaFlushedLSN < f.ReplicaFlushedLSN {
				return nil, invalid(f.State, evt.Kind)
			}
			f.ReplicaFlushedLSN = evt.ReplicaFlushedLSN
			if evt.ReplicaFlushedLSN >= f.PromotionBarrierLSN {
				f.State = StatePromotionHold
				f.PromotionHoldUntil = evt.PromotionHoldTill
				return []Action{ActionEnterPromotionHold}, nil
			}
			return nil, nil
		case EventRetentionLost, EventCatchupTimeout, EventDisconnect:
			f.State = StateNeedsRebuild
			f.clearCatchup()
			f.clearRebuild()
			return []Action{ActionAbortRecovery}, nil
		}
	case StateFailed:
		return nil, nil
	}

	return nil, invalid(f.State, evt.Kind)
}
