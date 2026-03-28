package volumefsm

import fsmv2 "github.com/seaweedfs/seaweedfs/sw-block/prototype/fsmv2"

type EventKind string

const (
	EventWriteCommitted     EventKind = "WriteCommitted"
	EventCheckpointAdvanced EventKind = "CheckpointAdvanced"
	EventBarrierCompleted   EventKind = "BarrierCompleted"
	EventBootstrapReplica   EventKind = "BootstrapReplica"
	EventReplicaDisconnect  EventKind = "ReplicaDisconnect"
	EventReplicaReconnect   EventKind = "ReplicaReconnect"
	EventReplicaNeedsRebuild EventKind = "ReplicaNeedsRebuild"
	EventReplicaCatchupProgress EventKind = "ReplicaCatchupProgress"
	EventReplicaPromotionHealthy EventKind = "ReplicaPromotionHealthy"
	EventReplicaStartRebuild EventKind = "ReplicaStartRebuild"
	EventReplicaRebuildBaseApplied EventKind = "ReplicaRebuildBaseApplied"
	EventReplicaReservationLost EventKind = "ReplicaReservationLost"
	EventReplicaCatchupTimeout EventKind = "ReplicaCatchupTimeout"
	EventReplicaRebuildTooSlow EventKind = "ReplicaRebuildTooSlow"
	EventPrimaryLeaseLost   EventKind = "PrimaryLeaseLost"
	EventPromoteReplica     EventKind = "PromoteReplica"
)

type Event struct {
	Kind      EventKind
	ReplicaID string

	LSN             uint64
	CheckpointLSN   uint64
	ReplicaFlushedLSN uint64
	TargetLSN       uint64
	Now             uint64
	HoldUntil       uint64
	SnapshotID      string
	SnapshotCpLSN   uint64
	ReservationID   string
	ReservationTTL  uint64
}

func (m *Model) Apply(evt Event) error {
	switch evt.Kind {
	case EventWriteCommitted:
		if evt.LSN > m.HeadLSN {
			m.HeadLSN = evt.LSN
		} else {
			m.HeadLSN++
		}
		return nil
	case EventCheckpointAdvanced:
		if evt.CheckpointLSN > m.CheckpointLSN {
			m.CheckpointLSN = evt.CheckpointLSN
		}
		return nil
	case EventPrimaryLeaseLost:
		m.PrimaryState = PrimaryLost
		m.Epoch++
		for _, r := range m.Replicas {
			_, err := r.FSM.Apply(fsmv2.Event{Kind: fsmv2.EventEpochChanged, Epoch: m.Epoch})
			if err != nil {
				return err
			}
		}
		return nil
	case EventPromoteReplica:
		m.PrimaryID = evt.ReplicaID
		m.PrimaryState = PrimaryServing
		m.Epoch++
		for _, r := range m.Replicas {
			_, err := r.FSM.Apply(fsmv2.Event{Kind: fsmv2.EventEpochChanged, Epoch: m.Epoch})
			if err != nil {
				return err
			}
		}
		return nil
	}

	r := m.Replica(evt.ReplicaID)
	if r == nil {
		return nil
	}

	var fEvt fsmv2.Event
	switch evt.Kind {
	case EventBarrierCompleted:
		fEvt = fsmv2.Event{Kind: fsmv2.EventDurableProgress, ReplicaFlushedLSN: evt.ReplicaFlushedLSN}
	case EventBootstrapReplica:
		fEvt = fsmv2.Event{Kind: fsmv2.EventBootstrapComplete, ReplicaFlushedLSN: evt.ReplicaFlushedLSN}
	case EventReplicaDisconnect:
		fEvt = fsmv2.Event{Kind: fsmv2.EventDisconnect}
	case EventReplicaReconnect:
		if evt.ReservationID != "" {
			fEvt = fsmv2.Event{Kind: fsmv2.EventReconnectCatchup, ReplicaFlushedLSN: evt.ReplicaFlushedLSN, TargetLSN: evt.TargetLSN, ReservationID: evt.ReservationID, ReservationTTL: evt.ReservationTTL}
		} else {
			fEvt = fsmv2.Event{Kind: fsmv2.EventReconnectRebuild}
		}
	case EventReplicaNeedsRebuild:
		fEvt = fsmv2.Event{Kind: fsmv2.EventReconnectRebuild}
	case EventReplicaCatchupProgress:
		fEvt = fsmv2.Event{Kind: fsmv2.EventCatchupProgress, ReplicaFlushedLSN: evt.ReplicaFlushedLSN, PromotionHoldTill: evt.HoldUntil}
	case EventReplicaPromotionHealthy:
		fEvt = fsmv2.Event{Kind: fsmv2.EventPromotionHealthy, Now: evt.Now}
	case EventReplicaStartRebuild:
		fEvt = fsmv2.Event{Kind: fsmv2.EventStartRebuild, SnapshotID: evt.SnapshotID, SnapshotCpLSN: evt.SnapshotCpLSN, ReservationID: evt.ReservationID, ReservationTTL: evt.ReservationTTL}
	case EventReplicaRebuildBaseApplied:
		fEvt = fsmv2.Event{Kind: fsmv2.EventRebuildBaseApplied, TargetLSN: evt.TargetLSN}
	case EventReplicaReservationLost:
		fEvt = fsmv2.Event{Kind: fsmv2.EventRetentionLost}
	case EventReplicaCatchupTimeout:
		fEvt = fsmv2.Event{Kind: fsmv2.EventCatchupTimeout}
	case EventReplicaRebuildTooSlow:
		fEvt = fsmv2.Event{Kind: fsmv2.EventRebuildTooSlow}
	default:
		return nil
	}
	_, err := r.FSM.Apply(fEvt)
	return err
}

func (m *Model) EvaluateReconnect(replicaID string, flushedLSN, targetLSN uint64) (RecoveryDecision, error) {
	decision := m.Planner.PlanReconnect(replicaID, flushedLSN, targetLSN)
	r := m.Replica(replicaID)
	if r == nil {
		return decision, nil
	}
	switch decision.Disposition {
	case RecoveryCatchup:
		err := m.Apply(Event{
			Kind:              EventReplicaReconnect,
			ReplicaID:         replicaID,
			ReplicaFlushedLSN: flushedLSN,
			TargetLSN:         targetLSN,
			ReservationID:     decision.ReservationID,
			ReservationTTL:    decision.ReservationTTL,
		})
		return decision, err
	default:
		if r.FSM.State == fsmv2.StateNeedsRebuild {
			return decision, nil
		}
		err := m.Apply(Event{
			Kind:      EventReplicaNeedsRebuild,
			ReplicaID: replicaID,
		})
		return decision, err
	}
}
