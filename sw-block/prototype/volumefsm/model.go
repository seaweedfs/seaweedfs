package volumefsm

import fsmv2 "github.com/seaweedfs/seaweedfs/sw-block/prototype/fsmv2"

type Mode string

const (
	ModeBestEffort Mode = "best_effort"
	ModeSyncAll    Mode = "sync_all"
	ModeSyncQuorum Mode = "sync_quorum"
)

type PrimaryState string

const (
	PrimaryServing  PrimaryState = "serving"
	PrimaryDraining PrimaryState = "draining"
	PrimaryLost     PrimaryState = "lost"
)

type Replica struct {
	ID string
	FSM *fsmv2.FSM
}

type Model struct {
	Epoch        uint64
	PrimaryID    string
	PrimaryState PrimaryState
	Mode         Mode

	HeadLSN       uint64
	CheckpointLSN uint64

	RequiredReplicaIDs []string
	Replicas           map[string]*Replica
	Planner            RecoveryPlanner
}

func New(primaryID string, mode Mode, epoch uint64, replicaIDs ...string) *Model {
	m := &Model{
		Epoch:        epoch,
		PrimaryID:    primaryID,
		PrimaryState: PrimaryServing,
		Mode:         mode,
		Replicas:     make(map[string]*Replica, len(replicaIDs)),
		Planner:      StaticRecoveryPlanner{},
	}
	for _, id := range replicaIDs {
		m.Replicas[id] = &Replica{ID: id, FSM: fsmv2.New(epoch)}
		m.RequiredReplicaIDs = append(m.RequiredReplicaIDs, id)
	}
	return m
}

func (m *Model) Replica(id string) *Replica {
		return m.Replicas[id]
}

func (m *Model) SyncEligibleCount() int {
	count := 0
	for _, id := range m.RequiredReplicaIDs {
		r := m.Replicas[id]
		if r != nil && r.FSM.IsSyncEligible() {
			count++
		}
	}
	return count
}

func (m *Model) DurableReplicaCount(targetLSN uint64) int {
	count := 0
	for _, id := range m.RequiredReplicaIDs {
		r := m.Replicas[id]
		if r != nil && r.FSM.IsSyncEligible() && r.FSM.ReplicaFlushedLSN >= targetLSN {
			count++
		}
	}
	return count
}

func (m *Model) Quorum() int {
	rf := len(m.RequiredReplicaIDs) + 1
	return rf/2 + 1
}

func (m *Model) CanServeWrite() bool {
	return m.WriteAdmission().Allowed
}

type AdmissionDecision struct {
	Allowed bool
	Reason  string
}

func (m *Model) WriteAdmission() AdmissionDecision {
	if m.PrimaryState != PrimaryServing {
		return AdmissionDecision{Allowed: false, Reason: "primary_not_serving"}
	}
	switch m.Mode {
	case ModeBestEffort:
		return AdmissionDecision{Allowed: true, Reason: "best_effort_local_durable"}
	case ModeSyncAll:
		if m.SyncEligibleCount() == len(m.RequiredReplicaIDs) {
			return AdmissionDecision{Allowed: true, Reason: "all_replicas_sync_eligible"}
		}
		return AdmissionDecision{Allowed: false, Reason: "required_replica_not_in_sync"}
	case ModeSyncQuorum:
		if 1+m.SyncEligibleCount() >= m.Quorum() {
			return AdmissionDecision{Allowed: true, Reason: "quorum_sync_eligible"}
		}
		return AdmissionDecision{Allowed: false, Reason: "quorum_not_available"}
	default:
		return AdmissionDecision{Allowed: false, Reason: "unknown_mode"}
	}
}

func (m *Model) CanAcknowledgeLSN(targetLSN uint64) bool {
	return m.AckAdmission(targetLSN).Allowed
}

func (m *Model) AckAdmission(targetLSN uint64) AdmissionDecision {
	if m.PrimaryState != PrimaryServing {
		return AdmissionDecision{Allowed: false, Reason: "primary_not_serving"}
	}
	switch m.Mode {
	case ModeBestEffort:
		return AdmissionDecision{Allowed: true, Reason: "best_effort_local_durable"}
	case ModeSyncAll:
		if m.DurableReplicaCount(targetLSN) == len(m.RequiredReplicaIDs) {
			return AdmissionDecision{Allowed: true, Reason: "all_replicas_durable"}
		}
		return AdmissionDecision{Allowed: false, Reason: "required_replica_not_durable"}
	case ModeSyncQuorum:
		if 1+m.DurableReplicaCount(targetLSN) >= m.Quorum() {
			return AdmissionDecision{Allowed: true, Reason: "quorum_durable"}
		}
		return AdmissionDecision{Allowed: false, Reason: "durable_quorum_not_available"}
	default:
		return AdmissionDecision{Allowed: false, Reason: "unknown_mode"}
	}
}
