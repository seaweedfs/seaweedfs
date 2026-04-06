package weed_server

import (
	"fmt"
	"strings"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// replicaProtocolExecutionState is the host-side execution contract for one
// replica. It is derived from the engine-owned sender/session snapshot and is
// the only source the data plane should consult for live-tail eligibility.
type replicaProtocolExecutionState struct {
	ReplicaID       string
	SenderState     engine.ReplicaState
	SessionID       uint64
	SessionKind     engine.SessionKind
	SessionPhase    engine.SessionPhase
	StartLSN        uint64
	TargetLSN       uint64
	FrozenTargetLSN uint64
	RecoveredTo     uint64
	SessionActive   bool
	LiveEligible    bool
	Reason          string
}

// volumeProtocolExecutionState groups protocol-aware execution state for one
// primary volume. Keyed by stable ReplicaID.
type volumeProtocolExecutionState struct {
	VolumeID string
	Replicas map[string]replicaProtocolExecutionState
}

func (bs *BlockService) syncProtocolExecutionState(path string) {
	if bs == nil || path == "" {
		return
	}

	state := volumeProtocolExecutionState{
		VolumeID: path,
		Replicas: make(map[string]replicaProtocolExecutionState),
	}

	if bs.v2Orchestrator != nil {
		for _, sender := range bs.v2Orchestrator.Registry.All() {
			replicaID := sender.ReplicaID()
			if !strings.HasPrefix(replicaID, path+"/") {
				continue
			}
			state.Replicas[replicaID] = deriveReplicaProtocolExecutionState(sender)
		}
	}

	bs.protocolExecMu.Lock()
	if bs.protocolExec == nil {
		bs.protocolExec = make(map[string]volumeProtocolExecutionState)
	}
	if len(state.Replicas) == 0 {
		delete(bs.protocolExec, path)
	} else {
		bs.protocolExec[path] = state
	}
	bs.protocolExecMu.Unlock()

	bs.bindProtocolExecutionPolicy(path)
}

func deriveReplicaProtocolExecutionState(sender *engine.Sender) replicaProtocolExecutionState {
	state := replicaProtocolExecutionState{
		ReplicaID:    sender.ReplicaID(),
		SenderState:  sender.State(),
		LiveEligible: true,
	}

	snap := sender.SessionSnapshot()
	if snap == nil {
		return state
	}

	state.SessionID = snap.ID
	state.SessionKind = snap.Kind
	state.SessionPhase = snap.Phase
	state.StartLSN = snap.StartLSN
	state.TargetLSN = snap.TargetLSN
	state.FrozenTargetLSN = snap.FrozenTargetLSN
	state.RecoveredTo = snap.RecoveredTo
	state.SessionActive = snap.Active
	if snap.Active {
		state.LiveEligible = false
		targetLSN := snap.FrozenTargetLSN
		if targetLSN == 0 {
			targetLSN = snap.TargetLSN
		}
		state.Reason = fmt.Sprintf("active_%s_session phase=%s start=%d target=%d recovered=%d",
			snap.Kind, snap.Phase, snap.StartLSN, targetLSN, snap.RecoveredTo)
	}

	return state
}

func (bs *BlockService) bindProtocolExecutionPolicy(path string) {
	if bs == nil || bs.blockStore == nil || path == "" {
		return
	}
	_ = bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		vol.SetLiveShippingPolicy(func(replicaID string, entryLSN uint64) (bool, string) {
			return bs.protocolLiveShippingAllowed(path, replicaID, entryLSN)
		})
		return nil
	})
}

func (bs *BlockService) protocolLiveShippingAllowed(path, replicaID string, entryLSN uint64) (bool, string) {
	if bs == nil {
		return true, ""
	}
	bs.protocolExecMu.RLock()
	state, ok := bs.protocolExec[path]
	bs.protocolExecMu.RUnlock()
	if !ok {
		return true, ""
	}
	replica, ok := state.Replicas[replicaID]
	if !ok {
		return true, ""
	}
	if replica.LiveEligible {
		return true, ""
	}
	reason := replica.Reason
	if reason == "" {
		reason = fmt.Sprintf("live_tail_not_allowed lsn=%d", entryLSN)
	}
	return false, reason
}

// ProtocolExecutionState returns a copy of the cached protocol-aware execution
// state for tests and diagnostics.
func (bs *BlockService) ProtocolExecutionState(path string) (volumeProtocolExecutionState, bool) {
	if bs == nil {
		return volumeProtocolExecutionState{}, false
	}
	bs.protocolExecMu.RLock()
	defer bs.protocolExecMu.RUnlock()
	state, ok := bs.protocolExec[path]
	if !ok {
		return volumeProtocolExecutionState{}, false
	}
	out := volumeProtocolExecutionState{
		VolumeID: state.VolumeID,
		Replicas: make(map[string]replicaProtocolExecutionState, len(state.Replicas)),
	}
	for replicaID, replica := range state.Replicas {
		out.Replicas[replicaID] = replica
	}
	return out, true
}
