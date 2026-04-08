package weed_server

import (
	"fmt"
	"log"
	"time"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

type rebuildProgressPin struct {
	SessionID uint64
	FloorLSN  uint64
}

const defaultRebuildAckTimeout = 30 * time.Second

type rebuildAckWatch struct {
	SessionID uint64
	Timer     *time.Timer
}

// ReplicaRebuildSessionSnapshot is the host-visible rebuild session view for one
// local replica volume.
//
// This is the server-layer skeleton for future transport wiring:
//   - sessionControl(start_rebuild) -> StartReplicaRebuildSession
//   - walData -> ApplyReplicaRebuildWALEntry
//   - sessionData -> ApplyReplicaRebuildBaseBlock
//   - sessionData EOF -> MarkReplicaRebuildBaseComplete
//   - sessionAck/progress poll -> ReplicaRebuildSession
//   - completion gate -> TryCompleteReplicaRebuildSession
//   - cancel/supersede -> CancelReplicaRebuildSession
//
// These hooks intentionally do not decode wire messages or decide protocol
// semantics. They only route host-side intent into the local BlockVol surface.
type ReplicaRebuildSessionSnapshot struct {
	Path     string
	Config   blockvol.RebuildSessionConfig
	Progress blockvol.RebuildSessionProgress
}

// StartReplicaRebuildSession installs one local rebuild session for the
// specified replica volume.
func (bs *BlockService) StartReplicaRebuildSession(path string, config blockvol.RebuildSessionConfig) error {
	if bs == nil || bs.blockStore == nil {
		return fmt.Errorf("block service not enabled")
	}
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		return vol.StartRebuildSession(config)
	})
}

// ApplyReplicaRebuildWALEntry routes one WAL data message into the active local
// rebuild session after session ID validation.
func (bs *BlockService) ApplyReplicaRebuildWALEntry(path string, sessionID uint64, entry *blockvol.WALEntry) error {
	if bs == nil || bs.blockStore == nil {
		return fmt.Errorf("block service not enabled")
	}
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		return vol.ApplyRebuildSessionWALEntry(sessionID, entry)
	})
}

// ApplyReplicaRebuildBaseBlock routes one base-lane block into the active local
// rebuild session after session ID validation.
func (bs *BlockService) ApplyReplicaRebuildBaseBlock(path string, sessionID uint64, lba uint64, data []byte) (bool, error) {
	if bs == nil || bs.blockStore == nil {
		return false, fmt.Errorf("block service not enabled")
	}
	applied := false
	err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		var err error
		applied, err = vol.ApplyRebuildSessionBaseBlock(sessionID, lba, data)
		return err
	})
	return applied, err
}

// MarkReplicaRebuildBaseComplete closes the base-copy lane for one local
// rebuild session.
func (bs *BlockService) MarkReplicaRebuildBaseComplete(path string, sessionID uint64, totalBlocks uint64) error {
	if bs == nil || bs.blockStore == nil {
		return fmt.Errorf("block service not enabled")
	}
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		return vol.MarkRebuildSessionBaseComplete(sessionID, totalBlocks)
	})
}

// TryCompleteReplicaRebuildSession evaluates the local dual-lane completion
// gate for one rebuild session.
func (bs *BlockService) TryCompleteReplicaRebuildSession(path string, sessionID uint64) (uint64, bool, error) {
	if bs == nil || bs.blockStore == nil {
		return 0, false, fmt.Errorf("block service not enabled")
	}
	var achieved uint64
	var completed bool
	err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		var err error
		achieved, completed, err = vol.TryCompleteRebuildSession(sessionID)
		return err
	})
	return achieved, completed, err
}

// CancelReplicaRebuildSession cancels the local rebuild session. Used by
// explicit cancel, supersede, or transport teardown paths.
func (bs *BlockService) CancelReplicaRebuildSession(path string, sessionID uint64, reason string) error {
	if bs == nil || bs.blockStore == nil {
		return fmt.Errorf("block service not enabled")
	}
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		return vol.CancelRebuildSession(sessionID, reason)
	})
}

// ReplicaRebuildSession returns the current local rebuild session snapshot for
// sessionAck construction or diagnostics.
func (bs *BlockService) ReplicaRebuildSession(path string) (ReplicaRebuildSessionSnapshot, bool, error) {
	if bs == nil || bs.blockStore == nil {
		return ReplicaRebuildSessionSnapshot{}, false, fmt.Errorf("block service not enabled")
	}
	var snap ReplicaRebuildSessionSnapshot
	var ok bool
	err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		cfg, progress, found := vol.ActiveRebuildSession()
		if !found {
			return nil
		}
		ok = true
		snap = ReplicaRebuildSessionSnapshot{
			Path:     path,
			Config:   cfg,
			Progress: progress,
		}
		return nil
	})
	return snap, ok, err
}

// WireLocalReplicaRebuildSessionAcks attaches the local volume's rebuild-session
// callback surface to the server-layer ObserveReplicaRebuildSessionAck path.
// This is the host-side bridge for automatic ack emission on local rebuild
// execution paths.
func (bs *BlockService) WireLocalReplicaRebuildSessionAcks(path, replicaID string) error {
	if bs == nil || bs.blockStore == nil {
		return fmt.Errorf("block service not enabled")
	}
	if path == "" {
		return fmt.Errorf("path is required")
	}
	if replicaID == "" {
		return fmt.Errorf("replicaID is required")
	}
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		vol.SetOnRebuildSessionAck(func(ack blockvol.SessionAckMsg) {
			if err := bs.ObserveReplicaRebuildSessionAck(path, replicaID, ack); err != nil {
				log.Printf("block service: observe local rebuild session ack %s/%s: %v", path, replicaID, err)
			}
		})
		return nil
	})
}

// ObserveReplicaRebuildSessionAck converts one replica-reported rebuild session
// ack into the corresponding core observation event on the primary.
//
// Accepted is intentionally a no-op for core state: primary-owned session start
// already entered core state when the start_rebuild command was issued.
// The ack path still matters for retention-floor lifecycle, and later phases
// carry progress/completion/failure facts from the replica.
func (bs *BlockService) ObserveReplicaRebuildSessionAck(path, replicaID string, ack blockvol.SessionAckMsg) error {
	if bs == nil {
		return fmt.Errorf("block service not enabled")
	}
	if path == "" {
		return fmt.Errorf("path is required")
	}
	if replicaID == "" {
		return fmt.Errorf("replicaID is required")
	}
	if ack.SessionID == 0 {
		return fmt.Errorf("session ack: session ID is required")
	}
	snap, err := bs.requireReplicaSession(replicaID, ack.SessionID, engine.SessionRebuild)
	if err != nil {
		return err
	}
	bs.updateRebuildAckWatch(path, replicaID, ack)
	bs.updateRebuildProgressPin(path, replicaID, snap, ack)
	if bs.v2Core == nil {
		return nil
	}

	switch ack.Phase {
	case blockvol.SessionAckAccepted:
		return nil
	case blockvol.SessionAckRunning, blockvol.SessionAckBaseComplete:
		achieved := ack.WALAppliedLSN
		if achieved == 0 {
			achieved = ack.AchievedLSN
		}
		if achieved == 0 {
			return nil
		}
		bs.applyCoreEvent(engine.SessionProgressObserved{
			ID:          path,
			ReplicaID:   replicaID,
			Kind:        engine.SessionRebuild,
			AchievedLSN: achieved,
		})
		return nil
	case blockvol.SessionAckCompleted:
		achieved := ack.AchievedLSN
		if achieved == 0 {
			achieved = ack.WALAppliedLSN
		}
		bs.applyCoreEvent(engine.SessionCompleted{
			ID:          path,
			ReplicaID:   replicaID,
			Kind:        engine.SessionRebuild,
			AchievedLSN: achieved,
		})
		return nil
	case blockvol.SessionAckFailed:
		reason := "session_ack_failed"
		if ack.BaseComplete {
			reason = "session_ack_failed_after_base_complete"
		}
		bs.applyCoreEvent(engine.SessionFailed{
			ID:        path,
			ReplicaID: replicaID,
			Kind:      engine.SessionRebuild,
			Reason:    reason,
		})
		return nil
	default:
		return fmt.Errorf("session ack: unsupported phase 0x%02x", ack.Phase)
	}
}

func (bs *BlockService) requireReplicaSession(replicaID string, sessionID uint64, kind engine.SessionKind) (*engine.SessionSnapshot, error) {
	if bs == nil || bs.v2Orchestrator == nil {
		return nil, nil
	}
	sender := bs.v2Orchestrator.Registry.Sender(replicaID)
	if sender == nil {
		return nil, fmt.Errorf("session ack: sender %q not found", replicaID)
	}
	snap := sender.SessionSnapshot()
	if snap == nil {
		return nil, fmt.Errorf("session ack: replica %q has no active session", replicaID)
	}
	if snap.ID != sessionID {
		return nil, fmt.Errorf("session ack: session mismatch: active=%d ack=%d", snap.ID, sessionID)
	}
	if snap.Kind != kind {
		return nil, fmt.Errorf("session ack: session kind mismatch: active=%s ack=%s", snap.Kind, kind)
	}
	return snap, nil
}

func (bs *BlockService) updateRebuildProgressPin(path, replicaID string, snap *engine.SessionSnapshot, ack blockvol.SessionAckMsg) {
	if bs == nil || path == "" || replicaID == "" {
		return
	}
	switch ack.Phase {
	case blockvol.SessionAckCompleted, blockvol.SessionAckFailed:
		bs.clearRebuildProgressPin(path, replicaID, ack.SessionID)
		return
	case blockvol.SessionAckAccepted, blockvol.SessionAckRunning, blockvol.SessionAckBaseComplete:
	default:
		return
	}
	floor := ack.WALAppliedLSN
	if snap != nil && snap.StartLSN > floor {
		floor = snap.StartLSN
	}
	if floor == 0 {
		return
	}
	bs.ensureRebuildProgressPinWired(path)
	bs.rebuildPinMu.Lock()
	if bs.rebuildPins == nil {
		bs.rebuildPins = make(map[string]map[string]rebuildProgressPin)
	}
	if bs.rebuildPins[path] == nil {
		bs.rebuildPins[path] = make(map[string]rebuildProgressPin)
	}
	current := bs.rebuildPins[path][replicaID]
	if current.SessionID == ack.SessionID && current.FloorLSN >= floor {
		bs.rebuildPinMu.Unlock()
		return
	}
	bs.rebuildPins[path][replicaID] = rebuildProgressPin{SessionID: ack.SessionID, FloorLSN: floor}
	bs.rebuildPinMu.Unlock()
}

func (bs *BlockService) updateRebuildAckWatch(path, replicaID string, ack blockvol.SessionAckMsg) {
	if bs == nil || path == "" || replicaID == "" || ack.SessionID == 0 {
		return
	}
	switch ack.Phase {
	case blockvol.SessionAckAccepted, blockvol.SessionAckRunning, blockvol.SessionAckBaseComplete:
		timeout := bs.rebuildAckWatchTimeout()
		bs.rebuildAckMu.Lock()
		if bs.rebuildAckWatches == nil {
			bs.rebuildAckWatches = make(map[string]map[string]*rebuildAckWatch)
		}
		if bs.rebuildAckWatches[path] == nil {
			bs.rebuildAckWatches[path] = make(map[string]*rebuildAckWatch)
		}
		if existing := bs.rebuildAckWatches[path][replicaID]; existing != nil {
			existing.Timer.Stop()
		}
		timer := time.AfterFunc(timeout, func() {
			bs.handleRebuildAckTimeout(path, replicaID, ack.SessionID)
		})
		bs.rebuildAckWatches[path][replicaID] = &rebuildAckWatch{
			SessionID: ack.SessionID,
			Timer:     timer,
		}
		bs.rebuildAckMu.Unlock()
	case blockvol.SessionAckCompleted, blockvol.SessionAckFailed:
		bs.clearRebuildAckWatch(path, replicaID, ack.SessionID)
	}
}

func (bs *BlockService) clearRebuildAckWatch(path, replicaID string, sessionID uint64) {
	if bs == nil || path == "" || replicaID == "" {
		return
	}
	bs.rebuildAckMu.Lock()
	defer bs.rebuildAckMu.Unlock()
	replicas := bs.rebuildAckWatches[path]
	if replicas == nil {
		return
	}
	watch := replicas[replicaID]
	if watch == nil {
		return
	}
	if sessionID != 0 && watch.SessionID != sessionID {
		return
	}
	watch.Timer.Stop()
	delete(replicas, replicaID)
	if len(replicas) == 0 {
		delete(bs.rebuildAckWatches, path)
	}
}

func (bs *BlockService) rebuildAckWatchTimeout() time.Duration {
	if bs == nil || bs.rebuildAckTimeout <= 0 {
		return defaultRebuildAckTimeout
	}
	return bs.rebuildAckTimeout
}

func (bs *BlockService) handleRebuildAckTimeout(path, replicaID string, sessionID uint64) {
	if bs == nil || path == "" || replicaID == "" || sessionID == 0 {
		return
	}
	bs.rebuildAckMu.Lock()
	replicas := bs.rebuildAckWatches[path]
	if replicas == nil {
		bs.rebuildAckMu.Unlock()
		return
	}
	watch := replicas[replicaID]
	if watch == nil || watch.SessionID != sessionID {
		bs.rebuildAckMu.Unlock()
		return
	}
	delete(replicas, replicaID)
	if len(replicas) == 0 {
		delete(bs.rebuildAckWatches, path)
	}
	bs.rebuildAckMu.Unlock()

	log.Printf("block service: rebuild ack timeout %s/%s session=%d", path, replicaID, sessionID)
	bs.clearRebuildProgressPin(path, replicaID, sessionID)
	if err := bs.CancelReplicaRebuildSession(path, sessionID, "rebuild_ack_timeout"); err != nil {
		log.Printf("block service: cancel rebuild session on timeout %s/%s session=%d: %v", path, replicaID, sessionID, err)
	}
}

func (bs *BlockService) clearRebuildProgressPin(path, replicaID string, sessionID uint64) {
	if bs == nil || path == "" || replicaID == "" {
		return
	}
	bs.rebuildPinMu.Lock()
	defer bs.rebuildPinMu.Unlock()
	replicas := bs.rebuildPins[path]
	if replicas == nil {
		return
	}
	current, ok := replicas[replicaID]
	if !ok {
		return
	}
	if sessionID != 0 && current.SessionID != sessionID {
		return
	}
	delete(replicas, replicaID)
	if len(replicas) == 0 {
		delete(bs.rebuildPins, path)
	}
}

func (bs *BlockService) ensureRebuildProgressPinWired(path string) {
	if bs == nil || bs.blockStore == nil || path == "" {
		return
	}
	bs.rebuildPinMu.Lock()
	if bs.rebuildPinInit == nil {
		bs.rebuildPinInit = make(map[string]bool)
	}
	if bs.rebuildPinInit[path] {
		bs.rebuildPinMu.Unlock()
		return
	}
	bs.rebuildPinInit[path] = true
	bs.rebuildPinMu.Unlock()

	_ = bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		vol.SetV2RetentionFloor(func() (uint64, bool) {
			return bs.rebuildProgressPinFloor(path)
		})
		return nil
	})
}

func (bs *BlockService) rebuildProgressPinFloor(path string) (uint64, bool) {
	if bs == nil || path == "" {
		return 0, false
	}
	bs.rebuildPinMu.RLock()
	defer bs.rebuildPinMu.RUnlock()
	replicas := bs.rebuildPins[path]
	if len(replicas) == 0 {
		return 0, false
	}
	var min uint64
	found := false
	for _, pin := range replicas {
		if pin.FloorLSN == 0 {
			continue
		}
		if !found || pin.FloorLSN < min {
			min = pin.FloorLSN
			found = true
		}
	}
	return min, found
}
