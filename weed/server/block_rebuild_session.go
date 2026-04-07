package weed_server

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

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
