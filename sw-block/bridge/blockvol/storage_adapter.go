package blockvol

import (
	"fmt"
	"sync"
	"sync/atomic"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// BlockVolState represents the real storage state from a blockvol instance.
// This is populated from actual blockvol fields, not reconstructed from
// metadata. Each field maps to a specific blockvol source:
//
//   WALHeadLSN        ← vol.wal.HeadLSN()
//   WALTailLSN        ← vol.flusher.RetentionFloor()
//   CommittedLSN      ← vol.coordinator.CommittedLSN (from group commit)
//   CheckpointLSN     ← vol.superblock.CheckpointLSN
//   CheckpointTrusted ← vol.superblock.Valid && checkpoint file exists
type BlockVolState struct {
	WALHeadLSN        uint64
	WALTailLSN        uint64
	CommittedLSN      uint64
	CheckpointLSN     uint64
	CheckpointTrusted bool
}

// StorageAdapter implements engine.StorageAdapter using real blockvol state.
// It does NOT decide recovery policy — it only exposes storage truth.
type StorageAdapter struct {
	mu        sync.Mutex
	state     BlockVolState
	nextPinID atomic.Uint64

	// Pin tracking (real implementation would hold actual file/WAL references).
	snapshotPins map[uint64]bool
	walPins      map[uint64]bool
	fullBasePins map[uint64]bool
}

// NewStorageAdapter creates a storage adapter. The caller must update
// state via UpdateState when blockvol state changes.
func NewStorageAdapter() *StorageAdapter {
	return &StorageAdapter{
		snapshotPins: map[uint64]bool{},
		walPins:      map[uint64]bool{},
		fullBasePins: map[uint64]bool{},
	}
}

// UpdateState refreshes the adapter's view of blockvol state.
// Called when blockvol completes a checkpoint, advances WAL tail, etc.
func (sa *StorageAdapter) UpdateState(state BlockVolState) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	sa.state = state
}

// GetRetainedHistory returns the current WAL retention state from real
// blockvol fields. This is NOT reconstructed from test inputs.
func (sa *StorageAdapter) GetRetainedHistory() engine.RetainedHistory {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	return engine.RetainedHistory{
		HeadLSN:           sa.state.WALHeadLSN,
		TailLSN:           sa.state.WALTailLSN,
		CommittedLSN:      sa.state.CommittedLSN,
		CheckpointLSN:     sa.state.CheckpointLSN,
		CheckpointTrusted: sa.state.CheckpointTrusted,
	}
}

// PinSnapshot pins a checkpoint for rebuild use.
func (sa *StorageAdapter) PinSnapshot(checkpointLSN uint64) (engine.SnapshotPin, error) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	if !sa.state.CheckpointTrusted || sa.state.CheckpointLSN != checkpointLSN {
		return engine.SnapshotPin{}, fmt.Errorf("no valid checkpoint at LSN %d", checkpointLSN)
	}
	id := sa.nextPinID.Add(1)
	sa.snapshotPins[id] = true
	return engine.SnapshotPin{LSN: checkpointLSN, PinID: id, Valid: true}, nil
}

// ReleaseSnapshot releases a pinned snapshot.
func (sa *StorageAdapter) ReleaseSnapshot(pin engine.SnapshotPin) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	delete(sa.snapshotPins, pin.PinID)
}

// PinWALRetention holds WAL entries from startLSN.
func (sa *StorageAdapter) PinWALRetention(startLSN uint64) (engine.RetentionPin, error) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	if startLSN < sa.state.WALTailLSN {
		return engine.RetentionPin{}, fmt.Errorf("WAL already recycled past LSN %d (tail=%d)", startLSN, sa.state.WALTailLSN)
	}
	id := sa.nextPinID.Add(1)
	sa.walPins[id] = true
	return engine.RetentionPin{StartLSN: startLSN, PinID: id, Valid: true}, nil
}

// ReleaseWALRetention releases a WAL retention hold.
func (sa *StorageAdapter) ReleaseWALRetention(pin engine.RetentionPin) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	delete(sa.walPins, pin.PinID)
}

// PinFullBase pins a full-extent base image for full-base rebuild.
func (sa *StorageAdapter) PinFullBase(committedLSN uint64) (engine.FullBasePin, error) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	id := sa.nextPinID.Add(1)
	sa.fullBasePins[id] = true
	return engine.FullBasePin{CommittedLSN: committedLSN, PinID: id, Valid: true}, nil
}

// ReleaseFullBase releases a pinned full base image.
func (sa *StorageAdapter) ReleaseFullBase(pin engine.FullBasePin) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	delete(sa.fullBasePins, pin.PinID)
}
