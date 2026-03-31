package blockvol

import (
	"fmt"
	"sync"
	"sync/atomic"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// StorageAdapter implements engine.StorageAdapter by consuming
// BlockVolReader and BlockVolPinner interfaces. When backed by
// real implementations from weed/storage/blockvol/v2bridge/,
// all fields come from actual blockvol state.
//
// For testing, use PushStorageAdapter (push-based, no blockvol dependency).
type StorageAdapter struct {
	reader BlockVolReader
	pinner BlockVolPinner

	mu        sync.Mutex
	nextPinID atomic.Uint64

	// Release functions keyed by pin ID.
	releaseFuncs map[uint64]func()
}

// NewStorageAdapter creates a storage adapter backed by real blockvol
// reader and pinner interfaces.
func NewStorageAdapter(reader BlockVolReader, pinner BlockVolPinner) *StorageAdapter {
	return &StorageAdapter{
		reader:       reader,
		pinner:       pinner,
		releaseFuncs: map[uint64]func(){},
	}
}

// GetRetainedHistory reads real blockvol state via BlockVolReader.
func (sa *StorageAdapter) GetRetainedHistory() engine.RetainedHistory {
	state := sa.reader.ReadState()
	return engine.RetainedHistory{
		HeadLSN:           state.WALHeadLSN,
		TailLSN:           state.WALTailLSN,
		CommittedLSN:      state.CommittedLSN,
		CheckpointLSN:     state.CheckpointLSN,
		CheckpointTrusted: state.CheckpointTrusted,
	}
}

// PinSnapshot delegates to BlockVolPinner.HoldSnapshot.
func (sa *StorageAdapter) PinSnapshot(checkpointLSN uint64) (engine.SnapshotPin, error) {
	release, err := sa.pinner.HoldSnapshot(checkpointLSN)
	if err != nil {
		return engine.SnapshotPin{}, fmt.Errorf("snapshot pin at LSN %d: %w", checkpointLSN, err)
	}
	id := sa.nextPinID.Add(1)
	sa.mu.Lock()
	sa.releaseFuncs[id] = release
	sa.mu.Unlock()
	return engine.SnapshotPin{LSN: checkpointLSN, PinID: id, Valid: true}, nil
}

// ReleaseSnapshot calls the held release function.
func (sa *StorageAdapter) ReleaseSnapshot(pin engine.SnapshotPin) {
	sa.mu.Lock()
	release := sa.releaseFuncs[pin.PinID]
	delete(sa.releaseFuncs, pin.PinID)
	sa.mu.Unlock()
	if release != nil {
		release()
	}
}

// PinWALRetention delegates to BlockVolPinner.HoldWALRetention.
func (sa *StorageAdapter) PinWALRetention(startLSN uint64) (engine.RetentionPin, error) {
	release, err := sa.pinner.HoldWALRetention(startLSN)
	if err != nil {
		return engine.RetentionPin{}, fmt.Errorf("WAL retention pin at LSN %d: %w", startLSN, err)
	}
	id := sa.nextPinID.Add(1)
	sa.mu.Lock()
	sa.releaseFuncs[id] = release
	sa.mu.Unlock()
	return engine.RetentionPin{StartLSN: startLSN, PinID: id, Valid: true}, nil
}

// ReleaseWALRetention calls the held release function.
func (sa *StorageAdapter) ReleaseWALRetention(pin engine.RetentionPin) {
	sa.mu.Lock()
	release := sa.releaseFuncs[pin.PinID]
	delete(sa.releaseFuncs, pin.PinID)
	sa.mu.Unlock()
	if release != nil {
		release()
	}
}

// PinFullBase delegates to BlockVolPinner.HoldFullBase.
func (sa *StorageAdapter) PinFullBase(committedLSN uint64) (engine.FullBasePin, error) {
	release, err := sa.pinner.HoldFullBase(committedLSN)
	if err != nil {
		return engine.FullBasePin{}, fmt.Errorf("full base pin at LSN %d: %w", committedLSN, err)
	}
	id := sa.nextPinID.Add(1)
	sa.mu.Lock()
	sa.releaseFuncs[id] = release
	sa.mu.Unlock()
	return engine.FullBasePin{CommittedLSN: committedLSN, PinID: id, Valid: true}, nil
}

// ReleaseFullBase calls the held release function.
func (sa *StorageAdapter) ReleaseFullBase(pin engine.FullBasePin) {
	sa.mu.Lock()
	release := sa.releaseFuncs[pin.PinID]
	delete(sa.releaseFuncs, pin.PinID)
	sa.mu.Unlock()
	if release != nil {
		release()
	}
}

// PushStorageAdapter is a test-only adapter that uses push-based state
// updates instead of pulling from a BlockVolReader. For use in tests
// that don't have real blockvol instances.
type PushStorageAdapter struct {
	*StorageAdapter
	state BlockVolState
}

// NewPushStorageAdapter creates a push-based adapter for tests.
func NewPushStorageAdapter() *PushStorageAdapter {
	psa := &PushStorageAdapter{}
	psa.StorageAdapter = NewStorageAdapter(&pushReader{psa: psa}, &pushPinner{psa: psa})
	return psa
}

// UpdateState sets the adapter's state (push model for tests).
func (psa *PushStorageAdapter) UpdateState(state BlockVolState) {
	psa.state = state
}

type pushReader struct{ psa *PushStorageAdapter }

func (pr *pushReader) ReadState() BlockVolState { return pr.psa.state }

type pushPinner struct{ psa *PushStorageAdapter }

func (pp *pushPinner) HoldWALRetention(startLSN uint64) (func(), error) {
	if startLSN < pp.psa.state.WALTailLSN {
		return nil, fmt.Errorf("WAL recycled past %d (tail=%d)", startLSN, pp.psa.state.WALTailLSN)
	}
	return func() {}, nil
}

func (pp *pushPinner) HoldSnapshot(checkpointLSN uint64) (func(), error) {
	if !pp.psa.state.CheckpointTrusted || pp.psa.state.CheckpointLSN != checkpointLSN {
		return nil, fmt.Errorf("no trusted checkpoint at %d", checkpointLSN)
	}
	return func() {}, nil
}

func (pp *pushPinner) HoldFullBase(_ uint64) (func(), error) {
	return func() {}, nil
}
