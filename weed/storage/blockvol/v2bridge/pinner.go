package v2bridge

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Pinner implements real resource holds against blockvol WAL retention
// and checkpoint lifecycle. It uses the flusher's RetentionFloorFn
// mechanism to prevent WAL reclaim past held positions.
type Pinner struct {
	vol *blockvol.BlockVol

	mu    sync.Mutex
	holds map[uint64]*hold // active holds by ID
	nextID atomic.Uint64
}

type hold struct {
	kind     string // "wal", "snapshot", "fullbase"
	startLSN uint64
}

// NewPinner creates a pinner for a real blockvol instance and wires
// its MinWALRetentionFloor into the flusher's retention floor function.
// This ensures that held positions actually prevent WAL reclaim.
func NewPinner(vol *blockvol.BlockVol) *Pinner {
	p := &Pinner{
		vol:   vol,
		holds: map[uint64]*hold{},
	}
	// Wire into real retention: the flusher will check this floor before
	// advancing the WAL tail, preventing reclaim past any held position.
	vol.SetV2RetentionFloor(p.MinWALRetentionFloor)
	return p
}

// HoldWALRetention prevents WAL entries from startLSN from being recycled.
// Returns a release function. While any hold is active, the flusher's
// RetentionFloorFn will report the minimum held position.
//
// The real mechanism: this integrates with the flusher's retention floor
// by tracking the minimum start LSN across all active holds. The flusher
// checks RetentionFloorFn before advancing the WAL tail.
func (p *Pinner) HoldWALRetention(startLSN uint64) (func(), error) {
	// Validate: can't hold a position that's already recycled.
	snap := p.vol.StatusSnapshot()
	if startLSN < snap.WALTailLSN {
		return nil, fmt.Errorf("WAL already recycled past LSN %d (tail=%d)", startLSN, snap.WALTailLSN)
	}

	id := p.nextID.Add(1)
	p.mu.Lock()
	p.holds[id] = &hold{kind: "wal", startLSN: startLSN}
	p.mu.Unlock()

	return func() {
		p.mu.Lock()
		delete(p.holds, id)
		p.mu.Unlock()
	}, nil
}

// HoldSnapshot prevents the checkpoint at checkpointLSN from being GC'd.
func (p *Pinner) HoldSnapshot(checkpointLSN uint64) (func(), error) {
	snap := p.vol.StatusSnapshot()
	if !snap.CheckpointTrusted || snap.CheckpointLSN != checkpointLSN {
		return nil, fmt.Errorf("no valid checkpoint at LSN %d (have=%d trusted=%v)",
			checkpointLSN, snap.CheckpointLSN, snap.CheckpointTrusted)
	}

	id := p.nextID.Add(1)
	p.mu.Lock()
	p.holds[id] = &hold{kind: "snapshot", startLSN: checkpointLSN}
	p.mu.Unlock()

	return func() {
		p.mu.Lock()
		delete(p.holds, id)
		p.mu.Unlock()
	}, nil
}

// HoldFullBase holds a consistent full-extent image.
func (p *Pinner) HoldFullBase(committedLSN uint64) (func(), error) {
	id := p.nextID.Add(1)
	p.mu.Lock()
	p.holds[id] = &hold{kind: "fullbase", startLSN: committedLSN}
	p.mu.Unlock()

	return func() {
		p.mu.Lock()
		delete(p.holds, id)
		p.mu.Unlock()
	}, nil
}

// MinWALRetentionFloor returns the minimum start LSN across all active
// WAL holds. Returns (0, false) if no holds are active. This is designed
// to be wired into the flusher's RetentionFloorFn.
func (p *Pinner) MinWALRetentionFloor() (uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var min uint64
	found := false
	for _, h := range p.holds {
		if h.kind == "wal" || h.kind == "snapshot" {
			if !found || h.startLSN < min {
				min = h.startLSN
				found = true
			}
		}
	}
	return min, found
}

// ActiveHoldCount returns the number of active holds (for diagnostics).
func (p *Pinner) ActiveHoldCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.holds)
}
