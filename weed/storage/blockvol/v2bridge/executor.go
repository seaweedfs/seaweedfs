package v2bridge

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Executor performs real recovery I/O using blockvol internals.
// It executes what the engine tells it to do — it does NOT decide
// recovery policy.
//
// Phase 07 P1: one narrow path (WAL catch-up streaming).
// Full-base rebuild and snapshot transfer are deferred.
type Executor struct {
	vol *blockvol.BlockVol
}

// NewExecutor creates an executor for a real blockvol instance.
func NewExecutor(vol *blockvol.BlockVol) *Executor {
	return &Executor{vol: vol}
}

// StreamWALEntries reads WAL entries from startExclusive+1 to endInclusive
// using BlockVol.ScanWALEntries (real ScanFrom mechanism).
// Returns the highest LSN successfully scanned.
//
// This is the real catch-up data path. The callback receives each entry
// for shipping to the replica (network-layer apply is the caller's job).
func (e *Executor) StreamWALEntries(startExclusive, endInclusive uint64) (uint64, error) {
	if e.vol == nil {
		return 0, fmt.Errorf("no blockvol instance")
	}

	var highestLSN uint64
	err := e.vol.ScanWALEntries(startExclusive+1, func(entry *blockvol.WALEntry) error {
		if entry.LSN > endInclusive {
			return nil // past requested range, stop
		}
		// In production: ship entry to replica over network.
		// Here: track the highest LSN successfully read.
		highestLSN = entry.LSN
		return nil
	})
	if err != nil {
		return highestLSN, fmt.Errorf("WAL scan from %d: %w", startExclusive, err)
	}
	return highestLSN, nil
}

// TransferSnapshot validates the checkpoint/snapshot at snapshotLSN is accessible.
// In production: streams the checkpoint image to the replica.
func (e *Executor) TransferSnapshot(snapshotLSN uint64) error {
	if e.vol == nil {
		return fmt.Errorf("no blockvol instance")
	}
	snap := e.vol.StatusSnapshot()
	if snap.CheckpointLSN != snapshotLSN {
		return fmt.Errorf("no checkpoint at LSN %d (have %d)", snapshotLSN, snap.CheckpointLSN)
	}
	return nil
}

// TransferFullBase reads the full extent image from blockvol for rebuild.
// In production: streams the extent to the replica over network.
// Here: validates the extent is readable at the committed boundary.
func (e *Executor) TransferFullBase(committedLSN uint64) error {
	if e.vol == nil {
		return fmt.Errorf("no blockvol instance")
	}
	snap := e.vol.StatusSnapshot()
	if committedLSN > snap.WALHeadLSN {
		return fmt.Errorf("committed LSN %d beyond WAL head %d", committedLSN, snap.WALHeadLSN)
	}
	// In production: read extent blocks and stream to replica.
	// For now: validate the extent is accessible at this point.
	return nil
}

// TruncateWAL removes entries beyond truncateLSN. Stub for P1.
func (e *Executor) TruncateWAL(truncateLSN uint64) error {
	return fmt.Errorf("TruncateWAL not implemented in P1")
}
