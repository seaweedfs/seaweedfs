// Package v2bridge implements the V2 engine bridge contract interfaces
// using real blockvol internals. This package lives in the weed/ module
// so it can import blockvol directly.
//
// Import direction:
//   v2bridge → blockvol (real state)
//   v2bridge → sw-block/bridge/blockvol (contract types)
//   v2bridge → sw-block/engine/replication (engine types)
package v2bridge

import (
	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Reader implements bridge.BlockVolReader by reading real blockvol fields.
// This is a thin backend binding — it fetches a snapshot from real BlockVol
// and returns the contract type directly. No state-shaping logic here.
type Reader struct {
	vol *blockvol.BlockVol
}

// NewReader creates a reader for a real blockvol instance.
func NewReader(vol *blockvol.BlockVol) *Reader {
	return &Reader{vol: vol}
}

// ReadState reads current blockvol state from real fields.
// Returns bridge.BlockVolState directly (no intermediate local type).
//
// Field mapping:
//   WALHeadLSN        ← StatusSnapshot().WALHeadLSN
//   WALTailLSN        ← StatusSnapshot().WALTailLSN
//   CommittedLSN      ← StatusSnapshot().CommittedLSN
//   CheckpointLSN     ← StatusSnapshot().CheckpointLSN
//   CheckpointTrusted ← StatusSnapshot().CheckpointTrusted
func (r *Reader) ReadState() bridge.BlockVolState {
	snap := r.vol.StatusSnapshot()
	return bridge.BlockVolState{
		WALHeadLSN:        snap.WALHeadLSN,
		WALTailLSN:        snap.WALTailLSN,
		CommittedLSN:      snap.CommittedLSN,
		CheckpointLSN:     snap.CheckpointLSN,
		CheckpointTrusted: snap.CheckpointTrusted,
	}
}
