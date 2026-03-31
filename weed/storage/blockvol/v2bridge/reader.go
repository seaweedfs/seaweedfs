// Package v2bridge implements the V2 engine bridge contract interfaces
// using real blockvol internals. This package lives in the weed/ module
// so it can import blockvol directly.
//
// Import direction:
//   v2bridge → blockvol (real state)
//   v2bridge exports types matching sw-block/bridge/blockvol/ contracts
//   v2bridge does NOT import sw-block/ (to avoid cross-module dependency)
//
// The sw-block/bridge/blockvol/ package consumes these implementations
// through its contract interfaces (BlockVolReader, BlockVolPinner, etc.).
package v2bridge

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// BlockVolState mirrors the contract type from sw-block/bridge/blockvol/.
// Fields are populated from real blockvol internals.
type BlockVolState struct {
	WALHeadLSN        uint64
	WALTailLSN        uint64
	CommittedLSN      uint64
	CheckpointLSN     uint64
	CheckpointTrusted bool
}

// Reader implements BlockVolReader by reading real blockvol fields.
type Reader struct {
	vol *blockvol.BlockVol
}

// NewReader creates a reader for a real blockvol instance.
func NewReader(vol *blockvol.BlockVol) *Reader {
	return &Reader{vol: vol}
}

// ReadState reads current blockvol state from real fields.
// Each field maps to a specific blockvol source:
//
//   WALHeadLSN        ← vol.nextLSN - 1 (last written LSN)
//   WALTailLSN        ← vol.wal.Tail() (oldest retained WAL entry)
//   CommittedLSN      ← vol.flusher.CheckpointLSN() (last flushed = committed)
//   CheckpointLSN     ← vol.super.WALCheckpointLSN
//   CheckpointTrusted ← vol.super.Valid (superblock integrity)
//
// Note: CommittedLSN maps to CheckpointLSN in the current V1 model where
// barrier-confirmed = flusher-checkpointed. In V2, these may diverge when
// distributed commit is separated from local flush.
func (r *Reader) ReadState() BlockVolState {
	snap := r.vol.StatusSnapshot()
	return BlockVolState{
		WALHeadLSN:        snap.WALHeadLSN,
		WALTailLSN:        snap.WALTailLSN,
		CommittedLSN:      snap.CommittedLSN,
		CheckpointLSN:     snap.CheckpointLSN,
		CheckpointTrusted: snap.CheckpointTrusted,
	}
}
