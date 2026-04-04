package blockvol

import engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"

// === Phase 07 P1: Handoff contract ===
//
// This file defines the interface boundary between:
//   - sw-block/bridge/blockvol/ (engine-side, no weed imports)
//   - weed/storage/blockvol/v2bridge/ (weed-side, real blockvol imports)
//
// The engine-side bridge defines WHAT the weed-side must provide.
// The weed-side bridge implements HOW using real blockvol internals.
//
// Import direction:
//   weed/storage/blockvol/v2bridge/ → imports → sw-block/bridge/blockvol/
//   weed/storage/blockvol/v2bridge/ → imports → sw-block/engine/replication/
//   weed/storage/blockvol/v2bridge/ → imports → weed/storage/blockvol/
//   sw-block/bridge/blockvol/ → imports → sw-block/engine/replication/
//   sw-block/bridge/blockvol/ does NOT import weed/

// BlockVolState represents the real storage state from a blockvol instance.
// Each field maps to a specific blockvol source (current P1 implementation):
//
//	WALHeadLSN        ← vol.nextLSN - 1 (last written LSN)
//	WALTailLSN        ← vol.super.WALCheckpointLSN (LSN boundary, not byte offset)
//	CommittedLSN      ← vol.flusher.CheckpointLSN() (V1 interim: committed = checkpointed)
//	CheckpointLSN     ← vol.super.WALCheckpointLSN (durable base image)
//	CheckpointTrusted ← vol.super.Validate() == nil (superblock integrity)
type BlockVolState struct {
	WALHeadLSN        uint64
	WALTailLSN        uint64
	CommittedLSN      uint64
	CheckpointLSN     uint64
	CheckpointTrusted bool
}

// BlockVolReader reads real blockvol state. Implemented by the weed-side
// bridge using actual blockvol struct fields. The engine-side bridge
// consumes this interface via the StorageAdapter.
type BlockVolReader interface {
	// ReadState returns the current blockvol state snapshot.
	// Must read from real blockvol fields:
	//   WALHeadLSN        ← vol.nextLSN - 1 or vol.Status().WALHeadLSN
	//   WALTailLSN        ← vol.flusher.RetentionFloor()
	//   CommittedLSN      ← vol.distCommit.CommittedLSN()
	//   CheckpointLSN     ← vol.flusher.CheckpointLSN()
	//   CheckpointTrusted ← superblock valid + checkpoint file exists
	ReadState() BlockVolState
}

// BlockVolPinner manages real resource holds against WAL reclaim and
// checkpoint GC. Implemented by the weed-side bridge using actual
// blockvol retention machinery.
type BlockVolPinner interface {
	// HoldWALRetention prevents WAL entries from startLSN from being recycled.
	// Returns a release function that the caller MUST call when done.
	HoldWALRetention(startLSN uint64) (release func(), err error)

	// HoldSnapshot prevents the checkpoint at checkpointLSN from being GC'd.
	// Returns a release function.
	HoldSnapshot(checkpointLSN uint64) (release func(), err error)

	// HoldFullBase holds a consistent full-extent image at committedLSN.
	// Returns a release function.
	HoldFullBase(committedLSN uint64) (release func(), err error)
}

// BlockVolCatchUpIO is the weed-free catch-up execution port. It intentionally
// matches engine.CatchUpIO so executor implementations can plug directly into
// the V2 runtime without importing weed/ into sw-block.
type BlockVolCatchUpIO interface {
	engine.CatchUpIO
}

// BlockVolRebuildIO is the weed-free rebuild execution port. It intentionally
// matches engine.RebuildIO so rebuild mechanics can move behind sw-block-owned
// contracts while real blockvol calls remain in thin adapter implementations.
type BlockVolRebuildIO interface {
	engine.RebuildIO
}

// BlockVolExecutor is the combined execution-muscle surface for the current
// bounded runtime path. Implementations execute I/O only; they do not own
// recovery policy, lifecycle meaning, or publication semantics.
type BlockVolExecutor interface {
	BlockVolCatchUpIO
	BlockVolRebuildIO
}
