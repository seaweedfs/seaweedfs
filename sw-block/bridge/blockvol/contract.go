package blockvol

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
// Each field maps to a specific blockvol source:
//
//   WALHeadLSN        ← vol.nextLSN - 1 or vol.Status().WALHeadLSN
//   WALTailLSN        ← vol.flusher.RetentionFloor()
//   CommittedLSN      ← vol.distCommit.CommittedLSN()
//   CheckpointLSN     ← vol.flusher.CheckpointLSN()
//   CheckpointTrusted ← vol.superblock.Valid + checkpoint file exists
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

// BlockVolExecutor performs actual recovery I/O. Implemented by the
// weed-side bridge. It does NOT decide recovery policy — it only
// executes what the engine tells it to do.
type BlockVolExecutor interface {
	// StreamWALEntries streams WAL entries from startExclusive+1 to endInclusive
	// to the replica. Returns the highest LSN successfully transferred.
	StreamWALEntries(startExclusive, endInclusive uint64) (transferredTo uint64, err error)

	// TransferSnapshot transfers a checkpoint/snapshot at snapshotLSN to the replica.
	TransferSnapshot(snapshotLSN uint64) error

	// TransferFullBase transfers the full extent image to the replica.
	TransferFullBase(committedLSN uint64) error

	// TruncateWAL removes entries beyond truncateLSN from the replica.
	TruncateWAL(truncateLSN uint64) error
}
