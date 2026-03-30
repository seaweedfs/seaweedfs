package replication

// === Phase 06: Storage and Control-Plane Adapter Interfaces ===
//
// These interfaces define the boundary between the engine replication core
// and external systems (storage backend, coordinator/control plane).
// The engine consumes these interfaces — it does not reach into storage
// or control-plane internals directly.

// StorageAdapter provides real retained-history and checkpoint state
// from the storage backend. The engine uses this to make recovery
// decisions grounded in actual data, not reconstructed test inputs.
type StorageAdapter interface {
	// GetRetainedHistory returns the current WAL retention state.
	// Must reflect actual TailLSN, HeadLSN, CommittedLSN, and checkpoint.
	GetRetainedHistory() RetainedHistory

	// PinSnapshot pins a checkpoint/base image at the given LSN for
	// rebuild use. The snapshot must not be garbage-collected while pinned.
	// Returns an error if no valid snapshot exists at that LSN.
	PinSnapshot(checkpointLSN uint64) (SnapshotPin, error)

	// ReleaseSnapshot releases a previously pinned snapshot.
	ReleaseSnapshot(pin SnapshotPin)

	// PinWALRetention holds WAL entries from startLSN to prevent reclaim.
	// The engine calls this before starting catch-up to ensure the WAL
	// tail does not advance past the required range.
	PinWALRetention(startLSN uint64) (RetentionPin, error)

	// ReleaseWALRetention releases a WAL retention hold.
	ReleaseWALRetention(pin RetentionPin)
}

// SnapshotPin represents a held reference to a pinned snapshot/checkpoint.
type SnapshotPin struct {
	LSN    uint64
	PinID  uint64 // unique identifier for this pin
	Valid  bool
}

// RetentionPin represents a held reference to a WAL retention range.
type RetentionPin struct {
	StartLSN uint64
	PinID    uint64
	Valid    bool
}

// ControlPlaneAdapter converts external assignment events into
// AssignmentIntent for the orchestrator.
type ControlPlaneAdapter interface {
	// HandleHeartbeat processes a heartbeat from a volume server and
	// returns any assignment updates that should be applied.
	HandleHeartbeat(serverID string, volumes []VolumeHeartbeat) []AssignmentIntent

	// HandleFailover processes a failover event and returns assignments
	// for the affected replicas.
	HandleFailover(deadServerID string) []AssignmentIntent
}

// VolumeHeartbeat represents one volume's state in a heartbeat.
type VolumeHeartbeat struct {
	VolumeID    string
	ReplicaID   string
	Epoch       uint64
	FlushedLSN  uint64
	State       string
	DataAddr    string
	CtrlAddr    string
	AddrVersion uint64
}
