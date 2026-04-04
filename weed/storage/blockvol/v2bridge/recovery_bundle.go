package v2bridge

import (
	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// RecoveryBundle holds the concrete bindings built from a BlockVol instance
// for one recovery execution. The host calls BuildRecoveryBundle once per
// recovery attempt; the bundle is then consumed by the runtime coordinator.
type RecoveryBundle struct {
	// Storage is the engine-facing storage adapter for recovery planning.
	Storage engine.StorageAdapter

	// Executor implements both CatchUpIO and RebuildIO for this volume.
	Executor *Executor
}

// BuildRecoveryBundle assembles all recovery bindings from a real BlockVol.
// This is the backend-binding factory — it knows how to create Reader,
// Pinner, StorageAdapter, and Executor from a concrete BlockVol instance.
//
// The host shell (block_recovery.go) calls this inside WithVolume and
// does not need to know about Reader/Pinner/StorageAdapter construction.
func BuildRecoveryBundle(vol *blockvol.BlockVol, rebuildAddr string) *RecoveryBundle {
	reader := NewReader(vol)
	pinner := NewPinner(vol)
	sa := bridge.NewStorageAdapter(reader, pinner)
	executor := NewExecutor(vol, rebuildAddr)
	return &RecoveryBundle{
		Storage:  sa,
		Executor: executor,
	}
}
