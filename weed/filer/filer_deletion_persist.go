package filer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Persistent deletion ledger.
//
// Background: the in-memory FileIdDeletionQueue and DeletionRetryQueue lose every
// queued-but-unconfirmed deletion when the filer process restarts (the upstream
// TODO in filer_deletion.go notes this and proposes exactly the "periodic snapshot
// with recovery on startup" strategy implemented here). Because a delete only ever
// enters the pipeline through the queue, a crash between enqueue and the volume
// actually confirming the delete leaks the chunk: nothing remembers it, so it
// becomes an orphan the next fsck sees as 100% orphaned and a later
// meta-replay from a lagging peer can "resurrect" as if it were live data.
//
// The ledger is a single KV entry (KvKeyDeletionLedger) holding the set of fileIds
// that still need to be deleted but have not yet been confirmed gone. It is:
//   - additive: every EnQueue also records the fileId here,
//   - subtractive: only terminal outcomes (success / not-found / permanent)
//     remove it; retryable failures keep it,
//   - snapshotted on a timer and on Shutdown, so a crash loses at most one
//     snapshot interval of the *record*, never the deletion itself — recovery
//     re-enqueues the whole pending set and the idempotent volume delete
//     absorbs anything that actually completed before the crash.
//
// Safety on a lagging peer (the resurrection case): recovered entries are replayed
// into the queue only after a grace window (DeletionRecoveryGrace), long enough for
// the initial peer meta-aggregation to settle so we don't purge a chunk that a
// peer is about to re-reference as live. New live deletes are never gated.
const (
	// KvKeyDeletionLedger is the reserved store key for the persisted ledger,
	// namespaced next to FilerStoreId so it never collides with user data.
	KvKeyDeletionLedger = "filer.deleteQueue.ledger.v1"

	// Defaults; all three overridable via viper (filer.deleteQueue.*).
	defaultDeletionPersistInterval = 10 * time.Second
	defaultDeletionRecoveryGrace   = 30 * time.Second
)

// deletionPersistEnabled reports whether ledger persistence is on.
// Read from viper with a default of true so a plain `weed filer` gets the
// durability guarantee without extra flags.
func deletionPersistEnabled() bool {
	v := util.GetViper()
	v.SetDefault("filer.deleteQueue.persist", true)
	return v.GetBool("filer.deleteQueue.persist")
}

func deletionPersistInterval() time.Duration {
	v := util.GetViper()
	v.SetDefault("filer.deleteQueue.persistInterval", defaultDeletionPersistInterval)
	d := v.GetDuration("filer.deleteQueue.persistInterval")
	if d <= 0 {
		return defaultDeletionPersistInterval
	}
	return d
}

func deletionRecoveryGrace() time.Duration {
	v := util.GetViper()
	v.SetDefault("filer.deleteQueue.recoveryGrace", defaultDeletionRecoveryGrace)
	d := v.GetDuration("filer.deleteQueue.recoveryGrace")
	if d < 0 {
		return 0
	}
	return d
}

// startDeletionLedgerSnapshotter periodically flushes the pending deletion set
// to durable storage. Started from SetStore once the store exists; it exits when
// the process-wide deletionQuit fires (same signal that stops the delete workers),
// so a Shutdown never races a snapshotter writing to a closed store: the final
// synchronous snapshot in Shutdown() covers whatever the loop left dirty.
func (f *Filer) startDeletionLedgerSnapshotter() {
	if !deletionPersistEnabled() {
		return
	}
	go func() {
		ticker := time.NewTicker(deletionPersistInterval())
		defer ticker.Stop()
		for {
			select {
			case <-f.deletionQuit:
				return
			case <-ticker.C:
				f.snapshotDeletionLedger()
			}
		}
	}()
}

// queueDeletions is the single entry point for adding fileIds to the deletion
// pipeline. It keeps the in-memory hot queue AND the durable ledger in sync.
// Safe on a zero-value Filer (tests build struct literals without NewFiler):
// the map is lazily created and the mutex is zero-value friendly.
func (f *Filer) queueDeletions(fileIds ...string) {
	if len(fileIds) == 0 {
		return
	}
	// Hot path: existing in-memory queue, unchanged.
	f.FileIdDeletionQueue.EnQueue(fileIds...)

	// Durable ledger: record what still needs deleting.
	f.deletionLedgerLock.Lock()
	if f.pendingDeletions == nil {
		f.pendingDeletions = make(map[string]struct{}, len(fileIds))
	}
	for _, id := range fileIds {
		if id == "" {
			continue
		}
		if _, exists := f.pendingDeletions[id]; !exists {
			f.pendingDeletions[id] = struct{}{}
			f.deletionLedgerDirty = true
		}
	}
	f.deletionLedgerLock.Unlock()
}

// pendingDeletionCount returns the number of fileIds still tracked as needing
// deletion in the durable ledger. Handy for diagnostics and tests; 0 when the
// map was never initialised.
func (f *Filer) pendingDeletionCount() int {
	f.deletionLedgerLock.Lock()
	defer f.deletionLedgerLock.Unlock()
	return len(f.pendingDeletions)
}

// forgetDeletion removes a fileId from the ledger once its deletion is terminal
// (deleted, already absent, or permanently failed). Non-terminal outcomes
// (retryable) must NOT call this so the entry survives until confirmed.
func (f *Filer) forgetDeletion(fileId string) {
	if fileId == "" {
		return
	}
	f.deletionLedgerLock.Lock()
	if f.pendingDeletions != nil {
		if _, exists := f.pendingDeletions[fileId]; exists {
			delete(f.pendingDeletions, fileId)
			f.deletionLedgerDirty = true
		}
	}
	f.deletionLedgerLock.Unlock()
}

// snapshotDeletionLedger serialises the current pending set to the store.
// Only writes when something changed since the last snapshot to keep KV churn low.
// No-op if persistence is disabled or the store is not wired yet.
func (f *Filer) snapshotDeletionLedger() {
	if !deletionPersistEnabled() || f.Store == nil {
		return
	}

	f.deletionLedgerLock.Lock()
	if !f.deletionLedgerDirty {
		f.deletionLedgerLock.Unlock()
		return
	}
	// Take a stable copy under lock; marshal + KV write happen outside it.
	ids := make([]string, 0, len(f.pendingDeletions))
	for id := range f.pendingDeletions {
		ids = append(ids, id)
	}
	f.deletionLedgerDirty = false
	f.deletionLedgerLock.Unlock()

	payload, err := json.Marshal(ids)
	if err != nil {
		glog.Errorf("failed to marshal deletion ledger (%d ids): %v", len(ids), err)
		// Restore dirty so we retry on the next tick.
		f.deletionLedgerLock.Lock()
		f.deletionLedgerDirty = true
		f.deletionLedgerLock.Unlock()
		return
	}

	if err := f.Store.KvPut(context.Background(), []byte(KvKeyDeletionLedger), payload); err != nil {
		glog.Warningf("failed to persist deletion ledger (%d ids): %v", len(ids), err)
		f.deletionLedgerLock.Lock()
		f.deletionLedgerDirty = true
		f.deletionLedgerLock.Unlock()
		return
	}
	glog.V(3).Infof("persisted deletion ledger: %d pending deletions", len(ids))
}

// reloadDeletionLedger re-enqueues any pending deletions found in the store after
// a restart, so a crash that killed the in-memory queues does not leak chunks.
//
// Recovery is gated by DeletionRecoveryGrace: the read happens immediately (so we
// know what is pending) but the re-enqueue is deferred in a background goroutine
// until the initial peer meta-aggregation has had a chance to settle. This avoids
// purging a chunk that a lagging peer is about to re-reference as live data — the
// "resurrection" hazard — which would turn a stale read into a dangling read.
//
// Safe to call on a Filer with a nil Store (no-op). Idempotent for the volume
// side: a chunk that was actually deleted before the crash re-deletes as not-found.
func (f *Filer) reloadDeletionLedger() {
	if !deletionPersistEnabled() || f.Store == nil {
		return
	}

	payload, err := f.Store.KvGet(context.Background(), []byte(KvKeyDeletionLedger))
	if err != nil {
		if err == ErrKvNotFound {
			glog.V(2).Infof("no persisted deletion ledger to recover")
			return
		}
		glog.Warningf("failed to read persisted deletion ledger: %v", err)
		return
	}

	var ids []string
	if err := json.Unmarshal(payload, &ids); err != nil {
		glog.Warningf("failed to parse persisted deletion ledger (%d bytes): %v", len(payload), err)
		return
	}
	if len(ids) == 0 {
		return
	}

	grace := deletionRecoveryGrace()
	glog.V(0).Infof("recovered %d pending deletions from ledger, applying in %v", len(ids), grace)

	go func() {
		time.Sleep(grace)
		// Re-add to the durable set first (they may have drifted), then push to
		// the hot queue. We deliberately do NOT clear the ledger here: the ledger
		// shrinks only as the delete pipeline confirms each id terminal, so a
		// second crash mid-recovery still replays everything.
		f.deletionLedgerLock.Lock()
		if f.pendingDeletions == nil {
			f.pendingDeletions = make(map[string]struct{}, len(ids))
		}
		for _, id := range ids {
			if id != "" {
				f.pendingDeletions[id] = struct{}{}
			}
		}
		f.deletionLedgerDirty = true
		f.deletionLedgerLock.Unlock()

		f.queueDeletions(ids...)
		glog.V(0).Infof("re-queued %d recovered pending deletions", len(ids))
	}()
}
