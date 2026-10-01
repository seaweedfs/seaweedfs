package filer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
// The ledger is a KV entry per filer (KvKeyDeletionLedger suffixed with this
// filer's address, so filers sharing one store never overwrite each other's
// pending sets) holding the fileIds that still need to be deleted but have not
// yet been confirmed gone. It is:
//   - additive: every EnQueue also records the fileId here,
//   - subtractive: only terminal outcomes (success / not-found / permanent)
//     remove it; retryable failures keep it,
//   - snapshotted when it changes, on a timer, and on Shutdown, so a crash
//     loses only ids queued in the last in-flight write — recovery re-enqueues
//     the whole pending set and the idempotent volume delete absorbs anything
//     that actually completed before the crash.
//
// Stores that cap value size (FoundationDB at 100KB) get the set split into
// part keys when one value would exceed deletionLedgerPartSize.
//
// Safety on a lagging peer (the resurrection case): recovered entries are replayed
// into the queue only after a grace window (DeletionRecoveryGrace), long enough for
// the initial peer meta-aggregation to settle so we don't purge a chunk that a
// peer is about to re-reference as live. New live deletes are never gated.
const (
	// KvKeyDeletionLedger is the reserved store key prefix for the persisted
	// ledger, namespaced next to FilerStoreId so it never collides with user
	// data. The full key is this prefix plus the filer's own address.
	KvKeyDeletionLedger = "filer.deleteQueue.ledger.v1"

	// deletionLedgerPartSize bounds one ledger value; stores with a value cap
	// reject anything larger, which would strand a big backlog in memory only.
	deletionLedgerPartSize = 64 * 1024

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

// deletionLedgerKey scopes the ledger to this filer so several filers sharing
// one metadata store do not overwrite each other's pending sets. The filer's
// advertised address is stable across restarts; a filer literal without one
// (tests) falls back to the shared base key.
func (f *Filer) deletionLedgerKey() string {
	if f.Dlm != nil && f.Dlm.Host != "" {
		return KvKeyDeletionLedger + "." + f.Dlm.Host.ToHttpAddress()
	}
	return KvKeyDeletionLedger
}

func deletionLedgerPartKey(key string, part int) []byte {
	return []byte(fmt.Sprintf("%s.part.%05d", key, part))
}

// startDeletionLedgerSnapshotter periodically flushes the pending deletion set
// to durable storage. It also wakes on deletionLedgerFlush so a queued id is
// persisted within milliseconds instead of a full interval. Started from
// SetStore only after the ledger read succeeded.
func (f *Filer) startDeletionLedgerSnapshotter() {
	if f.deletionLedgerBlocked.Load() {
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
			case <-f.deletionLedgerFlush:
			}
			// A close of deletionQuit may be concurrent with this wake; the
			// final snapshot in Shutdown covers whatever the loop left dirty.
			select {
			case <-f.deletionQuit:
				return
			default:
			}
			f.snapshotDeletionLedger()
		}
	}()
}

// signalLedgerFlush wakes the snapshotter without blocking the caller.
func (f *Filer) signalLedgerFlush() {
	f.deletionLedgerLock.Lock()
	if f.deletionLedgerFlush == nil {
		f.deletionLedgerFlush = make(chan struct{}, 1)
	}
	ch := f.deletionLedgerFlush
	f.deletionLedgerLock.Unlock()
	select {
	case ch <- struct{}{}:
	default:
	}
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
	f.signalLedgerFlush()
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
	f.signalLedgerFlush()
}

// snapshotDeletionLedger serialises the current pending set to the store.
// Only writes when something changed since the last snapshot to keep KV churn low.
// Writes serialize on deletionSnapshotLock so a snapshot in flight when Shutdown
// starts cannot overwrite the final one with an older copy.
func (f *Filer) snapshotDeletionLedger() {
	if !deletionPersistEnabled() || f.Store == nil || f.deletionLedgerBlocked.Load() {
		return
	}
	f.deletionSnapshotLock.Lock()
	defer f.deletionSnapshotLock.Unlock()

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
	prevParts := f.deletionLedgerParts
	f.deletionLedgerLock.Unlock()

	if err := f.writeDeletionLedger(ids, prevParts); err != nil {
		glog.Warningf("failed to persist deletion ledger (%d ids): %v", len(ids), err)
		f.deletionLedgerLock.Lock()
		f.deletionLedgerDirty = true
		f.deletionLedgerLock.Unlock()
		return
	}
	glog.V(3).Infof("persisted deletion ledger: %d pending deletions", len(ids))
}

// writeDeletionLedger persists the id set, splitting it across part keys when
// one value would exceed deletionLedgerPartSize, and removes part keys a
// previous chunked snapshot left beyond the new part count.
func (f *Filer) writeDeletionLedger(ids []string, prevParts int) error {
	ctx := context.Background()
	key := f.deletionLedgerKey()

	var parts [][]byte
	var batch []string
	size := 2 // "[]"
	flush := func() error {
		if batch == nil {
			batch = []string{}
		}
		payload, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		parts = append(parts, payload)
		batch = nil
		size = 2
		return nil
	}
	for _, id := range ids {
		if need := len(id) + 3; len(batch) > 0 && size+need > deletionLedgerPartSize {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, id)
		size += len(id) + 3
	}
	if len(batch) > 0 || len(parts) == 0 {
		if err := flush(); err != nil {
			return err
		}
	}

	wroteParts := 0
	if len(parts) == 1 {
		if err := f.Store.KvPut(ctx, []byte(key), parts[0]); err != nil {
			return err
		}
	} else {
		for i, payload := range parts {
			if err := f.Store.KvPut(ctx, deletionLedgerPartKey(key, i), payload); err != nil {
				return err
			}
		}
		manifest, _ := json.Marshal(struct {
			Parts int `json:"parts"`
		}{len(parts)})
		if err := f.Store.KvPut(ctx, []byte(key), manifest); err != nil {
			return err
		}
		wroteParts = len(parts)
	}
	for i := wroteParts; i < prevParts; i++ {
		// Stale part keys beyond the new count no longer belong to the ledger.
		_ = f.Store.KvDelete(ctx, deletionLedgerPartKey(key, i))
	}
	f.deletionLedgerLock.Lock()
	f.deletionLedgerParts = wroteParts
	f.deletionLedgerLock.Unlock()
	return nil
}

// readDeletionLedger reads the manifest key: a JSON array is the whole set; a
// {"parts":N} manifest points at per-part values under the same key prefix.
func (f *Filer) readDeletionLedger(key string) (ids []string, parts int, err error) {
	ctx := context.Background()

	payload, err := f.Store.KvGet(ctx, []byte(key))
	if err != nil {
		return nil, 0, err
	}
	if !bytes.HasPrefix(bytes.TrimSpace(payload), []byte("{")) {
		if err := json.Unmarshal(payload, &ids); err != nil {
			return nil, 0, err
		}
		return ids, 0, nil
	}
	var manifest struct {
		Parts int `json:"parts"`
	}
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return nil, 0, err
	}
	for i := 0; i < manifest.Parts; i++ {
		partPayload, err := f.Store.KvGet(ctx, deletionLedgerPartKey(key, i))
		if err != nil {
			return nil, 0, err
		}
		var part []string
		if err := json.Unmarshal(partPayload, &part); err != nil {
			return nil, 0, err
		}
		ids = append(ids, part...)
	}
	return ids, manifest.Parts, nil
}

// reloadDeletionLedger re-enqueues any pending deletions found in the store after
// a restart, so a crash that killed the in-memory queues does not leak chunks.
// Recovered ids join the pending set immediately so snapshots rewrite the full
// ledger; only the re-queue waits out DeletionRecoveryGrace.
//
// It reports whether the ledger is usable. A read error or an unparseable
// payload leaves the persisted set unknown, so this run persists nothing
// rather than overwrite the unread ledger with a partial set.
//
// Safe to call on a Filer with a nil Store (no-op). Idempotent for the volume
// side: a chunk that was actually deleted before the crash re-deletes as not-found.
func (f *Filer) reloadDeletionLedger() bool {
	if !deletionPersistEnabled() || f.Store == nil {
		return false
	}

	key := f.deletionLedgerKey()
	ids, parts, err := f.readDeletionLedger(key)
	if err == ErrKvNotFound && key != KvKeyDeletionLedger {
		// Ledgers written before the key was scoped sit under the base key.
		// Any filer may claim one: deleting the chunks is not owner-specific,
		// and removing the key keeps a single filer from replaying it twice.
		ids, parts, err = f.readDeletionLedger(KvKeyDeletionLedger)
		if err == nil {
			for i := 0; i < parts; i++ {
				_ = f.Store.KvDelete(context.Background(), deletionLedgerPartKey(KvKeyDeletionLedger, i))
			}
			_ = f.Store.KvDelete(context.Background(), []byte(KvKeyDeletionLedger))
			parts = 0
		}
	}
	if err != nil {
		if err == ErrKvNotFound {
			return true
		}
		f.deletionLedgerBlocked.Store(true)
		glog.Warningf("failed to read persisted deletion ledger; persistence disabled this run: %v", err)
		return false
	}

	// Merge into the pending set now so an early snapshot rewrites the
	// recovered ids instead of overwriting the ledger with only new ones.
	f.deletionLedgerLock.Lock()
	if len(ids) > 0 {
		if f.pendingDeletions == nil {
			f.pendingDeletions = make(map[string]struct{}, len(ids))
		}
		for _, id := range ids {
			if id != "" {
				f.pendingDeletions[id] = struct{}{}
			}
		}
	}
	f.deletionLedgerParts = parts
	f.deletionLedgerLock.Unlock()

	if len(ids) == 0 {
		return true
	}

	grace := deletionRecoveryGrace()
	glog.V(0).Infof("recovered %d pending deletions from ledger, applying in %v", len(ids), grace)

	go func() {
		time.Sleep(grace)
		// The ids are already in the pending set; only the queue push waits
		// for peer meta-aggregation to settle. The ledger is deliberately NOT
		// cleared here: it shrinks only as the delete pipeline confirms each
		// id terminal, so a second crash mid-recovery replays everything.
		f.queueDeletions(ids...)
		glog.V(0).Infof("re-queued %d recovered pending deletions", len(ids))
	}()
	return true
}
