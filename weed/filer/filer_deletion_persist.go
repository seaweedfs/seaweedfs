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
// part keys when one value would exceed deletionLedgerPartSize. Multipart
// snapshots write each generation under generation-scoped part keys and
// publish only the manifest last, so a crash mid-write leaves the previous
// generation intact; orphaned parts from abandoned generations are tracked in
// deletionLedgerStale and retried on the next write.
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

	// KvKeyDeletionLedgerIndex lists the scoped ledger keys in the store so a
	// filer that restarts under a different advertised address can still find
	// and claim the ledger it left behind.
	KvKeyDeletionLedgerIndex = KvKeyDeletionLedger + ".index"

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
// one metadata store do not overwrite each other's pending sets. A filer
// literal without an advertised address (tests) falls back to the base key.
func (f *Filer) deletionLedgerKey() string {
	if f.Dlm != nil && f.Dlm.Host != "" {
		return KvKeyDeletionLedger + "." + f.Dlm.Host.ToHttpAddress()
	}
	return KvKeyDeletionLedger
}

func deletionLedgerPartKey(key string, part int) []byte {
	return []byte(fmt.Sprintf("%s.part.%05d", key, part))
}

// deletionLedgerGenPartKey returns the part key for one generation. Generation
// 0 keeps the original format so ledgers written before generation publishing
// still decode.
func deletionLedgerGenPartKey(key string, gen, part int) []byte {
	if gen == 0 {
		return deletionLedgerPartKey(key, part)
	}
	return []byte(fmt.Sprintf("%s.g%06d.part.%05d", key, gen, part))
}

// startDeletionLedgerSnapshotter periodically flushes the pending deletion set
// to durable storage. It also wakes on deletionLedgerFlush so a queued id is
// persisted within milliseconds instead of a full interval. Started from
// SetStore only after the ledger read succeeded.
func (f *Filer) startDeletionLedgerSnapshotter() {
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

	// Durable ledger: record what still needs deleting. Every enqueue bumps the
	// id's epoch so an expiry-forget carrying an older epoch cannot erase the
	// re-queued intent.
	f.deletionLedgerLock.Lock()
	if f.pendingDeletions == nil {
		f.pendingDeletions = make(map[string]uint64, len(fileIds))
	}
	for _, id := range fileIds {
		if id == "" {
			continue
		}
		_, exists := f.pendingDeletions[id]
		f.deletionSeq++
		f.pendingDeletions[id] = f.deletionSeq
		if !exists {
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

// deletionEpoch returns the pending record's current epoch for fileId.
func (f *Filer) deletionEpoch(fileId string) uint64 {
	f.deletionLedgerLock.Lock()
	defer f.deletionLedgerLock.Unlock()
	return f.pendingDeletions[fileId]
}

// forgetDeletion removes a fileId from the ledger once its deletion is terminal
// (deleted or already absent: the chunks are gone, so any pending record — even
// a re-queued one — refers to work that no longer exists). Non-terminal
// outcomes must NOT call this so the entry survives until confirmed.
func (f *Filer) forgetDeletion(fileId string) {
	if fileId == "" {
		return
	}
	f.deletionLedgerLock.Lock()
	if _, exists := f.pendingDeletions[fileId]; exists {
		delete(f.pendingDeletions, fileId)
		f.deletionLedgerDirty = true
	}
	f.deletionLedgerLock.Unlock()
	f.signalLedgerFlush()
}

// forgetDeletionEpoch removes the ledger record only when its epoch still
// matches the one captured when the deletion attempt began. A mismatch means
// the id was re-queued meanwhile, so the newer record wins.
func (f *Filer) forgetDeletionEpoch(fileId string, epoch uint64) {
	if fileId == "" {
		return
	}
	f.deletionLedgerLock.Lock()
	if cur, exists := f.pendingDeletions[fileId]; exists && cur == epoch {
		delete(f.pendingDeletions, fileId)
		f.deletionLedgerDirty = true
	}
	f.deletionLedgerLock.Unlock()
	f.signalLedgerFlush()
}

// snapshotDeletionLedger serialises the current pending set to the store.
// Only writes when something changed since the last snapshot to keep KV churn low.
// Writes serialize on deletionSnapshotLock so a snapshot in flight when Shutdown
// starts cannot overwrite the final one with an older copy.
func (f *Filer) snapshotDeletionLedger() {
	if !deletionPersistEnabled() || f.Store == nil {
		return
	}
	f.deletionSnapshotLock.Lock()
	defer f.deletionSnapshotLock.Unlock()

	if f.deletionLedgerBlocked.Load() && !f.tryUnblockDeletionLedger() {
		return
	}

	f.deletionLedgerLock.Lock()
	if !f.deletionLedgerDirty {
		hasStale := len(f.deletionLedgerStale) > 0
		f.deletionLedgerLock.Unlock()
		if !hasStale {
			return
		}
		// Nothing to republish, but orphaned part keys still need cleanup.
		ctx := context.Background()
		f.flushStaleLedgerParts(ctx)
		f.persistStaleLedgerParts(ctx, f.deletionLedgerKey())
		return
	}
	// Take a stable copy under lock; marshal + KV write happen outside it.
	ids := make([]string, 0, len(f.pendingDeletions))
	for id := range f.pendingDeletions {
		ids = append(ids, id)
	}
	f.deletionLedgerDirty = false
	f.deletionLedgerLock.Unlock()

	if err := f.writeDeletionLedger(ids); err != nil {
		glog.Warningf("failed to persist deletion ledger (%d ids): %v", len(ids), err)
		f.deletionLedgerLock.Lock()
		f.deletionLedgerDirty = true
		f.deletionLedgerLock.Unlock()
		return
	}
	glog.V(3).Infof("persisted deletion ledger: %d pending deletions", len(ids))
}

// tryUnblockDeletionLedger retries the ledger reload that originally failed.
// Once the read succeeds the persisted ids merge back into the pending set and
// snapshots resume; until then nothing is written so the unread ledger cannot
// be overwritten.
func (f *Filer) tryUnblockDeletionLedger() bool {
	f.reloadDeletionLedger()
	return !f.deletionLedgerBlocked.Load()
}

// writeDeletionLedger persists the id set. A single value is published
// atomically; a multipart set writes a new generation's part keys first and
// only then the manifest, so a crash or failed write leaves the previously
// published generation readable. Orphaned parts are retried on the next write.
func (f *Filer) writeDeletionLedger(ids []string) error {
	ctx := context.Background()
	key := f.deletionLedgerKey()

	f.deletionLedgerLock.Lock()
	prevGen, prevParts := f.deletionLedgerGen, f.deletionLedgerParts
	f.deletionLedgerLock.Unlock()

	f.flushStaleLedgerParts(ctx)

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

	if len(parts) == 1 {
		if err := f.Store.KvPut(ctx, []byte(key), parts[0]); err != nil {
			return err
		}
		f.deleteLedgerParts(ctx, key, prevGen, prevParts)
		f.deletionLedgerLock.Lock()
		f.deletionLedgerGen, f.deletionLedgerParts = 0, 0
		f.deletionLedgerLock.Unlock()
	} else {
		gen := prevGen + 1
		wrote := 0
		for i, payload := range parts {
			if err := f.Store.KvPut(ctx, deletionLedgerGenPartKey(key, gen, i), payload); err != nil {
				f.markStaleLedgerParts(key, gen, wrote)
				return err
			}
			wrote++
		}
		manifest, _ := json.Marshal(struct {
			Parts int `json:"parts"`
			Gen   int `json:"gen,omitempty"`
		}{len(parts), gen})
		if err := f.Store.KvPut(ctx, []byte(key), manifest); err != nil {
			f.markStaleLedgerParts(key, gen, wrote)
			return err
		}
		f.deleteLedgerParts(ctx, key, prevGen, prevParts)
		f.deletionLedgerLock.Lock()
		f.deletionLedgerGen, f.deletionLedgerParts = gen, len(parts)
		f.deletionLedgerLock.Unlock()
	}
	f.persistStaleLedgerParts(ctx, key)
	f.touchLedgerIndex(ctx, key)
	return nil
}

// deleteLedgerParts removes a published generation's part keys; failures are
// tracked as stale so the next snapshot retries them.
func (f *Filer) deleteLedgerParts(ctx context.Context, key string, gen, parts int) {
	for i := 0; i < parts; i++ {
		if err := f.Store.KvDelete(ctx, deletionLedgerGenPartKey(key, gen, i)); err != nil {
			f.deletionLedgerLock.Lock()
			f.deletionLedgerStale = append(f.deletionLedgerStale, string(deletionLedgerGenPartKey(key, gen, i)))
			f.deletionLedgerLock.Unlock()
		}
	}
}

// markStaleLedgerParts remembers parts of an abandoned generation so they are
// deleted by a later snapshot instead of leaking store space.
func (f *Filer) markStaleLedgerParts(key string, gen, wrote int) {
	if wrote == 0 {
		return
	}
	f.deletionLedgerLock.Lock()
	for i := 0; i < wrote; i++ {
		f.deletionLedgerStale = append(f.deletionLedgerStale, string(deletionLedgerGenPartKey(key, gen, i)))
	}
	f.deletionLedgerLock.Unlock()
}

func (f *Filer) flushStaleLedgerParts(ctx context.Context) {
	f.deletionLedgerLock.Lock()
	stale := f.deletionLedgerStale
	f.deletionLedgerStale = nil
	f.deletionLedgerLock.Unlock()

	var keep []string
	for _, k := range stale {
		if err := f.Store.KvDelete(ctx, []byte(k)); err != nil {
			keep = append(keep, k)
		}
	}
	if len(keep) > 0 {
		f.deletionLedgerLock.Lock()
		f.deletionLedgerStale = append(keep, f.deletionLedgerStale...)
		f.deletionLedgerLock.Unlock()
	}
}

// persistStaleLedgerParts mirrors the stale-part list under a sidecar key so a
// crash between manifest publication and part cleanup can retry after restart
// instead of orphaning the part keys.
func (f *Filer) persistStaleLedgerParts(ctx context.Context, key string) {
	f.deletionLedgerLock.Lock()
	stale := append([]string(nil), f.deletionLedgerStale...)
	f.deletionLedgerLock.Unlock()
	staleKey := []byte(key + ".stale")
	if len(stale) == 0 {
		_ = f.Store.KvDelete(ctx, staleKey)
		return
	}
	payload, _ := json.Marshal(stale)
	_ = f.Store.KvPut(ctx, staleKey, payload)
}

// loadStaleLedgerParts seeds the stale-part list left by a previous run.
func (f *Filer) loadStaleLedgerParts(ctx context.Context, key string) {
	payload, err := f.Store.KvGet(ctx, []byte(key+".stale"))
	if err != nil {
		return
	}
	var stale []string
	if json.Unmarshal(payload, &stale) != nil {
		return
	}
	f.deletionLedgerLock.Lock()
	f.deletionLedgerStale = append(f.deletionLedgerStale, stale...)
	f.deletionLedgerLock.Unlock()
}

// touchLedgerIndex records this filer's scoped key in the shared index so the
// ledger remains discoverable if the filer restarts under a new address.
func (f *Filer) touchLedgerIndex(ctx context.Context, key string) {
	if key == KvKeyDeletionLedger {
		return
	}
	keys, _ := f.readLedgerIndex(ctx)
	for _, k := range keys {
		if k == key {
			return
		}
	}
	payload, _ := json.Marshal(append(keys, key))
	if err := f.Store.KvPut(ctx, []byte(KvKeyDeletionLedgerIndex), payload); err != nil {
		glog.V(1).Infof("failed to update deletion ledger index: %v", err)
	}
}

func (f *Filer) readLedgerIndex(ctx context.Context) ([]string, error) {
	payload, err := f.Store.KvGet(ctx, []byte(KvKeyDeletionLedgerIndex))
	if err != nil {
		return nil, err
	}
	var keys []string
	if err := json.Unmarshal(payload, &keys); err != nil {
		return nil, err
	}
	return keys, nil
}

func (f *Filer) pruneLedgerIndex(ctx context.Context, key string) {
	keys, err := f.readLedgerIndex(ctx)
	if err != nil {
		return
	}
	kept := keys[:0]
	for _, k := range keys {
		if k != key {
			kept = append(kept, k)
		}
	}
	payload, _ := json.Marshal(kept)
	_ = f.Store.KvPut(ctx, []byte(KvKeyDeletionLedgerIndex), payload)
}

// readDeletionLedger reads the manifest key: a JSON array is the whole set; a
// {"parts":N,"gen":G} manifest points at that generation's part keys. A part
// the manifest references but the store lacks is corruption, not absence, so
// it surfaces as a wrapped error rather than ErrKvNotFound.
func (f *Filer) readDeletionLedger(key string) (ids []string, gen, parts int, err error) {
	ctx := context.Background()

	payload, err := f.Store.KvGet(ctx, []byte(key))
	if err != nil {
		return nil, 0, 0, err
	}
	if !bytes.HasPrefix(bytes.TrimSpace(payload), []byte("{")) {
		if err := json.Unmarshal(payload, &ids); err != nil {
			return nil, 0, 0, err
		}
		return ids, 0, 0, nil
	}
	var manifest struct {
		Parts int `json:"parts"`
		Gen   int `json:"gen,omitempty"`
	}
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return nil, 0, 0, err
	}
	for i := 0; i < manifest.Parts; i++ {
		partPayload, err := f.Store.KvGet(ctx, deletionLedgerGenPartKey(key, manifest.Gen, i))
		if err != nil {
			return nil, 0, 0, fmt.Errorf("deletion ledger part %d of %s unreadable: %w", i, key, err)
		}
		var part []string
		if err := json.Unmarshal(partPayload, &part); err != nil {
			return nil, 0, 0, fmt.Errorf("deletion ledger part %d of %s corrupt: %w", i, key, err)
		}
		ids = append(ids, part...)
	}
	return ids, manifest.Gen, manifest.Parts, nil
}

// recoverForeignDeletionLedgers runs when the scoped key is absent: the ledger
// may sit under the pre-scoping base key, or under a scoped key belonging to an
// earlier incarnation of this filer whose advertised address changed. Every
// discovered set is published under this filer's key first and the source keys
// deleted only after that write succeeds. A live peer's ledger claimed here is
// rewritten by the peer's next snapshot, so the ids only get processed twice —
// deletions are not owner-specific.
func (f *Filer) recoverForeignDeletionLedgers(key string) (ids []string, err error) {
	ctx := context.Background()

	claimed := map[string][2]int{} // source key -> {gen, parts}
	lIds, lGen, lParts, lErr := f.readDeletionLedger(KvKeyDeletionLedger)
	switch {
	case lErr == nil:
		ids = append(ids, lIds...)
		claimed[KvKeyDeletionLedger] = [2]int{lGen, lParts}
		f.loadStaleLedgerParts(ctx, KvKeyDeletionLedger)
	case lErr != ErrKvNotFound:
		return nil, lErr
	}

	indexKeys, idxErr := f.readLedgerIndex(ctx)
	if idxErr == nil {
		for _, other := range indexKeys {
			if other == key {
				continue
			}
			oIds, oGen, oParts, oErr := f.readDeletionLedger(other)
			if oErr == ErrKvNotFound {
				f.pruneLedgerIndex(ctx, other)
				continue
			}
			if oErr != nil {
				glog.Warningf("skipping unreadable deletion ledger %s: %v", other, oErr)
				continue
			}
			ids = append(ids, oIds...)
			claimed[other] = [2]int{oGen, oParts}
			f.loadStaleLedgerParts(ctx, other)
		}
	}

	if len(claimed) == 0 {
		return nil, ErrKvNotFound
	}
	if err := f.writeDeletionLedger(ids); err != nil {
		return nil, err
	}
	for src, gp := range claimed {
		f.deleteLedgerParts(ctx, src, gp[0], gp[1])
		_ = f.Store.KvDelete(ctx, []byte(src))
		_ = f.Store.KvDelete(ctx, []byte(src+".stale"))
		f.pruneLedgerIndex(ctx, src)
	}
	return ids, nil
}

// reloadDeletionLedger re-enqueues any pending deletions found in the store after
// a restart, so a crash that killed the in-memory queues does not leak chunks.
// Recovered ids join the pending set immediately so snapshots rewrite the full
// ledger; only the re-queue waits out DeletionRecoveryGrace.
//
// It reports whether the ledger is usable. A read error or an unparseable
// payload leaves the persisted set unknown, so snapshots retry the read rather
// than overwrite the unread ledger with a partial set.
//
// Safe to call on a Filer with a nil Store (no-op). Idempotent for the volume
// side: a chunk that was actually deleted before the crash re-deletes as not-found.
func (f *Filer) reloadDeletionLedger() bool {
	if !deletionPersistEnabled() || f.Store == nil {
		return false
	}

	key := f.deletionLedgerKey()
	ids, gen, parts, err := f.readDeletionLedger(key)
	stateWritten := false
	if err == ErrKvNotFound && key != KvKeyDeletionLedger {
		ids, err = f.recoverForeignDeletionLedgers(key)
		stateWritten = err == nil
	}
	if err != nil {
		if err == ErrKvNotFound {
			f.deletionLedgerBlocked.Store(false)
			return true
		}
		f.deletionLedgerBlocked.Store(true)
		glog.Warningf("failed to read persisted deletion ledger; persistence disabled until it reads: %v", err)
		return false
	}
	f.deletionLedgerBlocked.Store(false)
	f.loadStaleLedgerParts(context.Background(), key)

	// Merge into the pending set now so an early snapshot rewrites the
	// recovered ids instead of overwriting the ledger with only new ones.
	f.deletionLedgerLock.Lock()
	if len(ids) > 0 {
		if f.pendingDeletions == nil {
			f.pendingDeletions = make(map[string]uint64, len(ids))
		}
		for _, id := range ids {
			if id != "" {
				if _, exists := f.pendingDeletions[id]; !exists {
					f.deletionSeq++
					f.pendingDeletions[id] = f.deletionSeq
				}
			}
		}
	}
	if !stateWritten {
		f.deletionLedgerGen = gen
		f.deletionLedgerParts = parts
	}
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
