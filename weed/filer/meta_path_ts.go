package filer

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	// metaPathTsPrefix keyed the per-path commit records written before
	// entries carried their own stamp. Nothing writes it now; an unstamped
	// entry still consults it so its recorded order survives the upgrade.
	metaPathTsPrefix = "MpTs"
	// metaPathDelPrefix keys delete-only timestamps. A deleted path has no
	// entry left to carry its commit timestamp, so the tombstone preserves
	// the ordering a replayed peer event checks — including against an
	// ancestor, since a directory delete sweeps children without per-child
	// events (a dropped bucket's whole-store delete, or a walk aborted
	// partway).
	metaPathDelPrefix = "MpDl"
)

// metaPathKey hashes the path into a fixed-size key. SQL-backed KV stores
// split the key at byte 8 and store the remainder base64-encoded in a
// VARCHAR(766) name column: a raw path over ~578 bytes overflows it, the
// KvPut insert fails, and the fallback update touches no row yet returns no
// error — the delete record would never land and stale replay could restore
// the path.
func metaPathKey(prefix string, path util.FullPath) []byte {
	sum := sha256.Sum256([]byte(path))
	key := make([]byte, len(prefix)+16)
	copy(key, prefix)
	copy(key[len(prefix):], sum[:16])
	return key
}

// kvReadTsNs returns the timestamp recorded for key, or found=false when the
// key is absent or malformed. Any other store read failure is an error: a
// caller deciding whether a replayed change is stale must not guess.
func (f *Filer) kvReadTsNs(ctx context.Context, key []byte) (tsNs int64, found bool, err error) {
	value, err := f.Store.KvGet(ctx, key)
	if err != nil {
		if errors.Is(err, ErrKvNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if len(value) != 8 {
		return 0, false, nil
	}
	return int64(util.BytesToUint64(value)), true, nil
}

// notePathChange records a delete for path: the delete tombstone is the only
// ordering left once the entry is gone. Live entries carry their own commit
// timestamp (Entry.CommitTsNs) and need no record.
//
// The write uses a fresh context: callers notify after CommitTransaction,
// and SQL stores keep the already-committed *sql.Tx in the request context —
// a record written through it would fail and the path would lose ordering.
//
// A failed write is kept in metaTsPending and retried in the background: the
// mutation the record orders has already committed, so dropping it would let
// a stale replayed change resurrect the path while the store is unreachable.
// Ordering reads consult the pending record until the retry lands it.
func (f *Filer) notePathChange(path util.FullPath, tsNs int64, deleted bool) {
	if f.Store == nil || !deleted || f.skipDeleteTombstones {
		return
	}
	ctx := context.Background()
	key := string(path)
	rec := f.pendingRec(key)
	defer rec.mu.Unlock()

	value := make([]byte, 8)
	util.Uint64toBytes(value, uint64(tsNs))
	if err := f.Store.KvPut(ctx, metaPathKey(metaPathDelPrefix, path), value); err != nil {
		glog.WarningfCtx(ctx, "record meta delete time %s: %v", path, err)
		if tsNs > rec.deleteTsNs.Load() {
			rec.deleteTsNs.Store(tsNs)
		}
	} else if rec.deleteTsNs.Load() <= tsNs {
		// This write covers every delete record owed so far.
		rec.deleteTsNs.Store(0)
	}
	if rec.deleteTsNs.Load() == 0 {
		f.metaTsPending.CompareAndDelete(key, rec)
		return
	}
	if _, running := f.metaTsRepairing.LoadOrStore(key, struct{}{}); !running {
		go f.repairMetaPathTs(path)
	}
}

// metaTsPendingRec is a delete record still on its way to the store: the
// newest delete the MpDl record lacks. Store writes and the bookkeeping
// they discharge share mu, so a repair pass can never write an older record
// over a newer one that already landed; readers take the atomic field
// directly.
type metaTsPendingRec struct {
	mu         sync.Mutex
	deleteTsNs atomic.Int64
}

// pendingRec returns the live pending record for key, locked for mutation,
// recreating it when a repair already dropped the previous one.
func (f *Filer) pendingRec(key string) *metaTsPendingRec {
	for {
		cur, _ := f.metaTsPending.LoadOrStore(key, &metaTsPendingRec{})
		rec := cur.(*metaTsPendingRec)
		rec.mu.Lock()
		if live, ok := f.metaTsPending.Load(key); ok && live == cur {
			return rec
		}
		rec.mu.Unlock()
	}
}

// repairMetaPathTs retries a pending delete record until it lands. The retry
// is unbounded — the mutation is already committed and only the record
// preserves its ordering.
func (f *Filer) repairMetaPathTs(path util.FullPath) {
	key := string(path)
	ctx := f.flushCtx
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		f.metaTsRepairing.Delete(key)
		// A failure landing between the last pass and the flag release
		// still needs a repair pass — unless the retry context is done.
		if ctx.Err() != nil {
			return
		}
		if _, ok := f.metaTsPending.Load(key); ok {
			if _, running := f.metaTsRepairing.LoadOrStore(key, struct{}{}); !running {
				go f.repairMetaPathTs(path)
			}
		}
	}()
	for {
		if _, ok := f.metaTsPending.Load(key); !ok {
			return
		}
		rec := f.pendingRec(key)
		var delErr error
		if tsNs := rec.deleteTsNs.Load(); tsNs > 0 {
			value := make([]byte, 8)
			util.Uint64toBytes(value, uint64(tsNs))
			if delErr = f.Store.KvPut(ctx, metaPathKey(metaPathDelPrefix, path), value); delErr == nil {
				rec.deleteTsNs.Store(0)
			}
		}
		done := rec.deleteTsNs.Load() == 0
		if done {
			f.metaTsPending.CompareAndDelete(key, rec)
		}
		rec.mu.Unlock()
		if !done {
			glog.V(0).InfofCtx(ctx, "retry record meta delete time %s: %v", path, delErr)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

// pendingDeleteTsNs returns the newest delete timestamp for path still
// waiting to land in the store.
func (f *Filer) pendingDeleteTsNs(path util.FullPath) int64 {
	cur, ok := f.metaTsPending.Load(string(path))
	if !ok {
		return 0
	}
	return cur.(*metaTsPendingRec).deleteTsNs.Load()
}

// CommitTsNs returns the commit timestamp stamped on the entry at path, or
// fallbackTsNs when the entry predates stamping (or is gone). Peers stamp
// traversal entries with it so a bootstrap subscriber orders each entry on
// the sender's commit clock rather than the entry's client-settable mtime.
// A store read failure is an error, not a fallback: the mtime it would mint
// could outdate real commits.
func (f *Filer) CommitTsNs(ctx context.Context, path util.FullPath, fallbackTsNs int64) (int64, error) {
	if f.Store == nil {
		return fallbackTsNs, nil
	}
	entry, err := f.Store.FindEntry(ctx, path)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return fallbackTsNs, nil
		}
		return 0, err
	}
	if entry == nil {
		return fallbackTsNs, nil
	}
	return f.EntryCommitTsNs(ctx, entry, fallbackTsNs)
}

// EntryCommitTsNs resolves the commit timestamp of an entry already loaded:
// the stamp written on it, else the per-path record versions before entry
// stamping left behind, else fallbackTsNs.
func (f *Filer) EntryCommitTsNs(ctx context.Context, entry *Entry, fallbackTsNs int64) (int64, error) {
	if entry.CommitTsNs != 0 {
		return entry.CommitTsNs, nil
	}
	if f.Store == nil {
		return fallbackTsNs, nil
	}
	tsNs, found, err := f.kvReadTsNs(ctx, metaPathKey(metaPathTsPrefix, entry.FullPath))
	if err != nil {
		return 0, err
	}
	if found {
		return tsNs, nil
	}
	return fallbackTsNs, nil
}

// noteCommittedEvent records the event's commit timestamp for the path a
// delete or rename removes: the source keeps its ordering as a tombstone
// after the entry itself is gone. Targets carry their own commit timestamp
// on the stored entry.
func (f *Filer) noteCommittedEvent(event *filer_pb.SubscribeMetadataResponse) {
	message := event.EventNotification
	if message == nil {
		return
	}
	var source, target util.FullPath
	if message.OldEntry != nil {
		source = util.FullPath(filer_pb.MetadataEventSourceFullPath(event))
	}
	if message.NewEntry != nil {
		target = util.FullPath(filer_pb.MetadataEventTargetFullPath(event))
	}
	// A delete or a rename removes the source path; an update reports the
	// same path on both sides and counts once.
	if source != "" && source != target {
		f.notePathChange(source, event.TsNs, true)
	}
}

// deletedAfterTsNs returns the newest recorded delete timestamp among path
// and its ancestors.
func (f *Filer) deletedAfterTsNs(ctx context.Context, path util.FullPath) (int64, error) {
	var tsNs int64
	for p := string(path); p != "" && p != "/"; {
		ts, found, err := f.kvReadTsNs(ctx, metaPathKey(metaPathDelPrefix, util.FullPath(p)))
		if err != nil {
			return 0, err
		}
		if found && ts > tsNs {
			tsNs = ts
		}
		if pendDel := f.pendingDeleteTsNs(util.FullPath(p)); pendDel > tsNs {
			tsNs = pendDel
		}
		i := strings.LastIndexByte(p, '/')
		if i <= 0 {
			break
		}
		p = p[:i]
	}
	return tsNs, nil
}

// localCommitTsNs returns the newest timestamp this filer is known to have
// committed for path: the commit timestamp stamped on the entry — or the
// record a pre-stamping version left — neither client-controllable the way
// mtime is, else the entry's mtime for entries written before either
// existed, plus any recorded delete of the path or an ancestor.
func (f *Filer) localCommitTsNs(ctx context.Context, path util.FullPath) (int64, error) {
	if f.Store == nil {
		return 0, nil
	}
	var tsNs int64
	entry, findErr := f.Store.FindEntry(ctx, path)
	if findErr != nil {
		if !errors.Is(findErr, filer_pb.ErrNotFound) {
			return 0, findErr
		}
	} else if entry != nil {
		entryTsNs, tsErr := f.EntryCommitTsNs(ctx, entry, entry.Mtime.UnixNano())
		if tsErr != nil {
			return 0, tsErr
		}
		tsNs = entryTsNs
	}
	if pendDel := f.pendingDeleteTsNs(path); pendDel > tsNs {
		tsNs = pendDel
	}
	delTs, err := f.deletedAfterTsNs(ctx, path)
	if err != nil {
		return 0, err
	}
	if delTs > tsNs {
		tsNs = delTs
	}
	return tsNs, nil
}
