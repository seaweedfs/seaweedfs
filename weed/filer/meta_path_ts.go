package filer

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	// metaPathTsPrefix keys per-path commit timestamps in the filer store KV:
	// the newest event timestamp this filer committed or applied for the
	// path. Written for every emitted mutation event and kept after the entry
	// is deleted, so a replayed peer event can still be ordered against a
	// change that left no entry behind.
	metaPathTsPrefix = "MpTs"
	// metaPathDelPrefix keys delete-only timestamps. A replayed event checks
	// the path's ancestors too, so a directory delete also covers children
	// swept without per-child events (a dropped bucket's whole-store delete,
	// or a walk aborted partway).
	metaPathDelPrefix = "MpDl"
)

// metaPathKey hashes the path into a fixed-size key. SQL-backed KV stores
// split the key at byte 8 and store the remainder base64-encoded in a
// VARCHAR(766) name column: a raw path over ~578 bytes overflows it, the
// KvPut insert fails, and the fallback update touches no row yet returns no
// error — the commit record would never land and stale replay could restore
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
	if len(value) < 8 {
		return 0, false, nil
	}
	return int64(util.BytesToUint64(value)), true, nil
}

// notePathChange persists tsNs as path's newest committed change. Local event
// timestamps are stamped monotonically (metaLogInflight.stamp), so a plain
// put cannot regress an earlier local record; a higher peer record it may
// overwrite was already newer under last-writer-wins ordering anyway. A
// delete additionally writes a delete record the ancestor lookup consults.
//
// The write uses a fresh context: callers notify after CommitTransaction,
// and SQL stores keep the already-committed *sql.Tx in the request context —
// a record written through it would fail and the path would lose ordering.
func (f *Filer) notePathChange(path util.FullPath, tsNs int64, deleted bool) {
	if f.Store == nil {
		return
	}
	ctx := context.Background()
	value := make([]byte, 8)
	util.Uint64toBytes(value, uint64(tsNs))
	if err := f.Store.KvPut(ctx, metaPathKey(metaPathTsPrefix, path), value); err != nil {
		glog.WarningfCtx(ctx, "record meta change time %s: %v", path, err)
	}
	if deleted {
		if err := f.Store.KvPut(ctx, metaPathKey(metaPathDelPrefix, path), value); err != nil {
			glog.WarningfCtx(ctx, "record meta delete time %s: %v", path, err)
		}
	}
}

// noteCommittedEvent records the event's commit timestamp for every path it
// touches: the source of a delete or rename, and the target of a create,
// update, or rename. It runs after the local mutation commits while the
// caller still holds the path's entry lock (for filer-server mutations), so
// a peer replay ordered by the same lock sees either the whole local change
// or none of it.
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
	if target != "" {
		f.notePathChange(target, event.TsNs, false)
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
		i := strings.LastIndexByte(p, '/')
		if i <= 0 {
			break
		}
		p = p[:i]
	}
	return tsNs, nil
}

// localCommitTsNs returns the newest timestamp this filer is known to have
// committed for path: the recorded event time when one exists — it is not
// client-controllable the way an entry's mtime is — else the entry's mtime
// for entries written before recording existed, plus any recorded delete of
// the path or an ancestor.
func (f *Filer) localCommitTsNs(ctx context.Context, path util.FullPath) (int64, error) {
	if f.Store == nil {
		return 0, nil
	}
	var tsNs int64
	recTs, found, err := f.kvReadTsNs(ctx, metaPathKey(metaPathTsPrefix, path))
	if err != nil {
		return 0, err
	}
	if found {
		tsNs = recTs
	} else if entry, findErr := f.Store.FindEntry(ctx, path); findErr != nil {
		if !errors.Is(findErr, filer_pb.ErrNotFound) {
			return 0, findErr
		}
	} else if entry != nil {
		tsNs = entry.Mtime.UnixNano()
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
