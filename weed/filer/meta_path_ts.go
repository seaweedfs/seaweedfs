package filer

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	// metaPathTsPrefix keys the per-path commit timestamps older builds
	// wrote into the store KV for every mutation and kept after the entry
	// was deleted. They are no longer written — ordering lives on the
	// entry's CommitTsNs — but records left behind by an upgrade still
	// fence replayed events older than the change they recorded.
	metaPathTsPrefix = "MpTs"
	// metaPathDelPrefix keys the delete timestamps those builds kept. The
	// replay check consults the path's ancestors too, so a directory's
	// retained delete record covers children deleted without per-child
	// events.
	metaPathDelPrefix = "MpDl"
)

// metaPathKey hashes the path into the fixed-size key the writing builds
// used: SQL stores split the key at byte 8 and base64 the rest into a
// bounded name column, which a raw path can overflow.
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

// CommitTsNs resolves the commit timestamp at path for traversal: the
// entry's stamp, else fallbackTsNs for pre-stamp or missing entries.
func (f *Filer) CommitTsNs(ctx context.Context, path util.FullPath, fallbackTsNs int64) (int64, error) {
	if f.Store == nil {
		return fallbackTsNs, nil
	}
	entry, err := f.Store.FindEntry(ctx, path)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return f.retainedCommitTsNs(ctx, path, fallbackTsNs)
		}
		return 0, err
	}
	if entry == nil {
		return f.retainedCommitTsNs(ctx, path, fallbackTsNs)
	}
	return f.EntryCommitTsNs(ctx, entry, fallbackTsNs)
}

// EntryCommitTsNs returns the newest commit time known for an already-loaded
// entry: its stamp, a retained per-path record left by an older build —
// which can outdate a replayed peer stamp — else fallbackTsNs.
func (f *Filer) EntryCommitTsNs(ctx context.Context, entry *Entry, fallbackTsNs int64) (int64, error) {
	tsNs := entry.CommitTsNs
	if f.Store == nil {
		if tsNs == 0 {
			return fallbackTsNs, nil
		}
		return tsNs, nil
	}
	recTs, found, err := f.kvReadTsNs(ctx, metaPathKey(metaPathTsPrefix, entry.FullPath))
	if err != nil {
		return 0, err
	}
	if found && recTs > tsNs {
		tsNs = recTs
	}
	if tsNs == 0 {
		return fallbackTsNs, nil
	}
	return tsNs, nil
}

// retainedCommitTsNs reads the per-path record an older build may have left
// for a path that has no entry now — a delete under the recording builds
// kept its change record past the entry's removal.
func (f *Filer) retainedCommitTsNs(ctx context.Context, path util.FullPath, fallbackTsNs int64) (int64, error) {
	recTs, found, err := f.kvReadTsNs(ctx, metaPathKey(metaPathTsPrefix, path))
	if err != nil {
		return 0, err
	}
	if found {
		return recTs, nil
	}
	return fallbackTsNs, nil
}

// deletedAfterTsNs returns the newest retained delete record among path and
// its ancestors.
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

// localCommitTsNs returns this filer's newest known commit time for path:
// the entry's stamp — not client-controllable like mtime — else mtime, plus
// any retained change or delete record an older build left for the path or
// its ancestors.
func (f *Filer) localCommitTsNs(ctx context.Context, path util.FullPath) (int64, error) {
	if f.Store == nil {
		return 0, nil
	}
	var tsNs int64
	entry, err := f.Store.FindEntry(ctx, path)
	if err != nil {
		if !errors.Is(err, filer_pb.ErrNotFound) {
			return 0, err
		}
	}
	if entry == nil {
		if tsNs, err = f.retainedCommitTsNs(ctx, path, 0); err != nil {
			return 0, err
		}
	} else if tsNs, err = f.EntryCommitTsNs(ctx, entry, entry.Mtime.UnixNano()); err != nil {
		return 0, err
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
