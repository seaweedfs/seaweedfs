package filer

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Per-path ordering records. Nothing writes them anymore — live entries
// carry their own commit timestamp (Entry.CommitTsNs) — but records older
// builds left behind are still consulted so their ordering survives the
// upgrade.
const (
	// metaPathTsPrefix keyed the per-path commit records written before
	// entries carried their own stamp; an unstamped entry still consults it.
	metaPathTsPrefix = "MpTs"
	// metaPathDelPrefix keyed delete tombstones, which older builds kept
	// because a deleted path has no entry to carry a stamp. Still read: a
	// recorded delete keeps fencing stale replays, including against an
	// ancestor — a directory delete sweeps children without per-child
	// events.
	metaPathDelPrefix = "MpDl"
)

// metaPathKey hashes the path into a fixed-size key. SQL-backed KV stores
// split the key at byte 8 and store the remainder base64-encoded in a
// VARCHAR(766) name column: a raw path over ~578 bytes overflows it, the
// KvPut insert fails, and the fallback update touches no row yet returns no
// error — the record would never land and stale replay could restore the
// path.
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
// committed for path: the commit timestamp stamped on the entry — not
// client-controllable the way mtime is — else the per-path record versions
// before entry stamping left behind, else the entry's mtime, plus any
// recorded delete of the path or an ancestor.
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
		entryTsNs, err := f.EntryCommitTsNs(ctx, entry, entry.Mtime.UnixNano())
		if err != nil {
			return 0, err
		}
		tsNs = entryTsNs
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
