package filer

import (
	"context"
	"errors"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

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
// the stamp written on it, else fallbackTsNs for entries written before
// stamping existed.
func (f *Filer) EntryCommitTsNs(ctx context.Context, entry *Entry, fallbackTsNs int64) (int64, error) {
	if entry.CommitTsNs != 0 {
		return entry.CommitTsNs, nil
	}
	return fallbackTsNs, nil
}

// localCommitTsNs returns the timestamp this filer last committed for path:
// the commit timestamp stamped on the entry — not client-controllable the
// way mtime is — else the entry's mtime for versions written before
// stamping existed. A deleted path has no entry, so nothing orders a stale
// replayed create against it.
func (f *Filer) localCommitTsNs(ctx context.Context, path util.FullPath) (int64, error) {
	if f.Store == nil {
		return 0, nil
	}
	entry, err := f.Store.FindEntry(ctx, path)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if entry == nil {
		return 0, nil
	}
	return f.EntryCommitTsNs(ctx, entry, entry.Mtime.UnixNano())
}
