package filer

import (
	"context"
	"errors"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// CommitTsNs resolves the commit timestamp at path for traversal: the
// entry's stamp, else fallbackTsNs for pre-stamp or missing entries.
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

// EntryCommitTsNs returns the stamp on an already-loaded entry, else
// fallbackTsNs.
func (f *Filer) EntryCommitTsNs(ctx context.Context, entry *Entry, fallbackTsNs int64) (int64, error) {
	if entry.CommitTsNs != 0 {
		return entry.CommitTsNs, nil
	}
	return fallbackTsNs, nil
}

// localCommitTsNs returns this filer's newest known commit time for path:
// the entry's stamp — not client-controllable like mtime — else mtime.
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
