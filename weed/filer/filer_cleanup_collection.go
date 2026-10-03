package filer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ExtendedCollectionKey is the Entry.Extended key that records which
// collection an entry's chunks were written to. It is set by the filer
// HTTP write handler when the request carries ?collection=xxx, and lets
// collection-indexed stores maintain a reverse index for O(collection)
// metadata cleanup after the collection's volumes are deleted.
const ExtendedCollectionKey = "x-seaweedfs-collection"

// CollectionIndexedStore is implemented by filer stores that maintain a
// collection → path reverse index and can bulk-delete all entries of a
// collection without walking the directory tree.
//
// eachEntryFn, when non-nil, is invoked with each entry just before it is
// removed, so the caller can propagate the deletion (NotifyUpdateEvent to peer
// filers and metadata subscribers). It is optional.
type CollectionIndexedStore interface {
	DeleteCollectionEntries(ctx context.Context, collection string, eachEntryFn func(*Entry)) (deletedFiles int, parentDirs []util.FullPath, err error)
}

// CleanupCollection deletes all filer entries recorded under the given
// collection via the store's collection index, then removes directories
// that became empty. Complexity is O(entries in the collection), independent
// of the total filer size. Idempotent: re-running returns zero counts.
// recentDirSeconds guards empty-directory removal: directories created within
// that many seconds are kept (writers may be about to use them). Negative
// values use the default of 24 hours; 0 disables the guard.
func (f *Filer) CleanupCollection(ctx context.Context, collection string, recentDirSeconds int64) (deletedFiles int, deletedDirs int, err error) {
	if collection == "" {
		return 0, 0, fmt.Errorf("collection is required")
	}

	indexedStore, ok := f.Store.(CollectionIndexedStore)
	if !ok {
		return 0, 0, fmt.Errorf("filer store does not support collection index cleanup")
	}

	var parentDirs []util.FullPath
	deletedFiles, parentDirs, err = indexedStore.DeleteCollectionEntries(ctx, collection, func(entry *Entry) {
		// The normal delete path emits NotifyUpdateEvent so peer filers and
		// metadata subscribers stay in sync; a bulk store delete bypasses it,
		// so replay it here. Chunks were already removed when the collection's
		// volumes were deleted on the master, so deleteChunks is false.
		f.NotifyUpdateEvent(ctx, entry, nil, false, false, nil)
	})
	if err != nil {
		return deletedFiles, 0, err
	}

	// Remove directories that became empty, deepest first, looping until
	// stable so emptied grandparents are collected too.
	dirsToCheck := make(map[util.FullPath]bool)
	for _, dir := range parentDirs {
		if dir != "/" {
			dirsToCheck[dir] = true
		}
	}

	// Directories created recently are skipped: writers may be about to
	// place new files in them.
	if recentDirSeconds < 0 {
		recentDirSeconds = 24 * 3600
	}
	recentCutoff := time.Now().Add(-time.Duration(recentDirSeconds) * time.Second)

	for len(dirsToCheck) > 0 {
		changed := false
		sorted := make([]util.FullPath, 0, len(dirsToCheck))
		for d := range dirsToCheck {
			sorted = append(sorted, d)
		}
		sort.Slice(sorted, func(i, j int) bool {
			return strings.Count(string(sorted[i]), "/") > strings.Count(string(sorted[j]), "/")
		})

		for _, dirPath := range sorted {
			delete(dirsToCheck, dirPath)

			entries, _, listErr := f.ListDirectoryEntries(ctx, dirPath, "", false, 1, "", "", "")
			if listErr != nil || len(entries) > 0 {
				continue
			}
			dirEntry, findErr := f.FindEntry(ctx, dirPath)
			if findErr != nil {
				continue
			}
			if !dirEntry.IsDirectory() || dirEntry.Crtime.After(recentCutoff) {
				continue
			}
			if delErr := f.Store.DeleteOneEntry(ctx, dirEntry); delErr != nil {
				glog.V(1).InfofCtx(ctx, "CleanupCollection %s: delete empty dir %s: %v", collection, dirPath, delErr)
				continue
			}
			f.NotifyUpdateEvent(ctx, dirEntry, nil, false, false, nil)
			deletedDirs++
			changed = true
			parent, _ := dirPath.DirAndName()
			if parent != "/" {
				dirsToCheck[util.FullPath(parent)] = true
			}
		}

		if !changed {
			break
		}
	}

	glog.V(0).InfofCtx(ctx, "CleanupCollection %s: deleted %d files, %d dirs", collection, deletedFiles, deletedDirs)
	return deletedFiles, deletedDirs, nil
}
