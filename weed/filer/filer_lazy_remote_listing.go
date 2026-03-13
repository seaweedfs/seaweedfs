package filer

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const xattrRemoteListingSyncedAt = "remote.listing.synced_at"

type lazyListContextKey struct{}

// maybeLazyListFromRemote populates the local filer store with entries from the
// remote storage backend for directory p if the following conditions hold:
//   - p is under a remote mount with listing_cache_ttl_seconds > 0
//   - the cached listing has expired (based on the per-mount TTL)
//
// When listing_cache_ttl_seconds is 0 (the default), lazy listing is disabled
// for that mount.
//
// On success it updates the directory's xattrRemoteListingSyncedAt extended
// attribute so subsequent calls within the TTL window are no-ops.
//
// Errors are logged and swallowed (availability over consistency).
func (f *Filer) maybeLazyListFromRemote(ctx context.Context, p util.FullPath) {
	// Prevent recursion: CreateEntry → FindEntry → doListDirectoryEntries → here
	if ctx.Value(lazyListContextKey{}) != nil {
		return
	}
	// Also respect the lazy-fetch guard to prevent mutual recursion
	if ctx.Value(lazyFetchContextKey{}) != nil {
		return
	}

	if f.RemoteStorage == nil {
		return
	}

	// The ptrie stores mount rules with trailing "/". When p is exactly the
	// mount directory (e.g. "/buckets/mybucket"), we must also try matching
	// with a trailing "/" so the trie recognizes the mount root.
	lookupPath := p
	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(lookupPath)
	if remoteLoc == nil {
		lookupPath = util.FullPath(string(p) + "/")
		mountDir, remoteLoc = f.RemoteStorage.FindMountDirectory(lookupPath)
		if remoteLoc == nil {
			return
		}
	}

	// Lazy listing is opt-in: disabled when TTL is 0
	if remoteLoc.ListingCacheTtlSeconds <= 0 {
		return
	}
	cacheTTL := time.Duration(remoteLoc.ListingCacheTtlSeconds) * time.Second

	// Check staleness: read the directory entry's extended attributes
	dirEntry, _ := f.FindEntry(ctx, p)
	if dirEntry != nil {
		if syncedAtStr, ok := dirEntry.Extended[xattrRemoteListingSyncedAt]; ok {
			if syncedAt, err := strconv.ParseInt(string(syncedAtStr), 10, 64); err == nil {
				if time.Since(time.Unix(syncedAt, 0)) < cacheTTL {
					return
				}
			}
		}
	}

	client, _, found := f.RemoteStorage.FindRemoteStorageClient(lookupPath)
	if !found {
		return
	}

	key := "list:" + string(p)
	f.lazyListGroup.Do(key, func() (interface{}, error) {
		startTime := time.Now()
		objectLoc := MapFullPathToRemoteStorageLocation(mountDir, remoteLoc, p)

		// Decouple from the caller's cancellation/deadline while preserving
		// trace/span values for distributed tracing.
		persistCtx := context.WithValue(context.WithoutCancel(ctx), lazyListContextKey{}, true)
		persistCtx = context.WithValue(persistCtx, lazyFetchContextKey{}, true)

		listErr := client.ListDirectory(persistCtx, objectLoc, func(dir string, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
			childPath := p.Child(name)

			existingEntry, _ := f.FindEntry(persistCtx, childPath)

			// Skip entries that exist locally without a RemoteEntry (local-only uploads)
			if existingEntry != nil && existingEntry.Remote == nil {
				return nil
			}

			if existingEntry != nil {
				// Merge: update remote metadata while preserving local state
				// (Chunks, Extended, Uid/Gid/Mode, etc.)
				existingEntry.Remote = remoteEntry
				if !isDirectory && remoteEntry != nil {
					if remoteEntry.RemoteMtime > 0 {
						existingEntry.Attr.Mtime = time.Unix(remoteEntry.RemoteMtime, 0)
					}
					existingEntry.Attr.FileSize = uint64(remoteEntry.RemoteSize)
				}
				if saveErr := f.Store.UpdateEntry(persistCtx, existingEntry); saveErr != nil {
					glog.Warningf("maybeLazyListFromRemote: update %s: %v", childPath, saveErr)
				}
			} else {
				// New entry not yet in local store
				var entry *Entry
				if isDirectory {
					now := time.Now()
					entry = &Entry{
						FullPath: childPath,
						Attr: Attr{
							Mtime:  now,
							Crtime: now,
							Mode:   os.ModeDir | 0755,
							Uid:    OS_UID,
							Gid:    OS_GID,
						},
					}
				} else {
					mtime := time.Now()
					if remoteEntry != nil && remoteEntry.RemoteMtime > 0 {
						mtime = time.Unix(remoteEntry.RemoteMtime, 0)
					}
					entry = &Entry{
						FullPath: childPath,
						Attr: Attr{
							Mtime:  mtime,
							Crtime: mtime,
							Mode:   0644,
						},
						Remote: remoteEntry,
					}
					if remoteEntry != nil {
						entry.Attr.FileSize = uint64(remoteEntry.RemoteSize)
					}
				}
				if saveErr := f.CreateEntry(persistCtx, entry, false, false, nil, true, f.MaxFilenameLength); saveErr != nil {
					glog.Warningf("maybeLazyListFromRemote: persist %s: %v", childPath, saveErr)
				}
			}
			return nil
		})
		if listErr != nil {
			glog.Warningf("maybeLazyListFromRemote: list %s: %v", p, listErr)
			return nil, nil // swallow error
		}

		// Update the synced_at timestamp on the directory entry
		f.updateDirectoryListingSyncedAt(persistCtx, p, startTime)

		return nil, nil
	})
}

func (f *Filer) updateDirectoryListingSyncedAt(ctx context.Context, p util.FullPath, syncTime time.Time) {
	dirEntry, findErr := f.Store.FindEntry(ctx, p)
	if findErr != nil {
		// Directory doesn't exist yet, create it
		now := time.Now()
		dirEntry = &Entry{
			FullPath: p,
			Attr: Attr{
				Mtime:  now,
				Crtime: now,
				Mode:   os.ModeDir | 0755,
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}
	}
	if dirEntry.Extended == nil {
		dirEntry.Extended = make(map[string][]byte)
	}
	dirEntry.Extended[xattrRemoteListingSyncedAt] = []byte(fmt.Sprintf("%d", syncTime.Unix()))

	if saveErr := f.CreateEntry(ctx, dirEntry, false, false, nil, true, f.MaxFilenameLength); saveErr != nil {
		glog.Warningf("maybeLazyListFromRemote: update synced_at for %s: %v", p, saveErr)
	}
}
