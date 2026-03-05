package filer

import (
	"context"
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (f *Filer) remoteEntryNeedsRevalidation(entry *Entry) bool {
	if f.RemoteEntryTTL <= 0 || entry.Remote == nil {
		return false
	}
	if entry.Remote.LastLocalSyncTsNs == 0 {
		return false
	}
	syncTime := time.Unix(0, entry.Remote.LastLocalSyncTsNs)
	return time.Since(syncTime) > f.RemoteEntryTTL
}

func (f *Filer) revalidateRemoteEntry(ctx context.Context, p util.FullPath, existing *Entry) (*Entry, error) {
	if f.RemoteStorage == nil {
		return existing, nil
	}
	client, _, found := f.RemoteStorage.FindRemoteStorageClient(p)
	if !found {
		return existing, nil
	}

	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(p)
	if remoteLoc == nil {
		return existing, nil
	}

	remotePath := MapFullPathToRemoteStorageLocation(mountDir, remoteLoc, p)

	remoteEntry, statErr := client.StatFile(remotePath)
	if statErr != nil {
		if errors.Is(statErr, remote_storage.ErrRemoteObjectNotFound) {
			// Remote object was deleted
			if delErr := f.Store.DeleteOneEntry(ctx, existing); delErr != nil {
				glog.V(1).InfofCtx(ctx, "revalidateRemoteEntry delete %s: %v", p, delErr)
			}
			return nil, filer_pb.ErrNotFound
		}
		// On error, serve stale (availability over consistency)
		glog.V(1).InfofCtx(ctx, "revalidateRemoteEntry stat %s: %v, serving stale", p, statErr)
		return existing, nil
	}

	if existing.Remote.RemoteETag == remoteEntry.RemoteETag &&
		existing.Remote.RemoteMtime >= remoteEntry.RemoteMtime {
		// Unchanged, just update sync timestamp
		existing.Remote.LastLocalSyncTsNs = time.Now().UnixNano()
		if err := f.Store.UpdateEntry(ctx, existing); err != nil {
			glog.V(1).InfofCtx(ctx, "revalidateRemoteEntry update sync ts %s: %v", p, err)
		}
		return existing, nil
	}

	// Changed: update metadata, clear chunks to force re-fetch
	existing.Remote = remoteEntry
	existing.Remote.LastLocalSyncTsNs = time.Now().UnixNano()
	existing.Attr.FileSize = uint64(remoteEntry.RemoteSize)
	existing.Attr.Mtime = time.Unix(remoteEntry.RemoteMtime, 0)
	existing.Chunks = nil

	if err := f.Store.UpdateEntry(ctx, existing); err != nil {
		glog.V(1).InfofCtx(ctx, "revalidateRemoteEntry update %s: %v", p, err)
	}
	return existing, nil
}
