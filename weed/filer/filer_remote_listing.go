package filer

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// maybeMergeRemoteListings checks if dir is under a remote mount and, if so,
// queries the remote for entries not yet in the local store. New remote entries
// are persisted asynchronously so future listings hit cache.
func (f *Filer) maybeMergeRemoteListings(ctx context.Context, dir util.FullPath,
	localNames map[string]struct{}, startFileName string, limit int64, prefix string,
	eachEntryFunc ListEachEntryFunc,
) (additionalCount int64, mergeErr error) {
	if f.RemoteStorage == nil {
		return 0, nil
	}

	// FindMountDirectory requires a child path (the stored key has trailing /).
	// Append "/" so the directory itself matches the mount prefix.
	lookupPath := util.FullPath(string(dir) + "/")
	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(lookupPath)
	if remoteLoc == nil {
		return 0, nil
	}

	client, _, found := f.RemoteStorage.FindRemoteStorageClient(lookupPath)
	if !found {
		return 0, nil
	}

	// Build remote path for this directory
	relPath := strings.TrimPrefix(string(dir), string(mountDir))
	base := strings.TrimSuffix(remoteLoc.Path, "/")
	remotePath := "/" + strings.TrimLeft(base+relPath, "/")

	remoteDir := &remote_pb.RemoteStorageLocation{
		Name:   remoteLoc.Name,
		Bucket: remoteLoc.Bucket,
		Path:   remotePath,
	}

	remoteEntries, err := client.ListDirectory(remoteDir)
	if err != nil || remoteEntries == nil {
		if err != nil {
			glog.V(1).InfofCtx(ctx, "maybeMergeRemoteListings %s: %v", dir, err)
		}
		return 0, nil
	}

	sem := make(chan struct{}, 8)
	for _, re := range remoteEntries {
		if re == nil || re.Name == "" {
			continue
		}
		if strings.Contains(re.Name, "/") || re.Name == ".." || re.Name == "." {
			continue
		}
		if _, exists := localNames[re.Name]; exists {
			continue
		}
		if prefix != "" && !strings.HasPrefix(re.Name, prefix) {
			continue
		}
		if re.Name <= startFileName {
			continue
		}

		entry := buildRemoteOnlyEntry(dir, re)

		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			f.persistRemoteListingEntry(entry)
		}()

		ok, callbackErr := eachEntryFunc(entry)
		if callbackErr != nil {
			return additionalCount, callbackErr
		}
		additionalCount++
		localNames[re.Name] = struct{}{}
		if !ok || additionalCount >= limit {
			break
		}
	}
	return additionalCount, nil
}

func buildRemoteOnlyEntry(dir util.FullPath, re *remote_storage.RemoteListing) *Entry {
	mtime := time.Unix(re.Mtime, 0)
	entry := &Entry{
		FullPath: dir.Child(re.Name),
		Attr: Attr{
			Mtime:  mtime,
			Crtime: mtime,
		},
	}
	if re.IsDirectory {
		entry.Attr.Mode = os.ModeDir | 0755
	} else {
		entry.Attr.Mode = 0644
		entry.Attr.FileSize = uint64(re.Size)
		entry.Remote = &filer_pb.RemoteEntry{
			RemoteMtime:       re.Mtime,
			RemoteSize:        re.Size,
			RemoteETag:        re.ETag,
			StorageName:       re.StorageName,
			LastLocalSyncTsNs: time.Now().UnixNano(),
		}
	}
	return entry
}

func (f *Filer) persistRemoteListingEntry(entry *Entry) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	persistCtx := context.WithValue(ctx, lazyFetchContextKey{}, true)
	if err := f.CreateEntry(persistCtx, entry, false, false, nil, true, f.MaxFilenameLength); err != nil {
		glog.V(1).Infof("persistRemoteListingEntry %s: %v", entry.FullPath, err)
	}
}
