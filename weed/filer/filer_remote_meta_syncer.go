package filer

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type RemoteMetaSyncer struct {
	filer    *Filer
	interval time.Duration
	quit     chan struct{}
}

func (f *Filer) StartRemoteMetaSyncer(interval time.Duration) {
	syncer := &RemoteMetaSyncer{
		filer:    f,
		interval: interval,
		quit:     make(chan struct{}),
	}
	f.RemoteMetaSyncer = syncer
	go syncer.loop()
}

func (s *RemoteMetaSyncer) Stop() {
	close(s.quit)
}

func (s *RemoteMetaSyncer) loop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	glog.V(0).Infof("remote meta syncer started with interval %v", s.interval)

	for {
		select {
		case <-s.quit:
			glog.V(0).Infof("remote meta syncer stopped")
			return
		case <-ticker.C:
			s.syncAllMounts()
		}
	}
}

func (s *RemoteMetaSyncer) syncAllMounts() {
	if s.filer.RemoteStorage == nil {
		return
	}
	mappings := s.filer.RemoteStorage.GetAllMountMappings()
	for dir, loc := range mappings {
		if err := s.syncOneMount(dir, loc); err != nil {
			glog.V(1).Infof("remote meta sync %s: %v", dir, err)
		}
	}
}

func (s *RemoteMetaSyncer) syncOneMount(localDir string, remoteLoc *remote_pb.RemoteStorageLocation) error {
	client, _, found := s.filer.RemoteStorage.GetRemoteStorageClient(remoteLoc.Name)
	if !found {
		return nil
	}

	remote := &remote_pb.RemoteStorageLocation{
		Name:   remoteLoc.Name,
		Bucket: remoteLoc.Bucket,
		Path:   remoteLoc.Path,
	}

	return client.Traverse(remote, func(remoteDir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		localPath := MapRemoteStorageLocationPathToFullPath(
			util.FullPath(localDir), remoteLoc, remoteDir,
		)
		fullPath := localPath.Child(name)

		ctx := context.Background()
		existingEntry, findErr := s.filer.FindEntry(ctx, fullPath)

		if findErr != nil && findErr != filer_pb.ErrNotFound {
			return nil // skip on errors
		}

		if existingEntry == nil {
			// New remote entry, create locally
			mtime := time.Unix(remoteEntry.RemoteMtime, 0)
			entry := &Entry{
				FullPath: fullPath,
				Attr: Attr{
					Mtime:    mtime,
					Crtime:   mtime,
					Mode:     0644,
					FileSize: uint64(remoteEntry.RemoteSize),
				},
				Remote: remoteEntry,
			}
			if isDirectory {
				entry.Attr.Mode = 0755 | 0040000 // os.ModeDir
				entry.Remote = nil
			}

			persistCtx := context.WithValue(ctx, lazyFetchContextKey{}, true)
			if err := s.filer.CreateEntry(persistCtx, entry, false, false, nil, true, s.filer.MaxFilenameLength); err != nil {
				glog.V(1).Infof("remote meta sync create %s: %v", fullPath, err)
			}
			return nil
		}

		// Entry exists
		if existingEntry.Remote == nil {
			// Local-only entry, don't overwrite
			return nil
		}

		// Check if remote has changed
		if existingEntry.Remote.RemoteETag != remoteEntry.RemoteETag ||
			existingEntry.Remote.RemoteMtime < remoteEntry.RemoteMtime {
			// Update the entry
			existingEntry.Remote = remoteEntry
			existingEntry.Attr.FileSize = uint64(remoteEntry.RemoteSize)
			existingEntry.Attr.Mtime = time.Unix(remoteEntry.RemoteMtime, 0)
			existingEntry.Chunks = nil // Clear chunks to force re-fetch on next read
			if err := s.filer.Store.UpdateEntry(ctx, existingEntry); err != nil {
				glog.V(1).Infof("remote meta sync update %s: %v", fullPath, err)
			}
		}

		return nil
	})
}
