package s3api

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
)

type mountEntry struct {
	loc    *remote_pb.RemoteStorageLocation
	client remote_storage.RemoteStorageClient
}

type remoteStorageIndex struct {
	mu          sync.RWMutex
	mounts      map[string]mountEntry
	fetchGroup  singleflight.Group
}

func newRemoteStorageIndex() *remoteStorageIndex {
	return &remoteStorageIndex{
		mounts: make(map[string]mountEntry),
	}
}

func (idx *remoteStorageIndex) refresh(grpcDialOption grpc.DialOption, filerAddress pb.ServerAddress) error {
	mappings, err := filer.ReadMountMappings(grpcDialOption, filerAddress)
	if err != nil {
		return err
	}

	idx.mu.RLock()
	existingClients := make(map[string]remote_storage.RemoteStorageClient, len(idx.mounts))
	for _, m := range idx.mounts {
		existingClients[m.loc.Name] = m.client
	}
	idx.mu.RUnlock()

	newMounts := make(map[string]mountEntry, len(mappings.Mappings))
	for dir, loc := range mappings.Mappings {
		client, ok := existingClients[loc.Name]
		if !ok {
			conf, confErr := filer.ReadRemoteStorageConf(grpcDialOption, filerAddress, loc.Name)
			if confErr != nil {
				glog.Warningf("remoteStorageIndex: failed to load conf for %s: %v", loc.Name, confErr)
				continue
			}
			var clientErr error
			client, clientErr = remote_storage.GetRemoteStorage(conf)
			if clientErr != nil {
				glog.Warningf("remoteStorageIndex: failed to build client for %s: %v", loc.Name, clientErr)
				continue
			}
		}
		newMounts[dir] = mountEntry{loc: loc, client: client}
	}

	idx.mu.Lock()
	idx.mounts = newMounts
	idx.mu.Unlock()
	return nil
}

func (idx *remoteStorageIndex) isEmpty() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.mounts) == 0
}

func (idx *remoteStorageIndex) findForPath(filerPath string) (remote_storage.RemoteStorageClient, *remote_pb.RemoteStorageLocation, string, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var bestDir string
	var bestMount *mountEntry
	normalized := strings.TrimSuffix(filerPath, "/")
	for dir, m := range idx.mounts {
		trimmedDir := strings.TrimSuffix(dir, "/")
		if (normalized == trimmedDir || strings.HasPrefix(normalized+"/", trimmedDir+"/")) && len(trimmedDir) > len(bestDir) {
			bestDir = trimmedDir
			mc := m
			bestMount = &mc
		}
	}
	if bestMount == nil {
		return nil, nil, "", false
	}

	relPath := strings.TrimPrefix(normalized, bestDir)
	return bestMount.client, bestMount.loc, relPath, true
}

func (s3a *S3ApiServer) lazyFetchFromRemote(ctx context.Context, bucket, object string) (*filer_pb.Entry, error) {
	if s3a.remoteStorageIdx == nil || s3a.remoteStorageIdx.isEmpty() {
		return nil, nil
	}

	bucketFilerPath := s3a.bucketDir(bucket)
	objectFilerPath := bucketFilerPath + "/" + strings.TrimPrefix(object, "/")

	client, mountLoc, relPath, found := s3a.remoteStorageIdx.findForPath(objectFilerPath)
	if !found {
		return nil, nil
	}

	result, err, _ := s3a.remoteStorageIdx.fetchGroup.Do(objectFilerPath, func() (interface{}, error) {
		return s3a.doLazyFetch(context.WithoutCancel(ctx), objectFilerPath, client, mountLoc, relPath, bucket, object)
	})
	if err != nil {
		if errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
			return nil, nil
		}
		return nil, err
	}
	entry, _ := result.(*filer_pb.Entry)
	return entry, nil
}

func (s3a *S3ApiServer) doLazyFetch(ctx context.Context, objectFilerPath string, client remote_storage.RemoteStorageClient, mountLoc *remote_pb.RemoteStorageLocation, relPath, bucket, object string) (*filer_pb.Entry, error) {
	remotePath := strings.TrimSuffix(mountLoc.Path, "/") + relPath
	if remotePath == "" {
		remotePath = "/"
	}

	remoteLoc := &remote_pb.RemoteStorageLocation{
		Name:   mountLoc.Name,
		Bucket: mountLoc.Bucket,
		Path:   remotePath,
	}

	remoteEntry, err := client.StatFile(remoteLoc)
	if err != nil {
		if errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
			glog.V(3).Infof("lazyFetchFromRemote: %s/%s not found in remote", bucket, object)
		} else {
			glog.Warningf("lazyFetchFromRemote: stat %s/%s failed: %v", bucket, object, err)
		}
		return nil, err
	}

	dir, name := util.FullPath(objectFilerPath).DirAndName()
	entry := &filer_pb.Entry{
		Name:        name,
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    remoteEntry.RemoteMtime,
			Crtime:   remoteEntry.RemoteMtime,
			FileMode: 0644,
			FileSize: uint64(remoteEntry.RemoteSize),
		},
		RemoteEntry: remoteEntry,
	}

	// SkipCheckParentDirectory is required because parent directories are not
	// pre-populated when using -noSync. The entry will be served correctly for
	// direct GET requests; bucket listing will only show lazily-fetched entries.
	saveErr := s3a.WithFilerClient(false, func(filerClient filer_pb.SeaweedFilerClient) error {
		_, createErr := filerClient.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory:                dir,
			Entry:                    entry,
			IsFromOtherCluster:       false,
			SkipCheckParentDirectory: true,
		})
		return createErr
	})
	if saveErr != nil {
		glog.Warningf("lazyFetchFromRemote: failed to persist filer entry for %s/%s: %v — serving from remote metadata anyway", bucket, object, saveErr)
	}

	return entry, nil
}
