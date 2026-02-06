package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (entry *Entry) IsInRemoteOnly() bool {
	return len(entry.GetChunks()) == 0 && entry.Remote != nil && entry.Remote.RemoteSize > 0
}

func MapFullPathToRemoteStorageLocation(localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, fp util.FullPath) *remote_pb.RemoteStorageLocation {
	remoteLocation := &remote_pb.RemoteStorageLocation{
		Name:   remoteMountedLocation.Name,
		Bucket: remoteMountedLocation.Bucket,
		Path:   remoteMountedLocation.Path,
	}
	remoteLocation.Path = string(util.FullPath(remoteLocation.Path).Child(string(fp)[len(localMountedDir):]))
	return remoteLocation
}

func MapRemoteStorageLocationPathToFullPath(localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, remoteLocationPath string) (fp util.FullPath) {
	return localMountedDir.Child(remoteLocationPath[len(remoteMountedLocation.Path):])
}

// CacheRemoteObjectToLocalCluster caches a remote object to the local cluster.
// It returns the updated entry with local chunk locations.
// Parameters remoteConf and remoteLocation are kept for backward compatibility but are not used.
func CacheRemoteObjectToLocalCluster(filerClient filer_pb.FilerClient, remoteConf *remote_pb.RemoteConf, remoteLocation *remote_pb.RemoteStorageLocation, parent util.FullPath, entry *filer_pb.Entry) (*filer_pb.Entry, error) {
	var cachedEntry *filer_pb.Entry
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, cacheErr := client.CacheRemoteObjectToLocalCluster(context.Background(), &filer_pb.CacheRemoteObjectToLocalClusterRequest{
			Directory: string(parent),
			Name:      entry.Name,
		})
		if cacheErr == nil && resp != nil {
			cachedEntry = resp.Entry
		}
		return cacheErr
	})
	return cachedEntry, err
}
