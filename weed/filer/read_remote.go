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

// MergeRemoteContentEncoding stamps the remote object's Content-Encoding into the
// entry extended attributes so HTTP and S3 reads return the header. An unset
// remote value (a listing that does not report encodings) leaves the attribute
// alone; an authoritatively empty one removes it.
func MergeRemoteContentEncoding(remoteEntry *filer_pb.RemoteEntry, extended map[string][]byte) map[string][]byte {
	if remoteEntry == nil || remoteEntry.RemoteContentEncoding == nil {
		return extended
	}
	encoding := *remoteEntry.RemoteContentEncoding
	if encoding == "" {
		delete(extended, "Content-Encoding")
		return extended
	}
	if extended == nil {
		extended = make(map[string][]byte)
	}
	extended["Content-Encoding"] = []byte(encoding)
	return extended
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
// chunkConcurrency and downloadConcurrency of 0 mean use server defaults.
func CacheRemoteObjectToLocalCluster(filerClient filer_pb.FilerClient, parent util.FullPath, entry *filer_pb.Entry, chunkConcurrency int32, downloadConcurrency int32) (*filer_pb.Entry, error) {
	var cachedEntry *filer_pb.Entry
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, cacheErr := client.CacheRemoteObjectToLocalCluster(context.Background(), &filer_pb.CacheRemoteObjectToLocalClusterRequest{
			Directory:           string(parent),
			Name:                entry.Name,
			ChunkConcurrency:    chunkConcurrency,
			DownloadConcurrency: downloadConcurrency,
		})
		if cacheErr == nil && resp != nil {
			cachedEntry = resp.Entry
		}
		return cacheErr
	})
	return cachedEntry, err
}
