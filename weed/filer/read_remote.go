package filer

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/remote_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (entry *Entry) IsInRemoteOnly() bool {
	return len(entry.Chunks) == 0 && entry.Remote != nil && entry.Remote.RemoteSize > 0
}

func (f *Filer) ReadRemote(entry *Entry, offset int64, size int64) (data []byte, err error) {
	client, _, found := f.RemoteStorage.GetRemoteStorageClient(entry.Remote.StorageName)
	if !found {
		return nil, fmt.Errorf("remote storage %v not found", entry.Remote.StorageName)
	}

	mountDir, remoteLoation := f.RemoteStorage.FindMountDirectory(entry.FullPath)

	sourceLoc := MapFullPathToRemoteStorageLocation(mountDir, remoteLoation, entry.FullPath)

	return client.ReadFile(sourceLoc, offset, size)
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

func MapRemoteStorageLocationPathToFullPath(localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, remoteLocationPath string)(fp util.FullPath)  {
	return localMountedDir.Child(remoteLocationPath[len(remoteMountedLocation.Path):])
}

func DownloadToLocal(filerClient filer_pb.FilerClient, remoteConf *remote_pb.RemoteConf, remoteLocation *remote_pb.RemoteStorageLocation, parent util.FullPath, entry *filer_pb.Entry) error {
	return filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DownloadToLocal(context.Background(), &filer_pb.DownloadToLocalRequest{
			Directory: string(parent),
			Name:      entry.Name,
		})
		return err
	})
}
