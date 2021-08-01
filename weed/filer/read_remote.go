package filer

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (entry *Entry) IsInRemoteOnly() bool {
	return len(entry.Chunks) == 0 && entry.Remote != nil && entry.Remote.Size > 0
}

func (f *Filer) ReadRemote(entry *Entry, offset int64, size int64) (data[]byte, err error) {
	client, _, found := f.RemoteStorage.GetRemoteStorageClient(entry.Remote.StorageName)
	if !found {
		return nil, fmt.Errorf("remote storage %v not found", entry.Remote.StorageName)
	}

	mountDir, remoteLoation := f.RemoteStorage.FindMountDirectory(entry.FullPath)

	remoteFullPath := remoteLoation.Path + string(entry.FullPath[len(mountDir):])

	sourceLoc := &filer_pb.RemoteStorageLocation{
		Name:   remoteLoation.Name,
		Bucket: remoteLoation.Bucket,
		Path:   remoteFullPath,
	}

	return client.ReadFile(sourceLoc, offset, size)
}
