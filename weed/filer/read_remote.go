package filer

import (
	"fmt"
	"io"
)

func (entry *Entry) IsRemoteOnly() bool {
	return len(entry.Chunks) == 0 && entry.Remote != nil && entry.Remote.Size > 0
}

func (f *Filer) ReadRemote(w io.Writer, entry *Entry, offset int64, size int64) error {
	client, _, found := f.RemoteStorage.GetRemoteStorageClient(remoteEntry.Remote.StorageName)
	if !found {
		return fmt.Errorf("remote storage %v not found", entry.Remote.StorageName)
	}

	mountDir, remoteLoation := f.RemoteStorage.FindMountDirectory(entry.FullPath)
	_, bucket, path := remoteLoation.NameBucketPath()

	remoteFullPath := path + string(entry.FullPath[len(mountDir):])

	client.ReadFile(bucket, remoteFullPath[1:], offset, size, func(w io.Writer) error {

	})
	return nil
}
