package filer

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/remote_pb"
)

func SaveMountMapping(filerClient filer_pb.FilerClient, dir string, remoteStorageLocation *remote_pb.RemoteStorageLocation) (err error) {

	// read current mapping
	var oldContent, newContent []byte
	err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		oldContent, err = ReadInsideFiler(client, DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE)
		return err
	})
	if err != nil {
		if err != filer_pb.ErrNotFound {
			return fmt.Errorf("read existing mapping: %v", err)
		}
	}

	// add new mapping
	newContent, err = AddRemoteStorageMapping(oldContent, dir, remoteStorageLocation)
	if err != nil {
		return fmt.Errorf("add mapping %s~%s: %v", dir, remoteStorageLocation, err)
	}

	// save back
	err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		return SaveInsideFiler(client, DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE, newContent)
	})
	if err != nil {
		return fmt.Errorf("save mapping: %v", err)
	}

	return nil
}