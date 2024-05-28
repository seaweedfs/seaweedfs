package filer

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func ReadMountMappings(grpcDialOption grpc.DialOption, filerAddress pb.ServerAddress) (mappings *remote_pb.RemoteStorageMapping, readErr error) {
	var oldContent []byte
	if readErr = pb.WithFilerClient(false, 0, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		oldContent, readErr = ReadInsideFiler(client, DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE)
		return readErr
	}); readErr != nil {
		if readErr != filer_pb.ErrNotFound {
			return nil, fmt.Errorf("read existing mapping: %v", readErr)
		}
		oldContent = nil
	}
	mappings, readErr = UnmarshalRemoteStorageMappings(oldContent)
	if readErr != nil {
		return nil, fmt.Errorf("unmarshal mappings: %v", readErr)
	}

	return
}

func InsertMountMapping(filerClient filer_pb.FilerClient, dir string, remoteStorageLocation *remote_pb.RemoteStorageLocation) (err error) {

	// read current mapping
	var oldContent, newContent []byte
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		oldContent, err = ReadInsideFiler(client, DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE)
		return err
	})
	if err != nil {
		if err != filer_pb.ErrNotFound {
			return fmt.Errorf("read existing mapping: %v", err)
		}
	}

	// add new mapping
	newContent, err = addRemoteStorageMapping(oldContent, dir, remoteStorageLocation)
	if err != nil {
		return fmt.Errorf("add mapping %s~%s: %v", dir, remoteStorageLocation, err)
	}

	// save back
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return SaveInsideFiler(client, DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE, newContent)
	})
	if err != nil {
		return fmt.Errorf("save mapping: %v", err)
	}

	return nil
}

func DeleteMountMapping(filerClient filer_pb.FilerClient, dir string) (err error) {

	// read current mapping
	var oldContent, newContent []byte
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		oldContent, err = ReadInsideFiler(client, DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE)
		return err
	})
	if err != nil {
		if err != filer_pb.ErrNotFound {
			return fmt.Errorf("read existing mapping: %v", err)
		}
	}

	// add new mapping
	newContent, err = removeRemoteStorageMapping(oldContent, dir)
	if err != nil {
		return fmt.Errorf("delete mount %s: %v", dir, err)
	}

	// save back
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return SaveInsideFiler(client, DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE, newContent)
	})
	if err != nil {
		return fmt.Errorf("save mapping: %v", err)
	}

	return nil
}

func addRemoteStorageMapping(oldContent []byte, dir string, storageLocation *remote_pb.RemoteStorageLocation) (newContent []byte, err error) {
	mappings, unmarshalErr := UnmarshalRemoteStorageMappings(oldContent)
	if unmarshalErr != nil {
		// skip
	}

	// set the new mapping
	mappings.Mappings[dir] = storageLocation

	if newContent, err = proto.Marshal(mappings); err != nil {
		return oldContent, fmt.Errorf("marshal mappings: %v", err)
	}

	return
}

func removeRemoteStorageMapping(oldContent []byte, dir string) (newContent []byte, err error) {
	mappings, unmarshalErr := UnmarshalRemoteStorageMappings(oldContent)
	if unmarshalErr != nil {
		return nil, unmarshalErr
	}

	// set the new mapping
	delete(mappings.Mappings, dir)

	if newContent, err = proto.Marshal(mappings); err != nil {
		return oldContent, fmt.Errorf("marshal mappings: %v", err)
	}

	return
}
