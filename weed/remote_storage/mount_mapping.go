package remote_storage

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

func ReadMountMappings(grpcDialOption grpc.DialOption, filerAddress string) (mappings *filer_pb.RemoteStorageMapping, readErr error) {
	var oldContent []byte
	if readErr = pb.WithFilerClient(filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		oldContent, readErr = filer.ReadInsideFiler(client, filer.DirectoryEtcRemote, filer.REMOTE_STORAGE_MOUNT_FILE)
		return readErr
	}); readErr != nil {
		return nil, readErr
	}

	mappings, readErr = filer.UnmarshalRemoteStorageMappings(oldContent)
	if readErr != nil {
		return nil, fmt.Errorf("unmarshal mappings: %v", readErr)
	}

	return
}

func ReadRemoteStorageConf(grpcDialOption grpc.DialOption, filerAddress string, storageName string) (conf *filer_pb.RemoteConf, readErr error) {
	var oldContent []byte
	if readErr = pb.WithFilerClient(filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		oldContent, readErr = filer.ReadInsideFiler(client, filer.DirectoryEtcRemote, storageName+filer.REMOTE_STORAGE_CONF_SUFFIX)
		return readErr
	}); readErr != nil {
		return nil, readErr
	}

	// unmarshal storage configuration
	conf = &filer_pb.RemoteConf{}
	if unMarshalErr := proto.Unmarshal(oldContent, conf); unMarshalErr != nil {
		readErr = fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, storageName+filer.REMOTE_STORAGE_CONF_SUFFIX, unMarshalErr)
		return
	}

	return
}
