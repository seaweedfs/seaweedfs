package remote_storage

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
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
