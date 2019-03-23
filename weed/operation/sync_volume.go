package operation

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"io"
)

func GetVolumeSyncStatus(server string, grpcDialOption grpc.DialOption, vid uint32) (resp *volume_server_pb.VolumeSyncStatusResponse, err error) {

	WithVolumeServerClient(server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		resp, err = client.VolumeSyncStatus(context.Background(), &volume_server_pb.VolumeSyncStatusRequest{
			VolumeId: vid,
		})
		return nil
	})

	return
}

func GetVolumeIdxEntries(server string, grpcDialOption grpc.DialOption, vid uint32, eachEntryFn func(key NeedleId, offset Offset, size uint32)) error {

	return WithVolumeServerClient(server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		stream, err := client.VolumeSyncIndex(context.Background(), &volume_server_pb.VolumeSyncIndexRequest{
			VolumeId: vid,
		})
		if err != nil {
			return err
		}

		var indexFileContent []byte

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read index entries: %v", err)
			}
			indexFileContent = append(indexFileContent, resp.IndexFileContent...)
		}

		dataSize := len(indexFileContent)

		for idx := 0; idx+NeedleEntrySize <= dataSize; idx += NeedleEntrySize {
			line := indexFileContent[idx : idx+NeedleEntrySize]
			key := BytesToNeedleId(line[:NeedleIdSize])
			offset := BytesToOffset(line[NeedleIdSize : NeedleIdSize+OffsetSize])
			size := util.BytesToUint32(line[NeedleIdSize+OffsetSize : NeedleIdSize+OffsetSize+SizeSize])
			eachEntryFn(key, offset, size)
		}

		return nil
	})
}
