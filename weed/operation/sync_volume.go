package operation

import (
	"context"
	"net/url"

	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func GetVolumeSyncStatus(server string, vid uint32) (resp *volume_server_pb.VolumeSyncStatusResponse, err error) {

	WithVolumeServerClient(server, func(client volume_server_pb.VolumeServerClient) error {
		resp, err = client.VolumeSyncStatus(context.Background(), &volume_server_pb.VolumeSyncStatusRequest{
			VolumdId: vid,
		})
		return nil
	})

	return
}

func GetVolumeIdxEntries(server string, vid string, eachEntryFn func(key NeedleId, offset Offset, size uint32)) error {
	values := make(url.Values)
	values.Add("volume", vid)
	line := make([]byte, NeedleEntrySize)
	err := util.GetBufferStream("http://"+server+"/admin/sync/index", values, line, func(bytes []byte) {
		key := BytesToNeedleId(line[:NeedleIdSize])
		offset := BytesToOffset(line[NeedleIdSize : NeedleIdSize+OffsetSize])
		size := util.BytesToUint32(line[NeedleIdSize+OffsetSize : NeedleIdSize+OffsetSize+SizeSize])
		eachEntryFn(key, offset, size)
	})
	if err != nil {
		return err
	}
	return nil
}
