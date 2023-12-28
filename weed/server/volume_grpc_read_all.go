package weed_server

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) ReadAllNeedles(req *volume_server_pb.ReadAllNeedlesRequest, stream volume_server_pb.VolumeServer_ReadAllNeedlesServer) (err error) {

	for _, vid := range req.VolumeIds {
		if err := vs.streamReadOneVolume(needle.VolumeId(vid), stream); err != nil {
			return err
		}
	}
	return nil
}

func (vs *VolumeServer) streamReadOneVolume(vid needle.VolumeId, stream volume_server_pb.VolumeServer_ReadAllNeedlesServer) error {
	v := vs.store.GetVolume(vid)
	if v == nil {
		return fmt.Errorf("not found volume id %d", vid)
	}

	scanner := &storage.VolumeFileScanner4ReadAll{
		Stream: stream,
		V:      v,
	}

	offset := int64(v.SuperBlock.BlockSize())

	return storage.ScanVolumeFileFrom(v.Version(), v.DataBackend, offset, scanner)
}
