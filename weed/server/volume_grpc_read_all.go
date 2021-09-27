package weed_server

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
)

func (vs *VolumeServer) ReadAllNeedles(req *volume_server_pb.ReadAllNeedlesRequest, stream volume_server_pb.VolumeServer_ReadAllNeedlesServer) (err error) {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	scanner := &VolumeFileScanner4ReadAll{
		stream: stream,
	}

	offset := int64(v.SuperBlock.BlockSize())

	err = storage.ScanVolumeFileFrom(v.Version(), v.DataBackend, offset, scanner)

	return err

}

type VolumeFileScanner4ReadAll struct {
	stream volume_server_pb.VolumeServer_ReadAllNeedlesServer
}

func (scanner *VolumeFileScanner4ReadAll) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	return nil

}
func (scanner *VolumeFileScanner4ReadAll) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4ReadAll) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {

	sendErr := scanner.stream.Send(&volume_server_pb.ReadAllNeedlesResponse{
		NeedleId:   uint64(n.Id),
		NeedleBlob: n.Data,
	})
	if sendErr != nil {
		return sendErr
	}
	return nil
}
