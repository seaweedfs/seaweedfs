package weed_server

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

func (vs *VolumeServer) VolumeSyncStatus(ctx context.Context, req *volume_server_pb.VolumeSyncStatusRequest) (*volume_server_pb.VolumeSyncStatusResponse, error) {

	v := vs.store.GetVolume(storage.VolumeId(req.VolumdId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumdId)
	}

	resp := v.GetVolumeSyncStatus()

	glog.V(2).Infof("volume sync status %d", req.VolumdId)

	return resp, nil

}

func (vs *VolumeServer) VolumeSyncIndex(req *volume_server_pb.VolumeSyncIndexRequest, stream volume_server_pb.VolumeServer_VolumeSyncIndexServer) error {

	v := vs.store.GetVolume(storage.VolumeId(req.VolumdId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumdId)
	}

	content, err := v.IndexFileContent()

	if err != nil {
		glog.Errorf("sync volume %d index: %v", req.VolumdId, err)
	} else {
		glog.V(2).Infof("sync volume %d index", req.VolumdId)
	}

	const blockSizeLimit = 1024 * 1024 * 2
	for i := 0; i < len(content); i += blockSizeLimit {
		blockSize := len(content) - i
		if blockSize > blockSizeLimit {
			blockSize = blockSizeLimit
		}
		resp := &volume_server_pb.VolumeSyncIndexResponse{}
		resp.IndexFileContent = content[i : i+blockSize]
		stream.Send(resp)
	}

	return nil

}

func (vs *VolumeServer) VolumeSyncData(req *volume_server_pb.VolumeSyncDataRequest, stream volume_server_pb.VolumeServer_VolumeSyncDataServer) error {

	v := vs.store.GetVolume(storage.VolumeId(req.VolumdId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumdId)
	}

	if uint32(v.SuperBlock.CompactRevision) != req.Revision {
		return fmt.Errorf("requested volume revision is %d, but current revision is %d", req.Revision, v.SuperBlock.CompactRevision)
	}

	content, err := storage.ReadNeedleBlob(v.DataFile(), int64(req.Offset)*types.NeedlePaddingSize, req.Size, v.Version())
	if err != nil {
		return fmt.Errorf("read offset:%d size:%d", req.Offset, req.Size)
	}

	id, err := types.ParseNeedleId(req.NeedleId)
	if err != nil {
		return fmt.Errorf("parsing needle id %s: %v", req.NeedleId, err)
	}
	n := new(storage.Needle)
	n.ParseNeedleHeader(content)
	if id != n.Id {
		return fmt.Errorf("expected file entry id %d, but found %d", id, n.Id)
	}

	if err != nil {
		glog.Errorf("sync volume %d data: %v", req.VolumdId, err)
	}

	const blockSizeLimit = 1024 * 1024 * 2
	for i := 0; i < len(content); i += blockSizeLimit {
		blockSize := len(content) - i
		if blockSize > blockSizeLimit {
			blockSize = blockSizeLimit
		}
		resp := &volume_server_pb.VolumeSyncDataResponse{}
		resp.FileContent = content[i : i+blockSize]
		stream.Send(resp)
	}

	return nil

}
