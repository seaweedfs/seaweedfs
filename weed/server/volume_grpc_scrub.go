package weed_server

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) ScrubVolume(ctx context.Context, req *volume_server_pb.ScrubVolumeRequest) (*volume_server_pb.ScrubVolumeResponse, error) {
	vids := []needle.VolumeId{}
	if len(req.GetVolumeIds()) == 0 {
		for _, l := range vs.store.Locations {
			vids = append(vids, l.VolumeIds()...)
		}
	} else {
		for _, vid := range req.GetVolumeIds() {
			vids = append(vids, needle.VolumeId(vid))
		}
	}

	var details []string
	var totalVolumes, totalFiles uint64
	var brokenVolumeIds []uint32
	for _, vid := range vids {
		v := vs.store.GetVolume(vid)
		if v == nil {
			return nil, fmt.Errorf("volume id %d not found", vid)
		}

		var files int64
		var serrs []error
		switch m := req.GetMode(); m {
		case volume_server_pb.VolumeScrubMode_INDEX:
			files, serrs = scrubVolumeIndex(v)
		case volume_server_pb.VolumeScrubMode_FULL:
			files, serrs = scrubVolumeFull(ctx, v)
		default:
			return nil, fmt.Errorf("unsupported volume scrub mode %d", m)
		}

		totalVolumes += 1
		totalFiles += uint64(files)
		if len(serrs) != 0 {
			brokenVolumeIds = append(brokenVolumeIds, uint32(vid))
			for _, err := range serrs {
				details = append(details, err.Error())
			}
		}
	}

	res := &volume_server_pb.ScrubVolumeResponse{
		TotalVolumes:    totalVolumes,
		TotalFiles:      totalFiles,
		BrokenVolumeIds: brokenVolumeIds,
		Details:         details,
	}
	return res, nil
}

func scrubVolumeIndex(v *storage.Volume) (int64, []error) {
	return v.CheckIndex()
}

func scrubVolumeFull(ctx context.Context, v *storage.Volume) (int64, []error) {
	return 0, []error{fmt.Errorf("scrubVolumeFull(): not implemented")}
}

func (vs *VolumeServer) ScrubEcVolume(ctx context.Context, req *volume_server_pb.ScrubEcVolumeRequest) (*volume_server_pb.ScrubEcVolumeResponse, error) {
	vids := []needle.VolumeId{}
	if len(req.GetVolumeIds()) == 0 {
		for _, l := range vs.store.Locations {
			vids = append(vids, l.EcVolumeIds()...)
		}
	} else {
		for _, vid := range req.GetVolumeIds() {
			vids = append(vids, needle.VolumeId(vid))
		}
	}

	var details []string
	var totalVolumes, totalFiles uint64
	var brokenVolumeIds []uint32
	var brokenShardInfos []*volume_server_pb.EcShardInfo
	for _, vid := range vids {
		v, found := vs.store.FindEcVolume(vid)
		if !found {
			return nil, fmt.Errorf("EC volume id %d not found", vid)
		}

		var files int64
		var shardInfos []*volume_server_pb.EcShardInfo
		var serrs []error
		switch m := req.GetMode(); m {
		case volume_server_pb.VolumeScrubMode_INDEX:
			files, shardInfos, serrs = scrubEcVolumeIndex(v)
		case volume_server_pb.VolumeScrubMode_FULL:
			files, shardInfos, serrs = scrubEcVolumeFull(ctx, v)
		default:
			return nil, fmt.Errorf("unsupported EC volume scrub mode %d", m)
		}

		totalVolumes += 1
		totalFiles += uint64(files)
		if len(serrs) != 0 || len(shardInfos) != 0 {
			brokenVolumeIds = append(brokenVolumeIds, uint32(vid))
			brokenShardInfos = append(brokenShardInfos, shardInfos...)
			for _, err := range serrs {
				details = append(details, err.Error())
			}
		}
	}

	res := &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes:     totalVolumes,
		TotalFiles:       totalFiles,
		BrokenVolumeIds:  brokenVolumeIds,
		BrokenShardInfos: brokenShardInfos,
		Details:          details,
	}
	return res, nil
}

func scrubEcVolumeIndex(ecv *erasure_coding.EcVolume) (int64, []*volume_server_pb.EcShardInfo, []error) {
	// index scrubs do not verify individual EC shards
	files, errs := ecv.CheckIndex()
	return files, nil, errs
}

func scrubEcVolumeFull(ctx context.Context, ecv *erasure_coding.EcVolume) (int64, []*volume_server_pb.EcShardInfo, []error) {
	return 0, nil, []error{fmt.Errorf("scrubEcVolumeFull(): not implemented")}
}
