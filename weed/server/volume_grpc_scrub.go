package weed_server

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
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
	var brokenVolumes []*storage.Volume
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
			files, serrs = v.ScrubIndex()
		case volume_server_pb.VolumeScrubMode_LOCAL:
			// LOCAL is equivalent to FULL for regular volumes
			fallthrough
		case volume_server_pb.VolumeScrubMode_FULL:
			files, serrs = v.Scrub()
		default:
			return nil, fmt.Errorf("unsupported volume scrub mode %d", m)
		}

		totalVolumes += 1
		totalFiles += uint64(files)
		if len(serrs) != 0 {
			brokenVolumes = append(brokenVolumes, v)
			brokenVolumeIds = append(brokenVolumeIds, uint32(v.Id))
			for _, err := range serrs {
				details = append(details, err.Error())
			}
		}
	}

	errs := []error{}
	if req.GetMarkBrokenVolumesReadonly() {
		for _, v := range brokenVolumes {
			if err := vs.makeVolumeReadonly(ctx, v, true); err != nil {
				errs = append(errs, err)
				details = append(details, err.Error())
			} else {
				details = append(details, fmt.Sprintf("volume %d is now read-only", v.Id))
			}
		}
	}
	if len(errs) != 0 {
		return nil, errors.Join(errs...)
	}

	res := &volume_server_pb.ScrubVolumeResponse{
		TotalVolumes:    totalVolumes,
		TotalFiles:      totalFiles,
		BrokenVolumeIds: brokenVolumeIds,
		Details:         details,
	}
	return res, nil
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
			// index scrubs do not verify individual EC shards
			files, serrs = v.ScrubIndex()
		case volume_server_pb.VolumeScrubMode_LOCAL:
			files, shardInfos, serrs = v.ScrubLocal()
		case volume_server_pb.VolumeScrubMode_FULL:
			files, shardInfos, serrs = vs.store.ScrubEcVolume(v.VolumeId)
		default:
			return nil, fmt.Errorf("unsupported EC volume scrub mode %d", m)
		}

		totalVolumes += 1
		totalFiles += uint64(files)
		if len(serrs) != 0 || len(shardInfos) != 0 {
			brokenVolumeIds = append(brokenVolumeIds, uint32(v.VolumeId))
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
