package weed_server

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) ScrubVolume(ctx context.Context, req *volume_server_pb.ScrubVolumeRequest) (*volume_server_pb.ScrubVolumeResponse, error) {
	vids := []needle.VolumeId{}
	if len(req.GetVolumeId()) == 0 {
		for _, l := range vs.store.Locations {
			vids = append(vids, l.VolumeIds()...)
		}
	} else {
		for _, vid := range req.GetVolumeId() {
			vids = append(vids, needle.VolumeId(vid))
		}
	}

	errs := []error{}
	var totalVolumes, totalFiles uint64
	for _, vid := range vids {
		v := vs.store.GetVolume(vid)
		if v == nil {
			return nil, fmt.Errorf("volume id %d not found", vid)
		}

		var files uint64
		var serrs []error
		switch m := req.GetMode(); m {
		case volume_server_pb.VolumeScrubMode_INDEX:
			files, serrs = scrubVolumeIndex(ctx, v)
		case volume_server_pb.VolumeScrubMode_FULL:
			files, serrs = scrubVolumeFull(ctx, v)
		default:
			return nil, fmt.Errorf("unsupported volume scrub mode %d", m)
		}

		totalVolumes += 1
		totalFiles += files
		errs = append(errs, serrs...)
	}

	res := &volume_server_pb.ScrubVolumeResponse{
		TotalVolumes: totalVolumes,
		TotalFiles:   totalFiles,
	}
	return res, errors.Join(errs...)
}

func scrubVolumeIndex(ctx context.Context, v *storage.Volume) (uint64, []error) {
	return 0, []error{fmt.Errorf("scrubVolumeIndex(): not implemented")}
}

func scrubVolumeFull(ctx context.Context, v *storage.Volume) (uint64, []error) {
	return 0, []error{fmt.Errorf("scrubVolumeFull(): not implemented")}
}

func (vs *VolumeServer) ScrubEcVolume(ctx context.Context, req *volume_server_pb.ScrubEcVolumeRequest) (*volume_server_pb.ScrubEcVolumeResponse, error) {
	vids := []needle.VolumeId{}
	if len(req.GetVolumeId()) == 0 {
		for _, l := range vs.store.Locations {
			vids = append(vids, l.EcVolumeIds()...)
		}
	} else {
		for _, vid := range req.GetVolumeId() {
			vids = append(vids, needle.VolumeId(vid))
		}
	}

	errs := []error{}
	var totalVolumes, totalFiles uint64
	for _, vid := range vids {
		v, found := vs.store.FindEcVolume(vid)
		if !found {
			return nil, fmt.Errorf("EC volume id %d not found", vid)
		}

		var files uint64
		var serrs []error
		switch m := req.GetMode(); m {
		case volume_server_pb.VolumeScrubMode_INDEX:
			files, serrs = scrubEcVolumeIndex(v)
		case volume_server_pb.VolumeScrubMode_FULL:
			files, serrs = scrubEcVolumeFull(ctx, v)
		default:
			return nil, fmt.Errorf("unsupported EC volume scrub mode %d", m)
		}

		totalVolumes += 1
		totalFiles += files
		errs = append(errs, serrs...)
	}

	res := &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes: totalVolumes,
		TotalFiles:   totalFiles,
	}
	return res, errors.Join(errs...)
}

func scrubEcVolumeIndex(ecv *erasure_coding.EcVolume) (uint64, []error) {
	return 0, []error{fmt.Errorf("scrubEcVolumeIndex(): not implemented")}
}

func scrubEcVolumeFull(ctx context.Context, v *erasure_coding.EcVolume) (uint64, []error) {
	return 0, []error{fmt.Errorf("scrubEcVolumeFull(): not implemented")}
}
