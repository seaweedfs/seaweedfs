package weed_server

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) VacuumVolumeCheck(ctx context.Context, req *volume_server_pb.VacuumVolumeCheckRequest) (*volume_server_pb.VacuumVolumeCheckResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCheckResponse{}

	garbageRatio, err := vs.store.CheckCompactVolume(needle.VolumeId(req.VolumeId))

	resp.GarbageRatio = garbageRatio

	if err != nil {
		glog.V(3).Infof("check volume %d: %v", req.VolumeId, err)
	}

	return resp, err

}

func (vs *VolumeServer) VacuumVolumeCompact(req *volume_server_pb.VacuumVolumeCompactRequest, stream volume_server_pb.VolumeServer_VacuumVolumeCompactServer) error {

	resp := &volume_server_pb.VacuumVolumeCompactResponse{}
	reportInterval := int64(1024 * 1024 * 128)
	nextReportTarget := reportInterval

	var sendErr error
	err := vs.store.CompactVolume(needle.VolumeId(req.VolumeId), req.Preallocate, vs.compactionBytePerSecond, func(processed int64) bool {
		if processed > nextReportTarget {
			resp.ProcessedBytes = processed
			if sendErr = stream.Send(resp); sendErr != nil {
				return false
			}
			nextReportTarget = processed + reportInterval
		}
		return true
	})

	if err != nil {
		glog.Errorf("compact volume %d: %v", req.VolumeId, err)
		return err
	}
	if sendErr != nil {
		glog.Errorf("compact volume %d report progress: %v", req.VolumeId, sendErr)
		return sendErr
	}

	glog.V(1).Infof("compact volume %d", req.VolumeId)
	return nil

}

func (vs *VolumeServer) VacuumVolumeCommit(ctx context.Context, req *volume_server_pb.VacuumVolumeCommitRequest) (*volume_server_pb.VacuumVolumeCommitResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCommitResponse{}

	readOnly, err := vs.store.CommitCompactVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("commit volume %d: %v", req.VolumeId, err)
	} else {
		glog.V(1).Infof("commit volume %d", req.VolumeId)
	}
	resp.IsReadOnly = readOnly
	return resp, err

}

func (vs *VolumeServer) VacuumVolumeCleanup(ctx context.Context, req *volume_server_pb.VacuumVolumeCleanupRequest) (*volume_server_pb.VacuumVolumeCleanupResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCleanupResponse{}

	err := vs.store.CommitCleanupVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("cleanup volume %d: %v", req.VolumeId, err)
	} else {
		glog.V(1).Infof("cleanup volume %d", req.VolumeId)
	}

	return resp, err

}
