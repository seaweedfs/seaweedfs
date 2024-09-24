package weed_server

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (vs *VolumeServer) DeleteCollection(ctx context.Context, req *volume_server_pb.DeleteCollectionRequest) (*volume_server_pb.DeleteCollectionResponse, error) {

	resp := &volume_server_pb.DeleteCollectionResponse{}

	err := vs.store.DeleteCollection(req.Collection)

	if err != nil {
		glog.Errorf("delete collection %s: %v", req.Collection, err)
	} else {
		glog.V(2).Infof("delete collection %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) AllocateVolume(ctx context.Context, req *volume_server_pb.AllocateVolumeRequest) (*volume_server_pb.AllocateVolumeResponse, error) {

	resp := &volume_server_pb.AllocateVolumeResponse{}

	err := vs.store.AddVolume(
		needle.VolumeId(req.VolumeId),
		req.Collection,
		vs.needleMapKind,
		req.Replication,
		req.Ttl,
		req.Preallocate,
		req.MemoryMapMaxSizeMb,
		types.ToDiskType(req.DiskType),
		vs.ldbTimout,
	)

	if err != nil {
		glog.Errorf("assign volume %v: %v", req, err)
	} else {
		glog.V(2).Infof("assign volume %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeMount(ctx context.Context, req *volume_server_pb.VolumeMountRequest) (*volume_server_pb.VolumeMountResponse, error) {

	resp := &volume_server_pb.VolumeMountResponse{}

	err := vs.store.MountVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume mount %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mount %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeUnmount(ctx context.Context, req *volume_server_pb.VolumeUnmountRequest) (*volume_server_pb.VolumeUnmountResponse, error) {

	resp := &volume_server_pb.VolumeUnmountResponse{}

	err := vs.store.UnmountVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume unmount %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume unmount %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeDelete(ctx context.Context, req *volume_server_pb.VolumeDeleteRequest) (*volume_server_pb.VolumeDeleteResponse, error) {

	resp := &volume_server_pb.VolumeDeleteResponse{}

	err := vs.store.DeleteVolume(needle.VolumeId(req.VolumeId), req.OnlyEmpty)

	if err != nil {
		glog.Errorf("volume delete %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume delete %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeConfigure(ctx context.Context, req *volume_server_pb.VolumeConfigureRequest) (*volume_server_pb.VolumeConfigureResponse, error) {

	resp := &volume_server_pb.VolumeConfigureResponse{}

	// check replication format
	if _, err := super_block.NewReplicaPlacementFromString(req.Replication); err != nil {
		resp.Error = fmt.Sprintf("volume configure replication %v: %v", req, err)
		return resp, nil
	}

	// unmount
	if err := vs.store.UnmountVolume(needle.VolumeId(req.VolumeId)); err != nil {
		glog.Errorf("volume configure unmount %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure unmount %v: %v", req, err)
		return resp, nil
	}

	// modify the volume info file
	if err := vs.store.ConfigureVolume(needle.VolumeId(req.VolumeId), req.Replication); err != nil {
		glog.Errorf("volume configure %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure %v: %v", req, err)
		return resp, nil
	}

	// mount
	if err := vs.store.MountVolume(needle.VolumeId(req.VolumeId)); err != nil {
		glog.Errorf("volume configure mount %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure mount %v: %v", req, err)
		return resp, nil
	}

	return resp, nil

}

func (vs *VolumeServer) VolumeMarkReadonly(ctx context.Context, req *volume_server_pb.VolumeMarkReadonlyRequest) (*volume_server_pb.VolumeMarkReadonlyResponse, error) {

	resp := &volume_server_pb.VolumeMarkReadonlyResponse{}

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}

	// step 1: stop master from redirecting traffic here
	if err := vs.notifyMasterVolumeReadonly(v, true); err != nil {
		return resp, err
	}

	// rare case 1.5: it will be unlucky if heartbeat happened between step 1 and 2.

	// step 2: mark local volume as readonly
	err := vs.store.MarkVolumeReadonly(needle.VolumeId(req.VolumeId), req.GetPersist())

	if err != nil {
		glog.Errorf("volume mark readonly %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mark readonly %v", req)
	}

	// step 3: tell master from redirecting traffic here again, to prevent rare case 1.5
	if err := vs.notifyMasterVolumeReadonly(v, true); err != nil {
		return resp, err
	}

	return resp, err
}

func (vs *VolumeServer) notifyMasterVolumeReadonly(v *storage.Volume, isReadOnly bool) error {
	if grpcErr := pb.WithMasterClient(false, vs.GetMaster(context.Background()), vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		_, err := client.VolumeMarkReadonly(context.Background(), &master_pb.VolumeMarkReadonlyRequest{
			Ip:               vs.store.Ip,
			Port:             uint32(vs.store.Port),
			VolumeId:         uint32(v.Id),
			Collection:       v.Collection,
			ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
			Ttl:              v.Ttl.ToUint32(),
			DiskType:         string(v.DiskType()),
			IsReadonly:       isReadOnly,
		})
		if err != nil {
			return fmt.Errorf("set volume %d to read only on master: %v", v.Id, err)
		}
		return nil
	}); grpcErr != nil {
		glog.V(0).Infof("connect to %s: %v", vs.GetMaster(context.Background()), grpcErr)
		return fmt.Errorf("grpc VolumeMarkReadonly with master %s: %v", vs.GetMaster(context.Background()), grpcErr)
	}
	return nil
}

func (vs *VolumeServer) VolumeMarkWritable(ctx context.Context, req *volume_server_pb.VolumeMarkWritableRequest) (*volume_server_pb.VolumeMarkWritableResponse, error) {

	resp := &volume_server_pb.VolumeMarkWritableResponse{}

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}

	err := vs.store.MarkVolumeWritable(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume mark writable %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mark writable %v", req)
	}

	// enable master to redirect traffic here
	if err := vs.notifyMasterVolumeReadonly(v, false); err != nil {
		return resp, err
	}

	return resp, err
}

func (vs *VolumeServer) VolumeStatus(ctx context.Context, req *volume_server_pb.VolumeStatusRequest) (*volume_server_pb.VolumeStatusResponse, error) {

	resp := &volume_server_pb.VolumeStatusResponse{}

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}
	if v.DataBackend == nil {
		return nil, fmt.Errorf("volume %d data backend not found", req.VolumeId)
	}

	volumeSize, _, _ := v.DataBackend.GetStat()
	resp.IsReadOnly = v.IsReadOnly()
	resp.VolumeSize = uint64(volumeSize)
	resp.FileCount = v.FileCount()
	resp.FileDeletedCount = v.DeletedCount()

	return resp, nil
}

func (vs *VolumeServer) VolumeServerStatus(ctx context.Context, req *volume_server_pb.VolumeServerStatusRequest) (*volume_server_pb.VolumeServerStatusResponse, error) {

	resp := &volume_server_pb.VolumeServerStatusResponse{
		MemoryStatus: stats.MemStat(),
		Version:      util.Version(),
		DataCenter:   vs.dataCenter,
		Rack:         vs.rack,
	}

	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			resp.DiskStatuses = append(resp.DiskStatuses, stats.NewDiskStatus(dir))
		}
	}

	return resp, nil

}

func (vs *VolumeServer) VolumeServerLeave(ctx context.Context, req *volume_server_pb.VolumeServerLeaveRequest) (*volume_server_pb.VolumeServerLeaveResponse, error) {

	resp := &volume_server_pb.VolumeServerLeaveResponse{}

	vs.StopHeartbeat()

	return resp, nil

}

func (vs *VolumeServer) VolumeNeedleStatus(ctx context.Context, req *volume_server_pb.VolumeNeedleStatusRequest) (*volume_server_pb.VolumeNeedleStatusResponse, error) {

	resp := &volume_server_pb.VolumeNeedleStatusResponse{}

	volumeId := needle.VolumeId(req.VolumeId)

	n := &needle.Needle{
		Id: types.NeedleId(req.NeedleId),
	}

	var count int
	var err error
	hasVolume := vs.store.HasVolume(volumeId)
	if !hasVolume {
		_, hasEcVolume := vs.store.FindEcVolume(volumeId)
		if !hasEcVolume {
			return nil, fmt.Errorf("volume not found %d", req.VolumeId)
		}
		count, err = vs.store.ReadEcShardNeedle(volumeId, n, nil)
	} else {
		count, err = vs.store.ReadVolumeNeedle(volumeId, n, nil, nil)
	}
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("needle not found %d", n.Id)
	}

	resp.NeedleId = uint64(n.Id)
	resp.Cookie = uint32(n.Cookie)
	resp.Size = uint32(n.Size)
	resp.LastModified = n.LastModified
	resp.Crc = n.Checksum.Value()
	if n.HasTtl() {
		resp.Ttl = n.Ttl.String()
	}
	return resp, nil

}

func (vs *VolumeServer) Ping(ctx context.Context, req *volume_server_pb.PingRequest) (resp *volume_server_pb.PingResponse, pingErr error) {
	resp = &volume_server_pb.PingResponse{
		StartTimeNs: time.Now().UnixNano(),
	}
	if req.TargetType == cluster.FilerType {
		pingErr = pb.WithFilerClient(false, 0, pb.ServerAddress(req.Target), vs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			pingResp, err := client.Ping(ctx, &filer_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if req.TargetType == cluster.VolumeServerType {
		pingErr = pb.WithVolumeServerClient(false, pb.ServerAddress(req.Target), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			pingResp, err := client.Ping(ctx, &volume_server_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if req.TargetType == cluster.MasterType {
		pingErr = pb.WithMasterClient(false, pb.ServerAddress(req.Target), vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			pingResp, err := client.Ping(ctx, &master_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if pingErr != nil {
		pingErr = fmt.Errorf("ping %s %s: %v", req.TargetType, req.Target, pingErr)
	}
	resp.StopTimeNs = time.Now().UnixNano()
	return
}
