package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/raft"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/topology"
)

func (ms *MasterServer) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.LookupVolumeResponse{}
	volumeLocations := ms.lookupVolumeId(req.VolumeIds, req.Collection)

	for _, result := range volumeLocations {
		var locations []*master_pb.Location
		for _, loc := range result.Locations {
			locations = append(locations, &master_pb.Location{
				Url:       loc.Url,
				PublicUrl: loc.PublicUrl,
			})
		}
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeId:  result.VolumeId,
			Locations: locations,
			Error:     result.Error,
		})
	}

	return resp, nil
}

func (ms *MasterServer) Assign(ctx context.Context, req *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Count == 0 {
		req.Count = 1
	}

	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	option := &topology.VolumeGrowOption{
		Collection:         req.Collection,
		ReplicaPlacement:   replicaPlacement,
		Ttl:                ttl,
		Prealloacte:        ms.preallocateSize,
		DataCenter:         req.DataCenter,
		Rack:               req.Rack,
		DataNode:           req.DataNode,
		MemoryMapMaxSizeMb: req.MemoryMapMaxSizeMb,
	}

	if !ms.Topo.HasWritableVolume(option) {
		if ms.Topo.FreeSpace() <= 0 {
			return nil, fmt.Errorf("No free volumes left!")
		}
		ms.vgLock.Lock()
		if !ms.Topo.HasWritableVolume(option) {
			if _, err = ms.vg.AutomaticGrowByType(option, ms.grpcDialOption, ms.Topo, int(req.WritableVolumeCount)); err != nil {
				ms.vgLock.Unlock()
				return nil, fmt.Errorf("Cannot grow volume group! %v", err)
			}
		}
		ms.vgLock.Unlock()
	}
	fid, count, dn, err := ms.Topo.PickForWrite(req.Count, option)
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	return &master_pb.AssignResponse{
		Fid:       fid,
		Url:       dn.Url(),
		PublicUrl: dn.PublicUrl,
		Count:     count,
		Auth:      string(security.GenJwt(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fid)),
	}, nil
}

func (ms *MasterServer) Statistics(ctx context.Context, req *master_pb.StatisticsRequest) (*master_pb.StatisticsResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	volumeLayout := ms.Topo.GetVolumeLayout(req.Collection, replicaPlacement, ttl)
	stats := volumeLayout.Stats()

	totalSize := ms.Topo.GetMaxVolumeCount() * int64(ms.option.VolumeSizeLimitMB) * 1024 * 1024

	resp := &master_pb.StatisticsResponse{
		TotalSize: uint64(totalSize),
		UsedSize:  stats.UsedSize,
		FileCount: stats.FileCount,
	}

	return resp, nil
}

func (ms *MasterServer) VolumeList(ctx context.Context, req *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VolumeListResponse{
		TopologyInfo:      ms.Topo.ToTopologyInfo(),
		VolumeSizeLimitMb: uint64(ms.option.VolumeSizeLimitMB),
	}

	return resp, nil
}

func (ms *MasterServer) LookupEcVolume(ctx context.Context, req *master_pb.LookupEcVolumeRequest) (*master_pb.LookupEcVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.LookupEcVolumeResponse{}

	ecLocations, found := ms.Topo.LookupEcShards(needle.VolumeId(req.VolumeId))

	if !found {
		return resp, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}

	resp.VolumeId = req.VolumeId

	for shardId, shardLocations := range ecLocations.Locations {
		var locations []*master_pb.Location
		for _, dn := range shardLocations {
			locations = append(locations, &master_pb.Location{
				Url:       string(dn.Id()),
				PublicUrl: dn.PublicUrl,
			})
		}
		resp.ShardIdLocations = append(resp.ShardIdLocations, &master_pb.LookupEcVolumeResponse_EcShardIdLocation{
			ShardId:   uint32(shardId),
			Locations: locations,
		})
	}

	return resp, nil
}

func (ms *MasterServer) VacuumVolume(ctx context.Context, req *master_pb.VacuumVolumeRequest) (*master_pb.VacuumVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VacuumVolumeResponse{}

	ms.Topo.Vacuum(ms.grpcDialOption, float64(req.GarbageThreshold), ms.preallocateSize)

	return resp, nil
}
