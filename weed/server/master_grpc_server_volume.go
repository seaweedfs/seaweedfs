package weed_server

import (
	"context"
	"fmt"

	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
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
		req.Replication = ms.defaultReplicaPlacement
	}
	replicaPlacement, err := storage.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := storage.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	option := &topology.VolumeGrowOption{
		Collection:       req.Collection,
		ReplicaPlacement: replicaPlacement,
		Ttl:              ttl,
		Prealloacte:      ms.preallocate,
		DataCenter:       req.DataCenter,
		Rack:             req.Rack,
		DataNode:         req.DataNode,
	}

	if !ms.Topo.HasWritableVolume(option) {
		if ms.Topo.FreeSpace() <= 0 {
			return nil, fmt.Errorf("No free volumes left!")
		}
		ms.vgLock.Lock()
		if !ms.Topo.HasWritableVolume(option) {
			if _, err = ms.vg.AutomaticGrowByType(option, ms.grpcDialOpiton, ms.Topo); err != nil {
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
		Auth:      string(security.GenJwt(ms.guard.SigningKey, fid)),
	}, nil
}

func (ms *MasterServer) Statistics(ctx context.Context, req *master_pb.StatisticsRequest) (*master_pb.StatisticsResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Replication == "" {
		req.Replication = ms.defaultReplicaPlacement
	}
	replicaPlacement, err := storage.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := storage.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	volumeLayout := ms.Topo.GetVolumeLayout(req.Collection, replicaPlacement, ttl)
	stats := volumeLayout.Stats()

	resp := &master_pb.StatisticsResponse{
		TotalSize: stats.TotalSize,
		UsedSize:  stats.UsedSize,
		FileCount: stats.FileCount,
	}

	return resp, nil
}
