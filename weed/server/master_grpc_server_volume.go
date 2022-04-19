package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/raft"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/topology"
)

func (ms *MasterServer) ProcessGrowRequest() {
	go func() {
		filter := sync.Map{}
		for {
			req, ok := <-ms.vgCh
			if !ok {
				break
			}

			if !ms.Topo.IsLeader() {
				//discard buffered requests
				time.Sleep(time.Second * 1)
				continue
			}

			// filter out identical requests being processed
			found := false
			filter.Range(func(k, v interface{}) bool {
				if reflect.DeepEqual(k, req) {
					found = true
				}
				return !found
			})

			option := req.Option
			vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

			// not atomic but it's okay
			if !found && vl.ShouldGrowVolumes(option) {
				filter.Store(req, nil)
				// we have lock called inside vg
				go func() {
					glog.V(1).Infoln("starting automatic volume grow")
					start := time.Now()
					_, err := ms.vg.AutomaticGrowByType(req.Option, ms.grpcDialOption, ms.Topo, req.Count)
					glog.V(1).Infoln("finished automatic volume grow, cost ", time.Now().Sub(start))
					vl.DoneGrowRequest()

					if req.ErrCh != nil {
						req.ErrCh <- err
						close(req.ErrCh)
					}

					filter.Delete(req)
				}()

			} else {
				glog.V(4).Infoln("discard volume grow request")
			}
		}
	}()
}

func (ms *MasterServer) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {

	resp := &master_pb.LookupVolumeResponse{}
	volumeLocations := ms.lookupVolumeId(req.VolumeOrFileIds, req.Collection)

	for _, result := range volumeLocations {
		var locations []*master_pb.Location
		for _, loc := range result.Locations {
			locations = append(locations, &master_pb.Location{
				Url:       loc.Url,
				PublicUrl: loc.PublicUrl,
			})
		}
		var auth string
		if strings.Contains(result.VolumeOrFileId, ",") { // this is a file id
			auth = string(security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, result.VolumeOrFileId))
		}
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: result.VolumeOrFileId,
			Locations:      locations,
			Error:          result.Error,
			Auth:           auth,
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
	diskType := types.ToDiskType(req.DiskType)

	option := &topology.VolumeGrowOption{
		Collection:         req.Collection,
		ReplicaPlacement:   replicaPlacement,
		Ttl:                ttl,
		DiskType:           diskType,
		Preallocate:        ms.preallocateSize,
		DataCenter:         req.DataCenter,
		Rack:               req.Rack,
		DataNode:           req.DataNode,
		MemoryMapMaxSizeMb: req.MemoryMapMaxSizeMb,
	}

	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

	if !vl.HasGrowRequest() && vl.ShouldGrowVolumes(option) {
		if ms.Topo.AvailableSpaceFor(option) <= 0 {
			return nil, fmt.Errorf("no free volumes left for " + option.String())
		}
		vl.AddGrowRequest()
		ms.vgCh <- &topology.VolumeGrowRequest{
			Option: option,
			Count:  int(req.WritableVolumeCount),
		}
	}

	var (
		lastErr    error
		maxTimeout = time.Second * 10
		startTime  = time.Now()
	)

	for time.Now().Sub(startTime) < maxTimeout {
		fid, count, dnList, err := ms.Topo.PickForWrite(req.Count, option)
		if err == nil {
			dn := dnList.Head()
			var replicas []*master_pb.Location
			for _, r := range dnList.Rest() {
				replicas = append(replicas, &master_pb.Location{
					Url:       r.Url(),
					PublicUrl: r.PublicUrl,
					GrpcPort:  uint32(r.GrpcPort),
				})
			}
			return &master_pb.AssignResponse{
				Fid: fid,
				Location: &master_pb.Location{
					Url:       dn.Url(),
					PublicUrl: dn.PublicUrl,
					GrpcPort:  uint32(dn.GrpcPort),
				},
				Count:    count,
				Auth:     string(security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fid)),
				Replicas: replicas,
			}, nil
		}
		//glog.V(4).Infoln("waiting for volume growing...")
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return nil, lastErr
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

	volumeLayout := ms.Topo.GetVolumeLayout(req.Collection, replicaPlacement, ttl, types.ToDiskType(req.DiskType))
	stats := volumeLayout.Stats()
	resp := &master_pb.StatisticsResponse{
		TotalSize: stats.TotalSize,
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

	ms.Topo.Vacuum(ms.grpcDialOption, float64(req.GarbageThreshold), req.VolumeId, req.Collection, ms.preallocateSize)

	return resp, nil
}
