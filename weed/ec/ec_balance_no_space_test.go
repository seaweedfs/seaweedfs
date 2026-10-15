package ec

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ecBalanceFakeServer answers the EC-shard RPC subset a balance run drives:
// copy (rejected when full), mount, inventory, unmount, delete. The disk state
// a real server tracks lives in shards; what the planner saw is rebuilt from it
// through FetchTopology, so a landed move is visible to the next plan.
type ecBalanceFakeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer

	mu        sync.Mutex
	shards    map[uint32]map[uint32]bool // vid -> shardId -> present
	full      bool                       // VolumeEcShardsCopy answers "no space left"
	copyCalls int
	grpcAddr  string
	stop      func()
}

func (f *ecBalanceFakeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copyCalls++
	if f.full {
		return nil, fmt.Errorf("no space left")
	}
	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

func (f *ecBalanceFakeServer) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sid := range req.ShardIds {
		f.shards[req.VolumeId][sid] = true
	}
	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (f *ecBalanceFakeServer) VolumeEcShardsInfo(ctx context.Context, req *volume_server_pb.VolumeEcShardsInfoRequest) (*volume_server_pb.VolumeEcShardsInfoResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &volume_server_pb.VolumeEcShardsInfoResponse{}
	for sid := range f.shards[req.VolumeId] {
		resp.EcShardInfos = append(resp.EcShardInfos, &volume_server_pb.EcShardInfo{
			VolumeId: req.VolumeId,
			ShardId:  sid,
			Size:     1,
		})
	}
	return resp, nil
}

func (f *ecBalanceFakeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {
	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (f *ecBalanceFakeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sid := range req.ShardIds {
		delete(f.shards[req.VolumeId], sid)
	}
	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

// nodeId returns the "host:httpPort.grpcPort" node id form the cluster uses, so
// ToGrpcAddress resolves to this server's listener.
func (f *ecBalanceFakeServer) nodeId() string {
	host, port, err := net.SplitHostPort(f.grpcAddr)
	if err != nil {
		return f.grpcAddr
	}
	return fmt.Sprintf("%s:1.%s", host, port)
}

func (f *ecBalanceFakeServer) shardCount(vid uint32) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.shards[vid])
}

func startEcBalanceFakeServer(t *testing.T, vid uint32, present ...uint32) *ecBalanceFakeServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	f := &ecBalanceFakeServer{
		shards:   map[uint32]map[uint32]bool{vid: {}},
		grpcAddr: listener.Addr().String(),
	}
	for _, sid := range present {
		f.shards[vid][sid] = true
	}

	server := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(server, f)
	go func() { _ = server.Serve(listener) }()
	f.stop = server.Stop
	t.Cleanup(server.Stop)
	return f
}

// TestEcBalanceReplanAfterNoSpaceLeft reproduces seaweedfs/seaweedfs#11753:
// the planner works from heartbeat free-slot counts that can disagree with the
// volume server's own check, so a planned target can reject the copy with
// "no space left" mid-run. The balance must re-plan without that node as a
// target rather than fail the whole run.
func TestEcBalanceReplanAfterNoSpaceLeft(t *testing.T) {
	const vid = uint32(1)
	var allShards []uint32
	for sid := 0; sid < erasure_coding.DataShardsCount+erasure_coding.ParityShardsCount; sid++ {
		allShards = append(allShards, uint32(sid))
	}

	full := startEcBalanceFakeServer(t, vid)
	accept := startEcBalanceFakeServer(t, vid)
	source := startEcBalanceFakeServer(t, vid, allShards...)
	full.full = true

	servers := []*ecBalanceFakeServer{source, full, accept}
	fetchTopology := func(delay time.Duration) (*master_pb.TopologyInfo, uint64, error) {
		var nodes []*master_pb.DataNodeInfo
		for _, f := range servers {
			di := &master_pb.DiskInfo{Type: string(types.HardDriveType), MaxVolumeCount: 100}
			f.mu.Lock()
			for v, set := range f.shards {
				if len(set) == 0 {
					continue
				}
				si := erasure_coding.NewShardsInfo()
				for sid := range set {
					si.Set(erasure_coding.NewShardInfo(erasure_coding.ShardId(sid), 1))
				}
				di.EcShardInfos = append(di.EcShardInfos, &master_pb.VolumeEcShardInformationMessage{
					Id:          v,
					Collection:  "c1",
					EcIndexBits: si.Bitmap(),
					ShardSizes:  si.SizesInt64(),
					DiskType:    string(types.HardDriveType),
				})
			}
			f.mu.Unlock()
			nodes = append(nodes, &master_pb.DataNodeInfo{
				Id:        f.nodeId(),
				DiskInfos: map[string]*master_pb.DiskInfo{string(types.HardDriveType): di},
			})
		}
		return &master_pb.TopologyInfo{
			DataCenterInfos: []*master_pb.DataCenterInfo{{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{{
					Id:            "r1",
					DataNodeInfos: nodes,
				}},
			}},
		}, 0, nil
	}

	env := &Env{
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		FetchTopology:  fetchTopology,
	}

	err := EcBalance(env, []string{"c1"}, "", nil, types.HardDriveType, 3, 0, true, nil, nil, nil)
	if err != nil {
		t.Fatalf("balance should re-plan around the full target, got: %v", err)
	}
	if full.copyCalls == 0 {
		t.Fatalf("expected the full server to have rejected at least one copy")
	}
	if got := full.shardCount(vid); got != 0 {
		t.Fatalf("full server should receive no shards, holds %d", got)
	}
	total := source.shardCount(vid) + full.shardCount(vid) + accept.shardCount(vid)
	if total != len(allShards) {
		t.Fatalf("shards lost or duplicated: total %d, want %d", total, len(allShards))
	}
	if source.shardCount(vid) == len(allShards) {
		t.Fatalf("no shards were moved off the source")
	}
}
