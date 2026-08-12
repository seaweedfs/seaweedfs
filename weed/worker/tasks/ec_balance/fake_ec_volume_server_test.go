package ec_balance

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// fakeEcVolumeServer is a volume server that only knows about EC shard
// inventory. It exists so the destructive half of a balance job can be driven
// in-process: what a real volume server has on disk is the thing the master's
// topology can be wrong about, and being wrong about it is what makes a delete
// unsafe. Holding that state here lets a test say "the topology claims this,
// the disk holds that" and watch what the job does.
type fakeEcVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer

	mu sync.Mutex
	// shards actually present on this server's disks, per volume.
	shards map[uint32]map[uint32]bool
	// collection the held volume belongs to. Reported back on the inventory so
	// a caller can tell this server's volume N from another collection's N.
	collection string
	// deleted/unmounted record what the job asked for, so a test can assert on
	// the request even when it was a no-op against the inventory.
	deleted   []string
	unmounted []string
	// grpcAddr is host:port; the code under test dials by address string.
	grpcAddr string
	stop     func()
}

func (f *fakeEcVolumeServer) VolumeEcShardsInfo(ctx context.Context, req *volume_server_pb.VolumeEcShardsInfoRequest) (*volume_server_pb.VolumeEcShardsInfoResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &volume_server_pb.VolumeEcShardsInfoResponse{}
	for sid := range f.shards[req.VolumeId] {
		resp.EcShardInfos = append(resp.EcShardInfos, &volume_server_pb.EcShardInfo{
			VolumeId:   req.VolumeId,
			ShardId:    sid,
			Collection: f.collection,
		})
	}
	return resp, nil
}

func (f *fakeEcVolumeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sid := range req.ShardIds {
		f.deleted = append(f.deleted, fmt.Sprintf("%d.%d", req.VolumeId, sid))
		delete(f.shards[req.VolumeId], sid)
	}
	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

func (f *fakeEcVolumeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sid := range req.ShardIds {
		f.unmounted = append(f.unmounted, fmt.Sprintf("%d.%d", req.VolumeId, sid))
	}
	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

// has reports whether the shard is still on this server's disks.
func (f *fakeEcVolumeServer) has(volumeID, shardID uint32) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shards[volumeID][shardID]
}

func (f *fakeEcVolumeServer) deletedShards() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

// startFakeEcVolumeServer listens on a loopback port and serves the EC subset
// of the volume server API. present lists the shards this server really holds.
func startFakeEcVolumeServer(t *testing.T, volumeID uint32, present ...uint32) *fakeEcVolumeServer {
	return startFakeEcVolumeServerInCollection(t, dedupTestCollection, volumeID, present...)
}

// startFakeEcVolumeServerInCollection is the same, for a named collection, so a
// test can stand up two servers holding the same volume id in different ones.
func startFakeEcVolumeServerInCollection(t *testing.T, collection string, volumeID uint32, present ...uint32) *fakeEcVolumeServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	f := &fakeEcVolumeServer{
		shards:     map[uint32]map[uint32]bool{volumeID: {}},
		collection: collection,
		grpcAddr:   listener.Addr().String(),
	}
	for _, sid := range present {
		f.shards[volumeID][sid] = true
	}

	server := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(server, f)
	go func() { _ = server.Serve(listener) }()
	f.stop = server.Stop
	t.Cleanup(server.Stop)

	return f
}

// address returns a node id in the "host:httpPort.grpcPort" form the cluster
// uses (e.g. mg01-s3-intelistor-15:8087.18087), so ToGrpcAddress resolves to
// this fake's listener. The http port is unused and only has to parse.
func (f *fakeEcVolumeServer) address() string {
	host, port, err := net.SplitHostPort(f.grpcAddr)
	if err != nil {
		return f.grpcAddr
	}
	return fmt.Sprintf("%s:1.%s", host, port)
}

// shardBitsOf is a small readability helper for asserting inventory.
func shardBitsOf(f *fakeEcVolumeServer, volumeID uint32) erasure_coding.ShardBits {
	f.mu.Lock()
	defer f.mu.Unlock()
	var b erasure_coding.ShardBits
	for sid := range f.shards[volumeID] {
		b = b.Set(erasure_coding.ShardId(sid))
	}
	return b
}
