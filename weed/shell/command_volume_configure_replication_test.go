package shell

import (
	"context"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type configureReplicationTestVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	failure    string
	mu         sync.Mutex
	configured map[uint32]string
}

func (s *configureReplicationTestVolumeServer) VolumeConfigure(_ context.Context, req *volume_server_pb.VolumeConfigureRequest) (*volume_server_pb.VolumeConfigureResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.configured == nil {
		s.configured = make(map[uint32]string)
	}
	s.configured[req.VolumeId] = req.Replication
	return &volume_server_pb.VolumeConfigureResponse{Error: s.failure}, nil
}

// configureReplicationOverlapServer can back several volume servers at once and
// records whether two calls for the same volume were ever in flight together.
type configureReplicationOverlapServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	mu         sync.Mutex
	inFlight   map[uint32]int
	overlapped bool
}

func (s *configureReplicationOverlapServer) VolumeConfigure(_ context.Context, req *volume_server_pb.VolumeConfigureRequest) (*volume_server_pb.VolumeConfigureResponse, error) {
	s.mu.Lock()
	s.inFlight[req.VolumeId]++
	if s.inFlight[req.VolumeId] > 1 {
		s.overlapped = true
	}
	s.mu.Unlock()

	time.Sleep(20 * time.Millisecond)

	s.mu.Lock()
	s.inFlight[req.VolumeId]--
	s.mu.Unlock()
	return &volume_server_pb.VolumeConfigureResponse{}, nil
}

func startConfigureReplicationTestVolumeServer(t *testing.T, server volume_server_pb.VolumeServerServer, volumeIds ...uint32) *master_pb.DataNodeInfo {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, server)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	port := lis.Addr().(*net.TCPAddr).Port
	disk := &master_pb.DiskInfo{}
	for _, vid := range volumeIds {
		disk.VolumeInfos = append(disk.VolumeInfos, &master_pb.VolumeInformationMessage{Id: vid})
	}
	return &master_pb.DataNodeInfo{
		Id:        fmt.Sprintf("127.0.0.1:%d", port),
		GrpcPort:  uint32(port),
		DiskInfos: map[string]*master_pb.DiskInfo{"": disk},
	}
}

func configureReplicationTestEnv(dataNodes ...*master_pb.DataNodeInfo) (*CommandEnv, *master_pb.TopologyInfo) {
	commandEnv := &CommandEnv{option: &ShellOptions{
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}
	topologyInfo := &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{
		RackInfos: []*master_pb.RackInfo{{DataNodeInfos: dataNodes}},
	}}}
	return commandEnv, topologyInfo
}

func allVolumes(*master_pb.VolumeInformationMessage) bool { return true }

func TestConfigureReplicationOnVolumeServersReportsEveryFailingServer(t *testing.T) {
	for _, maxParallelization := range []int{1, DefaultMaxParallelization} {
		t.Run(fmt.Sprintf("maxParallelization=%d", maxParallelization), func(t *testing.T) {
			first := &configureReplicationTestVolumeServer{failure: "first server failed"}
			healthy := &configureReplicationTestVolumeServer{}
			last := &configureReplicationTestVolumeServer{failure: "last server failed"}
			commandEnv, topologyInfo := configureReplicationTestEnv(
				startConfigureReplicationTestVolumeServer(t, first, 1),
				startConfigureReplicationTestVolumeServer(t, healthy, 2, 3),
				startConfigureReplicationTestVolumeServer(t, last, 4),
			)

			err := configureReplicationOnVolumeServers(commandEnv, topologyInfo, allVolumes, "001", maxParallelization)

			if err == nil {
				t.Fatal("expected an error")
			}
			for _, failure := range []string{first.failure, last.failure} {
				if !strings.Contains(err.Error(), failure) {
					t.Errorf("error %q does not mention %q", err, failure)
				}
			}
			healthy.mu.Lock()
			defer healthy.mu.Unlock()
			if expected := map[uint32]string{2: "001", 3: "001"}; !maps.Equal(healthy.configured, expected) {
				t.Errorf("healthy server configured %v, expected %v", healthy.configured, expected)
			}
		})
	}
}

func TestConfigureReplicationOnVolumeServersConfiguresReplicasOneAtATime(t *testing.T) {
	server := &configureReplicationOverlapServer{inFlight: make(map[uint32]int)}
	commandEnv, topologyInfo := configureReplicationTestEnv(
		startConfigureReplicationTestVolumeServer(t, server, 1, 2),
		startConfigureReplicationTestVolumeServer(t, server, 1, 2),
		startConfigureReplicationTestVolumeServer(t, server, 1, 2),
	)

	if err := configureReplicationOnVolumeServers(commandEnv, topologyInfo, allVolumes, "002", DefaultMaxParallelization); err != nil {
		t.Fatal(err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.overlapped {
		t.Error("replicas of one volume were configured at the same time")
	}
}
