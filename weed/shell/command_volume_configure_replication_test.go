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

// configureReplicationGateServer blocks inside VolumeConfigure until the test
// releases it, so the test can observe how many servers run at once.
type configureReplicationGateServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	gate *configureReplicationGate
}

type configureReplicationGate struct {
	entered   chan struct{}
	release   chan struct{}
	mu        sync.Mutex
	active    int
	maxActive int
	calls     int
}

func (s *configureReplicationGateServer) VolumeConfigure(_ context.Context, _ *volume_server_pb.VolumeConfigureRequest) (*volume_server_pb.VolumeConfigureResponse, error) {
	s.gate.mu.Lock()
	s.gate.calls++
	s.gate.active++
	if s.gate.active > s.gate.maxActive {
		s.gate.maxActive = s.gate.active
	}
	s.gate.mu.Unlock()

	s.gate.entered <- struct{}{}
	<-s.gate.release

	s.gate.mu.Lock()
	s.gate.active--
	s.gate.mu.Unlock()
	return &volume_server_pb.VolumeConfigureResponse{}, nil
}

func TestConfigureReplicationOnVolumeServersRunsServersInParallelUpToTheLimit(t *testing.T) {
	const servers = 4
	const maxParallelization = 2
	gate := &configureReplicationGate{
		entered: make(chan struct{}, servers),
		release: make(chan struct{}),
	}
	var dataNodes []*master_pb.DataNodeInfo
	for i := 0; i < servers; i++ {
		dataNodes = append(dataNodes, startConfigureReplicationTestVolumeServer(t,
			&configureReplicationGateServer{gate: gate}, uint32(i+1)))
	}
	commandEnv, topologyInfo := configureReplicationTestEnv(dataNodes...)

	done := make(chan error, 1)
	go func() {
		done <- configureReplicationOnVolumeServers(commandEnv, topologyInfo, allVolumes, "001", maxParallelization)
	}()

	for i := 0; i < maxParallelization; i++ {
		select {
		case <-gate.entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d servers started; serial execution would hold the second call", i)
		}
	}

	select {
	case <-gate.entered:
		t.Fatalf("more than %d servers ran while calls were held open", maxParallelization)
	case <-time.After(100 * time.Millisecond):
	}

	close(gate.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.maxActive != maxParallelization {
		t.Errorf("at most %d servers ran together, expected %d", gate.maxActive, maxParallelization)
	}
	if gate.calls != servers {
		t.Errorf("%d servers ran, expected %d", gate.calls, servers)
	}
}
