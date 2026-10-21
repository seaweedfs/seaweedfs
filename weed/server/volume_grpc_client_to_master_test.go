package weed_server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLookupRaftLeaderMasterDoesNotUpdateCurrentMaster(t *testing.T) {
	follower := startFakeMasterServerForLeaderLookup(t, &fakeLeaderConfigServer{
		leader: "leader.example.com:9333.19333",
	})
	heartbeatPeer := pb.ServerAddress("10.0.0.1:9333")
	vs := &VolumeServer{
		SeedMasterNodes: []pb.ServerAddress{follower},
		grpcDialOption:  grpc.WithTransportCredentials(insecure.NewCredentials()),
		store:           &storage.Store{Ip: "127.0.0.1", Port: 8080},
	}
	vs.setCurrentMaster(heartbeatPeer)

	leader, err := vs.lookupRaftLeaderMaster(context.Background())
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got, want := leader.ToHttpAddress(), "leader.example.com:9333"; got != want {
		t.Fatalf("leader = %q, want %q", got, want)
	}
	if got := vs.getCurrentMaster(); got != heartbeatPeer {
		t.Fatalf("currentMaster = %v, want unchanged %v", got, heartbeatPeer)
	}
}

type fakeLeaderConfigServer struct {
	master_pb.UnimplementedSeaweedServer
	leader string
}

func (s *fakeLeaderConfigServer) GetMasterConfiguration(_ context.Context, _ *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {
	return &master_pb.GetMasterConfigurationResponse{Leader: s.leader}, nil
}

func startFakeMasterServerForLeaderLookup(t *testing.T, srv master_pb.SeaweedServer) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.GracefulStop)

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port))
}

// fakeHeartbeatMaster answers SendHeartbeat the way a master does: a follower
// names the leader on every heartbeat, the leader answers the first one with
// its volume options and never names itself.
type fakeHeartbeatMaster struct {
	master_pb.UnimplementedSeaweedServer
	leader     string        // set on a follower
	release    chan struct{} // a follower waits on it before answering
	heartbeats chan *master_pb.Heartbeat
}

func (m *fakeHeartbeatMaster) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	answered := false
	for {
		hb, err := stream.Recv()
		if err != nil {
			return err
		}
		if m.heartbeats != nil {
			m.heartbeats <- hb
		}
		if m.leader != "" {
			if m.release != nil {
				<-m.release
			}
			if err := stream.Send(&master_pb.HeartbeatResponse{Leader: m.leader}); err != nil {
				return err
			}
			continue
		}
		if !answered {
			answered = true
			if err := stream.Send(&master_pb.HeartbeatResponse{VolumeSizeLimit: 1 << 30}); err != nil {
				return err
			}
		}
	}
}

func newHeartbeatTestVolumeServer(t *testing.T) *VolumeServer {
	t.Helper()
	store := storage.NewStore(nil, "127.0.0.1", 8080, 18080, "http://127.0.0.1:8080", "store-id",
		[]string{t.TempDir()},
		[]int32{0},
		[]util.MinFreeSpace{{}},
		"",
		storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType},
		nil,
		3,
		stats.DefaultDiskIOProbeConfig(),
	)
	t.Cleanup(store.Close)
	vs := &VolumeServer{
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		store:          store,
		stopChan:       make(chan bool),
		isHeartbeating: true,
	}
	store.MasterFn = vs.getCurrentMaster
	return vs
}

func waitForCurrentMaster(t *testing.T, vs *VolumeServer, want pb.ServerAddress) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !vs.getCurrentMaster().Equals(want) {
		if time.Now().After(deadline) {
			t.Fatalf("currentMaster = %q, want %q", vs.getCurrentMaster(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A follower must never become the master lookups go to: until it names the
// leader the previous one stays, and then the named leader takes over before
// the heartbeat loop has reconnected.
func TestHeartbeatToFollowerPublishesNamedLeaderOnly(t *testing.T) {
	const leader = pb.ServerAddress("leader.example.com:9333.19333")
	follower := &fakeHeartbeatMaster{
		leader:     string(leader),
		release:    make(chan struct{}),
		heartbeats: make(chan *master_pb.Heartbeat, 8),
	}
	followerAddr := startFakeMasterServerForLeaderLookup(t, follower)
	// cleanups run last-in first-out, so a failed assertion still lets the
	// follower answer before its server stops gracefully
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(follower.release) }) }
	t.Cleanup(release)

	vs := newHeartbeatTestVolumeServer(t)
	t.Cleanup(func() { close(vs.stopChan) })
	previous := pb.ServerAddress("previous.example.com:9333.19333")
	vs.setCurrentMaster(previous)

	type result struct {
		newLeader pb.ServerAddress
		err       error
	}
	done := make(chan result, 1)
	go func() {
		newLeader, err := vs.doHeartbeatWithRetry(followerAddr, vs.grpcDialOption, time.Hour, 0)
		done <- result{newLeader, err}
	}()

	select {
	case <-follower.heartbeats:
	case <-time.After(5 * time.Second):
		t.Fatal("follower got no heartbeat")
	}
	if got := vs.getCurrentMaster(); got != previous {
		t.Fatalf("currentMaster = %q while connected to follower %q, want unchanged %q", string(got), string(followerAddr), string(previous))
	}
	if got := vs.store.MasterFn(); got != previous {
		t.Fatalf("store master = %q, want %q", got, previous)
	}
	release()

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("heartbeat: %v", r.err)
		}
		if r.newLeader != leader {
			t.Fatalf("newLeader = %q, want %q", r.newLeader, leader)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat did not return after the follower named the leader")
	}
	if got := vs.getCurrentMaster(); got != leader {
		t.Fatalf("currentMaster = %q, want named leader %q", got, leader)
	}
}

// The leader's first answer confirms it, and the size limit that answer carries
// makes the receiver ask for a fresh heartbeat, which the send loop delivers.
func TestHeartbeatToLeaderPublishesItAndResendsFromSendLoop(t *testing.T) {
	leader := &fakeHeartbeatMaster{heartbeats: make(chan *master_pb.Heartbeat, 8)}
	leaderAddr := startFakeMasterServerForLeaderLookup(t, leader)

	vs := newHeartbeatTestVolumeServer(t)
	done := make(chan error, 1)
	go func() {
		_, err := vs.doHeartbeatWithRetry(leaderAddr, vs.grpcDialOption, time.Hour, 0)
		done <- err
	}()

	waitForCurrentMaster(t, vs, leaderAddr)
	if got := vs.store.MasterFn(); got != leaderAddr {
		t.Fatalf("store master = %q, want %q", got, leaderAddr)
	}

	// the initial volume and ec heartbeats, then the resend
	for i := 0; i < 3; i++ {
		select {
		case <-leader.heartbeats:
		case <-time.After(5 * time.Second):
			t.Fatalf("leader got %d heartbeats, want 3", i)
		}
	}

	close(vs.stopChan)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat did not stop")
	}
}

// After the heartbeat loses its master, lookups move to the leader the other
// masters report instead of dialing the lost one until a new stream answers.
func TestRefreshLeaderFromPeersPublishesReportedLeader(t *testing.T) {
	const leader = pb.ServerAddress("leader.example.com:9333.19333")
	peer := startFakeMasterServerForLeaderLookup(t, &fakeLeaderConfigServer{leader: string(leader)})
	vs := &VolumeServer{
		SeedMasterNodes: []pb.ServerAddress{peer},
		grpcDialOption:  grpc.WithTransportCredentials(insecure.NewCredentials()),
		store:           &storage.Store{Ip: "127.0.0.1", Port: 8080},
	}
	vs.store.MasterFn = vs.getCurrentMaster
	vs.setCurrentMaster("lost.example.com:9333.19333")

	vs.refreshLeaderFromPeers(5 * time.Second)

	if got := vs.getCurrentMaster(); got != leader {
		t.Fatalf("currentMaster = %q, want reported leader %q", string(got), string(leader))
	}
	if got := vs.store.MasterFn(); got != leader {
		t.Fatalf("store master = %q, want %q", string(got), string(leader))
	}
}

// Mid-election no master names a leader; the last known one stays rather
// than an address nobody vouched for.
func TestRefreshLeaderFromPeersKeepsLastLeaderWithoutAnswer(t *testing.T) {
	peer := startFakeMasterServerForLeaderLookup(t, &fakeLeaderConfigServer{})
	vs := &VolumeServer{
		SeedMasterNodes: []pb.ServerAddress{peer},
		grpcDialOption:  grpc.WithTransportCredentials(insecure.NewCredentials()),
		store:           &storage.Store{Ip: "127.0.0.1", Port: 8080},
	}
	last := pb.ServerAddress("last.example.com:9333.19333")
	vs.setCurrentMaster(last)

	vs.refreshLeaderFromPeers(5 * time.Second)

	if got := vs.getCurrentMaster(); got != last {
		t.Fatalf("currentMaster = %q, want unchanged %q", string(got), string(last))
	}
}
