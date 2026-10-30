package weed_server

import (
	"context"
	"fmt"
	"net"
	"strings"
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

	return fakeMasterAddress(lis.Addr())
}

// fakeMasterAddress names a fake master by its listener port. The port doubles
// as the HTTP port so that ServerAddress.Equals, which compares HTTP
// addresses, tells two fake masters apart.
func fakeMasterAddress(addr net.Addr) pb.ServerAddress {
	_, port, _ := net.SplitHostPort(addr.String())
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:%s.%s", port, port))
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

// heartbeatRun is one doHeartbeatWithRetry call running in the background.
type heartbeatRun struct {
	done   chan heartbeatResult // receives the call's result once
	exited chan struct{}
	stop   func()
}

type heartbeatResult struct {
	newLeader pb.ServerAddress
	err       error
}

// startHeartbeat runs doHeartbeatWithRetry against master until it returns or
// is stopped. Its cleanup stops the heartbeat and waits for it, and runs before
// the cleanups registered earlier, so a failed assertion cannot leave the stream
// open for the fake master's GracefulStop to wait on.
func startHeartbeat(t *testing.T, vs *VolumeServer, master pb.ServerAddress) *heartbeatRun {
	t.Helper()
	var stopOnce sync.Once
	run := &heartbeatRun{
		done:   make(chan heartbeatResult, 1),
		exited: make(chan struct{}),
		stop:   func() { stopOnce.Do(func() { close(vs.stopChan) }) },
	}
	go func() {
		defer close(run.exited)
		newLeader, err := vs.doHeartbeatWithRetry(master, vs.grpcDialOption, time.Hour, 0)
		run.done <- heartbeatResult{newLeader, err}
	}()
	t.Cleanup(func() {
		run.stop()
		select {
		case <-run.exited:
		case <-time.After(5 * time.Second):
			t.Error("heartbeat did not stop")
		}
	})
	return run
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
	previous := pb.ServerAddress("previous.example.com:9333.19333")
	vs.setCurrentMaster(previous)
	run := startHeartbeat(t, vs, followerAddr)

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
	case r := <-run.done:
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
	run := startHeartbeat(t, vs, leaderAddr)

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

	run.stop()
	select {
	case r := <-run.done:
		if r.err != nil {
			t.Fatalf("heartbeat: %v", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat did not stop")
	}
}

// duplicateUUIDMaster answers every heartbeat the way a leader holding a stale
// registration does: a duplicate-uuid response and nothing else.
type duplicateUUIDMaster struct {
	master_pb.UnimplementedSeaweedServer
}

func (m *duplicateUUIDMaster) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&master_pb.HeartbeatResponse{DuplicatedUuids: []string{"uuid-1"}}); err != nil {
			return err
		}
	}
}

// A leader that rejects the registration over duplicate uuids still answers
// lookups, so it is published before the retry error goes out.
func TestHeartbeatPublishesLeaderBeforeDuplicateUuidError(t *testing.T) {
	masterAddr := startFakeMasterServerForLeaderLookup(t, &duplicateUUIDMaster{})
	vs := newHeartbeatTestVolumeServer(t)
	vs.setCurrentMaster("previous.example.com:9333.19333")

	_, err := vs.doHeartbeatWithRetry(masterAddr, vs.grpcDialOption, time.Hour, 0)
	if err == nil || !strings.Contains(err.Error(), "duplicate UUIDs") {
		t.Fatalf("err = %v, want duplicate UUIDs", err)
	}
	if got := vs.getCurrentMaster(); got != masterAddr {
		t.Fatalf("currentMaster = %q, want the answering leader %q", got, masterAddr)
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

	if got := vs.refreshLeaderFromPeers("lost.example.com:9333.19333", 5*time.Second); got != leader {
		t.Fatalf("refresh returned %q, want reported leader %q", string(got), string(leader))
	}
	if got := vs.getCurrentMaster(); got != leader {
		t.Fatalf("currentMaster = %q, want reported leader %q", string(got), string(leader))
	}
	if got := vs.store.MasterFn(); got != leader {
		t.Fatalf("store master = %q, want %q", string(got), string(leader))
	}
}

// Mid-election no master names a leader. The lost master is dead or now a
// follower that would answer lookups from its own cache, so none is published.
func TestRefreshLeaderFromPeersClearsLeaderWithoutAnswer(t *testing.T) {
	peer := startFakeMasterServerForLeaderLookup(t, &fakeLeaderConfigServer{})
	vs := &VolumeServer{
		SeedMasterNodes: []pb.ServerAddress{peer},
		grpcDialOption:  grpc.WithTransportCredentials(insecure.NewCredentials()),
		store:           &storage.Store{Ip: "127.0.0.1", Port: 8080},
	}
	vs.store.MasterFn = vs.getCurrentMaster
	vs.setCurrentMaster("lost.example.com:9333.19333")

	if got := vs.refreshLeaderFromPeers("lost.example.com:9333.19333", 5*time.Second); got != "" {
		t.Fatalf("refresh returned %q, want none", string(got))
	}
	if got := vs.getCurrentMaster(); got != "" {
		t.Fatalf("currentMaster = %q, want none", string(got))
	}
	if got := vs.store.MasterFn(); got != "" {
		t.Fatalf("store master = %q, want none", string(got))
	}
}

// startUnresponsiveMaster listens without ever answering, the way a master
// behind a partition or on a powered-off host looks to a dialer: no refusal,
// just a connection that never becomes usable.
func startUnresponsiveMaster(t *testing.T) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	return fakeMasterAddress(lis.Addr())
}

// An unreachable lost master listed first must not use up the refresh before
// a live peer is asked who leads now.
func TestRefreshLeaderFromPeersAsksLostMasterLast(t *testing.T) {
	const leader = pb.ServerAddress("leader.example.com:9333.19333")
	lost := startUnresponsiveMaster(t)
	peer := startFakeMasterServerForLeaderLookup(t, &fakeLeaderConfigServer{leader: string(leader)})
	vs := &VolumeServer{
		SeedMasterNodes: []pb.ServerAddress{lost, peer},
		grpcDialOption:  grpc.WithTransportCredentials(insecure.NewCredentials()),
		store:           &storage.Store{Ip: "127.0.0.1", Port: 8080},
	}
	vs.setCurrentMaster(lost)

	if got := vs.refreshLeaderFromPeers(lost, 2*time.Second); got != leader {
		t.Fatalf("refresh returned %q, want %q from the live peer", string(got), string(leader))
	}
	if got := vs.getCurrentMaster(); got != leader {
		t.Fatalf("currentMaster = %q, want %q", string(got), string(leader))
	}
}

func TestMastersAskingLast(t *testing.T) {
	a, b, c := pb.ServerAddress("a:9333"), pb.ServerAddress("b:9333"), pb.ServerAddress("c:9333")
	got := mastersAskingLast([]pb.ServerAddress{a, b, c}, "a:9333.19333")
	want := []pb.ServerAddress{b, c, a}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := mastersAskingLast([]pb.ServerAddress{a, b}, ""); fmt.Sprint(got) != fmt.Sprint([]pb.ServerAddress{a, b}) {
		t.Fatalf("no lost master: got %v", got)
	}
}

// switchableFollower names whichever master it currently believes leads, on
// heartbeat streams and in its configuration alike.
type switchableFollower struct {
	master_pb.UnimplementedSeaweedServer
	mu     sync.Mutex
	leader pb.ServerAddress
}

func (f *switchableFollower) setLeader(leader pb.ServerAddress) {
	f.mu.Lock()
	f.leader = leader
	f.mu.Unlock()
}

func (f *switchableFollower) currentLeader() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return string(f.leader)
}

func (f *switchableFollower) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&master_pb.HeartbeatResponse{Leader: f.currentLeader()}); err != nil {
			return err
		}
	}
}

func (f *switchableFollower) GetMasterConfiguration(_ context.Context, _ *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {
	return &master_pb.GetMasterConfigurationResponse{Leader: f.currentLeader()}, nil
}

// When the leader's stream breaks and the leader stays unreachable, the next
// stream goes to the leader the other masters name, not to the next seed. The
// seeds are ordered so that the next seed is the lost leader itself, which is
// how a volume server that reached the old leader through a redirect finds
// its seed list after the failure.
func TestHeartbeatAfterLosingLeaderDialsTheNewLeader(t *testing.T) {
	oldLeader := &fakeHeartbeatMaster{heartbeats: make(chan *master_pb.Heartbeat, 8)}
	oldLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	oldServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(oldServer, oldLeader)
	go func() { _ = oldServer.Serve(oldLis) }()
	t.Cleanup(oldServer.Stop)
	oldAddr := fakeMasterAddress(oldLis.Addr())

	newLeader := &fakeHeartbeatMaster{heartbeats: make(chan *master_pb.Heartbeat, 8)}
	newAddr := startFakeMasterServerForLeaderLookup(t, newLeader)
	follower := &switchableFollower{leader: oldAddr}
	followerAddr := startFakeMasterServerForLeaderLookup(t, follower)

	vs := newHeartbeatTestVolumeServer(t)
	vs.SeedMasterNodes = []pb.ServerAddress{followerAddr, newAddr, oldAddr}
	vs.pulsePeriod = time.Second
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		vs.heartbeat()
	}()
	t.Cleanup(func() {
		vs.StopHeartbeat()
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			t.Error("heartbeat loop did not stop")
		}
	})

	select {
	case <-oldLeader.heartbeats:
	case <-time.After(10 * time.Second):
		t.Fatal("old leader got no heartbeat")
	}

	// The old leader becomes unreachable: its streams break, and its port
	// accepts connections that never answer.
	follower.setLeader(newAddr)
	oldServer.Stop()
	blackHole, err := net.Listen("tcp", oldLis.Addr().String())
	if err != nil {
		t.Fatalf("reopen %s: %v", oldLis.Addr(), err)
	}
	t.Cleanup(func() { _ = blackHole.Close() })

	// A dial to the unreachable old leader alone would take the gRPC connect
	// timeout (20s).
	select {
	case <-newLeader.heartbeats:
	case <-time.After(10 * time.Second):
		t.Fatalf("new leader got no heartbeat within 10s of losing the old one; currentMaster = %q", vs.getCurrentMaster())
	}
}
