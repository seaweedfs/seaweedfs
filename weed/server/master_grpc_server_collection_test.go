package weed_server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// recordingVolumeServer reports what each DeleteCollection arrived carrying.
// failWith answers an error at once; hang holds the call until the caller gives
// up or the test releases it, the way a server that is still reachable but
// wedged behaves.
type recordingVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer

	hang     bool
	failWith error

	calls       chan deleteCollectionCall
	release     chan struct{}
	releaseOnce sync.Once
}

type deleteCollectionCall struct {
	collection string
	// budget is the time the RPC arrived with, or 0 when it carried no deadline.
	budget time.Duration
}

func newRecordingVolumeServer() *recordingVolumeServer {
	return &recordingVolumeServer{
		calls:   make(chan deleteCollectionCall, 8),
		release: make(chan struct{}),
	}
}

func (s *recordingVolumeServer) DeleteCollection(ctx context.Context, req *volume_server_pb.DeleteCollectionRequest) (*volume_server_pb.DeleteCollectionResponse, error) {
	call := deleteCollectionCall{collection: req.Collection}
	if deadline, ok := ctx.Deadline(); ok {
		call.budget = time.Until(deadline)
	}
	s.calls <- call

	if s.failWith != nil {
		return nil, s.failWith
	}
	if s.hang {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.release:
		}
	}
	return &volume_server_pb.DeleteCollectionResponse{}, nil
}

// Release unblocks every held DeleteCollection. Safe to call more than once.
func (s *recordingVolumeServer) Release() {
	s.releaseOnce.Do(func() { close(s.release) })
}

// addVolumeServer publishes stub on a fresh listener and registers it as a data
// node of topo. Only the grpc port is dialed; the http port just has to stay
// distinct per server so the topology does not treat two nodes as one address.
func addVolumeServer(t *testing.T, topo *topology.Topology, id string, stub *recordingVolumeServer) *topology.DataNode {
	t.Helper()
	t.Cleanup(stub.Release)
	grpcPort := serveGrpc(t, func(s *grpc.Server) {
		volume_server_pb.RegisterVolumeServerServer(s, stub)
	})
	return topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", grpcPort-10000, grpcPort, "", id, map[string]uint32{"": 10})
}

func newCollectionTestMaster(topo *topology.Topology) *MasterServer {
	return &MasterServer{
		Topo:           topo,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

// awaitCall waits for one DeleteCollection to reach the stub.
func awaitCall(t *testing.T, stub *recordingVolumeServer, what string) deleteCollectionCall {
	t.Helper()
	select {
	case call := <-stub.calls:
		return call
	case <-time.After(15 * time.Second):
		t.Fatalf("%s: the volume server never received DeleteCollection", what)
		return deleteCollectionCall{}
	}
}

// A caller with no deadline of its own must still not wait forever: the master
// has to bound each RPC, or one wedged volume server holds the fan-out open with
// nothing to end it.
func TestDoDeleteNormalCollectionBoundsEachVolumeServerRPC(t *testing.T) {
	const collection = "bucket-a"

	stub := newRecordingVolumeServer()
	stub.hang = true

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	dn := addVolumeServer(t, topo, "vs1", stub)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, dn)
	ms := newCollectionTestMaster(topo)

	done := make(chan error, 1)
	go func() { done <- ms.doDeleteNormalCollection(context.Background(), collection) }()

	call := awaitCall(t, stub, "bounded RPC")
	if call.budget <= 0 {
		t.Fatal("DeleteCollection reached the volume server with no deadline: a wedged server holds the fan-out open with nothing to end it")
	}
	if call.budget > deleteCollectionTimeout {
		t.Errorf("DeleteCollection budget = %v, want at most deleteCollectionTimeout %v", call.budget, deleteCollectionTimeout)
	}

	stub.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("doDeleteNormalCollection returned %v, want nil once the volume server answers", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("doDeleteNormalCollection did not return after the volume server answered")
	}
}

// The fan-out must outlive the caller. A collection can span many servers and
// the deletes are sequential, so letting an inbound cancellation end the loop
// would abandon a large delete part-done, with volumes left behind and no
// request still running to come back for them.
func TestDeleteCollectionOutlivesTheCallersCancellation(t *testing.T) {
	const collection = "bucket-b"

	normal := newRecordingVolumeServer()
	ec := newRecordingVolumeServer()

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, addVolumeServer(t, topo, "vs-normal", normal))
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 9, Collection: collection, EcIndexBits: 0x1f},
	}, addVolumeServer(t, topo, "vs-ec", ec))
	ms := newCollectionTestMaster(topo)

	// The caller is already gone by the time the delete starts.
	expired, cancel := context.WithCancel(context.Background())
	cancel()

	if err := ms.deleteCollection(expired, collection); err != nil {
		t.Fatalf("deleteCollection returned %v; a cancelled caller must not fail the fan-out", err)
	}

	// The EC pass runs second, so it is the one most likely to find the caller
	// already gone.
	for _, tc := range []struct {
		stub *recordingVolumeServer
		what string
	}{{normal, "normal pass"}, {ec, "ec pass"}} {
		call := awaitCall(t, tc.stub, "cancelled caller, "+tc.what)
		if call.collection != collection {
			t.Errorf("%s: server was told to delete %q, want %q", tc.what, call.collection, collection)
		}
		if call.budget <= 0 {
			t.Errorf("%s: DeleteCollection arrived with no deadline; each RPC should still be bounded", tc.what)
		}
	}

	if _, found := topo.FindCollection(collection); found {
		t.Error("the collection was left in the topology; the delete did not run to completion")
	}
}

// ListVolumeServers reports a node once per replica it holds. DeleteCollection
// removes the whole collection from the server it reaches, so the master must
// send it once per server, not once per replica.
func TestDoDeleteNormalCollectionSendsOneRPCPerVolumeServer(t *testing.T) {
	const collection = "bucket-c"

	stub := newRecordingVolumeServer()

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	dn := addVolumeServer(t, topo, "vs1", stub)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
		{Id: 2, Collection: collection, Version: 3},
		{Id: 3, Collection: collection, Version: 3},
	}, dn)

	collectionInTopology, found := topo.FindCollection(collection)
	if !found {
		t.Fatalf("test setup: collection %s was not registered", collection)
	}
	if listed := len(collectionInTopology.ListVolumeServers()); listed < 2 {
		t.Fatalf("test setup: the one node is listed %d times, expected once per replica", listed)
	}

	ms := newCollectionTestMaster(topo)
	if err := ms.doDeleteNormalCollection(context.Background(), collection); err != nil {
		t.Fatalf("doDeleteNormalCollection: %v", err)
	}

	awaitCall(t, stub, "one RPC per server")
	select {
	case extra := <-stub.calls:
		t.Errorf("the same server was told to delete %q more than once", extra.collection)
	default:
	}
}

// One server refusing must not leave the collection on every server after it in
// the list: returning at the first failure meant a single node that was down
// stranded the rest, and the failure is reported either way.
func TestDoDeleteNormalCollectionKeepsGoingPastAFailingServer(t *testing.T) {
	const collection = "bucket-d"

	failing := newRecordingVolumeServer()
	failing.failWith = errors.New("volume server is out of disk")
	healthy := newRecordingVolumeServer()

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	failingNode := addVolumeServer(t, topo, "vs-failing", failing)
	healthyNode := addVolumeServer(t, topo, "vs-healthy", healthy)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, failingNode)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, healthyNode)

	ms := newCollectionTestMaster(topo)
	if err := ms.doDeleteNormalCollection(context.Background(), collection); err == nil {
		t.Fatal("doDeleteNormalCollection reported success while a server refused")
	}

	awaitCall(t, failing, "failing server")
	awaitCall(t, healthy, "server behind the failing one")

	if _, found := topo.FindCollection(collection); !found {
		t.Error("the collection was dropped from the topology despite failing; nothing would come back for the rest")
	}
}

// A collection can hold normal volumes and EC shards at once. Returning after a
// failed normal pass left the EC shards in place with no request left to come
// back for them, so both passes have to run.
func TestDeleteCollectionRunsTheEcPassWhenTheNormalPassFails(t *testing.T) {
	const collection = "bucket-e"

	normal := newRecordingVolumeServer()
	normal.failWith = errors.New("volume server is out of disk")
	ec := newRecordingVolumeServer()

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, addVolumeServer(t, topo, "vs-normal", normal))
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 9, Collection: collection, EcIndexBits: 0x1f},
	}, addVolumeServer(t, topo, "vs-ec", ec))

	ms := newCollectionTestMaster(topo)
	if err := ms.deleteCollection(context.Background(), collection); err == nil {
		t.Fatal("deleteCollection reported success while the normal pass failed")
	}

	// The failure is reported, and the EC shards are cleaned up anyway.
	awaitCall(t, ec, "ec pass after a failed normal pass")

	if _, found := topo.FindCollection(collection); !found {
		t.Error("the normal collection was dropped from the topology despite failing; nothing would retry it")
	}
}

// growingVolumeServer keeps the volumes AllocateVolume creates until
// DeleteCollection drops them. The holdAt-th allocation, counted from 1, waits
// for release before it creates its volume.
type growingVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer

	holdAt  int
	held    chan struct{}
	release chan struct{}

	mu          sync.Mutex
	allocations int
	deletes     int
	volumes     map[uint32]string
}

func (s *growingVolumeServer) AllocateVolume(ctx context.Context, req *volume_server_pb.AllocateVolumeRequest) (*volume_server_pb.AllocateVolumeResponse, error) {
	s.mu.Lock()
	s.allocations++
	hold := s.allocations == s.holdAt
	s.mu.Unlock()
	if hold {
		close(s.held)
		<-s.release
	}
	s.mu.Lock()
	s.volumes[req.VolumeId] = req.Collection
	s.mu.Unlock()
	return &volume_server_pb.AllocateVolumeResponse{}, nil
}

func (s *growingVolumeServer) DeleteCollection(ctx context.Context, req *volume_server_pb.DeleteCollectionRequest) (*volume_server_pb.DeleteCollectionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	for vid, collection := range s.volumes {
		if collection == req.Collection {
			delete(s.volumes, vid)
		}
	}
	return &volume_server_pb.DeleteCollectionResponse{}, nil
}

// growthRaftServer answers as leader and applies the volume id bound growth asks for.
type growthRaftServer struct {
	raft.Server
	topo *topology.Topology
}

func (growthRaftServer) State() string { return raft.Leader }

func (s growthRaftServer) Do(c raft.Command) (interface{}, error) {
	if command, ok := c.(*topology.MaxVolumeIdCommand); ok {
		s.topo.UpAdjustMaxVolumeId(command.MaxVolumeId)
	}
	return nil, nil
}

// A bucket's first write starts a grow of several volumes, and the bucket can be
// deleted before that grow is done. The volumes created after the delete
// recreated the collection and were never removed.
func TestDeleteCollectionStopsAGrowInFlight(t *testing.T) {
	const collection = "bucket-g"

	stub := &growingVolumeServer{
		holdAt:  2,
		held:    make(chan struct{}),
		release: make(chan struct{}),
		volumes: make(map[uint32]string),
	}
	releaseOnce := sync.OnceFunc(func() { close(stub.release) })
	t.Cleanup(releaseOnce)

	topo := topology.NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	topo.RaftServer = growthRaftServer{topo: topo}
	grpcPort := serveGrpc(t, func(s *grpc.Server) {
		volume_server_pb.RegisterVolumeServerServer(s, stub)
	})
	topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", grpcPort-10000, grpcPort, "", "vs1", map[string]uint32{"": 10})
	ms := newCollectionTestMaster(topo)

	rp, _ := super_block.NewReplicaPlacementFromString("000")
	option := &topology.VolumeGrowOption{
		Collection:       collection,
		ReplicaPlacement: rp,
		Ttl:              needle.EMPTY_TTL,
		Version:          uint32(needle.GetCurrentVersion()),
	}
	grown := make(chan struct{})
	go func() {
		defer close(grown)
		topology.NewDefaultVolumeGrowth().GrowByCountAndType(ms.grpcDialOption, 7, option, topo)
	}()

	select {
	case <-stub.held:
	case <-time.After(15 * time.Second):
		t.Fatal("the grow never reached its second volume")
	}
	deleted := make(chan error, 1)
	go func() { deleted <- ms.deleteCollection(context.Background(), collection) }()

	// The delete waits for the volume being created rather than racing it.
	time.Sleep(200 * time.Millisecond)
	stub.mu.Lock()
	early := stub.deletes
	stub.mu.Unlock()
	if early != 0 {
		t.Error("the delete reached the volume server while the grow was still creating a volume")
	}
	releaseOnce()

	select {
	case <-grown:
	case <-time.After(15 * time.Second):
		t.Fatal("the grow did not finish")
	}
	select {
	case err := <-deleted:
		if err != nil {
			t.Fatalf("deleteCollection: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the delete did not finish")
	}

	stub.mu.Lock()
	left := len(stub.volumes)
	stub.mu.Unlock()
	if left != 0 {
		t.Errorf("%d volumes of the deleted collection are left on the volume server", left)
	}
	if _, found := topo.FindCollection(collection); found {
		t.Error("the grow recreated the deleted collection in the topology")
	}
}
