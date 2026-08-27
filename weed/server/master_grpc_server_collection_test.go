package weed_server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// recordingVolumeServer answers DeleteCollection the way the test asks and
// reports what each call arrived carrying. failWith answers an error at once;
// hang holds the call until the caller gives up or the test releases it, the way
// a server that is still reachable but wedged behaves.
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

// serveVolumeServer publishes stub on a fresh localhost listener and returns the
// data node coordinates a topology needs in order to reach it.
func serveVolumeServer(t *testing.T, stub *recordingVolumeServer) (ip string, port, grpcPort int) {
	t.Helper()
	t.Cleanup(stub.Release)
	grpcPort = serveGrpc(t, func(s *grpc.Server) {
		volume_server_pb.RegisterVolumeServerServer(s, stub)
	})
	// Only the grpc port is dialed; the http port just has to stay distinct per
	// server so the topology does not treat two nodes as one address.
	port = grpcPort - 10000
	if port <= 0 {
		port = grpcPort + 1
	}
	return "127.0.0.1", port, grpcPort
}

func newTestMaster(topo *topology.Topology) *MasterServer {
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

// A caller with no deadline of its own must still not be able to wait forever:
// the master has to bound each RPC, or one wedged volume server holds the
// fan-out open with nothing to end it (issue #7234).
func TestDoDeleteNormalCollectionBoundsEachVolumeServerRPC(t *testing.T) {
	const collection = "bucket-a"

	stub := newRecordingVolumeServer()
	stub.hang = true
	ip, port, grpcPort := serveVolumeServer(t, stub)

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode(ip, port, grpcPort, "", "vs1", map[string]uint32{"": 10})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, dn)
	ms := newTestMaster(topo)

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

// The fan-out must outlive the caller's budget. A collection can span many
// servers and the deletes are sequential, so letting an inbound deadline end the
// loop would abandon a large delete part-done, with volumes left behind and no
// request still running to come back for them.
func TestDoDeleteNormalCollectionOutlivesTheCallersDeadline(t *testing.T) {
	const collection = "bucket-b"

	stub := newRecordingVolumeServer()
	ip, port, grpcPort := serveVolumeServer(t, stub)

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode(ip, port, grpcPort, "", "vs1", map[string]uint32{"": 10})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, dn)
	ms := newTestMaster(topo)

	// The caller is already gone by the time the delete starts.
	expired, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- ms.doDeleteNormalCollection(expired, collection) }()

	call := awaitCall(t, stub, "cancelled caller")
	if call.budget <= 0 {
		t.Error("DeleteCollection arrived with no deadline; each RPC should still be bounded")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("doDeleteNormalCollection returned %v; a cancelled caller must not fail the fan-out", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("doDeleteNormalCollection did not finish")
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
	ip, port, grpcPort := serveVolumeServer(t, stub)

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode(ip, port, grpcPort, "", "vs1", map[string]uint32{"": 10})
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

	ms := newTestMaster(topo)
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

// The EC pass needs the same decoupling from the caller as the normal one. It
// runs second, so it is the pass most likely to find the caller already gone,
// and a deadline reaching it would leave shards behind exactly when there is
// nothing left to retry them.
func TestDeleteCollectionEcPassOutlivesTheCallersDeadline(t *testing.T) {
	const collection = "bucket-e"

	ec := newRecordingVolumeServer()
	ecIP, ecPort, ecGrpcPort := serveVolumeServer(t, ec)

	// EC shards only, so the normal pass finds nothing and the EC pass is what is
	// under test.
	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	ecNode := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode(ecIP, ecPort, ecGrpcPort, "", "vs-ec", map[string]uint32{"": 10})
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 9, Collection: collection, EcIndexBits: 0x1f},
	}, ecNode)

	ms := newTestMaster(topo)

	// The caller is already gone by the time the delete starts.
	expired, cancel := context.WithCancel(context.Background())
	cancel()

	if err := ms.deleteCollection(expired, collection); err != nil {
		t.Fatalf("deleteCollection returned %v; a cancelled caller must not fail the EC pass", err)
	}

	call := awaitCall(t, ec, "cancelled caller, ec pass")
	if call.collection != collection {
		t.Errorf("the ec server was told to delete %q, want %q", call.collection, collection)
	}
	if call.budget <= 0 {
		t.Error("the EC DeleteCollection arrived with no deadline; each RPC should still be bounded")
	}
}

// A collection can hold normal volumes and EC shards at once. Returning after a
// failed normal pass left the EC shards in place with no request left to come
// back for them, so both passes have to run.
func TestDeleteCollectionRunsTheEcPassWhenTheNormalPassFails(t *testing.T) {
	const collection = "bucket-d"

	normal := newRecordingVolumeServer()
	normal.failWith = errors.New("volume server is out of disk")
	normalIP, normalPort, normalGrpcPort := serveVolumeServer(t, normal)

	ec := newRecordingVolumeServer()
	ecIP, ecPort, ecGrpcPort := serveVolumeServer(t, ec)

	topo := topology.NewTopology("test", nil, 32*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")

	normalNode := rack.GetOrCreateDataNode(normalIP, normalPort, normalGrpcPort, "", "vs-normal", map[string]uint32{"": 10})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, normalNode)

	ecNode := rack.GetOrCreateDataNode(ecIP, ecPort, ecGrpcPort, "", "vs-ec", map[string]uint32{"": 10})
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 9, Collection: collection, EcIndexBits: 0x1f},
	}, ecNode)

	ms := newTestMaster(topo)

	if err := ms.deleteCollection(context.Background(), collection); err == nil {
		t.Fatal("deleteCollection reported success while the normal pass failed")
	}

	// The failure is reported, and the EC shards are cleaned up anyway.
	awaitCall(t, ec, "ec pass after a failed normal pass")

	if _, found := topo.FindCollection(collection); !found {
		t.Error("the normal collection was dropped from the topology despite failing; nothing would retry it")
	}
}
