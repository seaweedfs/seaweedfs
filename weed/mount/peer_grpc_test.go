package mount

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/mount_peer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func newTestPeerGrpc(t *testing.T, cache chunk_cache.ChunkCache, ownerFor func(fid string) string, selfAddr string) (mount_peer_pb.MountPeerClient, func()) {
	t.Helper()
	dir := NewPeerDirectory()
	srv := NewPeerGrpcServer(cache, dir, ownerFor, selfAddr)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.listener = ln
	srv.grpcS = grpc.NewServer()
	mount_peer_pb.RegisterMountPeerServer(srv.grpcS, srv)
	go srv.grpcS.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := mount_peer_pb.NewMountPeerClient(conn)
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return client, cleanup
}

func TestPeerGrpcServer_AnnounceAndLookup(t *testing.T) {
	self := "self:18080"
	client, cleanup := newTestPeerGrpc(t, nil, func(fid string) string {
		return self
	}, self)
	defer cleanup()

	_, err := client.ChunkAnnounce(context.Background(), &mount_peer_pb.ChunkAnnounceRequest{
		FileIds:    []string{"3,a", "3,b"},
		PeerAddr:   "holder:18080",
		TtlSeconds: 60,
	})
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	resp, err := client.ChunkLookup(context.Background(), &mount_peer_pb.ChunkLookupRequest{
		FileIds: []string{"3,a", "3,missing"},
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got := len(resp.PeersByFid["3,a"].Peers); got != 1 {
		t.Errorf("3,a: expected 1 holder, got %d", got)
	}
	if _, ok := resp.PeersByFid["3,missing"]; !ok {
		t.Errorf("3,missing should have a (nil/empty) peer set entry")
	}
}

func TestPeerGrpcServer_OwnerMismatch(t *testing.T) {
	client, cleanup := newTestPeerGrpc(t, nil, func(fid string) string {
		return "some-other:18080"
	}, "self:18080")
	defer cleanup()

	ann, err := client.ChunkAnnounce(context.Background(), &mount_peer_pb.ChunkAnnounceRequest{
		FileIds:    []string{"3,x"},
		PeerAddr:   "holder:18080",
		TtlSeconds: 60,
	})
	if err != nil {
		t.Fatalf("announce: %v", err)
	}
	if len(ann.RejectedFileIds) != 1 || ann.RejectedFileIds[0] != "3,x" {
		t.Errorf("expected 3,x rejected, got %v", ann.RejectedFileIds)
	}
}

// TestPeerGrpcServer_FetchChunk_StreamsHit exercises the new byte-transfer
// path: a cached chunk returned as multiple gRPC frames, concatenated by
// the caller into the original payload.
func TestPeerGrpcServer_FetchChunk_StreamsHit(t *testing.T) {
	cache := newFakeChunkCache()
	// A payload deliberately larger than fetchChunkStreamSize to force
	// multiple Send() calls.
	payload := make([]byte, fetchChunkStreamSize*2+123)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}
	cache.Put("3,stream", payload)

	client, cleanup := newTestPeerGrpc(t, cache, nil, "self:18080")
	defer cleanup()

	stream, err := client.FetchChunk(context.Background(), &mount_peer_pb.FetchChunkRequest{FileId: "3,stream"})
	if err != nil {
		t.Fatalf("FetchChunk: %v", err)
	}
	var got []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		got = append(got, resp.Data...)
	}
	if len(got) != len(payload) {
		t.Fatalf("got %d bytes, want %d", len(got), len(payload))
	}
	for i, b := range got {
		if b != payload[i] {
			t.Fatalf("byte mismatch at offset %d: got %02x want %02x", i, b, payload[i])
		}
	}
}

// TestPeerGrpcServer_FetchChunk_NotFound verifies that a miss returns
// gRPC NOT_FOUND so the client can distinguish miss from transport error.
func TestPeerGrpcServer_FetchChunk_NotFound(t *testing.T) {
	cache := newFakeChunkCache()
	client, cleanup := newTestPeerGrpc(t, cache, nil, "self:18080")
	defer cleanup()

	stream, err := client.FetchChunk(context.Background(), &mount_peer_pb.FetchChunkRequest{FileId: "3,missing"})
	if err != nil {
		t.Fatalf("FetchChunk dial: %v", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("expected error on missing fid, got nil")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", status.Code(err))
	}
}
