package mount

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/mount_peer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// newTestFetchServer stands up a minimal MountPeer gRPC server that only
// serves FetchChunk out of a fakeChunkCache. Useful for exercising the
// fetcher side without a full WFS.
func newTestFetchServer(t *testing.T, cache *fakeChunkCache) (addr string, stop func()) {
	t.Helper()
	dir := NewPeerDirectory()
	srv := NewPeerGrpcServer(cache, dir, nil, "")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.listener = ln
	srv.grpcS = grpc.NewServer()
	mount_peer_pb.RegisterMountPeerServer(srv.grpcS, srv)
	go srv.grpcS.Serve(ln)
	return ln.Addr().String(), func() { srv.Stop() }
}

func etagOf(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func TestFetchChunkFromPeer_Hit(t *testing.T) {
	cache := newFakeChunkCache()
	payload := []byte("hello from peer stream")
	cache.Put("3,abc", payload)
	addr, stop := newTestFetchServer(t, cache)
	defer stop()

	dial := DefaultMountPeerDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	got, err := fetchChunkFromPeer(context.Background(), dial, addr, "3,abc", uint64(len(payload)), etagOf(payload))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("bytes mismatch: got %q want %q", got, payload)
	}
}

func TestFetchChunkFromPeer_EtagMismatch(t *testing.T) {
	cache := newFakeChunkCache()
	payload := []byte("unexpected bytes")
	cache.Put("3,abc", payload)
	addr, stop := newTestFetchServer(t, cache)
	defer stop()

	dial := DefaultMountPeerDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	_, err := fetchChunkFromPeer(context.Background(), dial, addr, "3,abc", 0, "not-the-real-etag")
	if err == nil {
		t.Fatalf("expected etag mismatch error, got nil")
	}
}

func TestFetchChunkFromPeer_NotFound(t *testing.T) {
	cache := newFakeChunkCache()
	addr, stop := newTestFetchServer(t, cache)
	defer stop()

	dial := DefaultMountPeerDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	_, err := fetchChunkFromPeer(context.Background(), dial, addr, "3,missing", 0, "")
	if err == nil {
		t.Errorf("expected error for missing fid, got nil")
	}
}

// TestSortHoldersByLocality verifies that same-rack peers sort ahead of
// same-DC-different-rack peers, which in turn sort ahead of cross-DC
// peers, and that LRU order is preserved within each bucket.
func TestSortHoldersByLocality(t *testing.T) {
	selfDC, selfRack := "dc1", "r1"
	// Input order mimics a server-side LRU list (newest first).
	holders := []peerHolder{
		{addr: "far-newest", dc: "dc2", rack: "r9"},        // bucket 2 (diff DC)
		{addr: "mid-newer", dc: "dc1", rack: "r2"},         // bucket 1 (same DC, diff rack)
		{addr: "local-newer", dc: "dc1", rack: "r1"},       // bucket 0 (same rack)
		{addr: "far-older", dc: "dc2", rack: "r9"},         // bucket 2
		{addr: "mid-older", dc: "dc1", rack: "r2"},         // bucket 1
		{addr: "local-older", dc: "dc1", rack: "r1"},       // bucket 0
		{addr: "unlabeled-older", dc: "", rack: ""},        // bucket 2 (unknown)
	}

	sortHoldersByLocality(holders, selfDC, selfRack)

	want := []string{
		"local-newer", "local-older",
		"mid-newer", "mid-older",
		"far-newest", "far-older", "unlabeled-older",
	}
	if len(holders) != len(want) {
		t.Fatalf("len got %d want %d", len(holders), len(want))
	}
	for i, w := range want {
		if holders[i].addr != w {
			t.Errorf("pos %d: got %q want %q (full: %+v)", i, holders[i].addr, w, holders)
		}
	}
}

// TestSortHoldersByLocality_NoSelfLabels — when the caller has no DC/rack
// labels, every peer falls to bucket 2 and the server-returned LRU order
// must pass through unchanged.
func TestSortHoldersByLocality_NoSelfLabels(t *testing.T) {
	holders := []peerHolder{
		{addr: "a", dc: "dc1", rack: "r1"},
		{addr: "b", dc: "dc2", rack: "r2"},
		{addr: "c", dc: "", rack: ""},
	}
	sortHoldersByLocality(holders, "", "")
	want := []string{"a", "b", "c"}
	for i, w := range want {
		if holders[i].addr != w {
			t.Errorf("pos %d: got %q want %q", i, holders[i].addr, w)
		}
	}
}

func TestFetchChunkFromPeer_MultiFrameChunkAssembledCorrectly(t *testing.T) {
	cache := newFakeChunkCache()
	// Just over 2× the stream frame size so we get at least three frames.
	payload := make([]byte, fetchChunkStreamSize*2+42)
	for i := range payload {
		payload[i] = byte((i * 7) & 0xff)
	}
	cache.Put("3,large", payload)
	addr, stop := newTestFetchServer(t, cache)
	defer stop()

	dial := DefaultMountPeerDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	got, err := fetchChunkFromPeer(context.Background(), dial, addr, "3,large", uint64(len(payload)), etagOf(payload))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("len got %d want %d", len(got), len(payload))
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("mismatch at offset %d", i)
		}
	}
}
