package wdclient

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A filer connects to the master before its gRPC port serves so it can query
// masters during startup; the registration send that adds it to the master's
// filer list must wait until it is actually serving.
func TestAnnounceGateDelaysRegistration(t *testing.T) {
	srv := &fakeKeepConnectedServer{requests: make(chan *master_pb.KeepConnectedRequest, 4)}
	addr := startFakeMasterServer(t, srv)
	mc := NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"", "filer", "filer1:18888", "", "",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"m": addr}),
	)
	gate := make(chan struct{})
	mc.SetAnnounceCh(gate)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go mc.KeepConnectedToMaster(ctx)

	// WaitUntilConnected returns once the stream is established, before any
	// registration: queries (e.g. loading a chunked filer.conf during
	// startup) must not block behind the gate.
	mc.WaitUntilConnected(ctx)
	got := make(chan pb.ServerAddress, 1)
	go func() { got <- mc.GetMaster(ctx) }()
	select {
	case m := <-got:
		if m != addr {
			t.Fatalf("GetMaster = %q, want %q", m, addr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GetMaster blocked behind the announce gate")
	}

	// Registration must not have been sent yet.
	select {
	case req := <-srv.requests:
		t.Fatalf("client registered before the gate opened: %+v", req)
	default:
	}

	close(gate)
	req := nextRequest(t, srv.requests)
	if req.ClientAddress != "filer1:18888" {
		t.Fatalf("registration ClientAddress = %q, want filer1:18888", req.ClientAddress)
	}
}
