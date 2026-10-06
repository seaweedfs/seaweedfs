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

type fakeKeepConnectedServer struct {
	master_pb.UnimplementedSeaweedServer
	requests chan *master_pb.KeepConnectedRequest
	// closeAfterLeave ends the stream once a leave arrives, forcing a reconnect.
	closeAfterLeave bool
}

func (s *fakeKeepConnectedServer) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {
	if err := stream.Send(&master_pb.KeepConnectedResponse{VolumeLocation: &master_pb.VolumeLocation{}}); err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		s.requests <- req
		if req.LeaveLockRing && req.ClientType == "" {
			if s.closeAfterLeave {
				return nil
			}
			if err := stream.Send(&master_pb.KeepConnectedResponse{LockRingUpdate: &master_pb.LockRingUpdate{Servers: []string{"filer2:18888"}, Version: 2}}); err != nil {
				return err
			}
		}
	}
}

func nextRequest(t *testing.T, requests <-chan *master_pb.KeepConnectedRequest) *master_pb.KeepConnectedRequest {
	t.Helper()
	select {
	case req := <-requests:
		return req
	case <-time.After(5 * time.Second):
		t.Fatal("master received no KeepConnected message")
		return nil
	}
}

func startLeaveTestClient(t *testing.T, srv *fakeKeepConnectedServer, onLockRingUpdate func(*master_pb.LockRingUpdate)) *MasterClient {
	t.Helper()
	addr := startFakeMasterServer(t, srv)
	mc := NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"", "filer", "filer1:18888", "", "",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"m": addr}),
	)
	mc.SetOnLockRingUpdateFn(onLockRingUpdate)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go mc.KeepConnectedToMaster(ctx)
	return mc
}

func TestLeaveLockRingIsSentOnTheOpenStream(t *testing.T) {
	srv := &fakeKeepConnectedServer{requests: make(chan *master_pb.KeepConnectedRequest, 4)}
	ringUpdates := make(chan *master_pb.LockRingUpdate, 1)
	mc := startLeaveTestClient(t, srv, func(update *master_pb.LockRingUpdate) { ringUpdates <- update })

	if first := nextRequest(t, srv.requests); first.LeaveLockRing {
		t.Fatal("registration asked to leave the lock ring before LeaveLockRing")
	}
	if err := mc.LeaveLockRing(); err != nil {
		t.Fatalf("LeaveLockRing: %v", err)
	}
	if leave := nextRequest(t, srv.requests); !leave.LeaveLockRing {
		t.Fatalf("expected a leave message, got %+v", leave)
	}
	select {
	case update := <-ringUpdates:
		if update.Version != 2 {
			t.Fatalf("unexpected ring update %+v", update)
		}
	case req := <-srv.requests:
		t.Fatalf("leaving reconnected or re-registered: %+v", req)
	case <-time.After(5 * time.Second):
		t.Fatal("the stream stopped delivering ring updates after leaving")
	}
}

func TestReconnectAfterLeaveStaysOutOfLockRing(t *testing.T) {
	srv := &fakeKeepConnectedServer{requests: make(chan *master_pb.KeepConnectedRequest, 4), closeAfterLeave: true}
	mc := startLeaveTestClient(t, srv, nil)

	nextRequest(t, srv.requests)
	if err := mc.LeaveLockRing(); err != nil {
		t.Fatalf("LeaveLockRing: %v", err)
	}
	nextRequest(t, srv.requests)

	reconnect := nextRequest(t, srv.requests)
	if reconnect.ClientType != "filer" || !reconnect.LeaveLockRing {
		t.Fatalf("reconnect registration must stay out of the lock ring, got %+v", reconnect)
	}
}
