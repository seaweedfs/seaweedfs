package weed_server

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// TestBatchDelete_DeniesNonWhitelistedPeer verifies that BatchDelete refuses
// callers that fall outside the configured admin whitelist, matching the
// behaviour of the other destructive volume-server RPCs.
func TestBatchDelete_DeniesNonWhitelistedPeer(t *testing.T) {
	vs := &VolumeServer{
		guard: security.NewGuard([]string{"10.0.0.1"}, "", 0, "", 0),
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345},
	})

	resp, err := vs.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds:         []string{"1,deadbeef"},
		SkipCookieCheck: true,
	})
	if err == nil {
		t.Fatalf("expected PermissionDenied, got nil error (resp=%+v)", resp)
	}
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("expected codes.PermissionDenied, got %v (err=%v)", got, err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response even on auth failure")
	}
	if len(resp.Results) != 0 {
		t.Fatalf("expected no per-file results on auth failure, got %d", len(resp.Results))
	}
}
