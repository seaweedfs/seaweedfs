package s3_lifecycle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lifecycleAuthFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	key         security.SigningKey
	authResults chan bool
	configCalls atomic.Int32
}

func (f *lifecycleAuthFiler) GetFilerConfiguration(ctx context.Context, _ *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	f.configCalls.Add(1)
	err := security.CheckGrpcAdminToken(ctx, f.key)
	f.authResults <- err == nil
	if err != nil {
		return nil, err
	}
	return &filer_pb.GetFilerConfigurationResponse{DirBuckets: "/buckets"}, nil
}

func lifecycleTestSigningKey(t *testing.T) security.SigningKey {
	t.Helper()
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal(err)
	}
	return security.SigningKey(hex.EncodeToString(bytes))
}

func TestDialFilerUsesPerCallAdminToken(t *testing.T) {
	key := lifecycleTestSigningKey(t)
	wrongKey := lifecycleTestSigningKey(t)
	cases := []struct {
		name      string
		workerKey string
		wantErr   bool
	}{
		{name: "accepted key", workerKey: string(key)},
		{name: "missing key", workerKey: "", wantErr: true},
		{name: "wrong key", workerKey: string(wrongKey), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WEED_JWT_FILER_SIGNING_GRPC_KEY", tc.workerKey)
			t.Setenv("WEED_JWT_FILER_SIGNING_KEY", "")

			filer := &lifecycleAuthFiler{key: key, authResults: make(chan bool, 1)}
			address := serveGrpc(t, func(server *grpc.Server) {
				filer_pb.RegisterSeaweedFilerServer(server, filer)
			})
			h := newTestHandler()
			conn, client, err := h.dialFiler(context.Background(), address)
			if err != nil {
				t.Fatalf("dialFiler: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			_, lookupErr := lookupBucketsPath(context.Background(), client)
			if tc.wantErr {
				if status.Code(lookupErr) != codes.Unauthenticated {
					t.Fatalf("lookupBucketsPath error code = %s, want Unauthenticated", status.Code(lookupErr))
				}
			} else if lookupErr != nil {
				t.Fatalf("lookupBucketsPath: %v", lookupErr)
			}
			select {
			case valid := <-filer.authResults:
				if valid != !tc.wantErr {
					t.Fatalf("filer authentication result did not match case")
				}
			case <-time.After(time.Second):
				t.Fatal("filer did not receive the configuration request")
			}
			if got := filer.configCalls.Load(); got != 1 {
				t.Fatalf("configuration RPC count = %d, want 1", got)
			}
		})
	}
}
