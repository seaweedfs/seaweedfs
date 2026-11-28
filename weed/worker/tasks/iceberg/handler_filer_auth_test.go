package iceberg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type icebergAuthFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	key         security.SigningKey
	authResults chan bool
}

func (f *icebergAuthFiler) Ping(ctx context.Context, _ *filer_pb.PingRequest) (*filer_pb.PingResponse, error) {
	err := security.CheckGrpcAdminToken(ctx, f.key)
	f.authResults <- err == nil
	if err != nil {
		return nil, err
	}
	return &filer_pb.PingResponse{}, nil
}

func icebergTestSigningKey(t *testing.T) security.SigningKey {
	t.Helper()
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal(err)
	}
	return security.SigningKey(hex.EncodeToString(bytes))
}

func TestDialFilerUsesPerCallAdminToken(t *testing.T) {
	key := icebergTestSigningKey(t)
	wrongKey := icebergTestSigningKey(t)
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

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			filer := &icebergAuthFiler{key: key, authResults: make(chan bool, 1)}
			server := grpc.NewServer()
			filer_pb.RegisterSeaweedFilerServer(server, filer)
			go server.Serve(listener)
			t.Cleanup(func() {
				server.Stop()
				_ = listener.Close()
			})

			conn, err := NewHandler(grpc.WithTransportCredentials(insecure.NewCredentials())).dialFiler(context.Background(), listener.Addr().String())
			if tc.wantErr {
				if status.Code(err) != codes.Unauthenticated {
					t.Fatalf("dialFiler error code = %s, want Unauthenticated", status.Code(err))
				}
			} else {
				if err != nil {
					t.Fatalf("dialFiler: %v", err)
				}
				t.Cleanup(func() { _ = conn.Close() })
			}
			select {
			case valid := <-filer.authResults:
				if valid != !tc.wantErr {
					t.Fatalf("filer authentication result did not match case")
				}
			case <-time.After(time.Second):
				t.Fatal("filer did not receive the Ping request")
			}
		})
	}
}
