package framework

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DialVolumeServer(t testing.TB, address string) (*grpc.ClientConn, volume_server_pb.VolumeServerClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial volume grpc %s: %v", address, err)
	}

	return conn, volume_server_pb.NewVolumeServerClient(conn)
}
