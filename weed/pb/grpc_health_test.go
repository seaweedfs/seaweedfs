package pb

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// Every server built by NewGrpcServer answers grpc.health.v1, for the whole
// server and for each service registered after NewGrpcServer returned.
func TestNewGrpcServerServesHealth(t *testing.T) {
	server := NewGrpcServer()
	master_pb.RegisterSeaweedServer(server, master_pb.UnimplementedSeaweedServer{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(lis)
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, service := range []string{"", "master_pb.Seaweed", "grpc.health.v1.Health"} {
		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatalf("Check(%q): %v", service, err)
		}
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("Check(%q) = %v, want SERVING", service, resp.Status)
		}
	}

	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "volume_server_pb.VolumeServer"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Check(unregistered) err = %v, want NotFound", err)
	}

	list, err := client.List(ctx, &healthpb.HealthListRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, service := range []string{"", "master_pb.Seaweed"} {
		if list.Statuses[service].GetStatus() != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("List[%q] = %v, want SERVING", service, list.Statuses[service])
		}
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	for service, want := range map[string]healthpb.HealthCheckResponse_ServingStatus{
		"master_pb.Seaweed": healthpb.HealthCheckResponse_SERVING,
		"no.Such":           healthpb.HealthCheckResponse_SERVICE_UNKNOWN,
	} {
		stream, err := client.Watch(watchCtx, &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatalf("Watch(%q): %v", service, err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Watch(%q) Recv: %v", service, err)
		}
		if resp.Status != want {
			t.Fatalf("Watch(%q) = %v, want %v", service, resp.Status, want)
		}
	}
}
