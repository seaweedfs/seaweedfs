package dash

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestVectorNodeStatus(t *testing.T) {
	for _, want := range []string{"Ready", "Not ready", "Registered (health unavailable)", "Unknown service"} {
		t.Run(want, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			if want != "Registered (health unavailable)" {
				checker := health.NewServer()
				state := grpc_health_v1.HealthCheckResponse_NOT_SERVING
				if want == "Ready" {
					state = grpc_health_v1.HealthCheckResponse_SERVING
				}
				if want != "Unknown service" {
					checker.SetServingStatus("vector_pb.VectorService", state)
				}
				grpc_health_v1.RegisterHealthServer(server, checker)
			}
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			expected := want
			if want == "Unknown service" {
				expected = "Registered (health unavailable)"
			}
			if got := vectorNodeStatus(t.Context(), listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials())); got != expected {
				t.Fatalf("got %s want %s", got, want)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got := vectorNodeStatus(ctx, "127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials())); got != "Unreachable" {
		t.Fatal(got)
	}
}
