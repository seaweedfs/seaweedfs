package s3vectors

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func healthServer(t *testing.T, services ...string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	h := health.NewServer()
	for _, service := range services {
		h.SetServingStatus(service, grpc_health_v1.HealthCheckResponse_SERVING)
	}
	grpc_health_v1.RegisterHealthServer(server, h)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func TestCheckMetadataColumnsNeedsEveryEngine(t *testing.T) {
	dial := grpc.WithTransportCredentials(insecure.NewCredentials())
	capable := healthServer(t, "vector_pb.VectorService", MetadataColumnsCapability)
	old := healthServer(t, "vector_pb.VectorService")
	ctx := context.Background()
	if err := checkMetadataColumns(ctx, []string{capable}, dial); err != nil {
		t.Fatalf("a capable engine: %v", err)
	}
	err := checkMetadataColumns(ctx, []string{capable, old}, dial)
	if err == nil || !strings.Contains(err.Error(), old) || strings.Contains(err.Error(), capable+" ") {
		t.Fatalf("an old engine must be named: %v", err)
	}
	if err := checkMetadataColumns(ctx, nil, dial); err == nil {
		t.Fatal("no engine must refuse")
	}
}

func TestMetadataKeyIndexable(t *testing.T) {
	idx := &Index{IndexName: "i", MetadataConfiguration: &MetadataConfiguration{NonFilterableMetadataKeys: []string{"body"}}}
	if err := metadataKeyIndexable(idx, "author"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"body", "", " "} {
		if metadataKeyIndexable(idx, key) == nil {
			t.Fatalf("%q must be refused", key)
		}
	}
	if err := metadataKeyIndexable(&Index{}, "author"); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataKeysWithoutAnEngine(t *testing.T) {
	s := &Server{}
	if s.hasEngine() {
		t.Fatal("a server without a client has an engine")
	}
	s.vectors = &describedClient{}
	if s.hasEngine() {
		t.Fatal("a describe wrapper around no client has an engine")
	}
}
