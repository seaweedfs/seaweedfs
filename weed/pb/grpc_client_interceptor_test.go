package pb

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func healthClient(t *testing.T) grpc_health_v1.HealthClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	conn, err := GrpcDial(context.Background(), listener.Addr().String(), false, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return grpc_health_v1.NewHealthClient(conn)
}

func check(t *testing.T, client grpc_health_v1.HealthClient) error {
	t.Helper()
	_, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	return err
}

func TestClientUnaryInterceptors(t *testing.T) {
	previous := clientUnaryInterceptors.Load()
	t.Cleanup(func() { clientUnaryInterceptors.Store(previous) })
	clientUnaryInterceptors.Store(nil)

	// Dialed before any interceptor exists.
	client := healthClient(t)
	if err := check(t, client); err != nil {
		t.Fatalf("call without interceptors: %v", err)
	}

	var seen []string
	record := func(name string) grpc.UnaryClientInterceptor {
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			seen = append(seen, name+" "+method)
			err := invoker(ctx, method, req, reply, cc, opts...)
			seen = append(seen, name+" done")
			return err
		}
	}
	AddClientUnaryInterceptor(record("first"))
	AddClientUnaryInterceptor(record("second"))

	// The connection opened earlier picks them up, in order.
	if err := check(t, client); err != nil {
		t.Fatalf("call with interceptors: %v", err)
	}
	want := []string{
		"first /grpc.health.v1.Health/Check",
		"second /grpc.health.v1.Health/Check",
		"second done",
		"first done",
	}
	if len(seen) != len(want) {
		t.Fatalf("seen = %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("seen = %v, want %v", seen, want)
		}
	}

	// An interceptor may refuse the call without making it.
	refusal := context.DeadlineExceeded
	AddClientUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
		return refusal
	})
	if err := check(t, client); err != refusal {
		t.Fatalf("refused call returned %v", err)
	}
}
