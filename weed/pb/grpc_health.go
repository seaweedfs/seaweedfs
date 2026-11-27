package pb

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// serviceHealth answers the standard grpc.health.v1.Health protocol for a
// SeaweedFS gRPC server, so a probe such as blackbox_exporter's grpc prober,
// grpc_health_probe or a Kubernetes grpc liveness probe can check a port with
// no SeaweedFS-specific client.
//
// It reads the server's registered services on each call instead of keeping a
// status map: services are registered after NewGrpcServer returns, and the
// answer then needs no wiring at each call site. The empty service name stands
// for the whole server, which is SERVING once it accepts the RPC.
type serviceHealth struct {
	healthpb.UnimplementedHealthServer
	server  *grpc.Server
	stopped chan struct{}
}

var healthServers sync.Map // *grpc.Server -> *serviceHealth

func registerHealthServer(server *grpc.Server) {
	h := &serviceHealth{server: server, stopped: make(chan struct{})}
	healthServers.Store(server, h)
	healthpb.RegisterHealthServer(server, h)
}

// StopHealthWatches ends the server's open health Watch streams. Call it
// before GracefulStop, which waits on open streams.
func StopHealthWatches(server *grpc.Server) {
	if h, ok := healthServers.LoadAndDelete(server); ok {
		close(h.(*serviceHealth).stopped)
	}
}

func (h *serviceHealth) status(service string) (healthpb.HealthCheckResponse_ServingStatus, bool) {
	if service == "" {
		return healthpb.HealthCheckResponse_SERVING, true
	}
	if _, ok := h.server.GetServiceInfo()[service]; ok {
		return healthpb.HealthCheckResponse_SERVING, true
	}
	return healthpb.HealthCheckResponse_SERVICE_UNKNOWN, false
}

func (h *serviceHealth) Check(_ context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	st, ok := h.status(req.GetService())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown service %q", req.GetService())
	}
	return &healthpb.HealthCheckResponse{Status: st}, nil
}

func (h *serviceHealth) List(_ context.Context, _ *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	statuses := map[string]*healthpb.HealthCheckResponse{
		"": {Status: healthpb.HealthCheckResponse_SERVING},
	}
	for name := range h.server.GetServiceInfo() {
		statuses[name] = &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}
	}
	return &healthpb.HealthListResponse{Statuses: statuses}, nil
}

// Watch sends the current status and holds the stream open, as the protocol
// asks. Services do not come and go on a running server, so there is nothing
// further to send; the stream ends when the client goes away or
// StopHealthWatches is called.
func (h *serviceHealth) Watch(req *healthpb.HealthCheckRequest, stream grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	st, _ := h.status(req.GetService())
	if err := stream.Send(&healthpb.HealthCheckResponse{Status: st}); err != nil {
		return err
	}
	select {
	case <-stream.Context().Done():
	case <-h.stopped:
	}
	return nil
}
