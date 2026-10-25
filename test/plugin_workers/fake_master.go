package pluginworkers

import (
	"context"
	"net"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"testing"
)

// MasterServer provides a stub master gRPC service for topology responses.
type MasterServer struct {
	master_pb.UnimplementedSeaweedServer

	t *testing.T

	server   *grpc.Server
	listener net.Listener
	address  string

	mu       sync.RWMutex
	response *master_pb.VolumeListResponse
}

// NewMasterServer starts a stub master server that serves the provided response.
func NewMasterServer(t *testing.T, response *master_pb.VolumeListResponse) *MasterServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen master: %v", err)
	}

	server := pb.NewGrpcServer()
	ms := &MasterServer{
		t:        t,
		server:   server,
		listener: listener,
		address:  listener.Addr().String(),
		response: response,
	}

	master_pb.RegisterSeaweedServer(server, ms)
	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		ms.Shutdown()
	})

	return ms
}

// Address returns the gRPC address of the master server.
func (m *MasterServer) Address() string {
	return m.address
}

// SetVolumeListResponse updates the response served by VolumeList.
func (m *MasterServer) SetVolumeListResponse(response *master_pb.VolumeListResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.response = response
}

// VolumeList returns the configured topology response.
func (m *MasterServer) VolumeList(ctx context.Context, req *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.response == nil {
		return &master_pb.VolumeListResponse{}, nil
	}
	return proto.Clone(m.response).(*master_pb.VolumeListResponse), nil
}

// Shutdown stops the master gRPC server.
func (m *MasterServer) Shutdown() {
	if m.server != nil {
		m.server.GracefulStop()
	}
	if m.listener != nil {
		_ = m.listener.Close()
	}
}
