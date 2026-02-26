package pluginworkers

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
)

// VolumeServer provides a minimal volume server for erasure coding tests.
type VolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer

	t *testing.T

	server   *grpc.Server
	listener net.Listener
	address  string
	baseDir  string

	mu                sync.Mutex
	receivedFiles     map[string]uint64
	mountRequests     []*volume_server_pb.VolumeEcShardsMountRequest
	deleteRequests    []*volume_server_pb.VolumeDeleteRequest
	markReadonlyCalls int
}

// NewVolumeServer starts a test volume server using the provided base directory.
func NewVolumeServer(t *testing.T, baseDir string) *VolumeServer {
	t.Helper()

	if baseDir == "" {
		baseDir = t.TempDir()
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("create volume base dir: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen volume server: %v", err)
	}

	grpcPort := listener.Addr().(*net.TCPAddr).Port
	server := pb.NewGrpcServer()
	vs := &VolumeServer{
		t:             t,
		server:        server,
		listener:      listener,
		address:       fmt.Sprintf("127.0.0.1:0.%d", grpcPort),
		baseDir:       baseDir,
		receivedFiles: make(map[string]uint64),
	}

	volume_server_pb.RegisterVolumeServerServer(server, vs)
	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		vs.Shutdown()
	})

	return vs
}

// Address returns the gRPC address of the volume server.
func (v *VolumeServer) Address() string {
	return v.address
}

// BaseDir returns the base directory used by the server.
func (v *VolumeServer) BaseDir() string {
	return v.baseDir
}

// ReceivedFiles returns a snapshot of received files and byte counts.
func (v *VolumeServer) ReceivedFiles() map[string]uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()

	out := make(map[string]uint64, len(v.receivedFiles))
	for key, value := range v.receivedFiles {
		out[key] = value
	}
	return out
}

// MountRequests returns recorded mount requests.
func (v *VolumeServer) MountRequests() []*volume_server_pb.VolumeEcShardsMountRequest {
	v.mu.Lock()
	defer v.mu.Unlock()

	out := make([]*volume_server_pb.VolumeEcShardsMountRequest, len(v.mountRequests))
	copy(out, v.mountRequests)
	return out
}

// DeleteRequests returns recorded delete requests.
func (v *VolumeServer) DeleteRequests() []*volume_server_pb.VolumeDeleteRequest {
	v.mu.Lock()
	defer v.mu.Unlock()

	out := make([]*volume_server_pb.VolumeDeleteRequest, len(v.deleteRequests))
	copy(out, v.deleteRequests)
	return out
}

// MarkReadonlyCount returns the number of readonly calls.
func (v *VolumeServer) MarkReadonlyCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.markReadonlyCalls
}

// Shutdown stops the volume server.
func (v *VolumeServer) Shutdown() {
	if v.server != nil {
		v.server.GracefulStop()
	}
	if v.listener != nil {
		_ = v.listener.Close()
	}
}

func (v *VolumeServer) filePath(volumeID uint32, ext string) string {
	return filepath.Join(v.baseDir, fmt.Sprintf("%d%s", volumeID, ext))
}

func (v *VolumeServer) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {
	if req == nil {
		return fmt.Errorf("copy file request is nil")
	}
	path := v.filePath(req.VolumeId, req.Ext)
	file, err := os.Open(path)
	if err != nil {
		if req.IgnoreSourceFileNotFound {
			return nil
		}
		return err
	}
	defer file.Close()

	buf := make([]byte, 64*1024)
	for {
		n, readErr := file.Read(buf)
		if n > 0 {
			if err := stream.Send(&volume_server_pb.CopyFileResponse{FileContent: buf[:n]}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func (v *VolumeServer) ReceiveFile(stream volume_server_pb.VolumeServer_ReceiveFileServer) error {
	var (
		info         *volume_server_pb.ReceiveFileInfo
		file         *os.File
		bytesWritten uint64
		filePath     string
	)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if file != nil {
				_ = file.Close()
			}
			if info == nil {
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{Error: "missing file info"})
			}
			v.mu.Lock()
			v.receivedFiles[filePath] = bytesWritten
			v.mu.Unlock()
			return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{BytesWritten: bytesWritten})
		}
		if err != nil {
			return err
		}

		if reqInfo := req.GetInfo(); reqInfo != nil {
			info = reqInfo
			filePath = v.filePath(info.VolumeId, info.Ext)
			if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
				return err
			}
			file, err = os.Create(filePath)
			if err != nil {
				return err
			}
			continue
		}

		chunk := req.GetFileContent()
		if len(chunk) == 0 {
			continue
		}
		if file == nil {
			return fmt.Errorf("file info not received")
		}
		n, writeErr := file.Write(chunk)
		if writeErr != nil {
			return writeErr
		}
		bytesWritten += uint64(n)
	}
}

func (v *VolumeServer) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {
	v.mu.Lock()
	v.mountRequests = append(v.mountRequests, req)
	v.mu.Unlock()
	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (v *VolumeServer) VolumeDelete(ctx context.Context, req *volume_server_pb.VolumeDeleteRequest) (*volume_server_pb.VolumeDeleteResponse, error) {
	v.mu.Lock()
	v.deleteRequests = append(v.deleteRequests, req)
	v.mu.Unlock()

	if req != nil {
		_ = os.Remove(v.filePath(req.VolumeId, ".dat"))
		_ = os.Remove(v.filePath(req.VolumeId, ".idx"))
	}

	return &volume_server_pb.VolumeDeleteResponse{}, nil
}

func (v *VolumeServer) VolumeMarkReadonly(ctx context.Context, req *volume_server_pb.VolumeMarkReadonlyRequest) (*volume_server_pb.VolumeMarkReadonlyResponse, error) {
	v.mu.Lock()
	v.markReadonlyCalls++
	v.mu.Unlock()
	return &volume_server_pb.VolumeMarkReadonlyResponse{}, nil
}
