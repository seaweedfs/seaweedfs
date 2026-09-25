package shell

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type bucketCreateTestFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer

	createReqs []*filer_pb.CreateEntryRequest
}

func (s *bucketCreateTestFilerServer) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{DirBuckets: "/buckets"}, nil
}

func (s *bucketCreateTestFilerServer) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.createReqs = append(s.createReqs, req)
	return &filer_pb.CreateEntryResponse{
		ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS,
		Error:     "entry already exists",
	}, nil
}

func TestS3BucketCreateRequestsExclusiveCreate(t *testing.T) {
	filerServer := &bucketCreateTestFilerServer{}
	commandEnv, cleanup := newBucketCreateTestCommandEnv(t, filerServer)
	defer cleanup()

	var output bytes.Buffer
	err := (&commandS3BucketCreate{}).Do([]string{"-name", "my-bucket"}, commandEnv, &output)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	require.Len(t, filerServer.createReqs, 1)
	req := filerServer.createReqs[0]
	assert.True(t, req.OExcl, "bucket create must not replace an existing entry")
	assert.Equal(t, "/buckets", req.Directory)
	assert.Equal(t, "my-bucket", req.Entry.Name)
}

func newBucketCreateTestCommandEnv(t *testing.T, filerServer filer_pb.SeaweedFilerServer) (*CommandEnv, func()) {
	t.Helper()

	socketDir, err := os.MkdirTemp("", "swbucket-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })

	socketPath := filepath.Join(socketDir, "filer.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, filerServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	grpcPort := 48000 + os.Getpid()%1000
	pb.RegisterLocalGrpcSocket("127.0.0.1", grpcPort, socketPath)

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
	}

	return &CommandEnv{
		option: &ShellOptions{
			FilerAddress:   pb.ServerAddress(fmt.Sprintf("127.0.0.1:8888.%d", grpcPort)),
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			Directory:      "/",
		},
	}, cleanup
}
