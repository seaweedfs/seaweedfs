package erasure_coding

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeCopySource is a stub VolumeServer. Rejecting a pinned .dat copy advances
// the compaction revision and rewrites the volume contents, so a mid-copy
// compaction is replayed deterministically.
type fakeCopySource struct {
	volume_server_pb.UnimplementedVolumeServerServer

	mu          sync.Mutex
	revision    uint32
	datBytes    []byte
	idxBytes    []byte
	expireAtSec uint64
	rejectDat   int
	failErr     error
	statusCalls int
	copyCalls   int
}

func (f *fakeCopySource) snapshot() (revision uint32, expireAtSec uint64, statusCalls, copyCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.revision, f.expireAtSec, f.statusCalls, f.copyCalls
}

func (f *fakeCopySource) ReadVolumeFileStatus(context.Context, *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusCalls++
	return &volume_server_pb.ReadVolumeFileStatusResponse{
		CompactionRevision: f.revision,
		DatFileSize:        uint64(len(f.datBytes)),
		IdxFileSize:        uint64(len(f.idxBytes)),
		ExpireAtSec:        f.expireAtSec,
	}, nil
}

func (f *fakeCopySource) CopyFile(req *volume_server_pb.CopyFileRequest, stream grpc.ServerStreamingServer[volume_server_pb.CopyFileResponse]) error {
	f.mu.Lock()
	f.copyCalls++
	reject := req.GetExt() == ".dat" && f.rejectDat > 0
	if reject {
		f.rejectDat--
		f.revision++
		f.datBytes = append(f.datBytes, 0xEC)
		f.idxBytes = append(f.idxBytes, 0xEC)
		f.expireAtSec += 100
	}
	failErr := f.failErr
	revision := f.revision
	var content []byte
	switch req.GetExt() {
	case ".idx":
		content = f.idxBytes
	case ".dat":
		content = f.datBytes
	}
	f.mu.Unlock()

	if failErr != nil {
		return failErr
	}
	if reject || req.GetCompactionRevision() != revision {
		return fmt.Errorf("volume %d is compacted", req.GetVolumeId())
	}
	content = content[:req.GetStopOffset()]
	for len(content) > 0 {
		n := min(len(content), 4)
		if err := stream.Send(&volume_server_pb.CopyFileResponse{FileContent: content[:n]}); err != nil {
			return err
		}
		content = content[n:]
	}
	return nil
}

func startFakeCopySource(t *testing.T, src *fakeCopySource) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(grpcServer, src)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	// server addresses carry httpPort.grpcPort; the gRPC dialer uses the part
	// after the dot, so report the listening port that way
	_, port, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	return "127.0.0.1:0." + port
}

func newCopyRetryTask(addr string) *ErasureCodingTask {
	return NewErasureCodingTask("copy-retry", addr, 951, "", grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func TestCopyVolumeFilesToWorkerRetriesAfterMidCopyCompaction(t *testing.T) {
	src := &fakeCopySource{
		revision:    7,
		idxBytes:    []byte("idx-data-v7"),
		datBytes:    []byte("dat-payload-v7"),
		expireAtSec: 1000,
		rejectDat:   1,
	}
	task := newCopyRetryTask(startFakeCopySource(t, src))

	localFiles, err := task.copyVolumeFilesToWorker(context.Background(), t.TempDir())
	require.NoError(t, err)

	datContent, err := os.ReadFile(localFiles["dat"])
	require.NoError(t, err)
	require.Equal(t, []byte("dat-payload-v7\xEC"), datContent)
	idxContent, err := os.ReadFile(localFiles["idx"])
	require.NoError(t, err)
	require.Equal(t, []byte("idx-data-v7\xEC"), idxContent)
	revision, expireAtSec, statusCalls, copyCalls := src.snapshot()
	require.Equal(t, uint32(8), revision)
	require.Equal(t, 2, statusCalls)
	require.Equal(t, 4, copyCalls)
	require.Equal(t, expireAtSec, task.expireAtSec)
}

func TestCopyVolumeFilesToWorkerStopsAfterRetryLimit(t *testing.T) {
	src := &fakeCopySource{
		revision:  1,
		idxBytes:  []byte("i"),
		datBytes:  []byte("d"),
		rejectDat: 10,
	}
	task := newCopyRetryTask(startFakeCopySource(t, src))

	_, err := task.copyVolumeFilesToWorker(context.Background(), t.TempDir())
	require.ErrorContains(t, err, "is compacted")
	_, _, statusCalls, copyCalls := src.snapshot()
	require.Equal(t, ecCopyAttempts, statusCalls)
	require.Equal(t, 2*ecCopyAttempts, copyCalls)
}

func TestCopyVolumeFilesToWorkerReturnsNonCompactionErrors(t *testing.T) {
	src := &fakeCopySource{
		revision: 1,
		idxBytes: []byte("i"),
		datBytes: []byte("d"),
		failErr:  fmt.Errorf("boom"),
	}
	task := newCopyRetryTask(startFakeCopySource(t, src))

	_, err := task.copyVolumeFilesToWorker(context.Background(), t.TempDir())
	require.ErrorContains(t, err, "boom")
	_, _, statusCalls, copyCalls := src.snapshot()
	require.Equal(t, 1, statusCalls)
	require.Equal(t, 1, copyCalls)
}
