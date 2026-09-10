package weed_server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type volumeCopyStatusServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	delegate       *VolumeServer
	failStatusCall int32
	statusErr      error
	statusCalls    atomic.Int32
}

func (s *volumeCopyStatusServer) VolumeStatus(ctx context.Context, req *volume_server_pb.VolumeStatusRequest) (*volume_server_pb.VolumeStatusResponse, error) {
	if s.statusCalls.Add(1) == s.failStatusCall {
		return nil, s.statusErr
	}
	return s.delegate.VolumeStatus(ctx, req)
}

func (s *volumeCopyStatusServer) ReadVolumeFileStatus(ctx context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	return s.delegate.ReadVolumeFileStatus(ctx, req)
}

func (s *volumeCopyStatusServer) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {
	return s.delegate.CopyFile(req, stream)
}

func newVolumeCopyTestStore(t *testing.T, dir string) *storage.Store {
	t.Helper()
	store := storage.NewStore(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"127.0.0.1", 0, 0, "", "test-store",
		[]string{dir}, []int32{10}, []util.MinFreeSpace{{}},
		dir, storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, [][]string{nil},
		0, stats.DefaultDiskIOProbeConfig(),
	)
	store.Locations[0].AvailableSpace.Store(^uint64(0))
	t.Cleanup(store.Close)
	return store
}

func runVolumeCopyWithStatusFailure(t *testing.T, failStatusCall int32) (error, *storage.Store) {
	t.Helper()
	const vid = needle.VolumeId(43)

	sourceStore := newVolumeCopyTestStore(t, t.TempDir())
	if err := sourceStore.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "", 0,
		needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add source volume: %v", err)
	}
	source := &volumeCopyStatusServer{
		delegate:       &VolumeServer{store: sourceStore},
		failStatusCall: failStatusCall,
		statusErr:      errors.New("source volume status unavailable"),
	}
	port := serveGrpc(t, func(server *grpc.Server) {
		volume_server_pb.RegisterVolumeServerServer(server, source)
	})

	targetStore := newVolumeCopyTestStore(t, t.TempDir())
	target := &VolumeServer{
		store:          targetStore,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	err := target.VolumeCopy(&volume_server_pb.VolumeCopyRequest{
		VolumeId:       uint32(vid),
		SourceDataNode: fmt.Sprintf("127.0.0.1:%d.%d", port-10000, port),
	}, &fakeVolumeCopyStream{})
	return err, targetStore
}

func TestVolumeCopyContinuesWhenInitialStatusUnavailable(t *testing.T) {
	err, targetStore := runVolumeCopyWithStatusFailure(t, 1)
	if err != nil {
		t.Fatalf("VolumeCopy should continue when the initial status is unavailable: %v", err)
	}
	if targetStore.GetVolume(43) == nil {
		t.Fatal("copied volume was not mounted")
	}
}

func TestVolumeCopyFailsWhenFinalStatusUnavailable(t *testing.T) {
	err, targetStore := runVolumeCopyWithStatusFailure(t, 2)
	if err == nil || !strings.Contains(err.Error(), "status after copy") {
		t.Fatalf("VolumeCopy error = %v, want final status error", err)
	}
	if targetStore.GetVolume(43) != nil {
		t.Fatal("volume was mounted after final status validation failed")
	}
	select {
	case message := <-targetStore.NewVolumesChan:
		t.Fatalf("volume was announced after final status validation failed: %+v", message)
	default:
	}
	dataBaseFileName := storage.VolumeFileName(targetStore.Locations[0].Directory, "", 43)
	indexBaseFileName := storage.VolumeFileName(targetStore.Locations[0].IdxDirectory, "", 43)
	for _, fileName := range []string{
		dataBaseFileName + ".dat",
		indexBaseFileName + ".idx",
		dataBaseFileName + ".vif",
		dataBaseFileName + ".note",
	} {
		if _, statErr := os.Stat(fileName); !os.IsNotExist(statErr) {
			t.Fatalf("copy artifact %s remains after final status validation failed: %v", fileName, statErr)
		}
	}
}

func TestVolumeCopyKeepsExistingReplicaWhenDestinationFull(t *testing.T) {
	const vid = needle.VolumeId(44)

	sourceStore := newVolumeCopyTestStore(t, t.TempDir())
	if err := sourceStore.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "", 0,
		needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add source volume: %v", err)
	}
	source := &volumeCopyStatusServer{
		delegate:       &VolumeServer{store: sourceStore},
		failStatusCall: 1,
		statusErr:      errors.New("source volume status unavailable"),
	}
	port := serveGrpc(t, func(server *grpc.Server) {
		volume_server_pb.RegisterVolumeServerServer(server, source)
	})

	targetStore := newVolumeCopyTestStore(t, t.TempDir())
	if err := targetStore.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "", 0,
		needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add target volume: %v", err)
	}
	targetStore.Locations[0].AvailableSpace.Store(0)

	target := &VolumeServer{
		store:          targetStore,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	err := target.VolumeCopy(&volume_server_pb.VolumeCopyRequest{
		VolumeId:       uint32(vid),
		SourceDataNode: fmt.Sprintf("127.0.0.1:%d.%d", port-10000, port),
	}, &fakeVolumeCopyStream{})
	if err == nil {
		t.Fatal("VolumeCopy should fail when no destination location is available")
	}
	if targetStore.GetVolume(vid) == nil {
		t.Fatal("existing replica was destroyed before a destination was reserved")
	}
}

// fakeVolumeCopyStream is a no-op VolumeServer_VolumeCopyServer; VolumeCopy
// errors out before sending anything in this test.
type fakeVolumeCopyStream struct {
	grpc.ServerStream
}

func (s *fakeVolumeCopyStream) Send(*volume_server_pb.VolumeCopyResponse) error { return nil }
func (s *fakeVolumeCopyStream) Context() context.Context                        { return context.Background() }
func (s *fakeVolumeCopyStream) SetHeader(metadata.MD) error                     { return nil }
func (s *fakeVolumeCopyStream) SendHeader(metadata.MD) error                    { return nil }
func (s *fakeVolumeCopyStream) SetTrailer(metadata.MD)                          {}
func (s *fakeVolumeCopyStream) SendMsg(any) error                               { return nil }
func (s *fakeVolumeCopyStream) RecvMsg(any) error                               { return nil }

// TestVolumeCopy_KeepsExistingReplicaWhenSourceUnreachable verifies the
// verify-before-destroy invariant: a pre-existing healthy local replica must
// NOT be deleted when the source cannot be reached. The pre-fix code deleted
// the destination up front (and, on retry, could lose the volume entirely);
// the fix defers the delete until the source ReadVolumeFileStatus succeeds.
func TestVolumeCopy_KeepsExistingReplicaWhenSourceUnreachable(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewStore(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"127.0.0.1", 0, 0, "", "test-store",
		[]string{dir}, []int32{10}, []util.MinFreeSpace{{}},
		dir, storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, [][]string{nil},
		0, stats.DiskIOProbeConfig{},
	)

	const vid = needle.VolumeId(42)
	if err := store.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("AddVolume: %v", err)
	}
	if store.GetVolume(vid) == nil {
		t.Fatalf("setup: volume %d should exist", vid)
	}

	vs := &VolumeServer{
		store:          store,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// 127.0.0.1:1 is unreachable, so ReadVolumeFileStatus on the source fails.
	req := &volume_server_pb.VolumeCopyRequest{
		VolumeId:       uint32(vid),
		SourceDataNode: "127.0.0.1:1",
	}
	err := vs.VolumeCopy(req, &fakeVolumeCopyStream{})
	if err == nil {
		t.Fatalf("VolumeCopy should fail when the source is unreachable")
	}

	if store.GetVolume(vid) == nil {
		t.Fatalf("existing replica %d was destroyed before the source was verified", vid)
	}
}
