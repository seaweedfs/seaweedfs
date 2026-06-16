package weed_server

import (
	"context"
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
