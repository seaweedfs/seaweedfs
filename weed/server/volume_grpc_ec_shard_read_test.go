package weed_server

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type ecShardReadRecorder struct {
	grpc.ServerStream
	responses []*volume_server_pb.VolumeEcShardReadResponse
}

func (r *ecShardReadRecorder) Send(m *volume_server_pb.VolumeEcShardReadResponse) error {
	r.responses = append(r.responses, m)
	return nil
}

// ecVolumeWithLiveNeedle mounts shard 0 of an EC volume whose .ecx holds one
// live needle, and returns the server and the .ecx path.
func ecVolumeWithLiveNeedle(t *testing.T, vid needle.VolumeId, id types.NeedleId) (*VolumeServer, string) {
	t.Helper()
	dir := t.TempDir()
	base := filepath.Join(dir, vid.String())

	entry := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	types.NeedleIdToBytes(entry[0:types.NeedleIdSize], id)
	types.OffsetToBytes(entry[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], types.Offset{})
	types.SizeToBytes(entry[types.NeedleIdSize+types.OffsetSize:], types.Size(100))

	for name, content := range map[string][]byte{
		base + ".ecx":  entry,
		base + ".ecj":  nil,
		base + ".ec00": make([]byte, 8),
		base + ".vif":  {},
	} {
		if err := os.WriteFile(name, content, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	location := storage.NewDiskLocation(dir, 100, util.MinFreeSpace{}, dir, types.HardDriveType, nil, stats.DiskIOProbeConfig{})
	t.Cleanup(location.Close)
	if _, err := location.LoadEcShard("", vid, erasure_coding.ShardId(0)); err != nil {
		t.Fatalf("load ec shard: %v", err)
	}
	return &VolumeServer{store: &storage.Store{Locations: []*storage.DiskLocation{location}}}, base + ".ecx"
}

// Peers move on to the next holder or reconstruct when a shard sits on a
// quarantined disk.
func TestVolumeEcShardRead_RefusesQuarantinedShard(t *testing.T) {
	const vid = needle.VolumeId(7)
	vs, _ := ecVolumeWithLiveNeedle(t, vid, 1)
	req := &volume_server_pb.VolumeEcShardReadRequest{VolumeId: uint32(vid), ShardId: 0, Offset: 0, Size: 8, FileKey: 1}

	if err := vs.VolumeEcShardRead(req, &ecShardReadRecorder{}); err != nil {
		t.Fatalf("read from a healthy shard: %v", err)
	}

	ecVolume, _ := vs.store.FindEcVolume(vid)
	ecVolume.MarkIoQuarantined()
	recorder := &ecShardReadRecorder{}
	if err := vs.VolumeEcShardRead(req, recorder); err == nil {
		t.Error("a quarantined shard was served")
	}
	if len(recorder.responses) != 0 {
		t.Errorf("sent %d responses for a quarantined shard, want 0", len(recorder.responses))
	}
}

// An .ecx that cannot be read must fail the request: answering "deleted" would
// make the caller drop a live needle.
func TestVolumeEcShardRead_EcxReadErrorIsNotADelete(t *testing.T) {
	const vid = needle.VolumeId(7)
	vs, ecxPath := ecVolumeWithLiveNeedle(t, vid, 1)
	if err := os.Truncate(ecxPath, 0); err != nil {
		t.Fatalf("truncate .ecx: %v", err)
	}

	recorder := &ecShardReadRecorder{}
	err := vs.VolumeEcShardRead(&volume_server_pb.VolumeEcShardReadRequest{VolumeId: uint32(vid), ShardId: 0, Offset: 0, Size: 8, FileKey: 1}, recorder)
	for _, resp := range recorder.responses {
		if resp.IsDeleted {
			t.Fatal("an unreadable .ecx was reported as a deleted needle")
		}
	}
	if err == nil {
		t.Error("an unreadable .ecx did not fail the read")
	}
}
