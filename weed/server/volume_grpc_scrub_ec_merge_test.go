package weed_server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMultiDiskEcScrubStore builds a store with one DiskLocation per dir, so a
// volume whose shards are split across dirs mounts as one EcVolume per disk.
func newMultiDiskEcScrubStore(t *testing.T, dirs ...string) *storage.Store {
	t.Helper()
	max := make([]int32, len(dirs))
	minFree := make([]util.MinFreeSpace, len(dirs))
	diskTypes := make([]types.DiskType, len(dirs))
	tags := make([][]string, len(dirs))
	for i := range dirs {
		max[i] = 10
		diskTypes[i] = types.HardDriveType
	}
	// idxFolder shared so .ecx/.ecj resolve from any disk's index dir.
	s := storage.NewStore(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"127.0.0.1", 0, 0, "", "test-store",
		dirs, max, minFree,
		dirs[0], storage.NeedleMapInMemory,
		diskTypes, tags,
		0, stats.DiskIOProbeConfig{},
	)
	t.Cleanup(s.Close)
	return s
}

// seedEcShardOnDisk writes the minimal files for one shard id on one disk and
// mounts it. The .ecx is shared (one entry) so ScrubIndex/ScrubLocal can run.
func seedEcShardOnDisk(t *testing.T, store *storage.Store, vid needle.VolumeId, collection, dir string, shardId int, encodeTs int64, dataShards, parityShards int, blockSize int64) {
	t.Helper()
	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	require.NoError(t, os.WriteFile(base+".ecx", make([]byte, types.NeedleMapEntrySize), 0o644))
	require.NoError(t, os.WriteFile(base+".ecj", nil, 0o644))
	require.NoError(t, os.WriteFile(base+erasure_coding.ToExt(shardId), []byte("s"), 0o644))
	vif := &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   uint32(dataShards),
			ParityShards: uint32(parityShards),
			BlockSize:    blockSize,
			EncodeTsNs:   encodeTs,
		},
	}
	require.NoError(t, volume_info.SaveVolumeInfo(base+".vif", vif))
	require.NoError(t, store.MountEcShards(collection, vid, erasure_coding.ShardId(shardId), types.HardDriveType.String()))
}

// TestScrubEcVolume_DedupesSplitDiskVolume: a volume mounted on two disks must
// be scrubbed once (TotalVolumes == 1), not once per location.
func TestScrubEcVolume_DedupesSplitDiskVolume(t *testing.T) {
	diskA, diskB := t.TempDir(), t.TempDir()
	store := newMultiDiskEcScrubStore(t, diskA, diskB)
	const vid = needle.VolumeId(900)
	seedEcShardOnDisk(t, store, vid, "split", diskA, 0, 0, 10, 4, 0)
	seedEcShardOnDisk(t, store, vid, "split", diskB, 1, 0, 10, 4, 0)

	vs := &VolumeServer{store: store}
	res, err := vs.scrubEcVolumes(
		&volume_server_pb.ScrubEcVolumeRequest{Mode: volume_server_pb.VolumeScrubMode_INDEX},
		[]needle.VolumeId{vid}, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res.GetTotalVolumes(), "a split-disk volume must be scrubbed once, not once per disk")
}

// TestScrubEcVolume_FencesOnEncodeGeneration: a runtime from an older encode
// run must be excluded and reported, not merged into the scrub.
func TestScrubEcVolume_FencesOnEncodeGeneration(t *testing.T) {
	diskA, diskB := t.TempDir(), t.TempDir()
	store := newMultiDiskEcScrubStore(t, diskA, diskB)
	const vid = needle.VolumeId(901)
	seedEcShardOnDisk(t, store, vid, "gen", diskA, 0, 1000, 10, 4, 0)
	seedEcShardOnDisk(t, store, vid, "gen", diskB, 1, 500, 10, 4, 0)

	vs := &VolumeServer{store: store}
	res, err := vs.scrubEcVolumes(
		&volume_server_pb.ScrubEcVolumeRequest{Mode: volume_server_pb.VolumeScrubMode_INDEX},
		[]needle.VolumeId{vid}, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res.GetTotalVolumes())
	var foundSkip bool
	for _, d := range res.GetDetails() {
		if strings.Contains(d, "belong to encode run 500") {
			foundSkip = true
		}
	}
	assert.True(t, foundSkip, "older encode-generation runtime must be reported as skipped, got details: %v", res.GetDetails())
}

// TestScrubEcVolume_FencesOnGeometry: same encode timestamp but disagreeing
// geometry must exclude and report the incompatible runtime.
func TestScrubEcVolume_FencesOnGeometry(t *testing.T) {
	diskA, diskB := t.TempDir(), t.TempDir()
	store := newMultiDiskEcScrubStore(t, diskA, diskB)
	const vid = needle.VolumeId(902)
	seedEcShardOnDisk(t, store, vid, "geo", diskA, 0, 1000, 10, 4, 3*1024*1024)
	seedEcShardOnDisk(t, store, vid, "geo", diskB, 1, 1000, 12, 4, 3*1024*1024)

	vs := &VolumeServer{store: store}
	res, err := vs.scrubEcVolumes(
		&volume_server_pb.ScrubEcVolumeRequest{Mode: volume_server_pb.VolumeScrubMode_INDEX},
		[]needle.VolumeId{vid}, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res.GetTotalVolumes())
	var foundGeoSkip bool
	for _, d := range res.GetDetails() {
		if strings.Contains(d, "disagree on geometry") {
			foundGeoSkip = true
		}
	}
	assert.True(t, foundGeoSkip, "geometry-mismatched runtime must be reported as skipped, got details: %v", res.GetDetails())
}

// TestScrubEcVolume_LocalReachesSiblingDisk: LOCAL mode over a split-disk
// volume must see shards from both disks (the merged view), not just the
// first. With both shards local, the index walk has no missing-shard error.
func TestScrubEcVolume_LocalReachesSiblingDisk(t *testing.T) {
	diskA, diskB := t.TempDir(), t.TempDir()
	store := newMultiDiskEcScrubStore(t, diskA, diskB)
	const vid = needle.VolumeId(903)
	seedEcShardOnDisk(t, store, vid, "sib", diskA, 0, 0, 10, 4, 0)
	seedEcShardOnDisk(t, store, vid, "sib", diskB, 1, 0, 10, 4, 0)

	vs := &VolumeServer{store: store}
	res, err := vs.scrubEcVolumes(
		&volume_server_pb.ScrubEcVolumeRequest{Mode: volume_server_pb.VolumeScrubMode_LOCAL},
		[]needle.VolumeId{vid}, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res.GetTotalVolumes())
}

// TestMergeEcRuntimes_ReportsAnchorGeneration: the anchor is the maximum
// encode generation, and a single-runtime volume anchors on itself.
func TestMergeEcRuntimes_ReportsAnchorGeneration(t *testing.T) {
	dir := t.TempDir()
	store := newMultiDiskEcScrubStore(t, dir)
	const vid = needle.VolumeId(904)
	seedEcShardOnDisk(t, store, vid, "anchor", dir, 0, 7777, 10, 4, 0)

	runtimes := store.FindAllEcVolumes(vid)
	require.Len(t, runtimes, 1)
	merged := erasure_coding.MergeEcRuntimes(runtimes)
	require.NotNil(t, merged)
	assert.Equal(t, int64(7777), merged.Anchor.EncodeTsNs)
	assert.Empty(t, merged.Skipped)
	assert.Len(t, merged.Merged, 1)
}

// TestMergeEcRuntimes_NilForEmptyInput: an empty runtime slice (vanished
// volume) yields a nil merged view.
func TestMergeEcRuntimes_NilForEmptyInput(t *testing.T) {
	assert.Nil(t, erasure_coding.MergeEcRuntimes(nil))
	assert.Nil(t, erasure_coding.MergeEcRuntimes([]*erasure_coding.EcVolume{}))
}

// Ensure filepath import is used (helper for future multi-disk tests).
var _ = filepath.Join
