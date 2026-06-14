package erasure_coding

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

func TestPositioning(t *testing.T) {

	ecxFile, err := os.OpenFile("./test_files/389.ecx", os.O_RDONLY, 0)
	if err != nil {
		t.Errorf("failed to open ecx file: %v", err)
	}
	defer ecxFile.Close()

	stat, _ := ecxFile.Stat()
	fileSize := stat.Size()

	tests := []struct {
		needleId string
		offset   int64
		size     int
	}{
		{needleId: "0f0edb92", offset: 31300679656, size: 1167},
		{needleId: "0ef7d7f8", offset: 11513014944, size: 66044},
	}

	for _, test := range tests {
		needleId, _ := types.ParseNeedleId(test.needleId)
		offset, size, err := SearchNeedleFromSortedIndex(ecxFile, fileSize, needleId, nil)
		assert.Equal(t, nil, err, "SearchNeedleFromSortedIndex")
		fmt.Printf("offset: %d size: %d\n", offset.ToActualOffset(), size)
	}

	needleId, _ := types.ParseNeedleId("0f087622")
	offset, size, err := SearchNeedleFromSortedIndex(ecxFile, fileSize, needleId, nil)
	assert.Equal(t, nil, err, "SearchNeedleFromSortedIndex")
	fmt.Printf("offset: %d size: %d\n", offset.ToActualOffset(), size)

	var shardEcdFileSize int64 = 1118830592 // 1024*1024*1024*3
	intervals := LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, shardEcdFileSize, offset.ToActualOffset(), types.Size(needle.GetActualSize(size, needle.GetCurrentVersion())))

	for _, interval := range intervals {
		shardId, shardOffset := interval.ToShardIdAndOffset(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
		fmt.Printf("interval: %+v, shardId: %d, shardOffset: %d\n", interval, shardId, shardOffset)
	}

}

// TestNewEcVolumeLoadsEncodeTsNs pins that the per-encode identity stamped into
// .vif is loaded onto the EcVolume, so reads can reject a shard from a different
// encode run.
func TestNewEcVolumeLoadsEncodeTsNs(t *testing.T) {
	dir := t.TempDir()
	const vid = needle.VolumeId(123)
	base := EcShardFileName("", dir, int(vid))

	// A 0-byte .ecx is a valid index (no live needles) and lets NewEcVolume mount.
	if err := os.WriteFile(base+".ecx", nil, 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}

	const tsNs int64 = 1717000000000000123
	vi := &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   10,
			ParityShards: 4,
			EncodeTsNs:   tsNs,
		},
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", vi); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", vid)
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	defer ev.Close()

	if ev.EncodeTsNs != tsNs {
		t.Errorf("EncodeTsNs = %d, want %d", ev.EncodeTsNs, tsNs)
	}
}

// TestNewEcVolumeDoesNotWriteStubVif pins that mounting an EC volume whose .vif
// is missing does NOT fabricate a stub .vif. A version-only stub would imply
// the default ratio with DatFileSize=0 and no encode identity, which the
// custom-ratio resolver and startup credibility checks must not trust.
func TestNewEcVolumeDoesNotWriteStubVif(t *testing.T) {
	dir := t.TempDir()
	const vid = needle.VolumeId(124)
	base := EcShardFileName("", dir, int(vid))

	if err := os.WriteFile(base+".ecx", nil, 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}

	ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", vid)
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	defer ev.Close()

	if _, statErr := os.Stat(base + ".vif"); !os.IsNotExist(statErr) {
		t.Fatalf("mounting without a .vif must not create one, stat err=%v", statErr)
	}
	// Mount still succeeds with the build's default EC ratio in memory.
	if ev.ECContext == nil || ev.ECContext.DataShards != int(DataShardsCount) {
		t.Fatalf("expected default EC context, got %+v", ev.ECContext)
	}
}

// TestNewEcVolumeLoadsCustomRatio pins that a volume's own EC ratio is loaded from
// its .vif into ECContext, so the read/recover/decode paths reconstruct with the
// matrix that produced the shards rather than the build default. (Custom ratios are
// an enterprise feature; in OSS the .vif always records 10+4.)
func TestNewEcVolumeLoadsCustomRatio(t *testing.T) {
	dir := t.TempDir()
	const vid = needle.VolumeId(125)
	base := EcShardFileName("", dir, int(vid))

	if err := os.WriteFile(base+".ecx", nil, 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   9,
			ParityShards: 3,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", vid)
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	defer ev.Close()

	if ev.ECContext == nil {
		t.Fatalf("ECContext must be set from the .vif")
	}
	if ev.ECContext.DataShards != 9 || ev.ECContext.ParityShards != 3 {
		t.Fatalf("ECContext = %d+%d, want 9+3", ev.ECContext.DataShards, ev.ECContext.ParityShards)
	}
}
