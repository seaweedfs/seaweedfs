package erasure_coding

import (
	"fmt"
	"os"
	"path/filepath"
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

// ecIndexDirsFixture lays out an EC volume across a data dir and a separate
// index dir (the -dir.idx split), planting a .vif next to the data and .ecx
// files of the given sizes (-1 = absent). It returns the two bases.
func ecIndexDirsFixture(t *testing.T, vid needle.VolumeId, localEcx, sharedEcx int) (dataDir, idxDir, dataBase, idxBase string) {
	t.Helper()
	root := t.TempDir()
	dataDir = filepath.Join(root, "data")
	idxDir = filepath.Join(root, "idx")
	for _, d := range []string{dataDir, idxDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	dataBase = EcShardFileName("", dataDir, int(vid))
	idxBase = EcShardFileName("", idxDir, int(vid))
	writeEcx := func(base string, size int) {
		if size < 0 {
			return
		}
		if err := os.WriteFile(base+".ecx", make([]byte, size), 0o644); err != nil {
			t.Fatalf("write .ecx: %v", err)
		}
	}
	writeEcx(dataBase, localEcx)
	writeEcx(idxBase, sharedEcx)
	if err := volume_info.SaveVolumeInfo(dataBase+".vif", &volume_server_pb.VolumeInfo{
		Version:       uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}
	return
}

// TestNewEcVolumePrefersLocalEcx pins the local-first resolution: with a
// non-empty .ecx co-located with the shard data AND one in the shared index
// dir, NewEcVolume opens the local copy — where a move or reconstruct leaves
// the index.
func TestNewEcVolumePrefersLocalEcx(t *testing.T) {
	const vid = needle.VolumeId(200)
	dataDir, idxDir, _, _ := ecIndexDirsFixture(t, vid, 16, 16)

	ev, err := NewEcVolume(types.HardDriveType, dataDir, idxDir, "", vid)
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	defer ev.Close()

	if got := filepath.Dir(ev.FileName(".ecx")); got != dataDir {
		t.Errorf(".ecx resolved in %q, want the data dir %q (local-first)", got, dataDir)
	}
}

// TestNewEcVolumeZeroByteLocalYieldsToSharedEcx pins that a 0-byte local .ecx
// (an interrupted-copy stub is indistinguishable from a legitimately empty
// index by size) yields to a *non-empty* copy in the shared index dir, keeping
// the cross-disk fallback (#9212).
func TestNewEcVolumeZeroByteLocalYieldsToSharedEcx(t *testing.T) {
	const vid = needle.VolumeId(201)
	dataDir, idxDir, _, _ := ecIndexDirsFixture(t, vid, 0, 16)

	ev, err := NewEcVolume(types.HardDriveType, dataDir, idxDir, "", vid)
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	defer ev.Close()

	if got := filepath.Dir(ev.FileName(".ecx")); got != idxDir {
		t.Errorf(".ecx resolved in %q, want the shared idx dir %q (non-empty wins over 0-byte local)", got, idxDir)
	}
}

// TestEcVolumeDestroySweepsBothDirs pins that Destroy removes the EC index
// files from both the data and index directories, so a stale copy left by a
// move or reconstruct cannot re-mount as a phantom EC volume.
func TestEcVolumeDestroySweepsBothDirs(t *testing.T) {
	const vid = needle.VolumeId(202)
	dataDir, idxDir, dataBase, idxBase := ecIndexDirsFixture(t, vid, 16, 16)
	// A stale .ecj in each dir too.
	for _, base := range []string{dataBase, idxBase} {
		if err := os.WriteFile(base+".ecj", nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	ev, err := NewEcVolume(types.HardDriveType, dataDir, idxDir, "", vid)
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	ev.Destroy()

	for _, base := range []string{dataBase, idxBase} {
		for _, ext := range []string{".ecx", ".ecj"} {
			if _, err := os.Stat(base + ext); !os.IsNotExist(err) {
				t.Errorf("%s%s survived Destroy (err=%v), want removed from both dirs", base, ext, err)
			}
		}
	}
}
