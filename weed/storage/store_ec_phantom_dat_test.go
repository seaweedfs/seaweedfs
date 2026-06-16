package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestLoneVifDoesNotCreatePhantomDat: a lone .vif whose .ecx is on a
// sibling disk must not make loadExistingVolume create a phantom 8-byte
// .dat, which the sibling-.dat prune would then use to delete the real EC shards
// on the sibling. The same-disk hasEcxFile() guard misses this split.
func TestLoneVifDoesNotCreatePhantomDat(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection := "warp-loadtest"
	vid := needle.VolumeId(57)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)

	// dir0: a self-contained but partial (2 < 10) EC volume.
	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	for _, sid := range []int{2, 4} {
		f, err := os.Create(base0 + erasure_coding.ToExt(sid))
		if err != nil {
			t.Fatalf("create shard %d: %v", sid, err)
		}
		if err := f.Truncate(expectedShardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d: %v", sid, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close shard %d: %v", sid, err)
		}
	}
	if err := os.WriteFile(base0+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(base0+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
	// DatFileSize 0 mirrors the production .vif that triggered the bug and
	// makes the prune's credibility gate fall back to the superblock size.
	vif := &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   dataShards,
			ParityShards: parityShards,
		},
	}
	if err := volume_info.SaveVolumeInfo(base0+".vif", vif); err != nil {
		t.Fatalf("save .vif dir0: %v", err)
	}

	// dir1: ONLY the mirrored .vif — no .ecx, no shard, no .dat.
	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))
	if err := volume_info.SaveVolumeInfo(base1+".vif", vif); err != nil {
		t.Fatalf("save .vif dir1: %v", err)
	}

	diskIOProbeConfig := stats.DefaultDiskIOProbeConfig()
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		"",
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.HardDriveType},
		nil,
		3,
		diskIOProbeConfig,
	)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.NewEcShardsChan:
			case <-store.DeletedVolumesChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})

	// Fix A: the lone .vif on dir1 must not have spawned a phantom .dat.
	if util.FileExists(base1 + ".dat") {
		t.Errorf("phantom .dat was created on the lone-.vif disk %s", dir1)
	}

	// The real EC shards on dir0 must survive: with no phantom .dat, the
	// sibling-.dat prune finds no .dat owner and deletes nothing.
	loc0 := store.Locations[0]
	for _, sid := range []erasure_coding.ShardId{2, 4} {
		if _, found := loc0.FindEcShard(vid, sid); !found {
			t.Errorf("EC shard %d on dir0 was deleted", sid)
		}
		if !util.FileExists(base0 + erasure_coding.ToExt(int(sid))) {
			t.Errorf("EC shard file %d on dir0 was removed from disk", sid)
		}
	}
}

// TestEmptyEcDatStubIsSwept: an empty .dat (<= a superblock, i.e. zero needles)
// for an EC volume is a leftover stub from the pre-fix loader; the loader must
// sweep it on startup, not load it as a phantom empty volume. (On Rust this
// also unblocks the duplicate-vid startup check; Go loads each disk
// independently and does not crash, but the phantom must still go.)
func TestEmptyEcDatStubIsSwept(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection := "warp-cal"
	vid := needle.VolumeId(41)
	// A real (loadable) but empty superblock: without the sweep it loads as a
	// phantom empty volume.
	stub := (&super_block.SuperBlock{
		Version:          needle.Version3,
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              &needle.TTL{},
	}).Bytes()
	for _, d := range []string{dir0, dir1} {
		base := erasure_coding.EcShardFileName(collection, d, int(vid))
		if err := os.WriteFile(base+".dat", stub, 0o644); err != nil {
			t.Fatalf("write stub .dat: %v", err)
		}
		if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
			Version:       uint32(needle.Version3),
			EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4},
		}); err != nil {
			t.Fatalf("save .vif: %v", err)
		}
	}

	diskIOProbeConfig := stats.DefaultDiskIOProbeConfig()
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		"",
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.HardDriveType},
		nil,
		3,
		diskIOProbeConfig,
	)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.NewEcShardsChan:
			case <-store.DeletedVolumesChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})

	if store.findVolume(vid) != nil {
		t.Errorf("empty EC .dat stub was loaded as a phantom volume %d", vid)
	}
	for _, d := range []string{dir0, dir1} {
		if util.FileExists(erasure_coding.EcShardFileName(collection, d, int(vid)) + ".dat") {
			t.Errorf("empty .dat stub on %s was not removed", d)
		}
	}
}

// TestRemoveEmptyEcDatStubFindsVifInIdxDir: when -dir.idx is configured the EC
// .vif may live in the idx directory; removeEmptyEcDatStub must look there too,
// not only in the data dir. Calls the helper directly (the dir scan only
// discovers volumes via a .idx/.vif in the data dir, which a separate idx dir
// sidesteps).
func TestRemoveEmptyEcDatStubFindsVifInIdxDir(t *testing.T) {
	dataDir := t.TempDir()
	idxDir := t.TempDir()
	loc := &DiskLocation{
		Directory:              dataDir,
		DirectoryUuid:          "test-uuid",
		IdxDirectory:           idxDir,
		DiskType:               types.HddType,
		MaxVolumeCount:         100,
		OriginalMaxVolumeCount: 100,
		MinFreeSpace:           util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"},
	}

	const volumeName = "warp-cal_42"
	stub := (&super_block.SuperBlock{
		Version:          needle.Version3,
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              &needle.TTL{},
	}).Bytes()
	if err := os.WriteFile(dataDir+"/"+volumeName+".dat", stub, 0o644); err != nil {
		t.Fatalf("write stub .dat: %v", err)
	}
	// EC .vif lives in the idx dir, not next to the .dat.
	if err := volume_info.SaveVolumeInfo(idxDir+"/"+volumeName+".vif", &volume_server_pb.VolumeInfo{
		Version:       uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	if !loc.removeEmptyEcDatStub(volumeName, needle.VolumeId(42), "warp-cal") {
		t.Fatal("stub with EC .vif in the idx dir should be removed")
	}
	if util.FileExists(dataDir + "/" + volumeName + ".dat") {
		t.Error(".dat stub was not removed")
	}
}

// startEcTestStore starts a single-disk store over dir and drains its
// notification channels until cleanup.
func startEcTestStore(t *testing.T, dir string) *Store {
	t.Helper()
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir},
		[]int32{100},
		[]util.MinFreeSpace{{}},
		"",
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType},
		nil,
		3,
		stats.DefaultDiskIOProbeConfig(),
	)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.NewEcShardsChan:
			case <-store.DeletedVolumesChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})
	return store
}

// writeEcShardFixture lays down .ecx, .ecj, and the given shards for a
// distributed EC volume in dir, each shard truncated to shardSize.
func writeEcShardFixture(t *testing.T, base string, shardIds []int, shardSize int64) {
	t.Helper()
	for _, sid := range shardIds {
		f, err := os.Create(base + erasure_coding.ToExt(sid))
		if err != nil {
			t.Fatalf("create shard %d: %v", sid, err)
		}
		if err := f.Truncate(shardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d: %v", sid, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close shard %d: %v", sid, err)
		}
	}
	if err := os.WriteFile(base+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(base+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
}

// TestEmptyDatStubNextToEcxDoesNotDeleteShards: a disk holding a few local
// shards of a healthy distributed EC volume plus a leftover empty .dat stub.
// The stub used to make startup classify the volume as an interrupted local
// encode (fewer than dataShards local shards) and delete the only copies of
// those shards. The stub must be swept and the shards must load.
func TestEmptyDatStubNextToEcxDoesNotDeleteShards(t *testing.T) {
	dir := t.TempDir()
	collection := "warp-rec"
	vid := needle.VolumeId(87)
	base := erasure_coding.EcShardFileName(collection, dir, int(vid))

	shardIds := []int{0, 5}
	writeEcShardFixture(t, base, shardIds, int64(erasure_coding.ErasureCodingSmallBlockSize))
	// Zero-byte stub .dat: the phantom left by the pre-fix loader.
	if err := os.WriteFile(base+".dat", nil, 0o644); err != nil {
		t.Fatalf("write stub .dat: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version:       uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	store := startEcTestStore(t, dir)

	loc := store.Locations[0]
	for _, sid := range shardIds {
		if !util.FileExists(base + erasure_coding.ToExt(sid)) {
			t.Errorf("EC shard file %d was deleted", sid)
		}
		if _, found := loc.FindEcShard(vid, erasure_coding.ShardId(sid)); !found {
			t.Errorf("EC shard %d was not loaded", sid)
		}
	}
	if !util.FileExists(base + ".ecx") {
		t.Error(".ecx was deleted")
	}
	if util.FileExists(base + ".dat") {
		t.Error("empty .dat stub was not swept")
	}
	if store.findVolume(vid) != nil {
		t.Errorf("stub was loaded as a phantom volume %d", vid)
	}
}

// TestEmptyDatWithoutVifDoesNotDeleteShards: same shard-holding disk but the
// stub has no .vif at all, so the sweep has no EC evidence and must leave it.
// The empty .dat still must not count as an encode source: the shards survive
// and load as a distributed EC volume.
func TestEmptyDatWithoutVifDoesNotDeleteShards(t *testing.T) {
	dir := t.TempDir()
	collection := "warp-rec"
	vid := needle.VolumeId(88)
	base := erasure_coding.EcShardFileName(collection, dir, int(vid))

	shardIds := []int{1, 7}
	writeEcShardFixture(t, base, shardIds, int64(erasure_coding.ErasureCodingSmallBlockSize))
	if err := os.WriteFile(base+".dat", nil, 0o644); err != nil {
		t.Fatalf("write stub .dat: %v", err)
	}

	store := startEcTestStore(t, dir)

	loc := store.Locations[0]
	for _, sid := range shardIds {
		if !util.FileExists(base + erasure_coding.ToExt(sid)) {
			t.Errorf("EC shard file %d was deleted", sid)
		}
		if _, found := loc.FindEcShard(vid, erasure_coding.ShardId(sid)); !found {
			t.Errorf("EC shard %d was not loaded", sid)
		}
	}
	if store.findVolume(vid) != nil {
		t.Errorf("stub was loaded as a phantom volume %d", vid)
	}
}
