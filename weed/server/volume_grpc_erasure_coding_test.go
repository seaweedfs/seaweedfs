package weed_server

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCheckEcVolumeStatusCountOnlyDataShards(t *testing.T) {
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	idxDir := filepath.Join(tempDir, "idx")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	if err := os.MkdirAll(idxDir, 0o755); err != nil {
		t.Fatalf("mkdir idx dir: %v", err)
	}

	baseName := "7"
	filesToCreate := []string{
		filepath.Join(dataDir, baseName+".ec00"),
		filepath.Join(dataDir, baseName+".ec09"),
		filepath.Join(dataDir, baseName+".ec13"),
		filepath.Join(idxDir, baseName+".ecx"),
		filepath.Join(idxDir, baseName+".ecj"),
		filepath.Join(idxDir, baseName+".idx"),
	}
	for _, fileName := range filesToCreate {
		if err := os.WriteFile(fileName, []byte("x"), 0o644); err != nil {
			t.Fatalf("create %s: %v", fileName, err)
		}
	}

	location := &storage.DiskLocation{
		Directory:    dataDir,
		IdxDirectory: idxDir,
	}

	hasEcxFile, hasIdxFile, shardCount, err := checkEcVolumeStatus(baseName, location)
	if err != nil {
		t.Fatalf("checkEcVolumeStatus: %v", err)
	}

	if !hasEcxFile {
		t.Fatalf("expected hasEcxFile=true")
	}
	if !hasIdxFile {
		t.Fatalf("expected hasIdxFile=true")
	}
	if shardCount != 3 {
		t.Fatalf("expected shardCount=3, got %d", shardCount)
	}
}

// TestRemoveStaleEcArtifacts: a fresh encode deletes every prior EC artifact
// (incl. shard ids beyond the default ratio and versioned bitrot sidecars)
// while leaving the source .dat/.idx/.vif untouched.
func TestRemoveStaleEcArtifacts(t *testing.T) {
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	idxDir := filepath.Join(tempDir, "idx")
	for _, d := range []string{dataDir, idxDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	const baseName = "7"
	dataBase := filepath.Join(dataDir, baseName)
	idxBase := filepath.Join(idxDir, baseName)

	// EC artifacts that must be removed, including a shard id past the default
	// 10+4 ratio (proves the MaxShardCount scan) and a versioned sidecar.
	var ecFiles []string
	for _, id := range []int{0, 9, 13, 20, erasure_coding.MaxShardCount - 1} {
		ecFiles = append(ecFiles, dataBase+erasure_coding.ToExt(id))
	}
	ecFiles = append(ecFiles,
		dataBase+".ecx", dataBase+".ecj",
		idxBase+".ecx", idxBase+".ecj",
		dataBase+erasure_coding.BitrotSidecarExt,       // .ecsum (generation 0)
		dataBase+erasure_coding.BitrotSidecarExt+".v2", // versioned sidecar
	)

	// Source files that must survive — the authoritative input for the encode.
	srcFiles := []string{dataBase + ".dat", idxBase + ".idx", dataBase + ".vif"}

	for _, f := range append(append([]string{}, ecFiles...), srcFiles...) {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatalf("create %s: %v", f, err)
		}
	}

	removeStaleEcArtifacts(dataBase, idxBase, erasure_coding.MaxShardCount)

	for _, f := range ecFiles {
		if util.FileExists(f) {
			t.Errorf("expected EC artifact removed: %s", f)
		}
	}
	for _, f := range srcFiles {
		if !util.FileExists(f) {
			t.Errorf("expected source file preserved: %s", f)
		}
	}
}

// TestDeleteEcShardsWithoutLocalEcx: the delete handler removes the requested
// shard files even on a disk with no local .ecx, so a failed-copy orphan stays
// cleanable rather than being mounted later under a foreign index.
func TestDeleteEcShardsWithoutLocalEcx(t *testing.T) {
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	idxDir := filepath.Join(tempDir, "idx")
	for _, d := range []string{dataDir, idxDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	const baseName = "7"
	// Orphan shard files with NO .ecx/.idx anywhere — a failed-copy leftover.
	orphans := []string{
		filepath.Join(dataDir, baseName+".ec03"),
		filepath.Join(dataDir, baseName+".ec11"),
	}
	for _, f := range orphans {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatalf("create %s: %v", f, err)
		}
	}

	location := &storage.DiskLocation{Directory: dataDir, IdxDirectory: idxDir}
	if err := deleteEcShardIdsForEachLocation(baseName, location, []*storage.DiskLocation{location}, []uint32{3, 11}); err != nil {
		t.Fatalf("deleteEcShardIdsForEachLocation: %v", err)
	}
	for _, f := range orphans {
		if util.FileExists(f) {
			t.Errorf("expected orphan shard removed without a local .ecx: %s", f)
		}
	}
}

// TestVolumeEcShardsInfo_AggregatesAcrossDisks pins the multi-disk path:
// when a volume server mounts EC shards for the same volume on more than
// one disk (each disk holds its own EcVolume entry — Store.FindEcVolume
// returns only the first), VolumeEcShardsInfo used to report shards from
// a single disk. The ec.encode verification step (verifyEcShardsBeforeDelete)
// then refused to delete the source volume because the union across
// servers fell short of dataShards + parityShards. The handler must walk
// every DiskLocation so the response covers every shard the server holds.
func TestVolumeEcShardsInfo_AggregatesAcrossDisks(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	const collection = "ec-multi-disk-info"
	vid := needle.VolumeId(42)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024

	// Two shards on disk0, two on disk1. The .ecx / .ecj / .vif live on
	// disk0 so each disk's EcVolume can open the index files via the
	// cross-disk fallback in NewEcVolume.
	shardsOnDisk0 := []erasure_coding.ShardId{0, 5}
	shardsOnDisk1 := []erasure_coding.ShardId{7, 12}
	diskIOProbeConfig := stats.DefaultDiskIOProbeConfig()

	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		"",
		storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.HardDriveType},
		nil,
		3,
		diskIOProbeConfig,
	)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewEcShardsChan:
			case <-store.NewVolumesChan:
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

	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))

	// .ecx, .ecj, .vif live on disk0. NewEcVolume on disk1 falls back to
	// disk0's idx dir. The .ecx needs >0 bytes so HasEcxFileOnDisk does
	// not treat it as the corrupt-stub case; a single zero entry is the
	// smallest valid index file (WalkIndex iterates one zero-sized needle).
	if err := os.WriteFile(base0+".ecx", make([]byte, 16), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(base0+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base0+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   dataShards,
			ParityShards: parityShards,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	plant := func(base string, shardId erasure_coding.ShardId) {
		t.Helper()
		f, err := os.Create(base + erasure_coding.ToExt(int(shardId)))
		if err != nil {
			t.Fatalf("create shard %d: %v", shardId, err)
		}
		// MountEcShards does not validate shard size, so any non-empty
		// truncate avoids the zero-byte ignore branch in loadAllEcShards.
		if err := f.Truncate(1); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d: %v", shardId, err)
		}
		f.Close()
	}
	for _, sid := range shardsOnDisk0 {
		plant(base0, sid)
	}
	for _, sid := range shardsOnDisk1 {
		plant(base1, sid)
	}

	for _, sid := range append([]erasure_coding.ShardId{}, append(shardsOnDisk0, shardsOnDisk1...)...) {
		if err := store.MountEcShards(collection, vid, sid, ""); err != nil {
			t.Fatalf("MountEcShards %d.%d: %v", vid, sid, err)
		}
	}

	vs := &VolumeServer{store: store}
	resp, err := vs.VolumeEcShardsInfo(context.Background(), &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: uint32(vid),
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsInfo: %v", err)
	}

	gotShardIds := make([]int, 0, len(resp.GetEcShardInfos()))
	for _, info := range resp.GetEcShardInfos() {
		if info.GetVolumeId() != uint32(vid) {
			t.Errorf("EcShardInfo VolumeId=%d, want %d", info.GetVolumeId(), vid)
		}
		gotShardIds = append(gotShardIds, int(info.GetShardId()))
	}
	sort.Ints(gotShardIds)

	want := []int{0, 5, 7, 12}
	if len(gotShardIds) != len(want) {
		t.Fatalf("VolumeEcShardsInfo returned %d shards (ids=%v), want %d (ids=%v)",
			len(gotShardIds), gotShardIds, len(want), want)
	}
	for i, sid := range want {
		if gotShardIds[i] != sid {
			t.Fatalf("VolumeEcShardsInfo shard ids=%v, want %v", gotShardIds, want)
		}
	}
}
