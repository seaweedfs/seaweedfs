package storage

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// drainStoreChans consumes the heartbeat-style channels so NewStore's
// notify handler never blocks during loading.
func drainStoreChans(t *testing.T, store *Store) {
	t.Helper()
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
}

// TestEc9plus3MultiDiskRebootLoadsAllShards mirrors the reported setup:
// enterprise EC 9+3, one volume server with 6 disks, a distributed volume's
// 12 shards spread across the disks while the .ecx/.ecj/.vif sidecars live on
// a single disk (the distribute "owner"). After a reboot (NewStore), every
// shard must load — the orphan-shard reconciler is supposed to mirror the
// sidecars onto the other disks and mount their shards.
func TestEc9plus3MultiDiskRebootLoadsAllShards(t *testing.T) {
	// Deliberately do NOT call SetGlobalECConfig: a real volume server never
	// loads the cluster EC config into memory, so the 9+3 ratio must come
	// from each volume's .vif. Relying on the .vif keeps these faithful to
	// production and guards the fixes that read the ratio from the .vif.

	const dataShards, parityShards = 9, 3
	const totalShards = dataShards + parityShards // 12

	tempDir := t.TempDir()
	const diskCount = 6
	dirs := make([]string, diskCount)
	for i := range dirs {
		dirs[i] = filepath.Join(tempDir, "disk"+strconv.Itoa(i))
		if err := os.MkdirAll(dirs[i], 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dirs[i], err)
		}
	}

	collection := "loki"
	vid := needle.VolumeId(7)
	const datSize int64 = 64 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)

	// 12 shards, 2 per disk: disk0→{0,1}, disk1→{2,3}, ... disk5→{10,11}.
	shardToDisk := func(shardId int) int { return shardId / 2 }

	for s := 0; s < totalShards; s++ {
		dir := dirs[shardToDisk(s)]
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		f, err := os.Create(base + erasure_coding.ToExt(s))
		if err != nil {
			t.Fatalf("create shard %d: %v", s, err)
		}
		if err := f.Truncate(expectedShardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d: %v", s, err)
		}
		f.Close()
	}

	// Sidecars live ONLY on disk0 (the distribute owner). No .dat anywhere —
	// this is a fully distributed EC volume.
	base0 := erasure_coding.EcShardFileName(collection, dirs[0], int(vid))
	if err := os.WriteFile(base0+".ecx", make([]byte, 20), 0o644); err != nil {
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

	diskTypes := make([]types.DiskType, diskCount)
	maxCounts := make([]int32, diskCount)
	minFree := make([]util.MinFreeSpace, diskCount)
	for i := range diskTypes {
		diskTypes[i] = types.HardDriveType
		maxCounts[i] = 100
	}

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		dirs, maxCounts, minFree, "", NeedleMapInMemory, diskTypes, nil, 3, stats.DefaultDiskIOProbeConfig())
	drainStoreChans(t, store)

	missing := []int{}
	for s := 0; s < totalShards; s++ {
		loc := store.Locations[shardToDisk(s)]
		if _, found := loc.FindEcShard(vid, erasure_coding.ShardId(s)); !found {
			missing = append(missing, s)
		}
	}
	if len(missing) > 0 {
		t.Errorf("after reboot, %d/%d shards missing for 9+3 volume %d: shards %v not loaded",
			len(missing), totalShards, vid, missing)
	}

	// Also confirm the heartbeat would report all 12 shards (master's view).
	hb := store.CollectErasureCodingHeartbeat()
	reported := map[int]bool{}
	for _, msg := range hb.EcShards {
		if needle.VolumeId(msg.Id) != vid {
			continue
		}
		si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(msg)
		for _, id := range si.Ids() {
			reported[int(id)] = true
		}
	}
	if len(reported) != totalShards {
		t.Errorf("heartbeat reports %d/%d shards for volume %d (master would show %d missing)",
			len(reported), totalShards, vid, totalShards-len(reported))
	}
}

// TestEc9plus3ValidateKeepsVolumeWithDat reproduces the reboot-time deletion
// path for a custom 9+3 ratio. When a .dat coexists with the EC shards on a
// disk (a just-encoded volume whose original .dat has not been deleted yet, or
// a local-EC layout), startup loading runs validateEcVolume, which derives the
// expected per-shard size from the .dat size and the DATA-shard count.
//
// At 4.29 that count was the compiled-in OSS default (10); on a 9+3 volume the
// shards are sized for 9 data shards, so the size check fails, validation
// returns false, and handleFoundEcxFile deletes every EC file for the volume —
// i.e. the shards "go missing" on the next reboot. HEAD derives the count from
// GetECConfig(collection), so the volume survives.
func TestEc9plus3ValidateKeepsVolumeWithDat(t *testing.T) {
	// Deliberately do NOT call SetGlobalECConfig: a real volume server never
	// loads the cluster EC config into memory, so the 9+3 ratio must come
	// from each volume's .vif. Relying on the .vif keeps these faithful to
	// production and guards the fixes that read the ratio from the .vif.

	const dataShards, parityShards = 9, 3
	const totalShards = dataShards + parityShards

	// The 4.29 bug is a wrong expected shard size; prove the two ratios really
	// do disagree for a realistic .dat size, so the size check would reject.
	const datSize int64 = 64 * 1024 * 1024
	sizeFor9 := calculateExpectedShardSize(datSize, dataShards)
	sizeFor10 := calculateExpectedShardSize(datSize, erasure_coding.DataShardsCount) // 10
	if sizeFor9 == sizeFor10 {
		t.Fatalf("test premise broken: shard size for 9 and 10 data shards must differ (got %d)", sizeFor9)
	}

	dir := t.TempDir()
	collection := "loki"
	vid := needle.VolumeId(42)
	base := erasure_coding.EcShardFileName(collection, dir, int(vid))

	// .dat sized as the encode source.
	if err := os.WriteFile(base+".dat", make([]byte, datSize), 0o644); err != nil {
		t.Fatalf("write .dat: %v", err)
	}
	// 12 shards, each sized for the real 9-data-shard layout.
	for s := 0; s < totalShards; s++ {
		f, err := os.Create(base + erasure_coding.ToExt(s))
		if err != nil {
			t.Fatalf("create shard %d: %v", s, err)
		}
		if err := f.Truncate(sizeFor9); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d: %v", s, err)
		}
		f.Close()
	}
	if err := os.WriteFile(base+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(base+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   dataShards,
			ParityShards: parityShards,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{100}, []util.MinFreeSpace{{}}, "",
		NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
	drainStoreChans(t, store)

	// HEAD: validateEcVolume uses the 9+3 ratio, the size check passes, and the
	// shards stay on disk + get loaded. 4.29 would have deleted them here.
	for s := 0; s < totalShards; s++ {
		if _, err := os.Stat(base + erasure_coding.ToExt(s)); err != nil {
			t.Errorf("shard %d was deleted at startup (ratio-mismatch cleanup): %v", s, err)
		}
	}
	loc := store.Locations[0]
	loaded := 0
	for s := 0; s < totalShards; s++ {
		if _, found := loc.FindEcShard(vid, erasure_coding.ShardId(s)); found {
			loaded++
		}
	}
	if loaded != totalShards {
		t.Errorf("loaded %d/%d shards for 9+3 volume with .dat present; expected all to survive validation", loaded, totalShards)
	}
}

// TestEc9plus3PruneKeepsFullDataShardSet reproduces a CURRENT (HEAD) custom-ratio
// bug. pruneIncompleteEcWithSiblingDat deletes EC shards on a disk when the disk
// holds fewer than DataShards shards AND a healthy .dat for the same volume lives
// on a sibling disk (issue 9478 cleanup). The threshold is hardcoded to the OSS
// default DataShardsCount (10), so for a 9+3 volume a disk holding a COMPLETE
// 9-data-shard set (fully recoverable on its own) is treated as a partial
// leftover and wiped on the next reboot.
//
// With the volume's real ratio (9), 9 >= 9, so the set must be kept.
func TestEc9plus3PruneKeepsFullDataShardSet(t *testing.T) {
	// Deliberately do NOT call SetGlobalECConfig: a real volume server never
	// loads the cluster EC config into memory, so the 9+3 ratio must come
	// from each volume's .vif. Relying on the .vif keeps these faithful to
	// production and guards the fixes that read the ratio from the .vif.

	const dataShards, parityShards = 9, 3

	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0") // 9 data shards + index sidecars
	dir1 := filepath.Join(tempDir, "disk1") // healthy .dat for the same volume
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection := "loki"
	vid := needle.VolumeId(77)
	const datSize int64 = 64 * 1024 * 1024
	shardSize := calculateExpectedShardSize(datSize, dataShards)

	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	for s := 0; s < dataShards; s++ { // shards 0..8 — a complete data set
		f, err := os.Create(base0 + erasure_coding.ToExt(s))
		if err != nil {
			t.Fatalf("create shard %d: %v", s, err)
		}
		if err := f.Truncate(shardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d: %v", s, err)
		}
		f.Close()
	}
	if err := os.WriteFile(base0+".ecx", make([]byte, 20), 0o644); err != nil {
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

	// Sibling disk holds a credible .dat (>= recorded datFileSize).
	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))
	if err := os.WriteFile(base1+".dat", make([]byte, datSize), 0o644); err != nil {
		t.Fatalf("write .dat: %v", err)
	}

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1}, []int32{100, 100}, []util.MinFreeSpace{{}, {}}, "",
		NeedleMapInMemory, []types.DiskType{types.HardDriveType, types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
	drainStoreChans(t, store)

	loc0 := store.Locations[0]
	kept := 0
	for s := 0; s < dataShards; s++ {
		if _, found := loc0.FindEcShard(vid, erasure_coding.ShardId(s)); found {
			kept++
		}
	}
	if kept != dataShards {
		t.Errorf("a complete %d-data-shard set was pruned at reboot: only %d/%d shards survived "+
			"(pruneIncompleteEcWithSiblingDat used the hardcoded 10-shard threshold instead of the volume's %d)",
			dataShards, kept, dataShards, dataShards)
	}
}
