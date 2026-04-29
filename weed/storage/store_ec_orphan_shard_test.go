package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestLoadEcShardsWhenIndexFilesOnDifferentDisk reproduces issue #9212.
// On the user's volume server, ec.balance moved EC shards onto a different
// physical disk than the disk that holds the .ecx/.ecj/.vif index files.
// The volume server must still load those orphan shards on startup and
// register them with the master — otherwise ec.rebuild reports the volume
// as unrepairable even though all shards are physically present.
//
// Layout under test (mirrors volume-0 + volume-2 rows for grafana-loki_1093
// in the ls -l attached to the issue):
//
//	dir0 (diskId 0): .ec00, .ec12      <-- shards but no .ecx / .ecj / .vif
//	dir1 (diskId 1): .ec01, .ecx, .ecj, .vif
func TestLoadEcShardsWhenIndexFilesOnDifferentDisk(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	if err := os.MkdirAll(dir0, 0o755); err != nil {
		t.Fatalf("mkdir dir0: %v", err)
	}
	if err := os.MkdirAll(dir1, 0o755); err != nil {
		t.Fatalf("mkdir dir1: %v", err)
	}

	collection := "grafana-loki"
	vid := needle.VolumeId(1093)

	// EC shape used to populate .vif. Kept as locals (not the package
	// constants) so the test stays valid when enterprise builds use a
	// different default ratio.
	const dataShards, parityShards = 10, 4

	// Use a small but realistic shard size so calculateExpectedShardSize
	// validation lines up with the .dat-derived size if it ever runs.
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)

	writeShard := func(dir string, shardId int) {
		t.Helper()
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		f, err := os.Create(base + erasure_coding.ToExt(shardId))
		if err != nil {
			t.Fatalf("create shard %d in %s: %v", shardId, dir, err)
		}
		if err := f.Truncate(expectedShardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d in %s: %v", shardId, dir, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close shard %d in %s: %v", shardId, dir, err)
		}
	}

	// dir0: orphan shards. No .ecx / .ecj / .vif on this disk.
	writeShard(dir0, 0)
	writeShard(dir0, 12)

	// dir1: one shard plus the index files.
	writeShard(dir1, 1)

	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))

	// Build a valid sealed .ecx with one entry so NewEcVolume can open it.
	// Layout: NeedleId(8) + Offset(8) + Size(4) = 20 bytes per entry.
	ecxBytes := make([]byte, 20)
	if err := os.WriteFile(base1+".ecx", ecxBytes, 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}

	// Empty .ecj is fine (no deletes journaled yet).
	if err := os.WriteFile(base1+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}

	// .vif with the EC config the test set above so EcVolume picks up the
	// right shape regardless of the build's default ratio.
	if err := volume_info.SaveVolumeInfo(base1+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   dataShards,
			ParityShards: parityShards,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	// Build the Store with both disks. NewStore triggers per-disk loading
	// during construction, which is the codepath under test.
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		"",
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.HardDriveType},
		nil,
		3,
	)

	// Drain heartbeat-style channels so loading does not block.
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

	if got, want := len(store.Locations), 2; got != want {
		t.Fatalf("store has %d disk locations, want %d", got, want)
	}

	// Sanity: dir1's shard ec01 lives on the disk that owns .ecx and must load.
	loc1 := store.Locations[1]
	if _, found := loc1.FindEcShard(vid, 1); !found {
		t.Fatalf("baseline broken: shard 1 on dir1 (which has .ecx) was not loaded")
	}

	// The bug: shards on dir0 have no local .ecx/.vif — they should still be
	// loaded by reaching across to dir1's index files, but currently the
	// per-disk loader silently drops them when no .dat file is present.
	loc0 := store.Locations[0]
	for _, sid := range []erasure_coding.ShardId{0, 12} {
		if _, found := loc0.FindEcShard(vid, sid); !found {
			t.Errorf("issue #9212: shard %d on dir0 was not loaded; .ecx is on dir1", sid)
		}
	}

	// Files on dir0 must not have been deleted by any cleanup path.
	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	for _, sid := range []int{0, 12} {
		shardPath := base0 + erasure_coding.ToExt(sid)
		fi, err := os.Stat(shardPath)
		if err != nil {
			t.Errorf("orphan shard %d was destroyed: %v", sid, err)
			continue
		}
		if fi.Size() != expectedShardSize {
			t.Errorf("orphan shard %d truncated: size %d, want %d", sid, fi.Size(), expectedShardSize)
		}
	}
}

// TestLoadEcShardsOrphanWithoutSiblingEcx exercises the truly-orphaned
// case from issue #9212: shard files exist on a disk but no .ecx exists
// anywhere on the volume server. We must not crash, and we must leave the
// shard files alone so an operator can restore the index later.
func TestLoadEcShardsOrphanWithoutSiblingEcx(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	if err := os.MkdirAll(dir0, 0o755); err != nil {
		t.Fatalf("mkdir dir0: %v", err)
	}
	if err := os.MkdirAll(dir1, 0o755); err != nil {
		t.Fatalf("mkdir dir1: %v", err)
	}

	collection := "grafana-loki"
	vid := needle.VolumeId(2222)
	const dataShards = 10
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)

	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	for _, sid := range []int{0, 12} {
		f, err := os.Create(base0 + erasure_coding.ToExt(sid))
		if err != nil {
			t.Fatalf("create shard %d: %v", sid, err)
		}
		if err := f.Truncate(expectedShardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d: %v", sid, err)
		}
		f.Close()
	}

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		"",
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.HardDriveType},
		nil,
		3,
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

	loc0 := store.Locations[0]
	if _, found := loc0.FindEcShard(vid, 0); found {
		t.Errorf("shard 0 should remain unloaded when no .ecx exists anywhere; reconciliation must not fabricate one")
	}

	for _, sid := range []int{0, 12} {
		shardPath := base0 + erasure_coding.ToExt(sid)
		fi, err := os.Stat(shardPath)
		if err != nil {
			t.Errorf("orphan shard %d destroyed by reconciliation: %v", sid, err)
			continue
		}
		if fi.Size() != expectedShardSize {
			t.Errorf("orphan shard %d truncated: size %d, want %d", sid, fi.Size(), expectedShardSize)
		}
	}
}

// TestReconcileNoOpWhenEachDiskIsSelfContained guards against the
// reconciliation pass double-loading shards or stomping on EcVolumes that
// were already populated by the per-disk pass.
func TestReconcileNoOpWhenEachDiskIsSelfContained(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	if err := os.MkdirAll(dir0, 0o755); err != nil {
		t.Fatalf("mkdir dir0: %v", err)
	}
	if err := os.MkdirAll(dir1, 0o755); err != nil {
		t.Fatalf("mkdir dir1: %v", err)
	}

	collection := "grafana-loki"
	vid := needle.VolumeId(3333)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)

	writeFullEcLayout := func(dir string, shardIds []int) {
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		for _, sid := range shardIds {
			f, err := os.Create(base + erasure_coding.ToExt(sid))
			if err != nil {
				t.Fatalf("create shard %d in %s: %v", sid, dir, err)
			}
			if err := f.Truncate(expectedShardSize); err != nil {
				f.Close()
				t.Fatalf("truncate shard %d in %s: %v", sid, dir, err)
			}
			f.Close()
		}
		if err := os.WriteFile(base+".ecx", make([]byte, 20), 0o644); err != nil {
			t.Fatalf("write .ecx in %s: %v", dir, err)
		}
		if err := os.WriteFile(base+".ecj", nil, 0o644); err != nil {
			t.Fatalf("write .ecj in %s: %v", dir, err)
		}
		if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
			Version:     uint32(needle.Version3),
			DatFileSize: datSize,
			EcShardConfig: &volume_server_pb.EcShardConfig{
				DataShards:   dataShards,
				ParityShards: parityShards,
			},
		}); err != nil {
			t.Fatalf("save .vif in %s: %v", dir, err)
		}
	}

	// Each disk is a fully self-contained EC layout — reconciliation
	// should leave them untouched.
	writeFullEcLayout(dir0, []int{0, 4})
	writeFullEcLayout(dir1, []int{8, 12})

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		"",
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.HardDriveType},
		nil,
		3,
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

	for diskIdx, sids := range [][]erasure_coding.ShardId{{0, 4}, {8, 12}} {
		loc := store.Locations[diskIdx]
		ev, found := loc.FindEcVolume(vid)
		if !found {
			t.Fatalf("disk %d: EcVolume for vid %d not loaded", diskIdx, vid)
		}
		if got := len(ev.Shards); got != len(sids) {
			t.Errorf("disk %d: EcVolume has %d shards, want %d (reconciliation may have double-loaded)", diskIdx, got, len(sids))
		}
		for _, sid := range sids {
			if _, ok := loc.FindEcShard(vid, sid); !ok {
				t.Errorf("disk %d: shard %d missing", diskIdx, sid)
			}
		}
	}
}

// TestLoadEcShardsWhenOwnerEcxIsInDataDir covers the legacy layout flagged
// in PR #9244 review: -dir.idx is configured (so every DiskLocation has
// IdxDirectory != Directory), but the owner's .ecx / .ecj / .vif were
// written into the owner's data dir before -dir.idx was set. indexEcxOwners
// must record the directory the .ecx was actually found in (Directory),
// not just the owner's IdxDirectory — otherwise NewEcVolume's same-disk
// fallback retries the orphan disk's data dir and ENOENTs there too.
func TestLoadEcShardsWhenOwnerEcxIsInDataDir(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1") // orphan: shards only
	dir1 := filepath.Join(tempDir, "data2") // owner: .ecx in data dir
	idxDir := filepath.Join(tempDir, "idx") // shared idx folder, intentionally empty
	for _, d := range []string{dir0, dir1, idxDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection := "grafana-loki"
	vid := needle.VolumeId(4242)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)

	writeShard := func(dir string, shardId int) {
		t.Helper()
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		f, err := os.Create(base + erasure_coding.ToExt(shardId))
		if err != nil {
			t.Fatalf("create shard %d in %s: %v", shardId, dir, err)
		}
		if err := f.Truncate(expectedShardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d in %s: %v", shardId, dir, err)
		}
		f.Close()
	}

	writeShard(dir0, 0)
	writeShard(dir0, 12)
	writeShard(dir1, 1)

	// Owner's .ecx / .ecj / .vif live in dir1 (data dir), NOT idxDir.
	// This mirrors a server that ran without -dir.idx, then later got it
	// configured — the index files stay in their original on-disk home.
	base1Data := erasure_coding.EcShardFileName(collection, dir1, int(vid))
	if err := os.WriteFile(base1Data+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("write .ecx in data dir: %v", err)
	}
	if err := os.WriteFile(base1Data+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj in data dir: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base1Data+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   dataShards,
			ParityShards: parityShards,
		},
	}); err != nil {
		t.Fatalf("save .vif in data dir: %v", err)
	}

	// idxDir is configured but intentionally empty for this volume — we
	// want IdxDirectory != Directory while the .ecx lives in Directory.
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		idxDir,
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.HardDriveType},
		nil,
		3,
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

	loc1 := store.Locations[1]
	if _, found := loc1.FindEcShard(vid, 1); !found {
		t.Fatalf("baseline broken: shard 1 on dir1 (which has .ecx in its data dir) was not loaded")
	}

	loc0 := store.Locations[0]
	for _, sid := range []erasure_coding.ShardId{0, 12} {
		if _, found := loc0.FindEcShard(vid, sid); !found {
			t.Errorf("issue #9212 (PR #9244 review): orphan shard %d on dir0 not loaded; reconcile pointed loader at IdxDirectory but .ecx was actually in owner.Directory", sid)
		}
	}
}
