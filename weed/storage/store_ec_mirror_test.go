package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Orphan layout: shards on dir0, sidecars on dir1. After NewStore,
// dir0 must own a local copy of every sidecar.
func TestMirrorEcMetadataOnStartup_PhysicallyCopiesSidecars(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection := "video-recordings"
	vid := needle.VolumeId(4121)
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
	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))

	ecxBytes := bytes.Repeat([]byte{0xA1}, 20)
	ecjBytes := bytes.Repeat([]byte{0xB2}, 16)
	if err := os.WriteFile(base1+".ecx", ecxBytes, 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(base1+".ecj", ecjBytes, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
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

	// .vif drifts on first mount via version-correction, so byte
	// equality is only the right invariant for .ecx and .ecj.
	for _, ext := range []string{".ecx", ".ecj"} {
		gotBytes, err := os.ReadFile(base0 + ext)
		if err != nil {
			t.Errorf("sidecar %s not mirrored to dir0: %v", ext, err)
			continue
		}
		wantBytes, err := os.ReadFile(base1 + ext)
		if err != nil {
			t.Fatalf("read source %s: %v", base1+ext, err)
		}
		if !bytes.Equal(gotBytes, wantBytes) {
			t.Errorf("sidecar %s content mismatch between dir0 and dir1", ext)
		}
	}

	dir0Info, _, found, err := volume_info.MaybeLoadVolumeInfo(base0 + ".vif")
	if err != nil || !found {
		t.Errorf("dir0 .vif missing or unreadable after mirror: err=%v found=%v", err, found)
	} else if dir0Info.EcShardConfig == nil ||
		dir0Info.EcShardConfig.DataShards != dataShards ||
		dir0Info.EcShardConfig.ParityShards != parityShards {
		t.Errorf("dir0 .vif has wrong EC ratio after mirror: got %+v, want %d+%d",
			dir0Info.EcShardConfig, dataShards, parityShards)
	}

	loc0 := store.Locations[0]
	ev0, found := loc0.FindEcVolume(vid)
	if !found {
		t.Fatalf("dir0 EcVolume %d not loaded after startup mirror", vid)
	}
	for _, sid := range []erasure_coding.ShardId{0, 12} {
		if _, ok := ev0.FindEcVolumeShard(sid); !ok {
			t.Errorf("shard %d not registered on dir0 after mirror", sid)
		}
	}
	if got, want := filepath.Dir(ev0.FileName(".ecx")), dir0; got != want {
		t.Errorf("dir0 EcVolume .ecx resolved at %q, want %q (local mirrored copy)", got, want)
	}

	loc1 := store.Locations[1]
	ev1, found := loc1.FindEcVolume(vid)
	if !found {
		t.Fatalf("dir1 EcVolume %d not loaded after startup mirror", vid)
	}
	if _, ok := ev1.FindEcVolumeShard(1); !ok {
		t.Errorf("dir1 shard 1 missing after mirror")
	}

	for _, sid := range []int{0, 12} {
		shardPath := base0 + erasure_coding.ToExt(sid)
		fi, err := os.Stat(shardPath)
		if err != nil {
			t.Errorf("shard %d on dir0 lost after mirror: %v", sid, err)
			continue
		}
		if fi.Size() != expectedShardSize {
			t.Errorf("shard %d on dir0 truncated by mirror pass: size %d, want %d", sid, fi.Size(), expectedShardSize)
		}
	}
}

// A pre-existing destination .ecx must not be overwritten — local
// copies are authoritative because they may have absorbed delete
// journal updates not yet on the owner.
func TestMirrorEcMetadataOnStartup_NoOpWhenAlreadyMirrored(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection := "video-recordings"
	vid := needle.VolumeId(7777)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)

	plantShard := func(dir string, shardId int) {
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

	plantSidecars := func(dir string, ecx, ecj []byte) {
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		if err := os.WriteFile(base+".ecx", ecx, 0o644); err != nil {
			t.Fatalf("write %s.ecx: %v", base, err)
		}
		if err := os.WriteFile(base+".ecj", ecj, 0o644); err != nil {
			t.Fatalf("write %s.ecj: %v", base, err)
		}
		if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
			Version:     uint32(needle.Version3),
			DatFileSize: datSize,
			EcShardConfig: &volume_server_pb.EcShardConfig{
				DataShards:   dataShards,
				ParityShards: parityShards,
			},
		}); err != nil {
			t.Fatalf("save %s.vif: %v", base, err)
		}
	}

	plantShard(dir0, 0)
	plantShard(dir0, 12)
	plantShard(dir1, 1)

	// Deliberately different .ecx bytes on each side so any overwrite
	// shows up as a diff. .ecx never rewrites at mount, unlike .vif.
	ecxOwner := bytes.Repeat([]byte{0xC3}, 20)
	ecxLocal := bytes.Repeat([]byte{0x5A}, 20)
	ecjBytes := bytes.Repeat([]byte{0xD4}, 16)
	plantSidecars(dir1, ecxOwner, ecjBytes)
	plantSidecars(dir0, ecxLocal, ecjBytes)

	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))

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

	postEcx, err := os.ReadFile(base0 + ".ecx")
	if err != nil {
		t.Fatalf("read dir0 .ecx after NewStore: %v", err)
	}
	if !bytes.Equal(ecxLocal, postEcx) {
		t.Errorf("dir0 .ecx was overwritten by mirror pass; pre-existing destination must be preserved (got first 4 bytes %x, want %x)",
			postEcx[:4], ecxLocal[:4])
	}

	if loc0 := store.Locations[0]; loc0 == nil {
		t.Fatal("loc0 unexpectedly nil")
	} else if ev, found := loc0.FindEcVolume(vid); !found {
		t.Errorf("dir0 EcVolume %d not loaded", vid)
	} else if got, want := filepath.Dir(ev.FileName(".ecx")), dir0; got != want {
		t.Errorf("dir0 .ecx resolved at %q, want %q", got, want)
	}
}

// Shards on a disk that still holds the source volume's config-free .vif must
// mount with the owner's encode identity after a restart: mirroring the index
// next to that .vif would make the disk mount through it.
func TestMirrorEcMetadataOnStartup_KeepsConfigFreeVifDiskCrossDisk(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection := "c"
	vid := needle.VolumeId(4122)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	const encodeTsNs = int64(1_700_000_000_000_000_042)
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)
	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))
	for base, shardId := range map[string]int{base0: 2, base1: 0} {
		if err := os.WriteFile(base+erasure_coding.ToExt(shardId), nil, 0o644); err != nil {
			t.Fatalf("create shard: %v", err)
		}
		if err := os.Truncate(base+erasure_coding.ToExt(shardId), expectedShardSize); err != nil {
			t.Fatalf("size shard: %v", err)
		}
	}
	if err := volume_info.SaveVolumeInfo(base0+".vif", &volume_server_pb.VolumeInfo{
		Version:  uint32(needle.Version3),
		ReadOnly: true,
	}); err != nil {
		t.Fatalf("save source .vif: %v", err)
	}
	if err := os.WriteFile(base1+".ecx", bytes.Repeat([]byte{0xA1}, 20), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base1+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   dataShards,
			ParityShards: parityShards,
			EncodeTsNs:   encodeTsNs,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
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
		stats.DefaultDiskIOProbeConfig(),
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

	if _, err := os.Stat(base0 + ".ecx"); !os.IsNotExist(err) {
		t.Errorf(".ecx mirrored next to the config-free .vif: %v", err)
	}
	ev0, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatalf("dir0 EcVolume %d not loaded", vid)
	}
	if _, ok := ev0.FindEcVolumeShard(2); !ok {
		t.Errorf("shard 2 not registered on dir0")
	}
	if ev0.EncodeTsNs != encodeTsNs {
		t.Errorf("dir0 EncodeTsNs = %d, want %d", ev0.EncodeTsNs, encodeTsNs)
	}
}

// With a shared -dir.idx every disk sees the .ecx, and only the disk that
// received it holds the encode's .vif. A sibling disk that still holds the
// source volume's config-free .vif must load its shards with that identity.
func TestLoadEcShardsOnStartup_SharedIndexTakesSiblingVif(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "data1")
	dir1 := filepath.Join(tempDir, "data2")
	idxDir := filepath.Join(tempDir, "idx")
	for _, d := range []string{dir0, dir1, idxDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection := "c"
	vid := needle.VolumeId(4123)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	const encodeTsNs = int64(1_700_000_000_000_000_043)
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)
	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))
	for base, shardId := range map[string]int{base0: 2, base1: 0} {
		if err := os.WriteFile(base+erasure_coding.ToExt(shardId), nil, 0o644); err != nil {
			t.Fatalf("create shard: %v", err)
		}
		if err := os.Truncate(base+erasure_coding.ToExt(shardId), expectedShardSize); err != nil {
			t.Fatalf("size shard: %v", err)
		}
	}
	if err := volume_info.SaveVolumeInfo(base0+".vif", &volume_server_pb.VolumeInfo{
		Version:  uint32(needle.Version3),
		ReadOnly: true,
	}); err != nil {
		t.Fatalf("save source .vif: %v", err)
	}
	if err := os.WriteFile(erasure_coding.EcShardFileName(collection, idxDir, int(vid))+".ecx", bytes.Repeat([]byte{0xA1}, 20), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base1+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   dataShards,
			ParityShards: parityShards,
			EncodeTsNs:   encodeTsNs,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		idxDir,
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.HardDriveType},
		nil,
		3,
		stats.DefaultDiskIOProbeConfig(),
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

	for i, shardId := range []erasure_coding.ShardId{2, 0} {
		ev, found := store.Locations[i].FindEcVolume(vid)
		if !found {
			t.Fatalf("disk %d EcVolume %d not loaded", i, vid)
		}
		if _, ok := ev.FindEcVolumeShard(shardId); !ok {
			t.Errorf("shard %d not registered on disk %d", shardId, i)
		}
		if ev.EncodeTsNs != encodeTsNs {
			t.Errorf("disk %d EncodeTsNs = %d, want %d", i, ev.EncodeTsNs, encodeTsNs)
		}
	}
}

func TestLoadEcShardsOnStartup_SharedIndexSiblingVifSizesValidation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		siblingRuns []int64
	}{
		{"one sibling", []int64{1_700_000_000_000_000_044}},
		{"siblings name different runs", []int64{1_700_000_000_000_000_044, 1_700_000_000_000_000_045}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			dirs := []string{filepath.Join(tempDir, "data0")}
			for i := range tc.siblingRuns {
				dirs = append(dirs, filepath.Join(tempDir, fmt.Sprintf("data%d", i+1)))
			}
			idxDir := filepath.Join(tempDir, "idx")
			for _, d := range append([]string{idxDir}, dirs...) {
				if err := os.MkdirAll(d, 0o755); err != nil {
					t.Fatalf("mkdir %s: %v", d, err)
				}
			}

			collection := "c"
			vid := needle.VolumeId(4124)
			const dataShards, parityShards = 12, 4
			const datSize int64 = 11 * 1024 * 1024
			shardSize := calculateExpectedShardSize(datSize, dataShards)
			if shardSize >= calculateExpectedShardSize(datSize, erasure_coding.DataShardsCount) {
				t.Fatalf("test premise broken: default-ratio shard size must exceed %d", shardSize)
			}
			base0 := erasure_coding.EcShardFileName(collection, dirs[0], int(vid))
			if err := os.WriteFile(base0+".dat", make([]byte, datSize), 0o644); err != nil {
				t.Fatalf("write .dat: %v", err)
			}
			if err := os.WriteFile(base0+erasure_coding.ToExt(2), nil, 0o644); err != nil {
				t.Fatalf("create shard: %v", err)
			}
			if err := os.Truncate(base0+erasure_coding.ToExt(2), shardSize); err != nil {
				t.Fatalf("size shard: %v", err)
			}
			if err := volume_info.SaveVolumeInfo(base0+".vif", &volume_server_pb.VolumeInfo{
				Version:  uint32(needle.Version3),
				ReadOnly: true,
			}); err != nil {
				t.Fatalf("save source .vif: %v", err)
			}
			ecx := erasure_coding.EcShardFileName(collection, idxDir, int(vid)) + ".ecx"
			if err := os.WriteFile(ecx, bytes.Repeat([]byte{0xA1}, 20), 0o644); err != nil {
				t.Fatalf("write .ecx: %v", err)
			}
			for i, run := range tc.siblingRuns {
				if err := volume_info.SaveVolumeInfo(erasure_coding.EcShardFileName(collection, dirs[i+1], int(vid))+".vif", &volume_server_pb.VolumeInfo{
					Version:     uint32(needle.Version3),
					DatFileSize: datSize,
					EcShardConfig: &volume_server_pb.EcShardConfig{
						DataShards:   dataShards,
						ParityShards: parityShards,
						EncodeTsNs:   run,
					},
				}); err != nil {
					t.Fatalf("save .vif: %v", err)
				}
			}

			maxCounts := make([]int32, len(dirs))
			minFree := make([]util.MinFreeSpace, len(dirs))
			diskTypes := make([]types.DiskType, len(dirs))
			for i := range dirs {
				maxCounts[i], diskTypes[i] = 100, types.HardDriveType
			}
			store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
				dirs, maxCounts, minFree, idxDir, NeedleMapInMemory, diskTypes, nil, 3, stats.DefaultDiskIOProbeConfig())
			drainStoreChans(t, store)

			for _, path := range []string{base0 + ".dat", base0 + erasure_coding.ToExt(2), ecx} {
				if _, err := os.Stat(path); err != nil {
					t.Errorf("%s removed at startup: %v", path, err)
				}
			}
			if len(tc.siblingRuns) == 1 {
				ev, found := store.Locations[0].FindEcVolume(vid)
				if !found {
					t.Fatalf("disk 0 EcVolume %d not loaded", vid)
				}
				if _, ok := ev.FindEcVolumeShard(2); !ok {
					t.Errorf("shard 2 not registered on disk 0")
				}
			}
		})
	}
}
