package storage

import (
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
