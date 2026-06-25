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

// newDrainingStore builds a Store over dirs and drains its announcement
// channels in the background so loads never block. Files planted before this
// call are picked up by the startup loaders; files planted after are not.
func newDrainingStore(t *testing.T, dirs []string) *Store {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	maxCounts := make([]int32, len(dirs))
	minFree := make([]util.MinFreeSpace, len(dirs))
	diskTypes := make([]types.DiskType, len(dirs))
	for i := range dirs {
		maxCounts[i] = 100
		diskTypes[i] = types.HardDriveType
	}
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		dirs, maxCounts, minFree, "",
		NeedleMapInMemory, diskTypes, nil, 3, stats.DefaultDiskIOProbeConfig())
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
	return store
}

func plantEcShard(t *testing.T, dir, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, size int64) {
	t.Helper()
	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	f, err := os.Create(base + erasure_coding.ToExt(int(shardId)))
	if err != nil {
		t.Fatalf("create shard %d.%d: %v", vid, shardId, err)
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		t.Fatalf("truncate shard %d.%d: %v", vid, shardId, err)
	}
	f.Close()
}

func plantEcIndex(t *testing.T, idxDir, dataDir, collection string, vid needle.VolumeId, datSize int64) {
	t.Helper()
	idxBase := erasure_coding.EcShardFileName(collection, idxDir, int(vid))
	if err := os.WriteFile(idxBase+".ecx", make([]byte, types.NeedleMapEntrySize), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(idxBase+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
	dataBase := erasure_coding.EcShardFileName(collection, dataDir, int(vid))
	if err := volume_info.SaveVolumeInfo(dataBase+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   10,
			ParityShards: 4,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}
}

// TestEcIndexRecovery_CrossServerOrphan reproduces issue #10104: a volume
// server reboots with EC shards on disk but no .ecx index anywhere local (the
// index lives only on a peer server). The startup loaders leave the shards
// unmounted, so the master never learns about them. After the recovery path
// drops the index fetched from a peer onto the local disk, MountRecoveredEcShards
// must mount and announce the shards.
func TestEcIndexRecovery_CrossServerOrphan(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")

	const collection = "video-recordings"
	vid := needle.VolumeId(6190)
	const datSize int64 = 10 * 1024 * 1024
	shardSize := calculateExpectedShardSize(datSize, 10)

	// Pre-seed the on-disk layout the issue describes: shards spread across
	// this server's disks, with no .ecx / .ecj / .vif on any of them.
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	plantEcShard(t, dir0, collection, vid, 0, shardSize)
	plantEcShard(t, dir0, collection, vid, 5, shardSize)
	plantEcShard(t, dir1, collection, vid, 6, shardSize)

	store := newDrainingStore(t, []string{dir0, dir1})

	// Bug reproduction: the startup loaders left every shard unmounted.
	if _, found := store.FindEcVolume(vid); found {
		t.Fatalf("EC volume %d unexpectedly mounted without any local .ecx", vid)
	}

	// The recovery scan must surface this volume as missing its index.
	m, ok := findMissingIndex(store.CollectEcVolumesMissingIndex(), collection, vid)
	if !ok {
		t.Fatalf("CollectEcVolumesMissingIndex did not report volume %d as a cross-server orphan", vid)
	}

	// Simulate the peer fetch: the server-side path copies .ecx/.ecj/.vif from a
	// peer that still has them into m.IdxDir / m.DataDir.
	plantEcIndex(t, m.IdxDir, m.DataDir, collection, vid, datSize)

	store.MountRecoveredEcShards()

	// All shards must now be mounted on the disks that physically hold them.
	loc0 := store.Locations[0]
	ev, found := loc0.FindEcVolume(vid)
	if !found {
		t.Fatalf("EC volume %d not mounted on disk0 after index recovery", vid)
	}
	for _, sid := range []erasure_coding.ShardId{0, 5} {
		if _, ok := ev.FindEcVolumeShard(sid); !ok {
			t.Errorf("shard %d.%d not registered on disk0 after recovery", vid, sid)
		}
	}
	loc1 := store.Locations[1]
	ev1, found := loc1.FindEcVolume(vid)
	if !found {
		t.Fatalf("EC volume %d not mounted on disk1 after index recovery", vid)
	}
	if _, ok := ev1.FindEcVolumeShard(6); !ok {
		t.Errorf("shard %d.6 not registered on disk1 after recovery", vid)
	}

	// A second scan must report nothing left to recover.
	if _, again := findMissingIndex(store.CollectEcVolumesMissingIndex(), collection, vid); again {
		t.Errorf("CollectEcVolumesMissingIndex still reports volume %d after recovery", vid)
	}
}

// findMissingIndex returns the entry for (collection, vid) among the reported
// missing-index volumes, if present.
func findMissingIndex(missing []EcVolumeMissingIndex, collection string, vid needle.VolumeId) (EcVolumeMissingIndex, bool) {
	for _, m := range missing {
		if m.Collection == collection && m.VolumeId == vid {
			return m, true
		}
	}
	return EcVolumeMissingIndex{}, false
}

// TestCollectEcVolumesMissingIndex_ExcludesCrossDiskOrphan guards the boundary
// with the existing same-server reconcile: a volume whose .ecx merely sits on a
// sibling disk is NOT a cross-server orphan and must not be reported here —
// mirrorEcMetadataToShardDisks / reconcileEcShardsAcrossDisks already mount it.
func TestCollectEcVolumesMissingIndex_ExcludesCrossDiskOrphan(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	store := newDrainingStore(t, []string{dir0, dir1})

	const collection = "mybucket"
	vid := needle.VolumeId(42)
	const datSize int64 = 10 * 1024 * 1024
	shardSize := calculateExpectedShardSize(datSize, 10)

	// Plant after NewStore so the startup reconcile does not consume the layout;
	// CollectEcVolumesMissingIndex is what's under test. Shard on disk0, index on
	// disk1 — the cross-disk layout handled by the same-server reconcile.
	plantEcShard(t, dir0, collection, vid, 3, shardSize)
	plantEcIndex(t, dir1, dir1, collection, vid, datSize)

	if _, missing := findMissingIndex(store.CollectEcVolumesMissingIndex(), collection, vid); missing {
		t.Errorf("cross-disk .ecx must not be reported as a cross-server orphan")
	}
}
