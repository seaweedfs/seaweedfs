package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// dataShardCount is the data-shard count threaded into
// FindEcShardTargetLocation by these tests. Kept as a literal so the test
// stays valid when enterprise builds use a different default ratio.
const dataShardCount = 10

// TestFindEcShardTargetLocation_PinsToEcxOnDisk reproduces the placement
// half of issue #9212. ec.rebuild copies the .ecx alongside the first
// shard, then sends subsequent shards with CopyEcxFile=false relying on
// the volume server's auto-select to land them on the same disk. The
// volume isn't mounted yet, so FindEcVolume can't see the .ecx — without
// an on-disk check the selection falls back to "any HDD with free space"
// and shards end up split from their index files across disks of the
// same node.
//
// The fix: FindEcShardTargetLocation also looks for the .ecx on disk
// before falling through to the generic disk-space heuristic.
func TestFindEcShardTargetLocation_PinsToEcxOnDisk(t *testing.T) {
	store := newEcTargetTestStore(t, 3)
	collection := "grafana-loki"
	vid := needle.VolumeId(1093)

	// Drop a sealed .ecx onto disk 2. Nothing is mounted yet — this is
	// the state right after ec.rebuild's first VolumeEcShardsCopy with
	// CopyEcxFile=true and before any VolumeEcShardsMount has run.
	base := erasure_coding.EcShardFileName(collection, store.Locations[2].IdxDirectory, int(vid))
	if err := os.WriteFile(base+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("seed .ecx on disk 2: %v", err)
	}

	got := store.FindEcShardTargetLocation(collection, vid, dataShardCount)
	if got == nil {
		t.Fatalf("FindEcShardTargetLocation returned nil; expected disk 2")
	}
	if got != store.Locations[2] {
		t.Errorf("placement leaked off the .ecx-owning disk: got %s, want %s (issue #9212)",
			got.Directory, store.Locations[2].Directory)
	}
}

// TestFindEcShardTargetLocation_PrefersMountedOverEcx checks that an
// already-mounted EC volume on disk 1 wins over a stray .ecx on disk 2.
// This protects the post-startup steady state from being perturbed by
// leftover index files from a prior failed move.
func TestFindEcShardTargetLocation_PrefersMountedOverEcx(t *testing.T) {
	store := newEcTargetTestStore(t, 3)
	collection := "grafana-loki"
	vid := needle.VolumeId(2222)

	// Mount a placeholder EC volume on disk 1 so FindEcVolume returns it.
	loc1 := store.Locations[1]
	loc1.ecVolumesLock.Lock()
	loc1.ecVolumes[vid] = &erasure_coding.EcVolume{VolumeId: vid, Collection: collection}
	loc1.ecVolumesLock.Unlock()

	// Drop a stray .ecx on disk 2 to make sure it does NOT win.
	base := erasure_coding.EcShardFileName(collection, store.Locations[2].IdxDirectory, int(vid))
	if err := os.WriteFile(base+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("seed .ecx on disk 2: %v", err)
	}

	got := store.FindEcShardTargetLocation(collection, vid, dataShardCount)
	if got != loc1 {
		t.Errorf("placement should follow mounted EC volume on disk 1, got %v", got)
	}
}

// TestFindEcShardTargetLocation_FallsThroughToHddWhenNothingMatches keeps
// the existing fallback behaviour intact for the cold-volume case (no
// mount, no .ecx anywhere on this server).
func TestFindEcShardTargetLocation_FallsThroughToHddWhenNothingMatches(t *testing.T) {
	store := newEcTargetTestStore(t, 2)
	collection := "grafana-loki"
	vid := needle.VolumeId(3333)

	got := store.FindEcShardTargetLocation(collection, vid, dataShardCount)
	if got == nil {
		t.Fatalf("FindEcShardTargetLocation returned nil; expected an HDD fallback")
	}
	if got.DiskType != types.HardDriveType {
		t.Errorf("fallback should pick an HDD; got disk type %q", got.DiskType)
	}
}

// TestFindEcShardTargetLocation_HonoursUnlimitedDisk pins the
// MaxVolumeCount==0 ("unlimited") convention shared with
// hasFreeDiskLocation. ecFreeShardCount used to return a negative free
// count for unlimited disks, which made FindEcShardTargetLocation skip
// them entirely. PR #9245 review by @gemini-code-assist.
func TestFindEcShardTargetLocation_HonoursUnlimitedDisk(t *testing.T) {
	store := newEcTargetTestStore(t, 1)
	store.Locations[0].MaxVolumeCount = 0 // unlimited

	got := store.FindEcShardTargetLocation("grafana-loki", needle.VolumeId(4444), dataShardCount)
	if got == nil {
		t.Fatalf("FindEcShardTargetLocation returned nil for an unlimited (MaxVolumeCount=0) disk")
	}
	if got != store.Locations[0] {
		t.Errorf("expected the only (unlimited) disk to be picked; got %v", got)
	}
}

// TestFindEcShardTargetLocation_TightProvisioningKeepsEcxDisk pins the
// truncation hazard PR #9245 review by @coderabbitai surfaced.
//
// With MaxVolumeCount=1, VolumesLen=0, and one EC shard already on the
// disk, the previous formula (free = (1*10 - 1) / 10 = 0) would treat
// the disk as full and route subsequent shards to a different disk —
// exactly the orphan-shard layout this PR exists to prevent. The fix
// keeps the free count in shard slots, so 9 free slots is reported as
// 9 rather than rounded down to 0.
func TestFindEcShardTargetLocation_TightProvisioningKeepsEcxDisk(t *testing.T) {
	store := newEcTargetTestStore(t, 2)
	store.Locations[0].MaxVolumeCount = 1
	store.Locations[1].MaxVolumeCount = 1

	collection := "grafana-loki"
	vid := needle.VolumeId(5555)

	// Seed disk 1 with a single EC shard for this volume so it owns the
	// .ecx and has 9 free shard slots remaining; the old formula would
	// have rounded that to 0.
	loc1 := store.Locations[1]
	loc1.ecVolumesLock.Lock()
	loc1.ecVolumes[vid] = &erasure_coding.EcVolume{
		VolumeId:   vid,
		Collection: collection,
		Shards:     []*erasure_coding.EcVolumeShard{{VolumeId: vid, ShardId: 0, Collection: collection}},
	}
	loc1.ecVolumesLock.Unlock()

	got := store.FindEcShardTargetLocation(collection, vid, dataShardCount)
	if got != loc1 {
		t.Errorf("expected the .ecx-owning disk (1 shard placed, 9 free shard slots) to be picked; got %v", got)
	}
}

// TestFindEcShardTargetLocation_PinsToTheDiskOwningTheShard covers the
// per-server invariant that a shard id is owned by at most one disk.
//
// A multi-disk server legitimately mounts one vid on several disks, each
// holding a disjoint subset of the shards, so the mounted tier ties and the
// free-count tie-break decides — and it points at whichever disk happens to
// be emptier, not at the disk that already has this shard. A re-copy of a
// shard the server already holds (a retried ec.balance / ec.rebuild move)
// then lands a second copy on the sibling disk, and both disks register the
// same shard id.
func TestFindEcShardTargetLocation_PinsToTheDiskOwningTheShard(t *testing.T) {
	store := newEcTargetTestStore(t, 2)
	collection := "grafana-loki"
	vid := needle.VolumeId(8888)

	// Disk 0 owns shards 0 and 1, disk 1 owns shard 2 — so disk 1 is the
	// emptier of the two and wins the free-count tie-break.
	mountEcShards(store.Locations[0], collection, vid, 0, 1)
	mountEcShards(store.Locations[1], collection, vid, 2)

	if got := store.FindEcShardTargetLocation(collection, vid, dataShardCount, 0); got != store.Locations[0] {
		t.Errorf("a copy of shard 0 left its owning disk: got %v, want %s", got, store.Locations[0].Directory)
	}
	if got := store.FindEcShardTargetLocation(collection, vid, dataShardCount, 2); got != store.Locations[1] {
		t.Errorf("a copy of shard 2 left its owning disk: got %v, want %s", got, store.Locations[1].Directory)
	}

	// A shard no disk owns yet is placed by the unchanged waterfall: both
	// disks have it mounted, so the emptier one wins.
	if got := store.FindEcShardTargetLocation(collection, vid, dataShardCount, 7); got != store.Locations[1] {
		t.Errorf("placement of an unclaimed shard changed: got %v, want the emptier mounted disk", got)
	}
}

// TestFindEcShardTargetLocation_OwningDiskWinsWhenFull pins the second half
// of the invariant: a disk with no free shard slots still wins for a shard it
// already owns. Re-copying that shard overwrites bytes the disk is already
// accounted for, while the alternative — routing to a sibling — splits the
// claim across two disks.
func TestFindEcShardTargetLocation_OwningDiskWinsWhenFull(t *testing.T) {
	store := newEcTargetTestStore(t, 2)
	collection := "grafana-loki"
	vid := needle.VolumeId(9999)

	store.Locations[0].MaxVolumeCount = 1
	mountEcShards(store.Locations[0], collection, vid, 0)
	// Fill disk 0 past its shard-slot budget so ecFreeShardCount reports 0.
	filler := needle.VolumeId(10000)
	fillerShards := make([]erasure_coding.ShardId, dataShardCount)
	for i := range fillerShards {
		fillerShards[i] = erasure_coding.ShardId(i)
	}
	mountEcShards(store.Locations[0], collection, filler, fillerShards...)

	if got := store.FindEcShardTargetLocation(collection, vid, dataShardCount, 0); got != store.Locations[0] {
		t.Errorf("a copy of shard 0 left its owning disk when the disk was full: got %v", got)
	}
	// A shard it does not own still respects the space filter.
	if got := store.FindEcShardTargetLocation(collection, vid, dataShardCount, 7); got != store.Locations[1] {
		t.Errorf("an unclaimed shard should go to the disk with free slots: got %v", got)
	}
}

// mountEcShards registers an EcVolume for vid on loc claiming shardIds.
func mountEcShards(loc *DiskLocation, collection string, vid needle.VolumeId, shardIds ...erasure_coding.ShardId) {
	ecVolume := &erasure_coding.EcVolume{VolumeId: vid, Collection: collection}
	for _, shardId := range shardIds {
		ecVolume.Shards = append(ecVolume.Shards, &erasure_coding.EcVolumeShard{
			VolumeId: vid, ShardId: shardId, Collection: collection,
		})
	}
	loc.ecVolumesLock.Lock()
	loc.ecVolumes[vid] = ecVolume
	loc.ecVolumesLock.Unlock()
}

// newEcTargetTestStore is a leaner cousin of the helper in
// store_load_balancing_test.go: it spins up an in-memory Store with N
// HDD disk locations under a single t.TempDir and consumes any heartbeat
// channel traffic so the placement helpers can be exercised directly.
func newEcTargetTestStore(t *testing.T, numDirs int) *Store {
	t.Helper()
	tempDir := t.TempDir()
	dirs := make([]string, 0, numDirs)
	maxCounts := make([]int32, 0, numDirs)
	minFreeSpaces := make([]util.MinFreeSpace, 0, numDirs)
	diskTypes := make([]types.DiskType, 0, numDirs)
	for i := 0; i < numDirs; i++ {
		dir := filepath.Join(tempDir, "data", filepath.Base(t.Name())+"-"+string(rune('a'+i)))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		dirs = append(dirs, dir)
		maxCounts = append(maxCounts, 100)
		minFreeSpaces = append(minFreeSpaces, util.MinFreeSpace{})
		diskTypes = append(diskTypes, types.HardDriveType)
	}
	diskIOProbeConfig := stats.DefaultDiskIOProbeConfig()
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		dirs, maxCounts, minFreeSpaces, "", NeedleMapInMemory, diskTypes, nil, 3, diskIOProbeConfig,
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
