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

// These tests walk one EC volume through the life a multi-disk production node
// gives it: shards spread across disks with the .ecx / .ecj / .vif sidecars on
// only one of them, then a balance moving the sidecar disk's shards away, then
// a restart, then a fresh shard arriving. Each transition is exercised against
// the real Store so the invariant under test is the one production depends on:
// every shard file on disk is either registered (visible to heartbeat and
// reads) or still on disk untouched — never silently dropped, never deleted.
//
// The layout mirrors a support case: a 10+4 volume on a node with a dozen
// disks, sidecars on the disk that ran the encode, shards scattered by
// ec.balance. The volume id and collection are kept from the case.

const (
	lifecycleCollection = "pm-itatiaiucu-01"
	lifecycleVolumeId   = needle.VolumeId(42561)
	lifecycleDatSize    = int64(10 * 1024 * 1024)
	lifecycleData       = 10
	lifecycleParity     = 4
)

// plantLifecycleShard creates a shard file of the exact size a real encode of
// lifecycleDatSize would produce, so size-based validation sees a consistent
// volume rather than an artifact of the fixture.
func plantLifecycleShard(t *testing.T, dir string, shardId int) {
	t.Helper()
	base := erasure_coding.EcShardFileName(lifecycleCollection, dir, int(lifecycleVolumeId))
	f, err := os.Create(base + erasure_coding.ToExt(shardId))
	if err != nil {
		t.Fatalf("create shard %d in %s: %v", shardId, dir, err)
	}
	defer f.Close()
	if err := f.Truncate(calculateExpectedShardSize(lifecycleDatSize, lifecycleData)); err != nil {
		t.Fatalf("size shard %d: %v", shardId, err)
	}
}

// plantLifecycleSidecars writes the .ecx / .ecj / .vif set on one disk. No
// .dat is planted: on a production EC node the .dat is gone after encode, and
// its absence is what marks the volume as distributed rather than as an
// interrupted local encode.
func plantLifecycleSidecars(t *testing.T, dir string) {
	t.Helper()
	base := erasure_coding.EcShardFileName(lifecycleCollection, dir, int(lifecycleVolumeId))
	if err := os.WriteFile(base+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(base+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: lifecycleDatSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   lifecycleData,
			ParityShards: lifecycleParity,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}
}

// newLifecycleStore starts a Store over dirs and returns it with a closer that
// also stops the notification drainer, so a test can restart the store over
// the same directories.
func newLifecycleStore(t *testing.T, dirs []string) (*Store, func()) {
	t.Helper()
	maxCounts := make([]int32, len(dirs))
	freeSpaces := make([]util.MinFreeSpace, len(dirs))
	diskTypes := make([]types.DiskType, len(dirs))
	for i := range dirs {
		maxCounts[i] = 100
		diskTypes[i] = types.HardDriveType
	}
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		dirs, maxCounts, freeSpaces, "", NeedleMapInMemory, diskTypes, nil, 3,
		stats.DefaultDiskIOProbeConfig())
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
	return store, func() {
		store.Close()
		close(done)
	}
}

// registeredShards reports which shard ids are registered on each disk, in
// the order of store.Locations — the store's answer to "what would heartbeat
// report", independent of what sits on disk.
func registeredShards(store *Store) map[int][]int {
	byDisk := make(map[int][]int)
	for diskId, loc := range store.Locations {
		if ecv, ok := loc.FindEcVolume(lifecycleVolumeId); ok {
			for _, sid := range ecv.ShardIdList() {
				byDisk[diskId] = append(byDisk[diskId], int(sid))
			}
		}
	}
	return byDisk
}

func countRegistered(store *Store) int {
	n := 0
	for _, ids := range registeredShards(store) {
		n += len(ids)
	}
	return n
}

// shardFilesOnDisk counts this volume's shard files under dir, the ground
// truth the registration view must never silently diverge from.
func shardFilesOnDisk(t *testing.T, dir string) int {
	t.Helper()
	base := erasure_coding.EcShardFileName(lifecycleCollection, dir, int(lifecycleVolumeId))
	n := 0
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		if _, err := os.Stat(base + erasure_coding.ToExt(i)); err == nil {
			n++
		}
	}
	return n
}

func TestMultiDiskLifecycle_SpreadLayout(t *testing.T) {
	tempDir := t.TempDir()
	dirs := []string{
		filepath.Join(tempDir, "disk0"),
		filepath.Join(tempDir, "disk1"),
		filepath.Join(tempDir, "disk2"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// The production layout: sidecars live with the first six shards on disk0,
	// the rest of the shards sit alone on disks 1 and 2.
	plantLifecycleSidecars(t, dirs[0])
	for sid := 0; sid <= 5; sid++ {
		plantLifecycleShard(t, dirs[0], sid)
	}
	for sid := 6; sid <= 10; sid++ {
		plantLifecycleShard(t, dirs[1], sid)
	}
	for sid := 11; sid <= 13; sid++ {
		plantLifecycleShard(t, dirs[2], sid)
	}

	// ── Phase A: cold start over the spread layout ──
	store, closeStore := newLifecycleStore(t, dirs)
	if got := registeredShards(store); len(got[0]) != 6 || len(got[1]) != 5 || len(got[2]) != 3 {
		t.Fatalf("cold start over spread layout registered %v, want 6/5/3 across the disks; "+
			"unregistered shards are invisible to heartbeat while their files sit on disk", got)
	}

	// ── Phase B: a balance moves the sidecar disk's shards away ──
	// Unmount and remove shards 0..5 from disk0. The sidecars must stay: disks
	// 1 and 2 still hold eight shards that read and delete through this .ecx.
	for sid := 0; sid <= 5; sid++ {
		if err := store.UnmountEcShards(lifecycleVolumeId, erasure_coding.ShardId(sid), 0); err != nil {
			t.Fatalf("unmount shard %d: %v", sid, err)
		}
		base := erasure_coding.EcShardFileName(lifecycleCollection, dirs[0], int(lifecycleVolumeId))
		if err := os.Remove(base + erasure_coding.ToExt(sid)); err != nil {
			t.Fatalf("remove shard %d: %v", sid, err)
		}
	}
	if got := countRegistered(store); got != 8 {
		t.Fatalf("after balancing away the sidecar disk's shards, %d shards registered, want 8: %v",
			got, registeredShards(store))
	}
	if _, _, found := store.FindEcVolumeWithShard(lifecycleVolumeId, 7); !found {
		t.Fatal("shard 7 lost its registration when the sidecar disk's shards moved away")
	}

	// ── Phase C: restart in that state ──
	// disk0 now holds only sidecars; every shard is on a disk with no local
	// .ecx. This is the steady state a balanced multi-disk node reboots in.
	closeStore()
	store, closeStore = newLifecycleStore(t, dirs)
	defer closeStore()

	if got := registeredShards(store); len(got[1]) != 5 || len(got[2]) != 3 {
		t.Fatalf("restart with sidecars on a shard-less disk registered %v, want 5 on disk1 and 3 on disk2; "+
			"shards that fail to register here exist on disk but are missing from the master's view — "+
			"rebuilds then count them missing and repair against the wrong set", got)
	}
	if n := shardFilesOnDisk(t, dirs[1]) + shardFilesOnDisk(t, dirs[2]); n != 8 {
		t.Fatalf("restart changed what is on disk: %d shard files, want 8 — startup must never delete distributed EC shards", n)
	}

	// ── Phase D: a fresh shard lands on a shard-only disk ──
	// A later balance copies shard 0 to disk2. The mount must find the .ecx on
	// disk0; failing that, the copy is unusable and the move that sent it
	// deletes the source after a copy nothing can read.
	plantLifecycleShard(t, dirs[2], 0)
	if err := store.MountEcShards(lifecycleCollection, lifecycleVolumeId, 0, ""); err != nil {
		t.Fatalf("mounting a shard delivered to a disk without local sidecars: %v", err)
	}
	if _, _, found := store.FindEcVolumeWithShard(lifecycleVolumeId, 0); !found {
		t.Fatal("shard 0 mounted without error but is not findable")
	}
}

// A dead disk that held the sidecars must degrade loudly but safely: the other
// disks' shards cannot serve reads without the .ecx, so they may drop out of
// the registered view, but their files must survive so restoring the sidecars
// (or the disk) restores the volume. Losing one disk of fourteen shards' worth
// of sidecars must never cost the other thirteen shards their data.
func TestMultiDiskLifecycle_SidecarDiskLost(t *testing.T) {
	tempDir := t.TempDir()
	dirs := []string{
		filepath.Join(tempDir, "disk0"),
		filepath.Join(tempDir, "disk1"),
		filepath.Join(tempDir, "disk2"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	plantLifecycleSidecars(t, dirs[0])
	for sid := 0; sid <= 5; sid++ {
		plantLifecycleShard(t, dirs[0], sid)
	}
	for sid := 6; sid <= 10; sid++ {
		plantLifecycleShard(t, dirs[1], sid)
	}
	for sid := 11; sid <= 13; sid++ {
		plantLifecycleShard(t, dirs[2], sid)
	}

	// The node comes back without disk0 — the disk with the sidecars died.
	store, closeStore := newLifecycleStore(t, dirs[1:])
	defer closeStore()

	// Degraded visibility is acceptable and expected; silent file loss is not.
	if n := shardFilesOnDisk(t, dirs[1]); n != 5 {
		t.Fatalf("disk1 shard files after sidecar-disk loss: %d, want 5 untouched", n)
	}
	if n := shardFilesOnDisk(t, dirs[2]); n != 3 {
		t.Fatalf("disk2 shard files after sidecar-disk loss: %d, want 3 untouched", n)
	}
	t.Logf("registered view without the sidecar disk: %v (files intact; shards without a reachable .ecx stay unloaded)",
		registeredShards(store))
}
