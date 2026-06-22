package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestMountEcShards_LocatesEcxOnSiblingDisk covers the cross-disk
// .ecx lookup at mount time. A multi-disk volume server receives a
// fresh EC shard via VolumeEcShardsCopy on disk0, while the matching
// .ecx / .ecj / .vif live on a sibling disk (disk1) — the on-the-wire
// situation after an ec.balance distributes shards across disks.
// MountEcShards used to pin idxDir to each disk's IdxDirectory, so
// NewEcVolume tried to open /disk0/.ecx, failed with "cannot open ec
// volume index", and the mount loop returned the error (the error is
// not os.ErrNotExist so the per-disk continue branch did not engage).
// With the fix, the mount path scans every DiskLocation for the .ecx
// owner and points NewEcVolume at that directory.
//
// The test plants files AFTER NewStore returns so the startup orphan-
// shard reconcile is a no-op for this volume — the mount path is what's
// under test, not the startup reconcile that already covers this layout.
func TestMountEcShards_LocatesEcxOnSiblingDisk(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	const collection = "mybucket"
	vid := needle.VolumeId(5)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)
	const shardOnDisk0 erasure_coding.ShardId = 6
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

	// Plant AFTER NewStore: the on-disk layout the master is about to
	// mount. .ecx / .ecj / .vif on disk1, shard 6 on disk0. This mirrors
	// VolumeEcShardsCopy having delivered the index files to one disk and
	// the shard to a sibling disk of the same server.
	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	f, err := os.Create(base0 + erasure_coding.ToExt(int(shardOnDisk0)))
	if err != nil {
		t.Fatalf("create shard %d: %v", shardOnDisk0, err)
	}
	if err := f.Truncate(expectedShardSize); err != nil {
		f.Close()
		t.Fatalf("truncate shard: %v", err)
	}
	f.Close()

	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))
	if err := os.WriteFile(base1+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(base1+".ecj", nil, 0o644); err != nil {
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

	if err := store.MountEcShards(collection, vid, shardOnDisk0, ""); err != nil {
		// The pre-fix error reads
		//   "/.../disk0 load ec shard 5.6: failed to create ec shard 5.6:
		//    cannot open ec volume index /.../disk0/mybucket_5.ecx: ..."
		if strings.Contains(err.Error(), "cannot open ec volume index") {
			t.Fatalf("mount fell back to local IdxDirectory; .ecx lives on a sibling disk: %v", err)
		}
		t.Fatalf("MountEcShards: %v", err)
	}

	// The shard must be registered against the disk that physically holds
	// the .ec?? file (disk0), so heartbeats carry the correct DiskId.
	loc0 := store.Locations[0]
	ev, found := loc0.FindEcVolume(vid)
	if !found {
		t.Fatalf("EC volume %d not found on disk0 after mount", vid)
	}
	if _, ok := ev.FindEcVolumeShard(shardOnDisk0); !ok {
		t.Errorf("shard %d.%d not registered on disk0 EcVolume", vid, shardOnDisk0)
	}
	// The EC volume must have opened the .ecx that lives on disk1, not
	// errored out from the missing one on disk0. FileName(".ecx") is
	// rooted at ecxActualDir, which NewEcVolume sets to whichever
	// directory the file was actually opened from.
	if got, want := filepath.Dir(ev.FileName(".ecx")), dir1; got != want {
		t.Errorf("EcVolume .ecx resolved at %q, want directory %q (sibling disk holding .ecx)", got, want)
	}

	// disk1 must not have been registered as a shard holder for this
	// volume — only the disk physically owning the .ec?? file should
	// carry an EcVolume entry for it.
	loc1 := store.Locations[1]
	if _, found := loc1.FindEcVolume(vid); found {
		t.Errorf("EC volume %d unexpectedly registered on disk1 (which only owns .ecx, not any .ec?? file)", vid)
	}
}

// TestMountEcShards_SameDiskEcxStillWorks pins the baseline path: when
// the .ecx and the .ec?? both live on the same disk, MountEcShards must
// keep using that disk's IdxDirectory. Regression guard so the
// cross-disk fan-out does not silently re-route same-disk mounts at the
// EC volume level.
func TestMountEcShards_SameDiskEcxStillWorks(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	for _, d := range []string{dir0, dir1} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	const collection = "mybucket"
	vid := needle.VolumeId(7)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)
	const shardOnDisk0 erasure_coding.ShardId = 3
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
	f, err := os.Create(base0 + erasure_coding.ToExt(int(shardOnDisk0)))
	if err != nil {
		t.Fatalf("create shard: %v", err)
	}
	if err := f.Truncate(expectedShardSize); err != nil {
		f.Close()
		t.Fatalf("truncate shard: %v", err)
	}
	f.Close()
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

	if err := store.MountEcShards(collection, vid, shardOnDisk0, ""); err != nil {
		t.Fatalf("MountEcShards (same-disk .ecx): %v", err)
	}

	loc0 := store.Locations[0]
	ev, found := loc0.FindEcVolume(vid)
	if !found {
		t.Fatalf("EC volume %d not found on disk0", vid)
	}
	if got, want := filepath.Dir(ev.FileName(".ecx")), dir0; got != want {
		t.Errorf("EcVolume .ecx resolved at %q, want directory %q (same disk as shard)", got, want)
	}
}

// startEcMountStore plants two empty disk directories and returns a Store
// configured over them, draining the announcement channels in the
// background so MountEcShards does not block. Shared setup for the
// missing-.ecx and 0-byte-.ecx tests below.
func startEcMountStore(t *testing.T, dirs []string) *Store {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	diskIOProbeConfig := stats.DefaultDiskIOProbeConfig()
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		dirs,
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
	return store
}

// TestMountEcShards_MissingEcxOnAllDisks covers the case where the EC
// distribute step has dropped shards on the volume server but no .ecx
// ever arrived (the operator-reported "Node 1 has 17 shard files but
// zero .ecx" pattern). MountEcShards used to bail on the first disk's
// "cannot open ec volume index" error — which is not os.ErrNotExist
// because NewEcVolume wrapped it with %v — and the operator saw a
// confusing per-disk error. The mount path now keeps scanning every
// local disk and, when none own an index, returns one clear error.
func TestMountEcShards_MissingEcxOnAllDisks(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	store := startEcMountStore(t, []string{dir0, dir1})

	const collection = "mybucket"
	vid := needle.VolumeId(11)
	const dataShards = 10
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)
	const shardOnDisk0 erasure_coding.ShardId = 6

	// Plant a shard file on disk0 but no .ecx anywhere.
	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	f, err := os.Create(base0 + erasure_coding.ToExt(int(shardOnDisk0)))
	if err != nil {
		t.Fatalf("create shard: %v", err)
	}
	if err := f.Truncate(expectedShardSize); err != nil {
		f.Close()
		t.Fatalf("truncate shard: %v", err)
	}
	f.Close()

	err = store.MountEcShards(collection, vid, shardOnDisk0, "")
	if err == nil {
		t.Fatalf("MountEcShards should fail when no .ecx exists on any disk")
	}
	if !strings.Contains(err.Error(), "no .ecx index found on any local disk") {
		t.Errorf("expected aggregated 'no .ecx' error, got: %v", err)
	}
}

// TestMountEcShards_ZeroByteEcxIsIgnored covers the user's "0-byte .ecx
// on one disk, valid .ecx on a sibling disk" case. writeToFile uses
// O_TRUNC and can leave an empty stub on a failed copy stream during EC
// distribute. The mount path must skip the stub and use the valid index
// from the sibling disk, not fail with "cannot open ec volume index".
func TestMountEcShards_ZeroByteEcxIsIgnored(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	store := startEcMountStore(t, []string{dir0, dir1})

	const collection = "mybucket"
	vid := needle.VolumeId(12)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)
	const shardOnDisk0 erasure_coding.ShardId = 7

	// shard on disk0; 0-byte .ecx stub on disk0; valid .ecx on disk1.
	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	f, err := os.Create(base0 + erasure_coding.ToExt(int(shardOnDisk0)))
	if err != nil {
		t.Fatalf("create shard: %v", err)
	}
	if err := f.Truncate(expectedShardSize); err != nil {
		f.Close()
		t.Fatalf("truncate shard: %v", err)
	}
	f.Close()
	if err := os.WriteFile(base0+".ecx", nil, 0o644); err != nil {
		t.Fatalf("write 0-byte .ecx stub on disk0: %v", err)
	}

	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))
	if err := os.WriteFile(base1+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("write valid .ecx on disk1: %v", err)
	}
	if err := os.WriteFile(base1+".ecj", nil, 0o644); err != nil {
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

	if err := store.MountEcShards(collection, vid, shardOnDisk0, ""); err != nil {
		t.Fatalf("MountEcShards should ignore the 0-byte .ecx stub and use disk1's valid index: %v", err)
	}

	loc0 := store.Locations[0]
	ev, found := loc0.FindEcVolume(vid)
	if !found {
		t.Fatalf("EC volume %d not found on disk0", vid)
	}
	if got, want := filepath.Dir(ev.FileName(".ecx")), dir1; got != want {
		t.Errorf("EcVolume .ecx resolved at %q, want %q (sibling disk with valid index)", got, want)
	}
}

// TestMountEcShards_EmptyEcxMountsSuccessfully pins the empty-volume
// case: when an EC volume's .idx had no live entries at encode time,
// WriteSortedFileFromIdx legitimately produces a 0-byte .ecx. Mount
// must accept that as a valid empty index and let the EC volume come
// online — there is no reliable way to distinguish a legitimately
// empty .ecx from a stub left by a failed copy, and preventing the
// stub case is writeToFile's job at copy time, not NewEcVolume's at
// mount time. Pre-fix this returned "no .ecx index found on any local
// disk" because NewEcVolume mapped 0-byte to os.ErrNotExist.
func TestMountEcShards_EmptyEcxMountsSuccessfully(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	store := startEcMountStore(t, []string{dir0, dir1})

	const collection = "mybucket"
	vid := needle.VolumeId(13)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 0
	const shardOnDisk0 erasure_coding.ShardId = 0

	// Plant a 0-byte shard, a 0-byte .ecx, an empty .ecj, and a .vif —
	// the on-disk layout after encoding a volume that had no live needles.
	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	if err := os.WriteFile(base0+erasure_coding.ToExt(int(shardOnDisk0)), nil, 0o644); err != nil {
		t.Fatalf("write 0-byte shard: %v", err)
	}
	if err := os.WriteFile(base0+".ecx", nil, 0o644); err != nil {
		t.Fatalf("write 0-byte .ecx (empty volume's legitimate empty index): %v", err)
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

	if err := store.MountEcShards(collection, vid, shardOnDisk0, ""); err != nil {
		t.Fatalf("MountEcShards should accept a 0-byte .ecx as a valid empty index: %v", err)
	}

	loc0 := store.Locations[0]
	if _, found := loc0.FindEcVolume(vid); !found {
		t.Errorf("EC volume %d expected to be loaded on disk0 after mount", vid)
	}
}

// TestIndexEcxOwners_IgnoresZeroByteStub guards the reconcile owner
// selection: a 0-byte .ecx on the orphan disk must not win against a
// valid .ecx on a sibling disk, otherwise reconcileEcShardsAcrossDisks
// would point loaders at the stub and leave orphan shards unloaded.
// Same invariant as findEcxIdxDirForVolume, but for the bulk scan path.
func TestIndexEcxOwners_IgnoresZeroByteStub(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0") // 0-byte .ecx stub
	dir1 := filepath.Join(tempDir, "disk1") // valid .ecx
	store := startEcMountStore(t, []string{dir0, dir1})

	const collection = "mybucket"
	vid := needle.VolumeId(99)

	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	if err := os.WriteFile(base0+".ecx", nil, 0o644); err != nil {
		t.Fatalf("write 0-byte .ecx stub on disk0: %v", err)
	}
	base1 := erasure_coding.EcShardFileName(collection, dir1, int(vid))
	if err := os.WriteFile(base1+".ecx", make([]byte, 20), 0o644); err != nil {
		t.Fatalf("write valid .ecx on disk1: %v", err)
	}

	owners := store.indexEcxOwners()
	owner, ok := owners[ecKeyForReconcile{collection: collection, vid: vid}]
	if !ok {
		t.Fatalf("indexEcxOwners did not find any owner; expected disk1 with the valid .ecx")
	}
	if owner.location != store.Locations[1] {
		t.Errorf("indexEcxOwners chose disk %s as owner; want disk1 (which holds the valid .ecx, not the 0-byte stub)", owner.location.Directory)
	}
}
