package storage

import (
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// newLocalEcVolume builds an EC volume holding one needle spread over several shards,
// mounts every shard locally, and seeds the shard-location cache with empty location
// lists: the cache then counts as fresh, so nothing calls the master these tests do not
// have, and no read leaves the process. Returns the needle that was encoded.
func newLocalEcVolume(t *testing.T, vid needle.VolumeId) (*Store, *erasure_coding.EcVolume, *needle.Needle) {
	t.Helper()

	store := newTestStore(t, 1)
	dir := store.Locations[0].Directory

	v, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("new volume: %v", err)
	}
	n := new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(42)
	n.Data = make([]byte, 3*erasure_coding.ErasureCodingSmallBlockSize+1234)
	rand.New(rand.NewSource(42)).Read(n.Data)
	n.Checksum = needle.NewCRC(n.Data)
	if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}
	baseFileName := v.DataFileName()
	v.Close()

	datSize, err := os.Stat(baseFileName + ".dat")
	if err != nil {
		t.Fatalf("stat .dat: %v", err)
	}
	ecCtx := erasure_coding.BackgroundECContext()
	if _, err := erasure_coding.WriteEcFiles(baseFileName, ecCtx); err != nil {
		t.Fatalf("write ec files: %v", err)
	}
	if err := erasure_coding.WriteSortedFileFromIdx(baseFileName, ".ecx"); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(baseFileName+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(baseFileName+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.GetCurrentVersion()),
		DatFileSize: datSize.Size(),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   erasure_coding.DataShardsCount,
			ParityShards: erasure_coding.ParityShardsCount,
			// The encode writes the uniform layout; a .vif that omitted its
			// block size would mount these shards as legacy and read every
			// interval at the wrong offset.
			BlockSize: ecCtx.BlockSize,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}
	for _, ext := range []string{".dat", ".idx"} {
		if err := os.Remove(baseFileName + ext); err != nil {
			t.Fatalf("remove %s: %v", ext, err)
		}
	}

	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}

	ecVolume.ShardLocationsLock.Lock()
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		ecVolume.ShardLocations[erasure_coding.ShardId(shardId)] = []pb.ServerAddress{}
	}
	ecVolume.ShardLocationsRefreshTime = time.Now()
	ecVolume.ShardLocationsLock.Unlock()

	return store, ecVolume, n
}

func TestScrubEcVolumeReadsMatchesFullWhenHealthy(t *testing.T) {
	const vid = needle.VolumeId(7)
	store, _, _ := newLocalEcVolume(t, vid)

	for _, mode := range []volume_server_pb.VolumeScrubMode{
		volume_server_pb.VolumeScrubMode_FULL,
		volume_server_pb.VolumeScrubMode_READS,
	} {
		count, brokenShards, errs := store.ScrubEcVolume(vid, mode, false)
		if count != 1 {
			t.Errorf("%s: scrubbed %d needles, want 1", mode, count)
		}
		if len(brokenShards) != 0 {
			t.Errorf("%s: reported broken shards %v on a healthy volume", mode, brokenShards)
		}
		if len(errs) != 0 {
			t.Errorf("%s: reported errors on a healthy volume: %v", mode, errs)
		}
	}
}

// A READS scrub rebuilds what it cannot read, but the shard it could not read is still
// broken and must still be reported - otherwise a volume missing shards scrubs clean and
// the operator never learns to repair it.
func TestScrubEcVolumeReadsReportsTheShardItRebuilt(t *testing.T) {
	const vid = needle.VolumeId(7)
	const missingShard = erasure_coding.ShardId(0)

	store, _, _ := newLocalEcVolume(t, vid)
	if err := store.UnmountEcShards(vid, missingShard, 0); err != nil {
		t.Fatalf("unmount shard %d: %v", missingShard, err)
	}

	// FULL gives up on the needles that lived on the missing shard...
	_, brokenShards, errs := store.ScrubEcVolume(vid, volume_server_pb.VolumeScrubMode_FULL, false)
	assertOnlyBrokenShard(t, "FULL", brokenShards, missingShard)
	if len(errs) == 0 {
		t.Fatalf("FULL reported no error for a shard it could not read")
	}
	if got := errs[0].Error(); !strings.Contains(got, "failed to read EC shard 0") {
		t.Fatalf("FULL error %q, want it to name the shard it could not read", got)
	}

	// ...READS rebuilds them from the shards that are left, and still names the shard.
	_, brokenShards, errs = store.ScrubEcVolume(vid, volume_server_pb.VolumeScrubMode_READS, false)
	assertOnlyBrokenShard(t, "READS", brokenShards, missingShard)
	if len(errs) != 0 {
		t.Fatalf("READS should rebuild every needle from parity, got: %v", errs)
	}
}

func TestScrubEcVolumeReadsErrorsWhenParityCannotCover(t *testing.T) {
	const vid = needle.VolumeId(7)

	store, _, _ := newLocalEcVolume(t, vid)
	// one shard more than parity can cover, so nothing can be rebuilt
	for shardId := 0; shardId <= erasure_coding.ParityShardsCount; shardId++ {
		if err := store.UnmountEcShards(vid, erasure_coding.ShardId(shardId), 0); err != nil {
			t.Fatalf("unmount shard %d: %v", shardId, err)
		}
	}

	_, brokenShards, errs := store.ScrubEcVolume(vid, volume_server_pb.VolumeScrubMode_READS, false)
	if len(brokenShards) == 0 {
		t.Fatalf("no broken shard reported for an unrecoverable volume")
	}
	if len(errs) == 0 {
		t.Fatalf("no error reported for an unrecoverable volume")
	}
	if got := errs[0].Error(); !strings.Contains(got, "failed to recover EC shard") {
		t.Fatalf("error %q, want it to say the rebuild failed", got)
	}
}

func assertOnlyBrokenShard(t *testing.T, mode string, brokenShards []*volume_server_pb.EcShardInfo, shardId erasure_coding.ShardId) {
	t.Helper()
	if len(brokenShards) != 1 || brokenShards[0].GetShardId() != uint32(shardId) {
		t.Fatalf("%s reported broken shards %v, want only shard %d", mode, brokenShards, shardId)
	}
}
