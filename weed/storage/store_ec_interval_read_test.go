package storage

import (
	"bytes"
	"math/rand"
	"os"
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

// A needle larger than one EC block is split over consecutive blocks, which
// live on different shards. The intervals are read concurrently, so check they
// still come back in order.
func TestReadEcShardNeedleSpanningBlocks(t *testing.T) {
	store := newTestStore(t, 1)
	dir := store.Locations[0].Directory
	const vid = needle.VolumeId(7)

	v, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("new volume: %v", err)
	}
	n := new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(42)
	n.Data = make([]byte, 3*erasure_coding.ErasureCodingSmallBlockSize+1234)
	rand.Read(n.Data)
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
	if _, err := erasure_coding.WriteEcFiles(baseFileName, erasure_coding.BackgroundECContext()); err != nil {
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

	// Every shard is local, so seed the location cache to keep the read off the
	// master this test does not have.
	ecVolume.ShardLocationsLock.Lock()
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		ecVolume.ShardLocations[erasure_coding.ShardId(shardId)] = []pb.ServerAddress{"localhost:8080"}
	}
	ecVolume.ShardLocationsRefreshTime = time.Now()
	ecVolume.ShardLocationsLock.Unlock()

	_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
	if err != nil {
		t.Fatalf("locate needle: %v", err)
	}
	if len(intervals) < 2 {
		t.Fatalf("needle covers %d interval(s), want it split over several", len(intervals))
	}

	got := new(needle.Needle)
	got.Id = n.Id
	if _, err := store.ReadEcShardNeedle(vid, got, nil); err != nil {
		t.Fatalf("read ec needle: %v", err)
	}
	if !bytes.Equal(got.Data, n.Data) {
		t.Fatalf("read back %d bytes, want the %d written", len(got.Data), len(n.Data))
	}
}
