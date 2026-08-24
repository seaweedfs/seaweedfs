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

// encodeAndMountEcVolume writes needles into a fresh volume, EC-encodes it
// (uniform layout), stamps the .vif the way VolumeEcShardsGenerate does, and
// mounts every shard on the store.
func encodeAndMountEcVolume(t *testing.T, store *Store, vid needle.VolumeId, needles []*needle.Needle) *erasure_coding.EcVolume {
	t.Helper()
	dir := store.Locations[0].Directory

	v, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("new volume: %v", err)
	}
	for _, n := range needles {
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write needle: %v", err)
		}
	}
	baseFileName := v.DataFileName()
	v.Close()

	datSize, err := os.Stat(baseFileName + ".dat")
	if err != nil {
		t.Fatalf("stat .dat: %v", err)
	}
	ecCtx := erasure_coding.NewDefaultECContext("", vid)
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
			DataShards:   uint32(ecCtx.DataShards),
			ParityShards: uint32(ecCtx.ParityShards),
			BlockSize:    ecCtx.BlockSize,
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
	if ecVolume.ECContext.BlockSize != ecCtx.BlockSize {
		t.Fatalf("mounted BlockSize = %d, want %d", ecVolume.ECContext.BlockSize, ecCtx.BlockSize)
	}

	// Every shard is local, so seed the location cache to keep the read off the
	// master this test does not have.
	ecVolume.ShardLocationsLock.Lock()
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		ecVolume.ShardLocations[erasure_coding.ShardId(shardId)] = []pb.ServerAddress{"localhost:8080"}
	}
	ecVolume.ShardLocationsRefreshTime = time.Now()
	ecVolume.ShardLocationsLock.Unlock()
	return ecVolume
}

func randomNeedleOfSize(id uint64, size int) *needle.Needle {
	n := new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(id)
	n.Data = make([]byte, size)
	rand.Read(n.Data)
	n.Checksum = needle.NewCRC(n.Data)
	return n
}

// A needle larger than one EC block is split over consecutive blocks, which
// live on different shards. The intervals are read concurrently, so check they
// still come back in order.
func TestReadEcShardNeedleSpanningBlocks(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(7)

	n := randomNeedleOfSize(42, 3*erasure_coding.ErasureCodingSmallBlockSize+1234)
	ecVolume := encodeAndMountEcVolume(t, store, vid, []*needle.Needle{n})

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

// On a volume big enough for multi-MB blocks, the uniform layout keeps a
// needle smaller than the block in one interval on one shard, and reads every
// needle back intact.
func TestReadEcShardNeedleUniformLayout(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(8)

	// ~26MB of needles: block size becomes 3MB, so the layouts diverge and a
	// 1MB needle would stripe across shards under the legacy layout.
	var needles []*needle.Needle
	for i := uint64(1); i <= 5; i++ {
		needles = append(needles, randomNeedleOfSize(i, 4*1024*1024))
	}
	for i := uint64(6); i <= 11; i++ {
		needles = append(needles, randomNeedleOfSize(i, 1024*1024))
	}
	ecVolume := encodeAndMountEcVolume(t, store, vid, needles)
	if ecVolume.ECContext.BlockSize <= erasure_coding.ErasureCodingSmallBlockSize {
		t.Fatalf("block size %d does not diverge from the legacy layout", ecVolume.ECContext.BlockSize)
	}

	// With 3MB blocks a 1MB needle sits inside one block (bar a boundary
	// straddle), where the legacy layout would stripe it at 1MB granularity.
	singleInterval := false
	for _, n := range needles[5:] {
		_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
		if err != nil {
			t.Fatalf("locate needle %v: %v", n.Id, err)
		}
		if len(intervals) == 1 {
			singleInterval = true
		}
	}
	if !singleInterval {
		t.Fatal("no 1MB needle mapped to a single interval; uniform layout not in effect")
	}

	for _, n := range needles {
		got := new(needle.Needle)
		got.Id = n.Id
		if _, err := store.ReadEcShardNeedle(vid, got, nil); err != nil {
			t.Fatalf("read needle %v: %v", n.Id, err)
		}
		if !bytes.Equal(got.Data, n.Data) {
			t.Fatalf("needle %v: read back %d bytes that do not match the %d written", n.Id, len(got.Data), len(n.Data))
		}
	}
}
