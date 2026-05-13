package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestIssue9478_PartialEcOnSiblingDiskOfHealthyDat reproduces
// https://github.com/seaweedfs/seaweedfs/issues/9478:
//
//   - A single volume server has two disks. Disk A holds a healthy
//     volume_x.dat (with .idx + .vif). Disk B holds the leftovers of an
//     interrupted EC encode for the SAME volume: a single .ec?? shard plus
//     .ecx + .ecj + .vif, with no .dat next to them.
//
// The current per-disk EC loader (handleFoundEcxFile / validateEcVolume) only
// checks for .dat in the same DiskLocation as the EC shards. Because Disk B
// has no .dat, it concludes "this must be a distributed EC volume" and
// happily loads the lone partial shard. The .dat on Disk A also loads as a
// regular replica. The volume server then heartbeats BOTH a regular volume
// and an EC shard for the same vid, which is the inconsistent state the
// issue is describing.
//
// The expected behaviour is to recognise that a healthy .dat exists on a
// sibling disk on the same store and clean up the partial EC files on
// Disk B, exactly as we already do today when .dat and the partial EC files
// sit on the same disk.
func TestIssue9478_PartialEcOnSiblingDiskOfHealthyDat(t *testing.T) {
	collection := ""
	vid := needle.VolumeId(122)

	root := t.TempDir()
	datDir := root + "/sdd"
	ecDir := root + "/sdf"
	if err := os.MkdirAll(datDir, 0o755); err != nil {
		t.Fatalf("mkdir datDir: %v", err)
	}
	if err := os.MkdirAll(ecDir, 0o755); err != nil {
		t.Fatalf("mkdir ecDir: %v", err)
	}

	// Disk A (sdd): a healthy-looking volume_x.dat + .idx + .vif. We don't
	// run NewVolume here, only check what loadAllEcShards on Disk B does in
	// the presence of a same-server .dat. A truncated 10 MiB .dat is enough
	// for the EC validator to compute an expected shard size from.
	datFileSize := int64(10 * 1024 * 1024)
	datBase := erasure_coding.EcShardFileName(collection, datDir, int(vid))
	if f, err := os.Create(datBase + ".dat"); err == nil {
		if err := f.Truncate(datFileSize); err != nil {
			t.Fatalf("truncate dat: %v", err)
		}
		f.Close()
	} else {
		t.Fatalf("create dat: %v", err)
	}
	if f, err := os.Create(datBase + ".idx"); err == nil {
		f.Close()
	}
	if f, err := os.Create(datBase + ".vif"); err == nil {
		f.Close()
	}

	// Disk B (sdf): only one EC shard plus .ecx + .ecj + .vif, no .dat.
	// This mirrors the issue 9478 listing for volume_server_4 / sdf.
	ecBase := erasure_coding.EcShardFileName(collection, ecDir, int(vid))
	expectedShardSize := calculateExpectedShardSize(datFileSize, erasure_coding.DataShardsCount)
	if f, err := os.Create(ecBase + erasure_coding.ToExt(1)); err == nil {
		if err := f.Truncate(expectedShardSize); err != nil {
			t.Fatalf("truncate shard: %v", err)
		}
		f.Close()
	} else {
		t.Fatalf("create shard: %v", err)
	}
	if f, err := os.Create(ecBase + ".ecx"); err == nil {
		f.WriteString("dummy ecx")
		f.Close()
	}
	if f, err := os.Create(ecBase + ".ecj"); err == nil {
		f.Close()
	}
	if f, err := os.Create(ecBase + ".vif"); err == nil {
		f.Close()
	}

	minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"}
	makeDisk := func(dir string) *DiskLocation {
		dl := &DiskLocation{
			Directory:     dir,
			DirectoryUuid: "test-uuid-" + dir,
			IdxDirectory:  dir,
			DiskType:      types.HddType,
			MinFreeSpace:  minFreeSpace,
		}
		dl.volumes = make(map[needle.VolumeId]*Volume)
		dl.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)
		return dl
	}
	datLoc := makeDisk(datDir)
	ecLoc := makeDisk(ecDir)

	// Stand the disks up the same way NewStore does: per-disk EC scan
	// first, then the Store-level passes. The per-disk pass alone cannot
	// see the .dat on the sibling disk, so it mounts the partial shards;
	// the Store-level prune is what brings the cluster back to a
	// consistent state.
	store := &Store{
		Locations:           []*DiskLocation{datLoc, ecLoc},
		NewEcShardsChan:     make(chan master_pb.VolumeEcShardInformationMessage, 16),
		DeletedEcShardsChan: make(chan master_pb.VolumeEcShardInformationMessage, 16),
	}
	ecLoc.ecShardNotifyHandler = func(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume) {
		store.NewEcShardsChan <- master_pb.VolumeEcShardInformationMessage{
			Id:         uint32(vid),
			Collection: collection,
		}
	}

	if err := ecLoc.loadAllEcShards(ecLoc.ecShardNotifyHandler); err != nil {
		t.Logf("loadAllEcShards on ecDir returned: %v", err)
	}
	t.Cleanup(func() { closeEcVolumes(ecLoc) })

	preShardCount := ecLoc.EcShardCount()
	preNewMessages := len(store.NewEcShardsChan)
	t.Logf("after per-disk EC load on sdf: inMemoryShards=%d newEcShardMessages=%d", preShardCount, preNewMessages)
	if preShardCount == 0 {
		t.Fatalf("test setup is no longer reproducing the bug: per-disk EC load did not mount the partial shard")
	}

	store.pruneIncompleteEcWithSiblingDat()
	store.reconcileEcShardsAcrossDisks()

	leftoverShard := util.FileExists(ecBase + erasure_coding.ToExt(1))
	leftoverEcx := util.FileExists(ecBase + ".ecx")
	leftoverEcj := util.FileExists(ecBase + ".ecj")
	loaded := ecLoc.EcShardCount()
	deleteMessages := len(store.DeletedEcShardsChan)

	t.Logf("after Store-level cleanup: shardFileLeft=%v ecxLeft=%v ecjLeft=%v inMemoryShards=%d deletedEcShardMessages=%d",
		leftoverShard, leftoverEcx, leftoverEcj, loaded, deleteMessages)

	if loaded != 0 {
		t.Fatalf("partial EC shard was still mounted (%d shards) after the Store-level prune; a healthy .dat exists on a sibling disk and the leftover should have been cleaned up", loaded)
	}
	if leftoverShard || leftoverEcx || leftoverEcj {
		t.Fatalf("partial EC files survived the Store-level prune (shard=%v ecx=%v ecj=%v); a healthy .dat exists on a sibling disk and the leftover should have been removed",
			leftoverShard, leftoverEcx, leftoverEcj)
	}
	if deleteMessages == 0 {
		t.Errorf("prune did not push DeletedEcShardsChan; master would have to wait for the next periodic heartbeat to forget the partial shard")
	}

	// The .dat on the sibling disk must survive — pruning the partial EC
	// must never touch the healthy replica we are falling back to.
	if !util.FileExists(datBase + ".dat") {
		t.Fatalf("healthy .dat on sibling disk was removed by the prune; only the partial EC files should be cleaned up")
	}
}
