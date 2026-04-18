package storage

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

/*
makediff test steps
1. launch weed server at your local/dev environment, (option
"garbageThreshold" for master and option "max" for volume should be set with specific value which would let
preparing test prerequisite easier )
   a) ./weed master -garbageThreshold=0.99 -mdir=./m
   b) ./weed volume -dir=./data -max=1 -master=localhost:9333 -port=8080
2. upload 4 different files, you could call dir/assign to get 4 different fids
   a)  upload file A with fid a
   b)  upload file B with fid b
   c)  upload file C with fid c
   d)  upload file D with fid d
3. update file A and C
   a)  modify file A and upload file A with fid a
   b)  modify file C and upload file C with fid c
   c)  record the current 1.idx's file size(lastCompactIndexOffset value)
4. Compacting the data file
   a)  run curl http://localhost:8080/admin/vacuum/compact?volumeId=1
   b)  verify the 1.cpd and 1.cpx is created under volume directory
5. update file B and delete file D
   a)  modify file B and upload file B with fid b
   d)  delete file B with fid b
6. Now you could run the following UT case, the case should be run successfully
7. Compact commit manually
   a)  mv 1.cpd 1.dat
   b)  mv 1.cpx 1.idx
8. Restart Volume Server
9. Now you should get updated file A,B,C
*/

func TestMakeDiff(t *testing.T) {

	v := new(Volume)
	// lastCompactIndexOffset value is the index file size before step 4
	v.lastCompactIndexOffset = 96
	v.SuperBlock.Version = 0x2
	/*
		err := v.makeupDiff(
			"/yourpath/1.cpd",
			"/yourpath/1.cpx",
			"/yourpath/1.dat",
			"/yourpath/1.idx")
		if err != nil {
			t.Errorf("makeupDiff err is %v", err)
		} else {
			t.Log("makeupDiff Succeeded")
		}
	*/
}

func TestMemIndexCompaction(t *testing.T) {
	testCompactionByIndex(t, NeedleMapInMemory)
}

func TestLDBIndexCompaction(t *testing.T) {
	testCompactionByIndex(t, NeedleMapLevelDb)
}

func testCompactionByIndex(t *testing.T, needleMapKind NeedleMapKind) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, needleMapKind, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	beforeCommitFileCount := 10000
	afterCommitFileCount := 10000

	infos := make([]*needleInfo, beforeCommitFileCount+afterCommitFileCount)

	for i := 1; i <= beforeCommitFileCount; i++ {
		doSomeWritesDeletes(i, v, t, infos)
	}

	startTime := time.Now()
	v.CompactByIndex(nil)
	speed := float64(v.ContentSize()) / time.Now().Sub(startTime).Seconds()
	t.Logf("compaction speed: %.2f bytes/s", speed)

	// update & delete original objects, upload & delete new objects
	for i := 1; i <= afterCommitFileCount+beforeCommitFileCount; i++ {
		doSomeWritesDeletes(i, v, t, infos)
	}
	v.CommitCompact()
	realRecordCount := v.nm.IndexFileSize() / types.NeedleMapEntrySize
	if needleMapKind == NeedleMapLevelDb {
		nm := reflect.ValueOf(v.nm).Interface().(*LevelDbNeedleMap)
		mm := nm.mapMetric
		watermark := getWatermark(nm.db)
		realWatermark := (nm.recordCount / watermarkBatchSize) * watermarkBatchSize
		t.Logf("watermark from levelDB: %d, realWatermark: %d, nm.recordCount: %d, realRecordCount:%d, fileCount=%d, deletedcount:%d", watermark, realWatermark, nm.recordCount, realRecordCount, mm.FileCount(), v.DeletedCount())
		if realWatermark != watermark {
			t.Fatalf("testing watermark failed")
		}
	} else {
		t.Logf("realRecordCount:%d, v.FileCount():%d mm.DeletedCount():%d", realRecordCount, v.FileCount(), v.DeletedCount())
	}
	if realRecordCount != v.FileCount() {
		t.Fatalf("testing file count failed")
	}

	v.Close()

	v, err = NewVolume(dir, dir, "", 1, needleMapKind, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume reloading: %v", err)
	}
	defer v.Close()

	for i := 1; i <= beforeCommitFileCount+afterCommitFileCount; i++ {

		if infos[i-1] == nil {
			t.Fatal("not found file", i)
		}

		if infos[i-1].size == 0 {
			continue
		}

		n := newEmptyNeedle(uint64(i))
		size, err := v.readNeedle(n, nil, nil)
		if err != nil {
			t.Fatalf("read file %d: %v", i, err)
		}
		if infos[i-1].size != types.Size(size) {
			t.Fatalf("read file %d size mismatch expected %d found %d", i, infos[i-1].size, size)
		}
		if infos[i-1].crc != n.Checksum {
			t.Fatalf("read file %d checksum mismatch expected %d found %d", i, infos[i-1].crc, n.Checksum)
		}

	}

}

func TestCompactVolumeFilesOffline(t *testing.T) {
	dir := t.TempDir()
	location := NewDiskLocation(dir, 10, util.MinFreeSpace{}, dir, "", nil)
	defer location.Close()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	infos := make([]*needleInfo, 32)
	for i := 1; i <= 32; i++ {
		doSomeWritesDeletes(i, v, t, infos)
	}
	v.Close()

	store := &Store{}
	if err := store.CompactVolumeFiles(needle.VolumeId(1), "", location, NeedleMapInMemory, 0, 0, 0); err != nil {
		t.Fatalf("CompactVolumeFiles: %v", err)
	}

	reloaded, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume reload: %v", err)
	}
	defer reloaded.Close()

	if _, err := os.Stat(filepath.Join(dir, "1.cpd")); !os.IsNotExist(err) {
		t.Fatalf("expected no .cpd after successful offline compaction, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "1.cpx")); !os.IsNotExist(err) {
		t.Fatalf("expected no .cpx after successful offline compaction, got err=%v", err)
	}
}

func TestCleanupCompactRemovesTempFiles(t *testing.T) {
	dir := t.TempDir()
	location := NewDiskLocation(dir, 10, util.MinFreeSpace{}, dir, "", nil)
	defer location.Close()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	infos := make([]*needleInfo, 16)
	for i := 1; i <= 16; i++ {
		doSomeWritesDeletes(i, v, t, infos)
	}
	v.Close()

	if err := os.WriteFile(filepath.Join(dir, "1.cpx"), []byte("broken"), 0o644); err != nil {
		t.Fatalf("write broken cpx: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "1.cpd"), []byte("temp"), 0o644); err != nil {
		t.Fatalf("write cpd: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "1.cpldb"), 0o755); err != nil {
		t.Fatalf("mkdir cpldb: %v", err)
	}

	tempVolume, err := loadVolumeWithoutWorker(dir, dir, "", needle.VolumeId(1), NeedleMapInMemory, 0)
	if err != nil {
		t.Fatalf("loadVolumeWithoutWorker: %v", err)
	}
	tempVolume.location = location
	defer tempVolume.doClose()

	if err := tempVolume.cleanupCompact(); err != nil {
		t.Fatalf("cleanupCompact: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "1.cpd")); !os.IsNotExist(err) {
		t.Fatalf("expected cleanup to remove .cpd, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "1.cpx")); !os.IsNotExist(err) {
		t.Fatalf("expected cleanup to remove .cpx, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "1.cpldb")); !os.IsNotExist(err) {
		t.Fatalf("expected cleanup to remove .cpldb, got err=%v", err)
	}
}

// TestCompactByIndex_DropsDanglingNeedle verifies that vacuum compaction
// tolerates an .idx entry whose offset points past the end of the .dat file
// (the failure mode reported in issue #8928). The bad entry should be silently
// dropped from the resulting .cpx, while every healthy needle is preserved.
func TestCompactByIndex_DropsDanglingNeedle(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	const goodNeedleCount = 8
	infos := make([]*needleInfo, goodNeedleCount)
	for i := 1; i <= goodNeedleCount; i++ {
		n := newRandomNeedle(uint64(i))
		_, size, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
		infos[i-1] = &needleInfo{size: size, crc: n.Checksum}
	}

	if err := v.DataBackend.Sync(); err != nil {
		t.Fatalf("sync .dat: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx: %v", err)
	}

	datSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		t.Fatalf("stat .dat: %v", err)
	}

	// Inject an .idx entry that points 1 MB past the end of the .dat. The
	// bad entry must go through nm.Put so it ends up in both the in-memory
	// map and the on-disk .idx — exactly the corruption pattern in #8928.
	badKey := types.Uint64ToNeedleId(uint64(goodNeedleCount + 100))
	badOffset := types.ToOffset(datSize + 1024*1024)
	badSize := types.Size(2048)
	if err := v.nm.Put(badKey, badOffset, badSize); err != nil {
		t.Fatalf("inject bad idx entry: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx after inject: %v", err)
	}

	if err := v.CompactByIndex(nil); err != nil {
		t.Fatalf("CompactByIndex should tolerate dangling entries, got: %v", err)
	}

	// Walk the new index and confirm the dangling entry was dropped while
	// all of the original keys made it through.
	cpx, err := os.Open(filepath.Join(dir, "1.cpx"))
	if err != nil {
		t.Fatalf("open .cpx: %v", err)
	}
	defer cpx.Close()
	keptKeys := map[types.NeedleId]bool{}
	if err := idx.WalkIndexFile(cpx, 0, func(key types.NeedleId, _ types.Offset, size types.Size) error {
		if !size.IsDeleted() {
			keptKeys[key] = true
		}
		return nil
	}); err != nil {
		t.Fatalf("walk .cpx: %v", err)
	}
	if keptKeys[badKey] {
		t.Fatalf("dangling key %d should have been dropped from .cpx", badKey)
	}
	for i := 1; i <= goodNeedleCount; i++ {
		if infos[i-1].size == 0 {
			continue
		}
		k := types.Uint64ToNeedleId(uint64(i))
		if !keptKeys[k] {
			t.Fatalf("healthy key %d missing from compacted .cpx", k)
		}
	}

	v.Close()
}

func doSomeWritesDeletes(i int, v *Volume, t *testing.T, infos []*needleInfo) {
	n := newRandomNeedle(uint64(i))
	_, size, _, err := v.writeNeedle2(n, true, false)
	if err != nil {
		t.Fatalf("write file %d: %v", i, err)
	}
	infos[i-1] = &needleInfo{
		size: size,
		crc:  n.Checksum,
	}
	// println("written file", i, "checksum", n.Checksum.Value(), "size", size)
	if rand.Float64() < 0.03 {
		toBeDeleted := rand.Intn(i) + 1
		oldNeedle := newEmptyNeedle(uint64(toBeDeleted))
		v.deleteNeedle2(oldNeedle)
		// println("deleted file", toBeDeleted)
		infos[toBeDeleted-1] = &needleInfo{
			size: 0,
			crc:  n.Checksum,
		}
	}
}

type needleInfo struct {
	size types.Size
	crc  needle.CRC
}

func newRandomNeedle(id uint64) *needle.Needle {
	n := new(needle.Needle)
	n.Data = make([]byte, rand.Intn(1024))
	rand.Read(n.Data)

	n.Checksum = needle.NewCRC(n.Data)
	n.Id = types.Uint64ToNeedleId(id)
	return n
}

func newEmptyNeedle(id uint64) *needle.Needle {
	n := new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(id)
	return n
}
