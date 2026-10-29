package command

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type verifyFixtureNeedle struct {
	id     uint64
	offset int64
	disk   int64
}

// buildVerifyFixture writes count needles with ids 1..count, then a delete
// record for id 1, and returns the .dat path with the layout of every record.
func buildVerifyFixture(t *testing.T, count int) (string, []verifyFixtureNeedle) {
	t.Helper()
	datName := filepath.Join(t.TempDir(), "1.dat")
	version := needle.GetCurrentVersion()

	df, err := os.OpenFile(datName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(df)
	defer datBackend.Close()
	sb := super_block.SuperBlock{
		Version:          version,
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              &needle.TTL{},
	}
	if _, err := datBackend.WriteAt(sb.Bytes(), 0); err != nil {
		t.Fatal(err)
	}

	var layout []verifyFixtureNeedle
	for i := 1; i <= count; i++ {
		n := new(needle.Needle)
		n.Id = types.Uint64ToNeedleId(uint64(i))
		n.Cookie = types.Uint32ToCookie(uint32(0x1000 + i))
		n.Data = make([]byte, 1000+i*37)
		if _, err := rand.Read(n.Data); err != nil {
			t.Fatal(err)
		}
		n.Checksum = needle.NewCRC(n.Data)
		offset, _, actual, err := n.Append(datBackend, version)
		if err != nil {
			t.Fatal(err)
		}
		layout = append(layout, verifyFixtureNeedle{uint64(i), int64(offset), actual})
	}
	del := new(needle.Needle)
	del.Id = types.Uint64ToNeedleId(1)
	offset, _, actual, err := del.Append(datBackend, version)
	if err != nil {
		t.Fatal(err)
	}
	layout = append(layout, verifyFixtureNeedle{1, int64(offset), actual})
	if err := datBackend.Sync(); err != nil {
		t.Fatal(err)
	}
	return datName, layout
}

func runVerify(t *testing.T, datName string) (*datVerifySummary, []datAnomaly, map[uint64]bool) {
	t.Helper()
	seen := map[uint64]bool{}
	sum, anomalies, err := verifyDatFile(datName, 1, datVerifyOptions{window: 64 << 10, zeroWindow: 512}, func(n *needle.Needle, offset int64) {
		if n.Size.IsValid() {
			seen[uint64(n.Id)] = true
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return sum, anomalies, seen
}

func patchDat(t *testing.T, datName string, off int64, data []byte) {
	t.Helper()
	f, err := os.OpenFile(datName, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(data, off); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyDatFileClean(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 20)
	sum, anomalies, seen := runVerify(t, datName)
	if len(anomalies) != 0 || sum.Needles != 20 || sum.Tombstones != 1 || len(seen) != 20 {
		t.Fatalf("clean volume: needles=%d tombstones=%d anomalies=%v", sum.Needles, sum.Tombstones, anomalies)
	}
	last := layout[len(layout)-1]
	if sum.CleanEnd != last.offset+last.disk || sum.TailGarbage {
		t.Fatalf("clean end %d, want %d", sum.CleanEnd, last.offset+last.disk)
	}
}

func TestVerifyDatFileZeroFilledMiddle(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 20)
	// zero needles 5..7 completely, like a lost block range
	start := layout[4].offset
	end := layout[6].offset + layout[6].disk
	patchDat(t, datName, start, make([]byte, end-start))

	sum, anomalies, seen := runVerify(t, datName)
	if len(anomalies) != 1 || anomalies[0].Kind != "zero-fill" || anomalies[0].Offset != start || anomalies[0].Length != end-start {
		t.Fatalf("want one zero-fill region [%d,%d), got %+v", start, end, anomalies)
	}
	for id := uint64(1); id <= 20; id++ {
		if want := id < 5 || id > 7; seen[id] != want {
			t.Errorf("needle %d: seen=%v want %v", id, seen[id], want)
		}
	}
	if sum.ZeroBytes != end-start || sum.TailGarbage {
		t.Errorf("zero bytes %d, tail garbage %v", sum.ZeroBytes, sum.TailGarbage)
	}
}

func TestVerifyDatFileFlippedPayloadByte(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 20)
	patchDat(t, datName, layout[9].offset+types.NeedleHeaderSize+100, []byte{0xde, 0xad})

	_, anomalies, seen := runVerify(t, datName)
	if len(anomalies) != 1 || !strings.Contains(anomalies[0].Detail, "invalid CRC") || !strings.Contains(anomalies[0].Detail, "fid:1,") {
		t.Fatalf("want one CRC anomaly naming the fid, got %+v", anomalies)
	}
	if seen[10] || len(seen) != 19 {
		t.Fatalf("needle 10 must be dropped and the other 19 kept, seen=%d", len(seen))
	}
}

func TestVerifyDatFileCorruptHeaderSizeDoesNotSwallowNeighbours(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 20)
	// inflate needle 3's size so it appears to cover several following needles
	badSize := make([]byte, 4)
	badSize[2], badSize[3] = 0x40, 0x00 // 16384
	patchDat(t, datName, layout[2].offset+types.CookieSize+types.NeedleIdSize, badSize)

	_, anomalies, seen := runVerify(t, datName)
	if len(anomalies) != 1 {
		t.Fatalf("want one anomaly, got %+v", anomalies)
	}
	if seen[3] || len(seen) != 19 {
		t.Fatalf("only needle 3 may be lost, seen=%d", len(seen))
	}
}

func TestVerifyDatFileRandomGarbageRegion(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 20)
	start := layout[11].offset + 10 // misaligned start inside needle 12
	end := layout[13].offset + layout[13].disk
	junk := make([]byte, end-start)
	if _, err := rand.Read(junk); err != nil {
		t.Fatal(err)
	}
	patchDat(t, datName, start, junk)

	_, anomalies, seen := runVerify(t, datName)
	if len(anomalies) != 1 || anomalies[0].Kind != "garbage" {
		t.Fatalf("want one garbage region, got %+v", anomalies)
	}
	for id := uint64(1); id <= 20; id++ {
		if want := id < 12 || id > 14; seen[id] != want {
			t.Errorf("needle %d: seen=%v want %v", id, seen[id], want)
		}
	}
}

func TestVerifyDatFileTornTailAndTruncate(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 20)
	// cut the file in the middle of the last data needle's body, then tack on zeros
	cut := layout[19].offset + layout[19].disk/2
	if err := os.Truncate(datName, cut); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(datName, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(make([]byte, 5000))
	f.Close()

	sum, anomalies, seen := runVerify(t, datName)
	if !sum.TailGarbage || sum.CleanEnd != layout[19].offset || len(seen) != 19 {
		t.Fatalf("tail garbage=%v cleanEnd=%d (want %d) seen=%d anomalies=%+v", sum.TailGarbage, sum.CleanEnd, layout[19].offset, len(seen), anomalies)
	}
	if err := truncateDatTail(datName, sum.CleanEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(datName + ".tail." + "0.bak"); err == nil {
		t.Fatal("unexpected backup name")
	}
	if matches, _ := filepath.Glob(datName + ".tail.*.bak"); len(matches) != 1 {
		t.Fatalf("the partially written needle should have been saved, got %v", matches)
	}
	sum, anomalies, _ = runVerify(t, datName)
	if sum.TailGarbage || len(anomalies) != 0 || sum.CleanEnd != layout[19].offset {
		t.Fatalf("after truncate: %+v %+v", sum, anomalies)
	}
}

// TestFixVerifyLoadsWritable corrupts a volume in the middle and at the tail,
// rebuilds it with the -verify path, and loads it with the real volume code to
// make sure the load-time integrity check no longer flips it read-only.
func TestFixVerifyLoadsWritable(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 30)
	dir := filepath.Dir(datName)

	start := layout[9].offset
	end := layout[11].offset + layout[11].disk
	patchDat(t, datName, start, make([]byte, end-start))
	cut := layout[29].offset + layout[29].disk/2
	if err := os.Truncate(datName, cut); err != nil {
		t.Fatal(err)
	}
	// a stale index left over from before the damage
	if err := os.WriteFile(filepath.Join(dir, "1.idx"), make([]byte, 16*5), 0644); err != nil {
		t.Fatal(err)
	}

	*fixVerify, *fixTruncateTail, *fixDryRun = true, true, false
	t.Cleanup(func() { *fixVerify, *fixTruncateTail = false, false })
	doFixOneVolumeVerified(dir, "1", 1, filepath.Join(dir, "1.idx"))

	if _, err := os.Stat(filepath.Join(dir, "1.idx.bak")); err != nil {
		t.Errorf("old index should have been kept: %v", err)
	}
	v, err := storage.NewVolume(dir, dir, "", 1, storage.NeedleMapInMemory, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("load volume: %v", err)
	}
	defer v.Close()
	// IsReadOnly needs a disk location the test volume does not have, so run
	// the same load-time integrity check the volume server uses to flip it.
	idxFile, err := os.Open(filepath.Join(dir, "1.idx"))
	if err != nil {
		t.Fatal(err)
	}
	defer idxFile.Close()
	if _, err := storage.CheckVolumeDataIntegrity(v, idxFile); err != nil {
		t.Fatalf("integrity check would mark the volume read-only: %v", err)
	}
	if _, errs := v.ScrubIndex(); len(errs) != 0 {
		t.Fatalf("index scrub: %v", errs)
	}
	// 29 needles were written before the torn one; 3 were zeroed
	if got := v.FileCount(); got != 26 {
		t.Errorf("file count %d, want 26", got)
	}
}

// A file that is legitimately all zeros is an ordinary needle: it has a real
// header (non-zero id) and a CRC, so it must verify, and it must not be
// mistaken for damage or swallowed when a damaged region sits next to it.
func TestVerifyDatFileLegitimateZeroPayload(t *testing.T) {
	datName := filepath.Join(t.TempDir(), "1.dat")
	version := needle.GetCurrentVersion()
	df, err := os.OpenFile(datName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	b := backend.NewDiskFile(df)
	defer b.Close()
	sb := super_block.SuperBlock{Version: version, ReplicaPlacement: &super_block.ReplicaPlacement{}, Ttl: &needle.TTL{}}
	if _, err := b.WriteAt(sb.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	var layout []verifyFixtureNeedle
	for i := 1; i <= 6; i++ {
		n := new(needle.Needle)
		n.Id = types.Uint64ToNeedleId(uint64(i))
		n.Cookie = types.Uint32ToCookie(7)
		n.Data = make([]byte, 100000) // i == 2, 3, 5: stays all zeros
		if i != 2 && i != 3 && i != 5 {
			rand.Read(n.Data)
		}
		n.Checksum = needle.NewCRC(n.Data)
		off, _, actual, err := n.Append(b, version)
		if err != nil {
			t.Fatal(err)
		}
		layout = append(layout, verifyFixtureNeedle{uint64(i), int64(off), actual})
	}
	_ = b.Sync()

	sum, anomalies, seen := runVerify(t, datName)
	if len(anomalies) != 0 || sum.Needles != 6 || len(seen) != 6 {
		t.Fatalf("zero-payload needles must verify: needles=%d anomalies=%+v", sum.Needles, anomalies)
	}

	// destroy needle 4 (random data) between two zero-payload needles
	patchDat(t, datName, layout[3].offset, make([]byte, layout[3].disk))
	sum, anomalies, seen = runVerify(t, datName)
	if len(anomalies) != 1 || anomalies[0].Kind != "zero-fill" || anomalies[0].Offset != layout[3].offset || anomalies[0].Length != layout[3].disk {
		t.Fatalf("want exactly needle 4 reported as zero-fill, got %+v", anomalies)
	}
	if len(seen) != 5 || seen[4] || !seen[2] || !seen[3] || !seen[5] {
		t.Fatalf("zero-payload neighbours must survive: seen=%v", seen)
	}
}
