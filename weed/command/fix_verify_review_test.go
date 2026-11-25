package command

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A zero fill that erases a live record's payload length and flags but keeps
// its header leaves a record that parses with no data, so its CRC is never
// checked. It must count as damage, not as an empty file.
func TestVerifyZeroedPayloadLengthIsDamage(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 5)
	n3 := layout[2]
	dataLen := 1000 + 3*37
	patchDat(t, datName, n3.offset+types.NeedleHeaderSize, make([]byte, 4+dataLen+1))

	sum, anomalies, seen := runVerify(t, datName)
	if seen[3] {
		t.Fatalf("needle 3 with an erased payload length was indexed as live")
	}
	if len(anomalies) == 0 || sum.BadRegions == 0 {
		t.Fatalf("no damage reported for the erased payload: %+v", anomalies)
	}
	for _, id := range []uint64{2, 4, 5} {
		if !seen[id] {
			t.Errorf("needle %d next to the damage was lost", id)
		}
	}
}

// -maxNeedleSizeMB bounds only the search for the next needle after damage. A
// healthy needle larger than it, reached at a known record boundary, must
// still verify.
func TestVerifyLargeNeedleAtBoundaryIsNotDamage(t *testing.T) {
	datName, _ := buildVerifyFixture(t, 5)
	seen := map[uint64]bool{}
	sum, anomalies, err := verifyDatFile(datName, 1, datVerifyOptions{maxNeedleSize: 512, window: 64 << 10, zeroWindow: 512}, func(n *needle.Needle, offset int64) {
		if n.Size.IsValid() {
			seen[uint64(n.Id)] = true
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(anomalies) != 0 || len(seen) != 5 || sum.Tombstones != 1 {
		t.Fatalf("seen=%d tombstones=%d anomalies=%+v, want all 5 needles and the delete record", len(seen), sum.Tombstones, anomalies)
	}
}

// After damage, a delete record followed by an unreadable tail must still be
// found; dropping it brings the deleted file back.
func TestVerifyTombstoneBeforeDamagedTailIsKept(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 5)
	patchDat(t, datName, layout[4].offset, bytes.Repeat([]byte{0xA5}, types.NeedleHeaderSize))
	f, err := os.OpenFile(datName, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte{0x5A}, 64)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	deleted := false
	if _, _, err := verifyDatFile(datName, 1, datVerifyOptions{window: 64 << 10, zeroWindow: 512}, func(n *needle.Needle, offset int64) {
		if uint64(n.Id) == 1 && !n.Size.IsValid() && offset == layout[5].offset {
			deleted = true
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatalf("the delete record for needle 1 before the damaged tail was dropped, so the file would come back")
	}
}

type failingReaderAt struct {
	r      io.ReaderAt
	failAt int64
}

func (f failingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > f.failAt {
		return 0, syscall.EIO
	}
	return f.r.ReadAt(p, off)
}

// A disk read error while searching for the next needle is not the end of
// the data: it must surface as an error, not as a garbage tail to cut.
func TestFindNextReadErrorIsReported(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 5)
	raw, err := os.Open(datName)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	st, _ := raw.Stat()
	r := failingReaderAt{r: raw, failAt: layout[2].offset}
	opts := datVerifyOptions{window: 64 << 10, zeroWindow: 512}
	opts.normalize()
	s := &datVerifier{f: r, vid: 1, version: needle.GetCurrentVersion(), base: 8, size: st.Size(), opts: opts,
		win: &datWindow{f: r, size: st.Size(), buf: make([]byte, opts.window)}}
	next, err := s.findNext(layout[0].offset + types.NeedlePaddingSize)
	if err == nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("findNext = %d, %v; want the read error", next, err)
	}
}

// When the newest record of a key is damaged, the rebuilt index points at an
// older copy. That must be reported, not done silently.
func TestStaleVersionWarnings(t *testing.T) {
	nm := needle_map.NewMemDb()
	defer nm.Close()
	if err := nm.Set(types.NeedleId(7), types.ToOffset(800), 100); err != nil {
		t.Fatal(err)
	}
	warnings := staleVersionWarnings(nm, []damagedRecord{
		{Key: 7, Offset: 4096}, // damaged copy after the indexed one: stale
		{Key: 7, Offset: 64},   // damaged copy before the indexed one: fine
		{Key: 9, Offset: 4096}, // key not indexed at all
	})
	if len(warnings) != 1 || warnings[0].Kind != "stale-version" || warnings[0].Offset != 4096 {
		t.Fatalf("warnings = %+v, want one stale-version at 4096", warnings)
	}
}

func TestVerifyRecordsKeysHitByDamage(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 5)
	n3 := layout[2]
	patchDat(t, datName, n3.offset+types.NeedleHeaderSize+4+100, []byte{0xFF, 0x00, 0xFF})
	sum, _, _ := runVerify(t, datName)
	if len(sum.DamagedRecords) != 1 || sum.DamagedRecords[0] != (damagedRecord{Key: 3, Offset: n3.offset}) {
		t.Fatalf("damaged records = %+v, want key 3 at %d", sum.DamagedRecords, n3.offset)
	}
}

func withVerifyFlags(t *testing.T, ignoreError bool) {
	t.Helper()
	savedVerify, savedIgnore, savedStatus := *fixVerify, *fixIgnoreError, commandExitStatus
	*fixVerify, *fixIgnoreError, commandExitStatus = true, ignoreError, 0
	t.Cleanup(func() { *fixVerify, *fixIgnoreError, commandExitStatus = savedVerify, savedIgnore, savedStatus })
}

// The rebuilt index is staged next to the live one and swapped in only once
// it is complete; a stale .ldb cache next to it is dropped so the volume
// server rebuilds it from the new index.
func TestFixVerifyStagesIndexAndDropsLevelDb(t *testing.T) {
	dir := t.TempDir()
	base, _ := writeFixTestVolume(t, dir, 3)
	if err := os.WriteFile(base+".idx", []byte("old index, 16 b"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(base+".ldb", 0755); err != nil {
		t.Fatal(err)
	}
	withVerifyFlags(t, false)

	doFixOneVolume(dir, "fix_7", "fix", 7, true)

	if got := indexedKeys(t, base+".idx"); len(got) != 3 {
		t.Fatalf("rebuilt index keys = %v, want 3", got)
	}
	if b, err := os.ReadFile(base + ".idx.bak"); err != nil || string(b) != "old index, 16 b" {
		t.Fatalf("old index not kept as .idx.bak: %q %v", b, err)
	}
	if _, err := os.Stat(base + ".ldb"); !os.IsNotExist(err) {
		t.Fatalf("stale .ldb cache was left next to the rebuilt index")
	}
	matches, _ := filepath.Glob(base + ".idx.tmp*")
	if len(matches) != 0 {
		t.Fatalf("staging files left behind: %v", matches)
	}
}

// With -ignoreError a volume that cannot be verified is reported and the run
// goes on to the next volume, like the plain fix path does.
func TestFixVerifyIgnoreErrorContinues(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fix_7.dat"), make([]byte, 4096), 0644); err != nil {
		t.Fatal(err)
	}
	withVerifyFlags(t, true)

	doFixOneVolume(dir, "fix_7", "fix", 7, true)

	if CommandExitStatus() != 1 {
		t.Fatalf("exit status = %d, want 1", CommandExitStatus())
	}
	if _, err := os.Stat(filepath.Join(dir, "fix_7.idx")); !os.IsNotExist(err) {
		t.Fatalf("an index was written for a volume that could not be verified")
	}
}

// Volume ids are uint32; a larger number in a file name is rejected while
// parsing instead of being narrowed.
func TestRunFixRejectsVolumeIdBeyondUint32(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "c_4294967296.dat"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if runFix(cmdFix, []string{dir}) {
			t.Errorf("runFix accepted an out-of-range volume id")
		}
	})
	if !strings.Contains(out, "Failed to parse volume id") {
		t.Fatalf("output %q, want a parse error", out)
	}
}
