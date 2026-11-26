package command

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
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
	s := &datVerifier{f: r, vid: 1, version: needle.Version3, base: 8, size: st.Size(), opts: opts,
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
	savedIgnore, savedStatus := *fixRecoverIgnoreError, commandExitStatus
	*fixRecoverIgnoreError, commandExitStatus = ignoreError, 0
	t.Cleanup(func() { *fixRecoverIgnoreError, commandExitStatus = savedIgnore, savedStatus })
}

// The rebuilt index is staged next to the live one and swapped in only once
// it is complete; a stale .ldb cache next to it is dropped so the volume
// server rebuilds it from the new index.
func TestFixVerifyStagesIndexAndDropsLevelDb(t *testing.T) {
	dir := t.TempDir()
	base, _ := writeFixTestVolume(t, dir, 3, needle.Version3)
	if err := os.WriteFile(base+".idx", []byte("old index, 16 b"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(base+".ldb", 0755); err != nil {
		t.Fatal(err)
	}
	for _, ext := range []string{".sdx", ".rdb"} {
		if err := os.WriteFile(base+ext, []byte("stale"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	withVerifyFlags(t, false)

	doRecoverOneVolume(dir, "fix_7", 7)

	if got := indexedKeys(t, base+".idx"); len(got) != 3 {
		t.Fatalf("rebuilt index keys = %v, want 3", got)
	}
	if b, err := os.ReadFile(base + ".idx.bak"); err != nil || string(b) != "old index, 16 b" {
		t.Fatalf("old index not kept as .idx.bak: %q %v", b, err)
	}
	for _, ext := range []string{".ldb", ".sdx", ".rdb"} {
		if _, err := os.Stat(base + ext); !os.IsNotExist(err) {
			t.Fatalf("stale %s cache was left next to the rebuilt index", ext)
		}
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

	doRecoverOneVolume(dir, "fix_7", 7)

	if CommandExitStatus() != 1 {
		t.Fatalf("exit status = %d, want 1", CommandExitStatus())
	}
	if _, err := os.Stat(filepath.Join(dir, "fix_7.idx")); !os.IsNotExist(err) {
		t.Fatalf("an index was written for a volume that could not be verified")
	}
}

// A remote-tiered volume is served from its remote .dat through the existing
// .idx, so a local .dat next to it must not replace that index.
func TestFixVerifyRefusesRemoteTier(t *testing.T) {
	dir := t.TempDir()
	base, _ := writeFixTestVolume(t, dir, 3, needle.GetCurrentVersion())
	if err := os.WriteFile(base+".idx", []byte("old index, 16 b"), 0644); err != nil {
		t.Fatal(err)
	}
	vi := &volume_server_pb.VolumeInfo{Files: []*volume_server_pb.RemoteFile{{BackendType: "s3", Key: "fix_7.dat"}}}
	if err := volume_info.SaveVolumeInfo(base+".vif", vi); err != nil {
		t.Fatal(err)
	}
	withVerifyFlags(t, false)

	err := recoverOneVolume(dir, "fix_7", 7, base+".idx", recoverOptions{includeDeleted: true, maxNeedleSizeMB: 64})
	if err == nil || !strings.Contains(err.Error(), "remote tier") {
		t.Fatalf("error %v, want a remote tier refusal", err)
	}
	if b, err := os.ReadFile(base + ".idx"); err != nil || string(b) != "old index, 16 b" {
		t.Fatalf("index of a remote-tiered volume was replaced: %q %v", b, err)
	}
}

// An unparsable .vif may hide a remote tier, so it is refused the same way.
func TestFixVerifyRefusesUnreadableVolumeInfo(t *testing.T) {
	dir := t.TempDir()
	base, _ := writeFixTestVolume(t, dir, 3, needle.GetCurrentVersion())
	if err := os.WriteFile(base+".idx", []byte("old index, 16 b"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base+".vif", []byte("{not json"), 0644); err != nil {
		t.Fatal(err)
	}
	withVerifyFlags(t, false)

	err := recoverOneVolume(dir, "fix_7", 7, base+".idx", recoverOptions{includeDeleted: true, maxNeedleSizeMB: 64})
	if err == nil || !strings.Contains(err.Error(), "cannot tell") {
		t.Fatalf("error %v, want a volume info refusal", err)
	}
	if b, err := os.ReadFile(base + ".idx"); err != nil || string(b) != "old index, 16 b" {
		t.Fatalf("index replaced despite an unreadable .vif: %q %v", b, err)
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

// Stray bytes in a zero-filled region can form a header with a small id and
// size 0. Unstamped, and followed by another such header, it used to pass as
// a delete record and delete a live file.
func TestVerifyStrayBytesDoNotDeleteLiveFiles(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 9)
	start, end := layout[4].offset, layout[7].offset
	z := make([]byte, end-start)
	z[64+11] = 0x02
	z[96+11] = 0x09
	patchDat(t, datName, start, z)

	if _, _, err := verifyDatFile(datName, 1, datVerifyOptions{window: 64 << 10, zeroWindow: 512}, func(n *needle.Needle, offset int64) {
		if !n.Size.IsValid() && offset >= start && offset < end {
			t.Errorf("a delete record for key %d was taken from the damaged region at %d", n.Id, offset)
		}
	}); err != nil {
		t.Fatal(err)
	}
}

// A run that found damage exits non-zero, like plain fix and -verifyCrc, so a
// script knows to restore from a replica; a clean volume exits 0.
func TestFixVerifyExitStatusReportsDamage(t *testing.T) {
	dir := t.TempDir()
	base, offsets := writeFixTestVolume(t, dir, 4, needle.Version3)
	withVerifyFlags(t, false)
	doRecoverOneVolume(dir, "fix_7", 7)
	if CommandExitStatus() != 0 {
		t.Fatalf("clean volume: exit status %d, want 0", CommandExitStatus())
	}
	patchDat(t, base+".dat", offsets[1], bytes.Repeat([]byte{0xA5}, types.NeedleHeaderSize))
	doRecoverOneVolume(dir, "fix_7", 7)
	if CommandExitStatus() != 1 {
		t.Fatalf("damaged volume: exit status %d, want 1", CommandExitStatus())
	}
}

// With -ignoreError a file name whose volume id does not fit uint32 is
// reported and the run goes on with the other volumes.
func TestRunFixIgnoreErrorSkipsBadVolumeId(t *testing.T) {
	dir := t.TempDir()
	writeFixTestVolume(t, dir, 2, needle.GetCurrentVersion())
	if err := os.WriteFile(filepath.Join(dir, "c_4294967296.dat"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	saved, savedStatus := *fixIgnoreError, commandExitStatus
	*fixIgnoreError, commandExitStatus = true, 0
	t.Cleanup(func() { *fixIgnoreError, commandExitStatus = saved, savedStatus })
	captureStdout(t, func() {
		if !runFix(cmdFix, []string{dir}) {
			t.Errorf("runFix stopped the batch at the bad file name")
		}
	})
	if CommandExitStatus() != 1 {
		t.Fatalf("exit status %d, want 1", CommandExitStatus())
	}
	if got := indexedKeys(t, filepath.Join(dir, "fix_7.idx")); len(got) != 2 {
		t.Fatalf("the other volume was not fixed: keys %v", got)
	}
}

// The guards that let a delete record found after damage stand on its own:
// a time older than the last verified record, or non-zero padding, rejects it.
func TestVerifyResyncedDeleteRecordGuards(t *testing.T) {
	cases := map[string]func(t *testing.T, datName string, tomb int64){
		"older than the last verified record": func(t *testing.T, datName string, tomb int64) {
			// a believable time, one hour before the record's own, so older than the needles before it
			at := tomb + types.NeedleHeaderSize + needle.NeedleChecksumSize
			raw, err := os.ReadFile(datName)
			if err != nil {
				t.Fatal(err)
			}
			ts := binary.BigEndian.Uint64(raw[at:]) - uint64(time.Hour)
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, ts)
			patchDat(t, datName, at, b)
		},
		"non-zero padding": func(t *testing.T, datName string, tomb int64) {
			patchDat(t, datName, tomb+types.NeedleHeaderSize+needle.NeedleChecksumSize+types.TimestampSize, []byte{1})
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			datName, layout := buildVerifyFixture(t, 5)
			tomb := layout[5].offset
			patchDat(t, datName, layout[4].offset, bytes.Repeat([]byte{0xA5}, types.NeedleHeaderSize))
			corrupt(t, datName, tomb)
			if _, _, err := verifyDatFile(datName, 1, datVerifyOptions{window: 64 << 10, zeroWindow: 512}, func(n *needle.Needle, offset int64) {
				if offset == tomb {
					t.Errorf("the delete record at %d was accepted", offset)
				}
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunFixVolumeIdFilterSkipsOutOfRangeName(t *testing.T) {
	dir := t.TempDir()
	writeFixTestVolume(t, dir, 2, needle.GetCurrentVersion())
	if err := os.WriteFile(filepath.Join(dir, "fix_4294967296.dat"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	saved, savedStatus := *fixVolumeId, commandExitStatus
	*fixVolumeId, commandExitStatus = 7, 0
	t.Cleanup(func() { *fixVolumeId, commandExitStatus = saved, savedStatus })
	captureStdout(t, func() {
		if !runFix(cmdFix, []string{dir}) {
			t.Errorf("runFix stopped at a file the -volumeId filter excludes")
		}
	})
	if got := indexedKeys(t, filepath.Join(dir, "fix_7.idx")); len(got) != 2 {
		t.Fatalf("the selected volume was not fixed: keys %v", got)
	}
}

// A damaged size field at a known boundary must be rejected from the data
// length that follows the header, before a body of that size is allocated.
func TestVerifyInflatedSizeRejectedBeforeBodyRead(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 5)
	raw, err := os.OpenFile(datName, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := raw.Stat()
	if err := raw.Truncate(st.Size() + 1<<20); err != nil {
		t.Fatal(err)
	}
	st, _ = raw.Stat()
	raw.Close()
	inflated := st.Size() - layout[1].offset - 64
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(inflated))
	patchDat(t, datName, layout[1].offset+types.CookieSize+types.NeedleIdSize, b[:])

	f, err := os.Open(datName)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	opts := datVerifyOptions{window: 64 << 10, zeroWindow: 512}
	opts.normalize()
	s := &datVerifier{f: f, vid: 1, version: needle.Version3, base: 8, size: st.Size(), opts: opts,
		win: &datWindow{f: f, size: st.Size(), buf: make([]byte, opts.window)}}
	if _, _, err := s.readNeedleAt(layout[1].offset, false); err == nil || !strings.Contains(err.Error(), "does not fit") {
		t.Fatalf("readNeedleAt = %v, want a data size rejection", err)
	}
	if cap(s.body) != 0 {
		t.Fatalf("allocated a %d byte body for the damaged record", cap(s.body))
	}
	_, _, seen := runVerify(t, datName)
	for _, id := range []uint64{1, 3, 4, 5} {
		if !seen[id] {
			t.Fatalf("needle %d after the damaged size was not recovered: %v", id, seen)
		}
	}
}

// A v2 delete chain that reaches the end of the file on its last probe proves
// itself just like a shorter one.
func TestVerifyDeleteChainEndingAtLastProbe(t *testing.T) {
	datName := buildVerifyFixtureVersion(t, 1, needle.Version2)
	f, err := os.OpenFile(datName, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	var first int64 = -1
	for i := 0; i < 17; i++ {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(uint64(100 + i))}
		offset, _, _, err := n.Append(datBackend, needle.Version2)
		if err != nil {
			t.Fatal(err)
		}
		if first < 0 {
			first = int64(offset)
		}
	}
	datBackend.Close()

	raw, err := os.Open(datName)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	st, _ := raw.Stat()
	opts := datVerifyOptions{window: 64 << 10, zeroWindow: 512}
	opts.normalize()
	s := &datVerifier{f: raw, vid: 1, version: needle.Version2, base: 8, size: st.Size(), opts: opts,
		win: &datWindow{f: raw, size: st.Size(), buf: make([]byte, opts.window)}}
	if !s.chainReachesData(first + needle.GetActualSize(0, needle.Version2)) {
		t.Fatalf("a delete chain of 16 records ending at the end of the file was rejected")
	}
}

// A live record whose timestamp alone is corrupted still passes its CRC. That
// timestamp must not become the lower bound a later delete record is held to.
func TestVerifyImplausibleTimestampIsNotResyncBaseline(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 5)
	raw, err := os.ReadFile(datName)
	if err != nil {
		t.Fatal(err)
	}
	live := layout[3].offset
	size := int64(binary.BigEndian.Uint32(raw[live+types.CookieSize+types.NeedleIdSize:]))
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, ^uint64(0))
	patchDat(t, datName, live+types.NeedleHeaderSize+size+needle.NeedleChecksumSize, b)
	patchDat(t, datName, layout[4].offset, bytes.Repeat([]byte{0xA5}, types.NeedleHeaderSize))

	deleted := false
	_, anomalies, err := verifyDatFile(datName, 1, datVerifyOptions{window: 64 << 10, zeroWindow: 512}, func(n *needle.Needle, offset int64) {
		if offset == layout[5].offset && !n.Size.IsValid() {
			deleted = true
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatalf("the delete record after damage was rejected; anomalies %+v", anomalies)
	}
	found := false
	for _, a := range anomalies {
		found = found || (a.Kind == "timestamp-implausible" && a.Offset == live)
	}
	if !found {
		t.Fatalf("the implausible timestamp was not reported: %+v", anomalies)
	}
}

// A read error at a known record boundary is not damage in the data: it must
// stop the repair instead of resynchronising past the unread record.
func TestVerifyReadErrorAtBoundaryIsReported(t *testing.T) {
	datName, layout := buildVerifyFixture(t, 5)
	raw, err := os.Open(datName)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	st, _ := raw.Stat()
	r := failingReaderAt{r: raw, failAt: layout[2].offset + 4}
	opts := datVerifyOptions{window: 64 << 10, zeroWindow: 512}
	opts.normalize()
	s := &datVerifier{f: r, vid: 1, version: needle.Version3, base: 8, size: st.Size(), opts: opts,
		win: &datWindow{f: r, size: st.Size(), buf: make([]byte, opts.window)}}
	if _, _, err := s.readNeedleAt(layout[2].offset, false); !errors.Is(err, errDatRead) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("readNeedleAt = %v, want the read error", err)
	}
}
