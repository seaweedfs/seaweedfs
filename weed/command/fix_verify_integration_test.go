package command

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// These integration tests pin the damage-recovery behaviour against the
// upstream issues that motivated it:
//
//   - #5674: a .dat whose head (right after the superblock) is zero-filled must
//     still yield every needle behind the damaged region.
//   - #12914: one bad header in the middle currently makes plain `weed fix`
//     drop every later needle and index nothing after the damage; `fix.recover`
//     must recover the survivors, including the newest copy of a rewritten
//     needle, and a zeroed superblock must be a clear error, not a silent
//     version-0 parse.

// TestIssue5674ZeroFilledHead covers the geometry reported in #5674: the
// volume starts with zeros instead of the first needle records. `fix.recover`
// must report the zero-filled region and index every surviving needle.
func TestIssue5674ZeroFilledHead(t *testing.T) {
	const lostRecords = 6
	for _, tc := range []struct {
		name       string
		misaligned bool
	}{
		{"damage ends on a record boundary", false},
		{"damage cuts the next record", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			datName, layout := buildVerifyFixture(t, 20)
			// layout[i] is needle i+1; layout[20] is the trailing delete record.
			zeroEnd := layout[lostRecords].offset
			if tc.misaligned {
				zeroEnd += layout[lostRecords].disk / 2
			}
			patchDat(t, datName, 8, make([]byte, zeroEnd-8)) // keep the 8-byte superblock

			sum, anomalies, seen := runVerify(t, datName)

			firstSurvivor := uint64(lostRecords + 1)
			if tc.misaligned {
				firstSurvivor = lostRecords + 2 // the cut record cannot verify
			}
			var want []uint64
			for id := firstSurvivor; id <= 20; id++ {
				want = append(want, id)
			}
			var got []uint64
			for id := range seen {
				got = append(got, id)
			}
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Fatalf("recovered needles %v, want %v", got, want)
			}
			if len(anomalies) == 0 {
				t.Fatal("the zero-filled head was not reported")
			}
			if anomalies[0].Offset != 8 {
				t.Errorf("first anomaly starts at %d, want the head of the needle area (8)", anomalies[0].Offset)
			}
			if !tc.misaligned && (anomalies[0].Kind != "zero-fill" || anomalies[0].Length != zeroEnd-8) {
				t.Errorf("aligned zero head: got kind %q length %d, want zero-fill %d",
					anomalies[0].Kind, anomalies[0].Length, zeroEnd-8)
			}
			if sum.BadRegions == 0 || sum.Tombstones != 1 {
				t.Errorf("bad regions=%d tombstones=%d, want damage reported and the trailing delete record kept",
					sum.BadRegions, sum.Tombstones)
			}
		})
	}
}

// TestIssue12914LegacyFixDropsNeedlesAfterDamage pins the behaviour reported in
// #12914 item 1: plain `weed fix` stops at the first bad header and loses every
// needle behind it; `weed fix.recover` recovers them.
func TestIssue12914LegacyFixDropsNeedlesAfterDamage(t *testing.T) {
	dir := t.TempDir()
	base, offsets := writeFixTestVolume(t, dir, 8)
	// A huge size field in the header of needle 5 makes the record run past EOF.
	patchDat(t, base+".dat", offsets[4]+types.NeedleIdSize+types.CookieSize, []byte{0x7f, 0xff, 0xff, 0xf0})

	savedIgnore, savedStatus := *fixIgnoreError, commandExitStatus
	*fixIgnoreError, commandExitStatus = true, 0
	t.Cleanup(func() {
		*fixIgnoreError, commandExitStatus = savedIgnore, savedStatus
	})

	doFixOneVolume(dir, "fix_7", "fix", 7, true)
	if got := indexedKeys(t, base+".idx"); !slices.Equal(got, []uint64{1, 2, 3, 4}) {
		t.Fatalf("plain weed fix indexed %v, want [1 2 3 4] before the damage (#12914)", got)
	}

	withVerifyFlags(t, true)
	doRecoverOneVolume(dir, "fix_7", 7)
	if got := indexedKeys(t, base+".idx"); !slices.Equal(got, []uint64{1, 2, 3, 4, 6, 7, 8}) {
		t.Fatalf("weed fix.recover indexed %v, want [1 2 3 4 6 7 8]", got)
	}
}

// TestIssue12914RewrittenNeedleBehindDamage checks that the newest copy of a
// needle rewritten after a damaged region wins over nothing: the damaged old
// record is skipped and the rebuild indexes the later copy at its real offset.
func TestIssue12914RewrittenNeedleBehindDamage(t *testing.T) {
	dir := t.TempDir()
	base, offsets := writeFixTestVolume(t, dir, 8)

	// Rewrite needle 5 at the end of the .dat, then destroy the original record.
	f, err := os.OpenFile(base+".dat", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	rewritten := &needle.Needle{Id: types.Uint64ToNeedleId(5), Data: bytes.Repeat([]byte{0x77}, 4321)}
	rewritten.Checksum = needle.NewCRC(rewritten.Data)
	newOffset, _, _, err := rewritten.Append(datBackend, needle.GetCurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	datBackend.Close()
	patchDat(t, base+".dat", offsets[4], make([]byte, offsets[5]-offsets[4]))

	withVerifyFlags(t, true)
	doRecoverOneVolume(dir, "fix_7", 7)

	got := idxOffsets(t, base+".idx")
	for _, id := range []uint64{1, 2, 3, 4, 6, 7, 8} {
		if _, ok := got[id]; !ok {
			t.Errorf("needle %d after the damage is missing from the rebuilt index", id)
		}
	}
	off, ok := got[5]
	if !ok {
		t.Fatal("rewritten needle 5 is missing from the rebuilt index")
	}
	if off != int64(newOffset) {
		t.Fatalf("needle 5 indexed at offset %d, want the newest copy at %d", off, newOffset)
	}
}

// TestIssue5674ZeroedSuperBlock covers the second half of #5674: when the first
// bytes are gone too, the file must fail with an explicit version error instead
// of being parsed as a version-0 volume.
func TestIssue5674ZeroedSuperBlock(t *testing.T) {
	datName := filepath.Join(t.TempDir(), "1.dat")
	if err := os.WriteFile(datName, make([]byte, 4096), 0644); err != nil {
		t.Fatal(err)
	}
	_, _, err := verifyDatFile(datName, 1, datVerifyOptions{}, func(*needle.Needle, int64) {})
	if err == nil || !strings.Contains(err.Error(), "unsupported needle version") {
		t.Fatalf("want an explicit unsupported-version error, got %v", err)
	}
}

// TestVerifyDatFileVersion1And2 guards the supported needle-format range: the
// scan must verify volumes written with the older needle layouts, not only the
// current version.
func TestVerifyDatFileVersion1And2(t *testing.T) {
	for _, version := range []needle.Version{needle.Version1, needle.Version2} {
		t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) {
			datName := buildVerifyFixtureVersion(t, 5, version)
			sum, anomalies, seen := runVerify(t, datName)
			if len(anomalies) != 0 || sum.Needles != 5 || len(seen) != 5 {
				t.Fatalf("needles=%d seen=%d anomalies=%+v, want a clean 5-needle volume", sum.Needles, len(seen), anomalies)
			}
		})
	}
}

// buildVerifyFixtureVersion writes count needles (ids 1..count) into a volume
// whose superblock and needle records use the given version.
func buildVerifyFixtureVersion(t *testing.T, count int, version needle.Version) string {
	t.Helper()
	datName := filepath.Join(t.TempDir(), "1.dat")
	f, err := os.OpenFile(datName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	defer datBackend.Close()
	sb := super_block.SuperBlock{
		Version:          version,
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              &needle.TTL{},
	}
	if _, err := datBackend.WriteAt(sb.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= count; i++ {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(uint64(i)), Data: bytes.Repeat([]byte{byte(i)}, 700+i*11)}
		n.Checksum = needle.NewCRC(n.Data)
		if _, _, _, err := n.Append(datBackend, version); err != nil {
			t.Fatalf("append needle %d: %v", i, err)
		}
	}
	return datName
}

// TestFixRefusesPendingCompactMarker: a .cpc marker means an interrupted
// vacuum commit is waiting for the volume server to reconcile it. Writing a
// rebuilt .idx on top of that decision must be refused with a
// clear, scriptable error.
func TestFixRefusesPendingCompactMarker(t *testing.T) {
	dir := t.TempDir()
	base, _ := writeFixTestVolume(t, dir, 3)
	if err := os.WriteFile(base+".cpc", nil, 0644); err != nil {
		t.Fatal(err)
	}

	withVerifyFlags(t, true)

	doRecoverOneVolume(dir, "fix_7", 7)
	if CommandExitStatus() != 1 {
		t.Fatalf("exit status = %d, want 1 (pending commit marker must fail the run)", CommandExitStatus())
	}
	if _, err := os.Stat(base + ".idx"); !os.IsNotExist(err) {
		t.Fatal("a rebuilt .idx was written despite the pending commit marker")
	}
	if _, err := os.Stat(base + ".cpc"); err != nil {
		t.Fatalf("the pending marker must be left for the volume server: %v", err)
	}
}

// idxOffsets returns key -> .dat offset for every entry of the rebuilt .idx.
func idxOffsets(t *testing.T, idxPath string) map[uint64]int64 {
	t.Helper()
	f, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("open idx: %v", err)
	}
	defer f.Close()
	offsets := map[uint64]int64{}
	if err := idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		offsets[uint64(key)] = offset.ToActualOffset()
		return nil
	}); err != nil {
		t.Fatalf("walk idx: %v", err)
	}
	return offsets
}
