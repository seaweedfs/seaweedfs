package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
)

func TestReadNeedleNilNeedleMap(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	v.dataFileAccessLock.Lock()
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}
	v.dataFileAccessLock.Unlock()

	n := new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(1)

	if _, err := v.readNeedle(n, &ReadOption{}, nil); err != ErrorNotFound {
		t.Fatalf("readNeedle: want ErrorNotFound, got %v", err)
	}

	err = v.readNeedleDataInto(n, &ReadOption{ReadBufferSize: 1024}, &bytes.Buffer{}, 0, 0)
	if err != ErrorNotFound {
		t.Fatalf("readNeedleDataInto: want ErrorNotFound, got %v", err)
	}
}

func TestReadNeedMetaWithWritesAndUpdates(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	type WriteInfo struct {
		offset int64
		size   int32
	}
	writeInfos := make([]WriteInfo, 30)
	mockLastUpdateTime := uint64(1000000000000)
	// initialize 20 needles then update first 10 needles
	for i := 1; i <= 30; i++ {
		n := newRandomNeedle(uint64(i % 20))
		n.Flags = 0x08
		n.LastModified = mockLastUpdateTime
		mockLastUpdateTime += 2000
		offset, _, _, err := v.writeNeedle2(n, true, false, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
		writeInfos[i-1] = WriteInfo{offset: int64(offset), size: int32(n.Size)}
	}
	expectedLastUpdateTime := uint64(1000000000000)
	for i := 0; i < 30; i++ {
		testNeedle := new(needle.Needle)
		testNeedle.Id = types.Uint64ToNeedleId(uint64(i + 1%20))
		testNeedle.Flags = 0x08
		v.readNeedleMetaAt(testNeedle, writeInfos[i].offset, writeInfos[i].size)
		actualLastModifiedTime := testNeedle.LastModified
		if writeInfos[i].size != 0 {
			assert.Equal(t, expectedLastUpdateTime, actualLastModifiedTime, "The two words should be the same.")
		}
		expectedLastUpdateTime += 2000
	}
}

func TestReadNeedMetaWithDeletesThenWrites(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	type WriteInfo struct {
		offset int64
		size   int32
	}
	writeInfos := make([]WriteInfo, 10)
	mockLastUpdateTime := uint64(1000000000000)
	for i := 1; i <= 10; i++ {
		n := newRandomNeedle(uint64(i % 5))
		n.Flags = 0x08
		n.LastModified = mockLastUpdateTime
		mockLastUpdateTime += 2000
		offset, _, _, err := v.writeNeedle2(n, true, false, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
		if i < 5 {
			size, err := v.deleteNeedle2(n)
			if err != nil {
				t.Fatalf("delete needle %d: %v", i, err)
			}
			writeInfos[i-1] = WriteInfo{offset: int64(offset), size: int32(size)}
		} else {
			writeInfos[i-1] = WriteInfo{offset: int64(offset), size: int32(n.Size)}
		}
	}

	expectedLastUpdateTime := uint64(1000000000000)
	for i := 0; i < 10; i++ {
		testNeedle := new(needle.Needle)
		testNeedle.Id = types.Uint64ToNeedleId(uint64(i + 1%5))
		testNeedle.Flags = 0x08
		v.readNeedleMetaAt(testNeedle, writeInfos[i].offset, writeInfos[i].size)
		actualLastModifiedTime := testNeedle.LastModified
		if writeInfos[i].size != 0 {
			assert.Equal(t, expectedLastUpdateTime, actualLastModifiedTime, "The two words should be the same.")
		}
		expectedLastUpdateTime += 2000
	}
}

// scanRecorder records visited offsets and fails once a scan visits more
// records than the file holds.
type scanRecorder struct {
	readBody  bool
	maxVisits int
	offsets   []int64
}

func (s *scanRecorder) VisitSuperBlock(super_block.SuperBlock) error { return nil }

func (s *scanRecorder) ReadNeedleBody() bool { return s.readBody }

func (s *scanRecorder) VisitNeedle(_ *needle.Needle, offset int64, _, _ []byte) error {
	s.offsets = append(s.offsets, offset)
	if len(s.offsets) > s.maxVisits {
		return fmt.Errorf("visited %d records in a file of %d, offsets %v", len(s.offsets), s.maxVisits, s.offsets)
	}
	return nil
}

var errDidNotReturn = errors.New("did not return in time")

// runWithTimeout returns errDidNotReturn if fn does not finish within d.
func runWithTimeout(d time.Duration, fn func() error) error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		return errDidNotReturn
	}
}

// A corrupt .dat header can make a record's length zero or negative; the scan
// must stop there with ErrorCorrupted rather than re-read the header or step
// back into the previous record. A negative size whose record length stays
// positive is stepped over as before.
func TestScanVolumeFileFrom_StopsAtRecordThatCannotAdvance(t *testing.T) {
	cases := []struct {
		version needle.Version
		size    types.Size
		stops   bool
	}{
		{needle.Version3, -1, false},   // record length 32
		{needle.Version3, -36, true},   // record length 0
		{needle.Version3, -43, true},   // record length 0
		{needle.Version3, -44, true},   // record length -8
		{needle.Version3, -4096, true}, // record length -4056
		{needle.Version2, -1, false},   // record length 24
		{needle.Version2, -28, true},   // record length 0
		{needle.Version2, -35, true},   // record length 0
		{needle.Version2, -36, true},   // record length -8
	}
	for _, tc := range cases {
		recordLen := needle.GetActualSize(tc.size, tc.version)
		if (recordLen <= 0) != tc.stops {
			t.Fatalf("v%d size %d: record length %d, case expects stops=%v", tc.version, tc.size, recordLen, tc.stops)
		}
		for _, readBody := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/size%d/readBody=%v", tc.version, tc.size, readBody), func(t *testing.T) {
				f, err := os.Create(filepath.Join(t.TempDir(), "1.dat"))
				if err != nil {
					t.Fatalf("create dat: %v", err)
				}
				dat := backend.NewDiskFile(f)
				defer dat.Close()

				first, _, _, err := newRandomNeedle(1).Append(dat, tc.version)
				if err != nil {
					t.Fatalf("append needle 1: %v", err)
				}
				corruptAt, _, err := dat.GetStat()
				if err != nil {
					t.Fatalf("stat dat: %v", err)
				}
				raw := make([]byte, max(recordLen, types.NeedleHeaderSize))
				types.NeedleIdToBytes(raw[types.CookieSize:types.CookieSize+types.NeedleIdSize], types.Uint64ToNeedleId(99))
				types.SizeToBytes(raw[types.CookieSize+types.NeedleIdSize:types.NeedleHeaderSize], tc.size)
				if _, err := dat.WriteAt(raw, corruptAt); err != nil {
					t.Fatalf("append corrupt record: %v", err)
				}
				second, _, _, err := newRandomNeedle(2).Append(dat, tc.version)
				if err != nil {
					t.Fatalf("append needle 2: %v", err)
				}

				scanner := &scanRecorder{readBody: readBody, maxVisits: 3}
				err = runWithTimeout(10*time.Second, func() error {
					return ScanVolumeFileFrom(tc.version, dat, 0, scanner)
				})

				want := []int64{int64(first), corruptAt, int64(second)}
				if tc.stops {
					want = want[:2]
					if !errors.Is(err, needle.ErrorCorrupted) {
						t.Errorf("scan error = %v, want one wrapping ErrorCorrupted", err)
					}
				} else if err != nil {
					t.Errorf("scan error = %v, want nil", err)
				}
				if !reflect.DeepEqual(scanner.offsets, want) {
					t.Errorf("visited offsets %v, want %v", scanner.offsets, want)
				}
			})
		}
	}
}
