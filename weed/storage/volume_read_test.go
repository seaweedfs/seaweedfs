package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
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

// The CRC is known only after the last byte, so a full read must keep that
// page unwritten when the checksum mismatches; otherwise the GET has already
// committed a corrupt body.
func TestReadNeedleDataIntoChecksumMismatchHoldsLastPage(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	// 2048 is two pages (one buffer swap), 3000 is three, and 2500 with a
	// buffer larger than the needle is the single Write that used to commit
	// the whole body before the CRC check.
	cases := []struct {
		name string
		size int
		page int
	}{
		{"two pages", 2048, 1024},
		{"three pages", 3000, 1024},
		{"one buffer", 2500, 4096},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Repeat([]byte("abcdefghij"), (tc.size+9)/10)[:tc.size]
			n := new(needle.Needle)
			n.Data = append([]byte(nil), data...)
			n.Checksum = needle.NewCRC(n.Data)
			n.Id = types.Uint64ToNeedleId(uint64(i + 1))
			offset, _, _, err := v.writeNeedle2(n, true, false, false)
			if err != nil {
				t.Fatalf("write needle: %v", err)
			}
			nv, ok := v.nm.Get(n.Id)
			if !ok {
				t.Fatal("needle missing from index")
			}
			actual := nv.Offset.ToActualOffset()

			read := func() (bytes.Buffer, error) {
				t.Helper()
				meta := new(needle.Needle)
				meta.Id = n.Id
				if err := v.readNeedleMetaAt(meta, actual, int32(nv.Size)); err != nil {
					t.Fatalf("read meta at %d size %d: %v", actual, nv.Size, err)
				}
				var buf bytes.Buffer
				err := v.readNeedleDataInto(meta, &ReadOption{ReadBufferSize: tc.page}, &buf, 0, int64(meta.DataSize))
				return buf, err
			}

			intact, err := read()
			if err != nil {
				t.Fatalf("intact read: %v", err)
			}
			if !bytes.Equal(intact.Bytes(), data) {
				t.Fatalf("intact read len %d, want %d", intact.Len(), len(data))
			}

			dataOff := int64(offset) + types.NeedleHeaderSize + types.DataSizeSize
			if _, err := v.DataBackend.WriteAt([]byte{0xff}, dataOff); err != nil {
				t.Fatalf("damage needle: %v", err)
			}
			damaged, err := read()
			if err == nil || !strings.Contains(err.Error(), "checksum") {
				t.Fatalf("damaged read: got %v, want a checksum error", err)
			}
			held := len(data) % tc.page
			if held == 0 {
				held = tc.page
			}
			if damaged.Len() != len(data)-held {
				t.Fatalf("damaged read wrote %d bytes, want %d with the last page held back", damaged.Len(), len(data)-held)
			}
		})
	}
}

// With MustMetaOnly set, the payload is skipped whatever the needle's size.
// This is the option a delete uses. A chunk manifest is the exception: its
// data is still read, because a delete needs it to find the chunks.
func TestReadNeedleMustMetaOnly(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	plain := newRandomNeedle(1)
	plain.Cookie = 0x1234
	plain.Name = []byte("a.jpg")
	plain.NameSize = uint8(len(plain.Name))
	plain.SetHasName()
	plainOffset, _, _, err := v.writeNeedle2(plain, true, false, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	manifest := newRandomNeedle(2)
	manifest.SetIsChunkManifest()
	if _, _, _, err := v.writeNeedle2(manifest, true, false, false); err != nil {
		t.Fatalf("write chunk manifest: %v", err)
	}

	read := func(id uint64, option *ReadOption) (*needle.Needle, error) {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(id)}
		_, err := v.readNeedle(n, option, nil)
		return n, err
	}
	metaOnly := func() *ReadOption { return &ReadOption{AttemptMetaOnly: true, MustMetaOnly: true} }

	option := metaOnly()
	got, err := read(1, option)
	if err != nil {
		t.Fatalf("metadata read: %v", err)
	}
	if !option.IsMetaOnly || len(got.Data) != 0 {
		t.Errorf("metadata read: IsMetaOnly %v, %d bytes of data", option.IsMetaOnly, len(got.Data))
	}
	if got.Cookie != plain.Cookie || got.Size != plain.Size || got.DataSize != plain.DataSize || string(got.Name) != "a.jpg" {
		t.Errorf("metadata read: cookie %x size %d data size %d name %q", got.Cookie, got.Size, got.DataSize, got.Name)
	}

	// With AttemptMetaOnly alone, a small needle is still read in full.
	option = &ReadOption{AttemptMetaOnly: true}
	if got, err = read(1, option); err != nil || option.IsMetaOnly || !bytes.Equal(got.Data, plain.Data) {
		t.Errorf("attempted metadata read of a small needle: IsMetaOnly %v, %d bytes, %v", option.IsMetaOnly, len(got.Data), err)
	}

	option = metaOnly()
	if got, err = read(2, option); err != nil || option.IsMetaOnly || !bytes.Equal(got.Data, manifest.Data) {
		t.Errorf("metadata read of a chunk manifest: IsMetaOnly %v, %d bytes, %v", option.IsMetaOnly, len(got.Data), err)
	}

	// A meta-only read never looks at the payload. Corrupt the payload and
	// check that only a full read reports the damage.
	damaged := []byte{^plain.Data[0]}
	if _, err := v.DataBackend.WriteAt(damaged, int64(plainOffset)+types.NeedleHeaderSize+types.DataSizeSize); err != nil {
		t.Fatalf("damage needle: %v", err)
	}
	if _, err := read(1, nil); !errors.Is(err, needle.ErrorCorrupted) {
		t.Errorf("full read of a damaged needle: %v, want %v", err, needle.ErrorCorrupted)
	}
	if _, err := read(1, metaOnly()); err != nil {
		t.Errorf("metadata read of a damaged needle: %v", err)
	}
}

// HasRange skips the payload at any size, like MustMetaOnly, but the range
// read that follows reports the outcome, so the metadata read leaves the io
// error streak alone.
func TestReadNeedleHasRange(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	plain := &needle.Needle{Id: 1, Cookie: 0x1234, Data: bytes.Repeat([]byte("0123456789"), 400)}
	plain.Checksum = needle.NewCRC(plain.Data)
	if _, _, _, err := v.writeNeedle2(plain, true, false, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}
	manifest := newRandomNeedle(2)
	manifest.SetIsChunkManifest()
	if _, _, _, err := v.writeNeedle2(manifest, true, false, false); err != nil {
		t.Fatalf("write chunk manifest: %v", err)
	}
	compressed := newRandomNeedle(3)
	compressed.SetIsCompressed()
	if _, _, _, err := v.writeNeedle2(compressed, true, false, false); err != nil {
		t.Fatalf("write compressed needle: %v", err)
	}

	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	option := &ReadOption{AttemptMetaOnly: true, HasRange: true, ReadBufferSize: 1024}
	got := &needle.Needle{Id: plain.Id}
	if _, err := v.readNeedle(got, option, nil); err != nil {
		t.Fatalf("metadata read: %v", err)
	}
	if !option.IsMetaOnly || len(got.Data) != 0 || got.Cookie != plain.Cookie || got.DataSize != plain.DataSize {
		t.Errorf("metadata read: IsMetaOnly %v, %d bytes of data, cookie %x, data size %d", option.IsMetaOnly, len(got.Data), got.Cookie, got.DataSize)
	}
	if _, count, _ := v.getIoErrorState(); count != 1 {
		t.Errorf("io error streak %d after the metadata read, want 1", count)
	}

	var buf bytes.Buffer
	if err := v.readNeedleDataInto(got, option, &buf, 1500, 2000); err != nil {
		t.Fatalf("range read: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), plain.Data[1500:3500]) {
		t.Errorf("range read returned %d bytes that differ from the needle", buf.Len())
	}
	if _, count, _ := v.getIoErrorState(); count != 0 {
		t.Errorf("io error streak %d after the range read, want 0", count)
	}

	// A chunk manifest or a compressed needle is still read whole.
	for _, want := range []*needle.Needle{manifest, compressed} {
		option = &ReadOption{AttemptMetaOnly: true, HasRange: true}
		got = &needle.Needle{Id: want.Id}
		if _, err := v.readNeedle(got, option, nil); err != nil || option.IsMetaOnly || !bytes.Equal(got.Data, want.Data) {
			t.Errorf("ranged read of needle %d: IsMetaOnly %v, %d bytes, %v", want.Id, option.IsMetaOnly, len(got.Data), err)
		}
	}
}

// A range streams from the record its metadata read resolved, not from a
// replacement written in between, whatever the replacement's size.
func TestReadNeedleDataIntoKeepsTheMetaRecord(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	write := func(data []byte) {
		t.Helper()
		n := &needle.Needle{Id: 1, Cookie: 0x1234, Data: data}
		n.Checksum = needle.NewCRC(n.Data)
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write needle: %v", err)
		}
	}
	old := bytes.Repeat([]byte("old-"), 1000)
	for _, replacement := range [][]byte{bytes.Repeat([]byte("new-"), 1000), bytes.Repeat([]byte("short"), 10)} {
		write(old)
		option := &ReadOption{AttemptMetaOnly: true, HasRange: true, ReadBufferSize: 1024}
		got := &needle.Needle{Id: 1}
		if _, err := v.readNeedle(got, option, nil); err != nil || !option.IsMetaOnly {
			t.Fatalf("metadata read: IsMetaOnly %v, %v", option.IsMetaOnly, err)
		}
		write(replacement)
		var buf bytes.Buffer
		if err := v.readNeedleDataInto(got, option, &buf, 100, 2000); err != nil {
			t.Fatalf("range read after a %d-byte replacement: %v", len(replacement), err)
		}
		if !bytes.Equal(buf.Bytes(), old[100:2100]) {
			t.Errorf("range read after a %d-byte replacement returned bytes of another record", len(replacement))
		}
	}
}

// A version 1 needle has no data size field and no metadata after its data,
// so the metadata-only reader cannot parse it. A metadata-only read of a
// version 1 volume must fall back to a full read instead of failing.
func TestReadNeedleMetaOnlyVersion1(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	// New volumes are always created with the current version, so switch this
	// one to version 1 before anything is written to it.
	v.volumeInfo.Version = uint32(needle.Version1)
	if v.Version() != needle.Version1 {
		t.Fatalf("volume version %d, want %d", v.Version(), needle.Version1)
	}

	// The payload starts with bytes that would be a huge data size if the
	// metadata-only reader took them for the version 2 data size field.
	small := &needle.Needle{Id: 1, Cookie: 0x1234, Data: []byte("hello world")}
	small.Checksum = needle.NewCRC(small.Data)
	if _, _, _, err := v.writeNeedle2(small, true, false, false); err != nil {
		t.Fatalf("write small needle: %v", err)
	}
	large := &needle.Needle{Id: 2, Cookie: 0x5678, Data: bytes.Repeat([]byte("hello world "), PagedReadLimit/12+1)}
	large.Checksum = needle.NewCRC(large.Data)
	if _, _, _, err := v.writeNeedle2(large, true, false, false); err != nil {
		t.Fatalf("write large needle: %v", err)
	}

	for _, tc := range []struct {
		name   string
		want   *needle.Needle
		option *ReadOption
	}{
		{"must be metadata only, small needle", small, &ReadOption{AttemptMetaOnly: true, MustMetaOnly: true}},
		{"must be metadata only, large needle", large, &ReadOption{AttemptMetaOnly: true, MustMetaOnly: true}},
		{"attempt metadata only, large needle", large, &ReadOption{AttemptMetaOnly: true}},
	} {
		got := &needle.Needle{Id: tc.want.Id}
		if _, err := v.readNeedle(got, tc.option, nil); err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if tc.option.IsMetaOnly || got.Cookie != tc.want.Cookie || !bytes.Equal(got.Data, tc.want.Data) {
			t.Errorf("%s: IsMetaOnly %v, cookie %x, %d bytes of data", tc.name, tc.option.IsMetaOnly, got.Cookie, len(got.Data))
		}
	}
}
