package filer

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// decodeBothWays round-trips entry and returns what each decoder made of it.
func decodeBothWays(t *testing.T, entry *Entry) (full, attrsOnly Entry) {
	t.Helper()
	blob, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	full.FullPath = entry.FullPath
	if err := full.DecodeAttributesAndChunks(blob); err != nil {
		t.Fatalf("full decode: %v", err)
	}
	attrsOnly.FullPath = entry.FullPath
	if err := attrsOnly.DecodeAttributesOnly(blob); err != nil {
		t.Fatalf("attributes-only decode: %v", err)
	}
	return full, attrsOnly
}

// assertSameButChunks checks the two decoders agree on everything a listing
// reads. Chunks are the deliberate exception; size is not, because an entry can
// carry a zero FileSize and take its size from the chunks.
func assertSameButChunks(t *testing.T, full, attrsOnly Entry) {
	t.Helper()
	if attrsOnly.Chunks != nil {
		t.Errorf("attributes-only decode built %d chunks, want none", len(attrsOnly.Chunks))
	}
	if full.FileSize != attrsOnly.FileSize {
		t.Errorf("FileSize = %d, want %d", attrsOnly.FileSize, full.FileSize)
	}
	if full.Size() != attrsOnly.Size() {
		t.Errorf("Size() = %d, want %d", attrsOnly.Size(), full.Size())
	}
	if !reflect.DeepEqual(full.Attr, attrsOnly.Attr) {
		t.Errorf("Attr = %+v, want %+v", attrsOnly.Attr, full.Attr)
	}
	if string(full.HardLinkId) != string(attrsOnly.HardLinkId) {
		t.Errorf("HardLinkId = %x, want %x", attrsOnly.HardLinkId, full.HardLinkId)
	}
	if full.HardLinkCounter != attrsOnly.HardLinkCounter {
		t.Errorf("HardLinkCounter = %d, want %d", attrsOnly.HardLinkCounter, full.HardLinkCounter)
	}
	if string(full.Content) != string(attrsOnly.Content) {
		t.Errorf("Content = %q, want %q", attrsOnly.Content, full.Content)
	}
	if full.Quota != attrsOnly.Quota {
		t.Errorf("Quota = %d, want %d", attrsOnly.Quota, full.Quota)
	}
	if full.WORMEnforcedAtTsNs != attrsOnly.WORMEnforcedAtTsNs {
		t.Errorf("WORMEnforcedAtTsNs = %d, want %d", attrsOnly.WORMEnforcedAtTsNs, full.WORMEnforcedAtTsNs)
	}
	if len(full.Extended) != len(attrsOnly.Extended) {
		t.Errorf("Extended has %d keys, want %d", len(attrsOnly.Extended), len(full.Extended))
	}
	for k, v := range full.Extended {
		if string(attrsOnly.Extended[k]) != string(v) {
			t.Errorf("Extended[%q] = %q, want %q", k, attrsOnly.Extended[k], v)
		}
	}
	if (full.Remote == nil) != (attrsOnly.Remote == nil) {
		t.Errorf("Remote presence differs: %v vs %v", attrsOnly.Remote != nil, full.Remote != nil)
	} else if full.Remote != nil && full.Remote.RemoteSize != attrsOnly.Remote.RemoteSize {
		t.Errorf("Remote.RemoteSize = %d, want %d", attrsOnly.Remote.RemoteSize, full.Remote.RemoteSize)
	}
}

func chunkAt(offset int64, size uint64, i int) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{
		FileId:       fmt.Sprintf("3,01637037d6%04d", i),
		Offset:       offset,
		Size:         size,
		ModifiedTsNs: int64(1700000000+i) * 1e9,
		ETag:         "1a2b3c4d5e6f7890",
		Fid:          &filer_pb.FileId{VolumeId: uint32(3 + i), FileKey: uint64(i), Cookie: 0x1637037d},
		CipherKey:    []byte{1, 2, 3, 4},
	}
}

func TestDecodeAttributesOnlyMatchesFullDecode(t *testing.T) {
	now := time.Unix(1700000000, 123456789)

	cases := []struct {
		name  string
		entry *Entry
	}{
		{"no chunks", &Entry{
			FullPath: util.FullPath("/d/plain"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now, Ctime: now, Uid: 99, Gid: 100, FileSize: 12},
		}},
		{"directory", &Entry{
			FullPath: util.FullPath("/d/sub"),
			Attr:     Attr{Mode: os.ModeDir | 0o755, Mtime: now, Crtime: now, Uid: 99, Gid: 100},
		}},
		{"one chunk", &Entry{
			FullPath: util.FullPath("/d/one"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now, Uid: 99, Gid: 100, FileSize: 4 << 20},
			Chunks:   []*filer_pb.FileChunk{chunkAt(0, 4<<20, 0)},
		}},
		// The S3 copy and multipart paths deliberately store a zero FileSize and
		// let the chunks define it, so this is the case that forces the extent
		// walk rather than just skipping the field.
		{"zero FileSize, size comes from chunks", &Entry{
			FullPath: util.FullPath("/d/zerosize"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now, Uid: 99, Gid: 100, FileSize: 0},
			Chunks: []*filer_pb.FileChunk{
				chunkAt(0, 4<<20, 0), chunkAt(4<<20, 4<<20, 1), chunkAt(8<<20, 1234, 2),
			},
		}},
		{"chunks out of order", &Entry{
			FullPath: util.FullPath("/d/unordered"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now, Uid: 99, Gid: 100},
			Chunks: []*filer_pb.FileChunk{
				chunkAt(8<<20, 99, 0), chunkAt(0, 4<<20, 1), chunkAt(4<<20, 4<<20, 2),
			},
		}},
		{"stored FileSize larger than chunks", &Entry{
			FullPath: util.FullPath("/d/sparse"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now, Uid: 99, Gid: 100, FileSize: 1 << 30},
			Chunks:   []*filer_pb.FileChunk{chunkAt(0, 16, 0)},
		}},
		{"symlink", &Entry{
			FullPath: util.FullPath("/d/link"),
			Attr:     Attr{Mode: os.ModeSymlink | 0o777, Mtime: now, Crtime: now, SymlinkTarget: "../target"},
		}},
		{"hard link", &Entry{
			FullPath:        util.FullPath("/d/hard"),
			Attr:            Attr{Mode: 0o644, Mtime: now, Crtime: now},
			HardLinkId:      HardLinkId([]byte{9, 8, 7, 6}),
			HardLinkCounter: 3,
		}},
		{"inline content", &Entry{
			FullPath: util.FullPath("/d/inline"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now},
			Content:  []byte("hello world"),
		}},
		{"extended attributes", &Entry{
			FullPath: util.FullPath("/d/xattr"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now},
			Extended: map[string][]byte{"a": []byte("1"), "b": []byte("2"), "Seaweed-X": []byte("y")},
		}},
		{"remote entry", &Entry{
			FullPath: util.FullPath("/d/remote"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now, FileSize: 5},
			Remote:   &filer_pb.RemoteEntry{RemoteSize: 4096, RemoteMtime: now.Unix() + 60, StorageName: "s3"},
		}},
		{"quota and worm", &Entry{
			FullPath:           util.FullPath("/d/bucket"),
			Attr:               Attr{Mode: os.ModeDir | 0o755, Mtime: now, Crtime: now},
			Quota:              1 << 40,
			WORMEnforcedAtTsNs: now.UnixNano(),
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			full, attrsOnly := decodeBothWays(t, tc.entry)
			assertSameButChunks(t, full, attrsOnly)
		})
	}
}

// TestDecodeAttributesOnlyRandomEntries fuzzes the field combinations, since the
// decoder walks the wire format by hand and has to stay in step with the
// generated one as filer.proto grows.
func TestDecodeAttributesOnlyRandomEntries(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		now := time.Unix(1600000000+rnd.Int63n(1e8), rnd.Int63n(1e9))
		entry := &Entry{
			FullPath: util.FullPath(fmt.Sprintf("/d/f%d", i)),
			Attr: Attr{
				Mode:  os.FileMode(rnd.Intn(0o777)),
				Mtime: now, Crtime: now, Ctime: now,
				Uid: uint32(rnd.Intn(70000)), Gid: uint32(rnd.Intn(70000)),
				FileSize: uint64(rnd.Int63n(1 << 34)),
				Inode:    rnd.Uint64(),
				Rdev:     uint32(rnd.Intn(1 << 20)),
				TtlSec:   int32(rnd.Intn(1000)),
			},
		}
		if rnd.Intn(2) == 0 {
			entry.Attr.FileSize = 0
		}
		if rnd.Intn(4) == 0 {
			entry.Content = make([]byte, rnd.Intn(64))
			rnd.Read(entry.Content)
		}
		if rnd.Intn(4) == 0 {
			entry.Extended = map[string][]byte{}
			for k := 0; k < rnd.Intn(4); k++ {
				entry.Extended[fmt.Sprintf("k%d", k)] = []byte(fmt.Sprintf("v%d", rnd.Intn(1000)))
			}
		}
		if rnd.Intn(8) == 0 {
			entry.Remote = &filer_pb.RemoteEntry{RemoteSize: rnd.Int63n(1 << 30), RemoteMtime: now.Unix() + int64(rnd.Intn(120)) - 60}
		}
		for c := 0; c < rnd.Intn(20); c++ {
			entry.Chunks = append(entry.Chunks, chunkAt(rnd.Int63n(1<<30), uint64(rnd.Int63n(1<<22)), c))
		}
		full, attrsOnly := decodeBothWays(t, entry)
		assertSameButChunks(t, full, attrsOnly)
	}
}

func TestDecodeAttributesOnlyRejectsGarbage(t *testing.T) {
	var entry Entry
	entry.FullPath = util.FullPath("/d/bad")
	if err := entry.DecodeAttributesOnly([]byte{0xff, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("expected an error for a malformed blob")
	}
}

func BenchmarkDecode(b *testing.B) {
	now := time.Unix(1700000000, 0)
	for _, n := range []int{0, 1, 4, 16, 64} {
		entry := &Entry{
			FullPath: util.FullPath("/images/image-00000001.jpg"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now, Ctime: now, Uid: 99, Gid: 100, FileSize: uint64(n) * 4 << 20},
		}
		for i := 0; i < n; i++ {
			entry.Chunks = append(entry.Chunks, chunkAt(int64(i)*4<<20, 4<<20, i))
		}
		blob, err := entry.EncodeAttributesAndChunks()
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("chunks=%d/decoder=full", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var out Entry
				if err := out.DecodeAttributesAndChunks(blob); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("chunks=%d/decoder=attrsonly", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var out Entry
				if err := out.DecodeAttributesOnly(blob); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
