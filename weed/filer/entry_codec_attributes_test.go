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
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

// TestProtoEntryWithoutChunksKeepsSize covers the wire contract the filer's
// omit_chunks listing relies on: the size a client needs survives in the
// attributes once the chunk list is dropped, including for the entries that
// store a zero FileSize and take their size from the chunks.
func TestProtoEntryWithoutChunksKeepsSize(t *testing.T) {
	now := time.Unix(1700000000, 0)
	for _, storedSize := range []uint64{0, 7, 1 << 30} {
		stored := &Entry{
			FullPath: util.FullPath("/d/obj"),
			Attr:     Attr{Mode: 0o644, Mtime: now, Crtime: now, FileSize: storedSize},
			Chunks:   []*filer_pb.FileChunk{chunkAt(0, 4<<20, 0), chunkAt(4<<20, 1234, 1)},
		}
		blob, err := stored.EncodeAttributesAndChunks()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		// What the filer holds after reading the entry out of its store.
		var loaded Entry
		loaded.FullPath = stored.FullPath
		if err := loaded.DecodeAttributesAndChunks(blob); err != nil {
			t.Fatalf("decode: %v", err)
		}
		want := FileSize(loaded.ToProtoEntry())

		pbEntry := loaded.ToProtoEntry()
		pbEntry.Chunks = nil
		if got := FileSize(pbEntry); got != want {
			t.Errorf("stored FileSize %d: size over the wire = %d, want %d", storedSize, got, want)
		}
		if got := FromPbEntry("/d", pbEntry).Size(); got != want {
			t.Errorf("stored FileSize %d: size at the client = %d, want %d", storedSize, got, want)
		}
	}
}

// TestChunkValidationCoversEveryField keeps the hand-rolled chunk walk honest as
// filer.proto grows. The generated unmarshaller never sees the chunk bytes, so
// every FileChunk field whose contents it would have checked -- a submessage, or
// a proto3 string's UTF-8 -- has to be listed for the walk to check instead.
func TestChunkValidationCoversEveryField(t *testing.T) {
	fields := (&filer_pb.FileChunk{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		switch f.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			if !fileChunkMessageFields[protowire.Number(f.Number())] {
				t.Errorf("FileChunk.%s (field %d) is a message but is not in fileChunkMessageFields, so a corrupt one would pass the listing decoder and fail the full one", f.Name(), f.Number())
			}
		case protoreflect.StringKind:
			if !fileChunkStringFields[protowire.Number(f.Number())] {
				t.Errorf("FileChunk.%s (field %d) is a string but is not in fileChunkStringFields, so invalid UTF-8 would pass the listing decoder and fail the full one", f.Name(), f.Number())
			}
		}
	}
}

// corruptChunkBlobs builds entry blobs whose chunk bytes are damaged in ways the
// generated unmarshaller rejects.
func corruptChunkBlobs(t *testing.T) map[string][]byte {
	t.Helper()
	now := time.Unix(1700000000, 0)
	base := func() *filer_pb.FileChunk {
		return &filer_pb.FileChunk{Offset: 0, Size: 1024, Fid: &filer_pb.FileId{VolumeId: 3, FileKey: 7, Cookie: 9}}
	}
	blobFor := func(mangle func(raw []byte) []byte) []byte {
		chunk := base()
		chunkBytes, err := proto.Marshal(chunk)
		if err != nil {
			t.Fatalf("marshal chunk: %v", err)
		}
		chunkBytes = mangle(chunkBytes)
		var blob []byte
		blob = protowire.AppendTag(blob, entryChunksField, protowire.BytesType)
		blob = protowire.AppendBytes(blob, chunkBytes)
		attrs, err := proto.Marshal(&filer_pb.FuseAttributes{FileSize: 0, Mtime: now.Unix(), FileMode: 0o644})
		if err != nil {
			t.Fatalf("marshal attrs: %v", err)
		}
		blob = protowire.AppendTag(blob, 4, protowire.BytesType)
		return protowire.AppendBytes(blob, attrs)
	}

	out := map[string][]byte{}
	// The exact probe from review: a nested fid whose payload is not a message.
	out["corrupt nested fid"] = blobFor(func(raw []byte) []byte {
		var b []byte
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, 0)
		b = protowire.AppendTag(b, 3, protowire.VarintType)
		b = protowire.AppendVarint(b, 1024)
		b = protowire.AppendTag(b, 7, protowire.BytesType)
		return protowire.AppendBytes(b, []byte{0xff, 0xff, 0xff, 0xff})
	})
	out["invalid utf8 in file_id"] = blobFor(func(raw []byte) []byte {
		var b []byte
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, []byte{0xff, 0xfe, 0xfd})
		b = protowire.AppendTag(b, 3, protowire.VarintType)
		return protowire.AppendVarint(b, 1024)
	})
	out["truncated chunk"] = blobFor(func(raw []byte) []byte { return raw[:len(raw)-1] })
	return out
}

// TestDecodeAttributesOnlyRejectsWhatFullDecodeRejects is the invariant that
// keeps a listing from showing a file that cannot then be opened: the fast path
// must never accept a blob the full decoder turns away.
func TestDecodeAttributesOnlyRejectsWhatFullDecodeRejects(t *testing.T) {
	for name, blob := range corruptChunkBlobs(t) {
		t.Run(name, func(t *testing.T) {
			var full, attrsOnly Entry
			full.FullPath = util.FullPath("/d/corrupt")
			attrsOnly.FullPath = full.FullPath
			fullErr := full.DecodeAttributesAndChunks(blob)
			attrsErr := attrsOnly.DecodeAttributesOnly(blob)
			if fullErr == nil {
				t.Skip("full decoder accepts this blob, nothing to match")
			}
			if attrsErr == nil {
				t.Errorf("full decode rejected the blob (%v) but attributes-only accepted it with FileSize=%d", fullErr, attrsOnly.FileSize)
			}
		})
	}
}

// TestDecodeAttributesOnlyManifestChunk pins that a manifest chunk needs no
// resolving: it carries the offset and size of the range it stands for, and
// TotalSize does not resolve it either, so both decoders see one number.
func TestDecodeAttributesOnlyManifestChunk(t *testing.T) {
	now := time.Unix(1700000000, 0)
	manifest := chunkAt(0, 64<<20, 0)
	manifest.IsChunkManifest = true
	entry := &Entry{
		FullPath: util.FullPath("/d/big"),
		// Zero stored size, so the manifest's extent is the only source.
		Attr:   Attr{Mode: 0o644, Mtime: now, Crtime: now, FileSize: 0},
		Chunks: []*filer_pb.FileChunk{manifest},
	}
	full, attrsOnly := decodeBothWays(t, entry)
	assertSameButChunks(t, full, attrsOnly)
	if attrsOnly.FileSize != 64<<20 {
		t.Errorf("FileSize = %d, want %d from the manifest extent", attrsOnly.FileSize, 64<<20)
	}
}

// TestDecodeAttributesOnlyFieldBeforeChunks exercises the prefix copy, which no
// other case reaches: EncodeAttributesAndChunks emits chunks before every field
// the other tests set, so `dropping` always turns on at pos 0 there.
func TestDecodeAttributesOnlyFieldBeforeChunks(t *testing.T) {
	now := time.Unix(1700000000, 0)
	attrs, err := proto.Marshal(&filer_pb.FuseAttributes{FileSize: 0, Mtime: now.Unix(), FileMode: 0o755})
	if err != nil {
		t.Fatalf("marshal attrs: %v", err)
	}
	chunkBytes, err := proto.Marshal(chunkAt(0, 4<<20, 0))
	if err != nil {
		t.Fatalf("marshal chunk: %v", err)
	}
	// is_directory (2) ahead of chunks (3), so the walk has a prefix to copy.
	var blob []byte
	blob = protowire.AppendTag(blob, 2, protowire.VarintType)
	blob = protowire.AppendVarint(blob, 1)
	blob = protowire.AppendTag(blob, entryChunksField, protowire.BytesType)
	blob = protowire.AppendBytes(blob, chunkBytes)
	blob = protowire.AppendTag(blob, 4, protowire.BytesType)
	blob = protowire.AppendBytes(blob, attrs)

	var full, attrsOnly Entry
	full.FullPath = util.FullPath("/d/dirwithchunks")
	attrsOnly.FullPath = full.FullPath
	if err := full.DecodeAttributesAndChunks(blob); err != nil {
		t.Fatalf("full decode: %v", err)
	}
	if err := attrsOnly.DecodeAttributesOnly(blob); err != nil {
		t.Fatalf("attributes-only decode: %v", err)
	}
	assertSameButChunks(t, full, attrsOnly)
	if attrsOnly.FileSize != 4<<20 {
		t.Errorf("FileSize = %d, want %d", attrsOnly.FileSize, 4<<20)
	}
}
