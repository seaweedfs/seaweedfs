package filer_pb

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
)

// sampleResponse builds a metadata event exercising the fields touched on the
// hot subscribe path: nested entries, a repeated chunk list, a map, and byte
// fields.
func sampleResponse() *SubscribeMetadataResponse {
	entry := func(name string) *Entry {
		return &Entry{
			Name:        name,
			IsDirectory: false,
			Chunks: []*FileChunk{
				{
					FileId:       "3,01637037d6",
					Offset:       0,
					Size:         1234,
					ModifiedTsNs: 111,
					ETag:         "abc",
					Fid:          &FileId{VolumeId: 3, FileKey: 0x1637037, Cookie: 0xd6},
					CipherKey:    []byte{1, 2, 3, 4},
				},
			},
			Attributes: &FuseAttributes{
				FileSize: 1234, Mtime: 222, FileMode: 0644, Uid: 1000, Gid: 1000,
				GroupName: []string{"staff", "wheel"}, Md5: []byte{9, 8, 7},
			},
			Extended: map[string][]byte{
				"foo": []byte("bar"),
				"baz": []byte("qux"),
				"k3":  []byte("v3"),
			},
			Content: []byte("hello"),
		}
	}
	return &SubscribeMetadataResponse{
		Directory: "/buckets/test/dir",
		EventNotification: &EventNotification{
			OldEntry:           entry("old.txt"),
			NewEntry:           entry("new.txt"),
			DeleteChunks:       true,
			NewParentPath:      "/buckets/test/dir2",
			IsFromOtherCluster: true,
			Signatures:         []int32{101, 202, 303},
		},
		TsNs: 1234567890,
	}
}

// eventWithChunks models a real create/update event: a file split into n chunks,
// each a nested FileChunk+FileId message. n scales the number of nested messages
// the decoder must allocate, which is where vtproto's edge over reflection grows.
func eventWithChunks(n int) *SubscribeMetadataResponse {
	chunks := make([]*FileChunk, n)
	for i := 0; i < n; i++ {
		chunks[i] = &FileChunk{
			FileId:       fmt.Sprintf("%d,%08x%04x", i%64, i*7919, i%251),
			Offset:       int64(i) * 8 << 20,
			Size:         8 << 20,
			ModifiedTsNs: 1700000000000000000 + int64(i),
			ETag:         fmt.Sprintf("etag-%d", i),
			Fid:          &FileId{VolumeId: uint32(i%64 + 1), FileKey: uint64(i * 7919), Cookie: uint32(i % 251)},
		}
	}
	return &SubscribeMetadataResponse{
		Directory: "/buckets/data/2026/07/15",
		EventNotification: &EventNotification{
			NewEntry: &Entry{
				Name:       "object.bin",
				Chunks:     chunks,
				Attributes: &FuseAttributes{FileSize: uint64(n) * 8 << 20, Mtime: 1700000000, FileMode: 0644, Uid: 1000, Gid: 1000},
			},
			Signatures: []int32{12345},
		},
		TsNs: 1700000000000000000,
	}
}

// TestVTWireCompatibility guards that the vtproto-generated MarshalVT/UnmarshalVT
// stay wire-compatible with the reflection-based proto path in both directions,
// so adopting VT on the metadata hot paths cannot silently corrupt events.
func TestVTWireCompatibility(t *testing.T) {
	orig := sampleResponse()

	// vt marshal -> proto unmarshal
	vtBytes, err := orig.MarshalVT()
	if err != nil {
		t.Fatalf("MarshalVT: %v", err)
	}
	var fromVT SubscribeMetadataResponse
	if err := proto.Unmarshal(vtBytes, &fromVT); err != nil {
		t.Fatalf("proto.Unmarshal(vtBytes): %v", err)
	}
	if !proto.Equal(orig, &fromVT) {
		t.Fatalf("proto.Unmarshal(MarshalVT) diverged from original")
	}

	// proto marshal -> vt unmarshal
	protoBytes, err := proto.Marshal(orig)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	var fromProto SubscribeMetadataResponse
	if err := fromProto.UnmarshalVT(protoBytes); err != nil {
		t.Fatalf("UnmarshalVT(protoBytes): %v", err)
	}
	if !proto.Equal(orig, &fromProto) {
		t.Fatalf("UnmarshalVT(proto.Marshal) diverged from original")
	}

	// MarshalToSizedBufferVT into a pre-sized slice matches MarshalVT, the write
	// path used by the log buffer.
	sized := make([]byte, orig.SizeVT())
	if _, err := orig.MarshalToSizedBufferVT(sized); err != nil {
		t.Fatalf("MarshalToSizedBufferVT: %v", err)
	}
	var fromSized SubscribeMetadataResponse
	if err := fromSized.UnmarshalVT(sized); err != nil {
		t.Fatalf("UnmarshalVT(sized): %v", err)
	}
	if !proto.Equal(orig, &fromSized) {
		t.Fatalf("MarshalToSizedBufferVT diverged from original")
	}

	// LogEntry carrying a marshaled event, mirroring the log buffer write path.
	le := &LogEntry{TsNs: 7, PartitionKeyHash: 9, Data: vtBytes, Key: []byte("k"), Offset: 42}
	leBytes, err := le.MarshalVT()
	if err != nil {
		t.Fatalf("LogEntry.MarshalVT: %v", err)
	}
	var leDecoded LogEntry
	if err := proto.Unmarshal(leBytes, &leDecoded); err != nil {
		t.Fatalf("proto.Unmarshal(LogEntry): %v", err)
	}
	if !proto.Equal(le, &leDecoded) {
		t.Fatalf("LogEntry vt/proto roundtrip diverged")
	}
}

// TestUTF8Validation documents the deliberate UTF-8 difference between the two
// decoders. UnmarshalVT skips proto3's UTF-8 validation for string fields, so
// the metadata delivery paths keep decoding SubscribeMetadataResponse with
// proto.Unmarshal; only LogEntry (bytes-only) decodes use UnmarshalVT.
func TestUTF8Validation(t *testing.T) {
	// SubscribeMetadataResponse{Directory: "\xff"}: field 1 (string), wire type 2,
	// length 1, byte 0xff.
	malformed := []byte{0x0a, 0x01, 0xff}

	var viaProto SubscribeMetadataResponse
	if err := proto.Unmarshal(malformed, &viaProto); err == nil {
		t.Fatal("proto.Unmarshal accepted invalid UTF-8 in a string field; expected rejection")
	}

	var viaVT SubscribeMetadataResponse
	if err := viaVT.UnmarshalVT(malformed); err != nil {
		t.Fatalf("UnmarshalVT rejected invalid UTF-8: %v", err)
	}
	if viaVT.Directory != "\xff" {
		t.Fatalf("UnmarshalVT Directory = %q, want \\xff", viaVT.Directory)
	}

	// LogEntry carries only bytes fields, so the decoders agree on arbitrary bytes;
	// that is why the log entry decode paths can use UnmarshalVT.
	// LogEntry{Data: "\xff"}: field 3 (bytes), wire type 2, length 1, byte 0xff.
	leBytes := []byte{0x1a, 0x01, 0xff}
	var leProto LogEntry
	if err := proto.Unmarshal(leBytes, &leProto); err != nil {
		t.Fatalf("proto.Unmarshal LogEntry: %v", err)
	}
	var leVT LogEntry
	if err := leVT.UnmarshalVT(leBytes); err != nil {
		t.Fatalf("UnmarshalVT LogEntry: %v", err)
	}
	if !proto.Equal(&leProto, &leVT) {
		t.Fatal("LogEntry proto/VT decode of a bytes field diverged")
	}
}

var chunkCounts = []int{1, 8, 32, 128}

// BenchmarkUnmarshal is the decode hot path: each subscriber decodes every
// delivered event, so this cost is paid once per (event, subscriber).
func BenchmarkUnmarshal(b *testing.B) {
	for _, n := range chunkCounts {
		data, _ := proto.Marshal(eventWithChunks(n))
		b.Run(fmt.Sprintf("proto/chunks=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var m SubscribeMetadataResponse
				if err := proto.Unmarshal(data, &m); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("vt/chunks=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var m SubscribeMetadataResponse
				if err := m.UnmarshalVT(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarshal is the encode path: each metadata write marshals one LogEntry
// payload.
func BenchmarkMarshal(b *testing.B) {
	for _, n := range chunkCounts {
		ev := eventWithChunks(n)
		b.Run(fmt.Sprintf("proto/chunks=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := proto.Marshal(ev); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("vt/chunks=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := ev.MarshalVT(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarshalIntoBuffer mirrors the log buffer write path: marshal into a
// reused destination buffer. proto.Marshal must allocate a fresh slice and copy;
// SizeVT + MarshalToSizedBufferVT writes straight into the destination.
func BenchmarkMarshalIntoBuffer(b *testing.B) {
	for _, n := range chunkCounts {
		ev := eventWithChunks(n)
		dst := make([]byte, 0, ev.SizeVT()*2)
		b.Run(fmt.Sprintf("proto+copy/chunks=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, err := proto.Marshal(ev)
				if err != nil {
					b.Fatal(err)
				}
				dst = dst[:len(data)]
				copy(dst, data)
			}
		})
		b.Run(fmt.Sprintf("vt-sized/chunks=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				size := ev.SizeVT()
				dst = dst[:size]
				if _, err := ev.MarshalToSizedBufferVT(dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
