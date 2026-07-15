package filer_pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func BenchmarkUnmarshalProto(b *testing.B) {
	data, _ := proto.Marshal(sampleResponse())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var m SubscribeMetadataResponse
		if err := proto.Unmarshal(data, &m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalVT(b *testing.B) {
	data, _ := proto.Marshal(sampleResponse())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var m SubscribeMetadataResponse
		if err := m.UnmarshalVT(data); err != nil {
			b.Fatal(err)
		}
	}
}

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
					Size:        1234,
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
