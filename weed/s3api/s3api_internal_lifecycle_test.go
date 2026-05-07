package s3api

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
)

func TestComputeEntryIdentity_BasicFields(t *testing.T) {
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileSize: 4096},
		Chunks: []*filer_pb.FileChunk{
			{FileId: "1,abc"},
			{FileId: "1,def"},
		},
	}
	id := computeEntryIdentity(entry)
	if id.MtimeNs != 1700000000 {
		t.Fatalf("MtimeNs want 1700000000, got %d", id.MtimeNs)
	}
	if id.Size != 4096 {
		t.Fatalf("Size want 4096, got %d", id.Size)
	}
	if id.HeadFid != "1,abc" {
		t.Fatalf("HeadFid want 1,abc, got %s", id.HeadFid)
	}
}

func TestComputeEntryIdentity_NilSafeMissingChunks(t *testing.T) {
	if got := computeEntryIdentity(nil); got != nil {
		t.Fatalf("nil entry should return nil, got %v", got)
	}
	id := computeEntryIdentity(&filer_pb.Entry{})
	if id == nil {
		t.Fatalf("entry with nil Attributes should still produce identity")
	}
	if id.HeadFid != "" {
		t.Fatalf("missing chunks should yield empty HeadFid, got %s", id.HeadFid)
	}
}

func TestHashExtended_OrderStable(t *testing.T) {
	a := map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")}
	b := map[string][]byte{"k2": []byte("v2"), "k1": []byte("v1")}
	if !bytes.Equal(hashExtended(a), hashExtended(b)) {
		t.Fatalf("hash should be insensitive to map iteration order")
	}
}

func TestHashExtended_DelimiterCollisionResistant(t *testing.T) {
	// Naively concatenated: "k1=v1k2v2" could collide with "k1=v1k" / "2v2".
	// Length-prefix encoding must keep them apart.
	a := map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")}
	b := map[string][]byte{"k1": []byte("v1k2v2")}
	if bytes.Equal(hashExtended(a), hashExtended(b)) {
		t.Fatalf("delimiter-forged Extended payloads must not collide")
	}
}

func TestHashExtended_NilEqualsEmpty(t *testing.T) {
	if got := hashExtended(nil); len(got) != 0 {
		t.Fatalf("nil should produce zero-length hash, got %d bytes", len(got))
	}
	if got := hashExtended(map[string][]byte{}); len(got) != 0 {
		t.Fatalf("empty map should produce zero-length hash, got %d bytes", len(got))
	}
}

func TestIdentityMatches_NilWantTreatedAsMatch(t *testing.T) {
	// Bootstrap callers that don't yet have an identity to CAS against
	// pass nil expected_identity; the server treats this as "no CAS".
	live := &s3_lifecycle_pb.EntryIdentity{MtimeNs: 1, Size: 2}
	if !identityMatches(live, nil) {
		t.Fatalf("nil want should match")
	}
}

func TestIdentityMatches_NilLiveDoesNotMatch(t *testing.T) {
	if identityMatches(nil, &s3_lifecycle_pb.EntryIdentity{MtimeNs: 1}) {
		t.Fatalf("nil live should not match a populated want")
	}
}

func TestIdentityMatches_AllFieldsCompared(t *testing.T) {
	base := &s3_lifecycle_pb.EntryIdentity{MtimeNs: 100, Size: 2048, HeadFid: "1,abc", ExtendedHash: []byte{0x01, 0x02}}
	cases := []struct {
		name string
		live *s3_lifecycle_pb.EntryIdentity
		want bool
	}{
		{"identical", &s3_lifecycle_pb.EntryIdentity{MtimeNs: 100, Size: 2048, HeadFid: "1,abc", ExtendedHash: []byte{0x01, 0x02}}, true},
		{"mtime-drift", &s3_lifecycle_pb.EntryIdentity{MtimeNs: 101, Size: 2048, HeadFid: "1,abc", ExtendedHash: []byte{0x01, 0x02}}, false},
		{"size-drift", &s3_lifecycle_pb.EntryIdentity{MtimeNs: 100, Size: 2049, HeadFid: "1,abc", ExtendedHash: []byte{0x01, 0x02}}, false},
		{"fid-drift", &s3_lifecycle_pb.EntryIdentity{MtimeNs: 100, Size: 2048, HeadFid: "1,xyz", ExtendedHash: []byte{0x01, 0x02}}, false},
		{"extended-drift", &s3_lifecycle_pb.EntryIdentity{MtimeNs: 100, Size: 2048, HeadFid: "1,abc", ExtendedHash: []byte{0x03, 0x04}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := identityMatches(c.live, base); got != c.want {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func TestLifecycleDelete_RejectsEmptyRequest(t *testing.T) {
	s := &S3ApiServer{}
	resp, err := s.LifecycleDelete(nil, &s3_lifecycle_pb.LifecycleDeleteRequest{})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if resp.Outcome != s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED {
		t.Fatalf("empty request should be BLOCKED, got %v", resp.Outcome)
	}
}
