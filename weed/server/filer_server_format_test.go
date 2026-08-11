package weed_server

import (
	"bytes"
	"math"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestRoundUpToVolumeTTL(t *testing.T) {
	tests := []struct {
		seconds int64
		want    int32
	}{
		{1, 60},
		{59, 60},
		{60, 60},
		{61, 120},
		{3599, 3600},
		{3600, 3600},
		{3601, 3660},
		{255 * 60, 255 * 60},
		{255*60 + 1, 5 * 3600},           // minutes overflow 255, ceil to hours
		{20_000_000, 232 * 24 * 3600}, // ~231.5 days, ceil to days
		{int64(math.MaxInt32), 0},     // beyond every unit's 255 cap: no TTL, never a shortened one
	}
	for _, test := range tests {
		got := roundUpToVolumeTTL(test.seconds)
		if got != test.want {
			t.Fatalf("roundUpToVolumeTTL(%d) = %d, want %d", test.seconds, got, test.want)
		}
		if got == 0 {
			continue // no volume TTL: chunks outlive the entry
		}
		if int64(got) < test.seconds {
			t.Fatalf("roundUpToVolumeTTL(%d) = %d shortened the lifetime", test.seconds, got)
		}
		// the rounded value must survive the volume TTL string conversion intact
		ttl, err := needle.ReadTTL(needle.SecondsToTTL(got))
		if err != nil || int64(ttl.Minutes())*60 != int64(got) {
			t.Fatalf("SecondsToTTL(%d) = %q does not round-trip (err %v)", got, needle.SecondsToTTL(got), err)
		}
	}
}

func TestFormatChunkIdentity(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{FileId: "1,ab", Offset: 0, Size: 10},
		{FileId: "2,cd", Offset: 10, Size: 20},
	}
	identity := formatChunkIdentity(chunks)
	if !bytes.Equal(identity, formatChunkIdentity(chunks)) {
		t.Fatalf("identity is not deterministic")
	}
	changedFid := []*filer_pb.FileChunk{
		{FileId: "1,ab", Offset: 0, Size: 10},
		{FileId: "3,ef", Offset: 10, Size: 20},
	}
	if bytes.Equal(identity, formatChunkIdentity(changedFid)) {
		t.Fatalf("identity ignored a chunk replacement")
	}
	changedOffset := []*filer_pb.FileChunk{
		{FileId: "1,ab", Offset: 0, Size: 10},
		{FileId: "2,cd", Offset: 12, Size: 20},
	}
	if bytes.Equal(identity, formatChunkIdentity(changedOffset)) {
		t.Fatalf("identity ignored an offset change")
	}
	// a truncate mutates Size while keeping the chunk id
	changedSize := []*filer_pb.FileChunk{
		{FileId: "1,ab", Offset: 0, Size: 10},
		{FileId: "2,cd", Offset: 10, Size: 15},
	}
	if bytes.Equal(identity, formatChunkIdentity(changedSize)) {
		t.Fatalf("identity ignored a size change")
	}
	changedMtime := []*filer_pb.FileChunk{
		{FileId: "1,ab", Offset: 0, Size: 10},
		{FileId: "2,cd", Offset: 10, Size: 20, ModifiedTsNs: 7},
	}
	if bytes.Equal(identity, formatChunkIdentity(changedMtime)) {
		t.Fatalf("identity ignored a modification timestamp change")
	}
	changedManifest := []*filer_pb.FileChunk{
		{FileId: "1,ab", Offset: 0, Size: 10},
		{FileId: "2,cd", Offset: 10, Size: 20, IsChunkManifest: true},
	}
	if bytes.Equal(identity, formatChunkIdentity(changedManifest)) {
		t.Fatalf("identity ignored a manifest flag change")
	}
}
