package filer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestEntryCodec_AtimeRoundTrip(t *testing.T) {
	mtime := time.Unix(1_700_000_000, 123_456_789)
	atime := time.Unix(1_700_001_000, 987_654_321)

	source := &Entry{
		FullPath: util.FullPath("/bucket/object"),
		Attr: Attr{
			Mtime: mtime,
			Ctime: mtime,
			Atime: atime,
		},
	}
	pb := EntryAttributeToPb(source)
	if pb.Atime != atime.Unix() {
		t.Fatalf("expected proto atime %d, got %d", atime.Unix(), pb.Atime)
	}
	if pb.AtimeNs != int32(atime.Nanosecond()) {
		t.Fatalf("expected proto atime_ns %d, got %d", atime.Nanosecond(), pb.AtimeNs)
	}

	decoded := PbToEntryAttribute(pb)
	if !decoded.Atime.Equal(atime) {
		t.Fatalf("expected decoded atime %v, got %v", atime, decoded.Atime)
	}
}

func TestEntryCodec_AtimeZeroFallsBackToMtime(t *testing.T) {
	mtime := time.Unix(1_700_000_000, 0)
	pb := &filer_pb.FuseAttributes{
		Mtime:   mtime.Unix(),
		MtimeNs: int32(mtime.Nanosecond()),
	}
	decoded := PbToEntryAttribute(pb)
	if !decoded.Atime.Equal(mtime) {
		t.Fatalf("expected atime to fall back to mtime %v, got %v", mtime, decoded.Atime)
	}
}

// Atime in the first second of the unix epoch encodes as Atime=0 with
// AtimeNs>0; the decode path must treat that as a valid timestamp rather than
// falling back to Mtime.
func TestEntryCodec_AtimeSubSecondEpochPreserved(t *testing.T) {
	mtime := time.Unix(1_700_000_000, 0)
	pb := &filer_pb.FuseAttributes{
		Mtime:   mtime.Unix(),
		MtimeNs: int32(mtime.Nanosecond()),
		Atime:   0,
		AtimeNs: 500_000,
	}
	decoded := PbToEntryAttribute(pb)
	want := time.Unix(0, 500_000)
	if !decoded.Atime.Equal(want) {
		t.Fatalf("expected sub-second-epoch atime %v, got %v", want, decoded.Atime)
	}
}
