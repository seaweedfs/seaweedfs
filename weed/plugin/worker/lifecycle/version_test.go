package lifecycle

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// makeVersionId creates a new-format version ID from a timestamp.
func makeVersionId(t time.Time) string {
	inverted := math.MaxInt64 - t.UnixNano()
	return fmt.Sprintf("%016x", inverted) + "0000000000000000"
}

func TestSortVersionsByTimestamp(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	entries := []*filer_pb.Entry{
		{Name: "v_" + makeVersionId(t1)},
		{Name: "v_" + makeVersionId(t3)},
		{Name: "v_" + makeVersionId(t2)},
	}

	sortVersionsByTimestamp(entries)

	// Should be sorted newest first: t3, t2, t1
	ts0 := getEntryVersionTimestamp(entries[0])
	ts1 := getEntryVersionTimestamp(entries[1])
	ts2 := getEntryVersionTimestamp(entries[2])

	if !ts0.After(ts1) {
		t.Errorf("expected entries[0] (%v) > entries[1] (%v)", ts0, ts1)
	}
	if !ts1.After(ts2) {
		t.Errorf("expected entries[1] (%v) > entries[2] (%v)", ts1, ts2)
	}
}

func TestGetEntryVersionTimestamp(t *testing.T) {
	t.Run("from_version_id", func(t *testing.T) {
		ts := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
		entry := &filer_pb.Entry{Name: "v_" + makeVersionId(ts)}
		got := getEntryVersionTimestamp(entry)
		diff := got.Sub(ts)
		if diff < -time.Second || diff > time.Second {
			t.Errorf("timestamp diff too large: %v", diff)
		}
	})

	t.Run("fallback_to_mtime", func(t *testing.T) {
		mtime := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		entry := &filer_pb.Entry{
			Name:       "v_invalidversionid",
			Attributes: &filer_pb.FuseAttributes{Mtime: mtime.Unix()},
		}
		got := getEntryVersionTimestamp(entry)
		if !got.Equal(mtime) {
			t.Errorf("expected mtime %v, got %v", mtime, got)
		}
	})
}

func TestVersionsDirectoryNaming(t *testing.T) {
	// Verify the .versions suffix constant matches what we expect.
	if s3_constants.VersionsFolder != ".versions" {
		t.Fatalf("unexpected VersionsFolder constant: %q", s3_constants.VersionsFolder)
	}

	// Verify object key derivation from .versions path.
	versionsDir := "/buckets/mybucket/path/to/key.versions"
	bucketPath := "/buckets/mybucket"
	relDir := versionsDir[len(bucketPath)+1:] // "path/to/key.versions"
	objKey := relDir[:len(relDir)-len(s3_constants.VersionsFolder)]
	if objKey != "path/to/key" {
		t.Errorf("expected 'path/to/key', got %q", objKey)
	}
}
