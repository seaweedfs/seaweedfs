package lifecycle

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// makeVersionId creates a new-format version ID from a timestamp.
func makeVersionId(t time.Time) string {
	inverted := math.MaxInt64 - t.UnixNano()
	return fmt.Sprintf("%016x", inverted) + "0000000000000000"
}

func TestSortVersionsByVersionId(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	vid1 := makeVersionId(t1)
	vid2 := makeVersionId(t2)
	vid3 := makeVersionId(t3)

	entries := []*filer_pb.Entry{
		{Name: "v_" + vid1},
		{Name: "v_" + vid3},
		{Name: "v_" + vid2},
	}

	sortVersionsByVersionId(entries)

	// Should be sorted newest first: t3, t2, t1.
	expected := []string{"v_" + vid3, "v_" + vid2, "v_" + vid1}
	for i, want := range expected {
		if entries[i].Name != want {
			t.Errorf("entries[%d].Name = %s, want %s", i, entries[i].Name, want)
		}
	}
}

func TestSortVersionsByVersionId_SameTimestampDifferentSuffix(t *testing.T) {
	// Two versions with the same timestamp prefix but different random suffix.
	// The sort must still produce a deterministic order.
	base := makeVersionId(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	vid1 := base[:16] + "aaaaaaaaaaaaaaaa"
	vid2 := base[:16] + "bbbbbbbbbbbbbbbb"

	entries := []*filer_pb.Entry{
		{Name: "v_" + vid2},
		{Name: "v_" + vid1},
	}

	sortVersionsByVersionId(entries)

	// New format: smaller hex = newer. vid1 ("aaa...") < vid2 ("bbb...") so vid1 is newer.
	if strings.TrimPrefix(entries[0].Name, "v_") != vid1 {
		t.Errorf("expected vid1 (newer) first, got %s", entries[0].Name)
	}
}

func TestCompareVersionIdsMixedFormats(t *testing.T) {
	// Old format: raw nanosecond timestamp (below threshold ~0x17...).
	// New format: inverted timestamp (above threshold ~0x68...).
	oldTs := time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)
	newTs := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	oldFormatId := fmt.Sprintf("%016x", oldTs.UnixNano()) + "abcdef0123456789"
	newFormatId := makeVersionId(newTs) // uses inverted timestamp

	// newTs is more recent, so newFormatId should sort as "newer".
	cmp := s3lifecycle.CompareVersionIds(newFormatId, oldFormatId)
	if cmp >= 0 {
		t.Errorf("expected new-format ID (2026) to be newer than old-format ID (2023), got cmp=%d", cmp)
	}

	// Reverse comparison.
	cmp2 := s3lifecycle.CompareVersionIds(oldFormatId, newFormatId)
	if cmp2 <= 0 {
		t.Errorf("expected old-format ID (2023) to be older than new-format ID (2026), got cmp=%d", cmp2)
	}

	// Sort a mixed slice: should be newest-first.
	entries := []*filer_pb.Entry{
		{Name: "v_" + oldFormatId},
		{Name: "v_" + newFormatId},
	}
	sortVersionsByVersionId(entries)

	if strings.TrimPrefix(entries[0].Name, "v_") != newFormatId {
		t.Errorf("expected new-format (newer) entry first after sort")
	}
}

func TestVersionsDirectoryNaming(t *testing.T) {
	if s3_constants.VersionsFolder != ".versions" {
		t.Fatalf("unexpected VersionsFolder constant: %q", s3_constants.VersionsFolder)
	}

	versionsDir := "/buckets/mybucket/path/to/key.versions"
	bucketPath := "/buckets/mybucket"
	relDir := strings.TrimPrefix(versionsDir, bucketPath+"/")
	objKey := strings.TrimSuffix(relDir, s3_constants.VersionsFolder)
	if objKey != "path/to/key" {
		t.Errorf("expected 'path/to/key', got %q", objKey)
	}
}
