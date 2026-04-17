package s3api

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// newVersionEntry builds a .versions directory child entry with the given
// version id and name, optionally tagged as a delete marker.
func newVersionEntry(name, versionId string, isDeleteMarker bool) *filer_pb.Entry {
	ext := map[string][]byte{
		s3_constants.ExtVersionIdKey: []byte(versionId),
	}
	if isDeleteMarker {
		ext[s3_constants.ExtDeleteMarkerKey] = []byte("true")
	}
	return &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{},
		Extended:   ext,
	}
}

// TestSelectLatestContentVersion_PicksNewestNonDeleteMarker verifies that the
// pure selection helper used by both updateLatestVersionAfterDeletion and
// healStaleLatestVersionPointer picks the chronologically newest content
// version and ignores delete markers.
func TestSelectLatestContentVersion_PicksNewestNonDeleteMarker(t *testing.T) {
	baseTs := int64(1700000000000000000)
	olderId := createOldFormatVersionId(baseTs)
	newerId := createOldFormatVersionId(baseTs + int64(time.Minute))
	dmId := createOldFormatVersionId(baseTs + int64(2*time.Minute)) // delete marker is newest chronologically

	entries := []*filer_pb.Entry{
		newVersionEntry("v-older."+olderId, olderId, false),
		newVersionEntry("dm-newest."+dmId, dmId, true),
		newVersionEntry("v-newer."+newerId, newerId, false),
	}

	latest, latestId, latestName, hasDM := selectLatestContentVersion(entries)

	assert.NotNil(t, latest, "expected to pick a content version")
	assert.Equal(t, newerId, latestId, "should pick the newest non-delete-marker version")
	assert.Equal(t, "v-newer."+newerId, latestName)
	assert.True(t, hasDM, "should report that a delete marker was observed")
}

// TestSelectLatestContentVersion_OnlyDeleteMarkers verifies that when only
// delete markers remain, the helper returns nil latestEntry and flags
// hasDeleteMarkers=true so the caller can preserve the .versions directory
// instead of clearing it.
func TestSelectLatestContentVersion_OnlyDeleteMarkers(t *testing.T) {
	baseTs := int64(1700000000000000000)
	dm1 := createOldFormatVersionId(baseTs)
	dm2 := createOldFormatVersionId(baseTs + int64(time.Minute))

	entries := []*filer_pb.Entry{
		newVersionEntry("dm-1."+dm1, dm1, true),
		newVersionEntry("dm-2."+dm2, dm2, true),
	}

	latest, latestId, latestName, hasDM := selectLatestContentVersion(entries)

	assert.Nil(t, latest)
	assert.Empty(t, latestId)
	assert.Empty(t, latestName)
	assert.True(t, hasDM)
}

// TestSelectLatestContentVersion_EmptyAndUntaggedEntries verifies the helper
// skips entries that lack Extended metadata or a version id (e.g. stray
// artifacts in the directory) and returns nil when no valid version is found.
func TestSelectLatestContentVersion_EmptyAndUntaggedEntries(t *testing.T) {
	entries := []*filer_pb.Entry{
		nil,
		{Name: "no-extended"},
		{Name: "empty-extended", Extended: map[string][]byte{}},
		{Name: "no-version-id", Extended: map[string][]byte{"some-other-key": []byte("x")}},
	}

	latest, latestId, latestName, hasDM := selectLatestContentVersion(entries)

	assert.Nil(t, latest)
	assert.Empty(t, latestId)
	assert.Empty(t, latestName)
	assert.False(t, hasDM)
}

// TestSelectLatestContentVersion_MixedFormats ensures the chronological
// comparator is used when the directory contains both old- and new-format
// version ids created across a format upgrade.
func TestSelectLatestContentVersion_MixedFormats(t *testing.T) {
	baseTs := int64(1700000000000000000)
	oldId := createOldFormatVersionId(baseTs)
	newIdLater := createNewFormatVersionId(baseTs + int64(time.Minute)) // chronologically newer

	entries := []*filer_pb.Entry{
		newVersionEntry("v-old."+oldId, oldId, false),
		newVersionEntry("v-new-later."+newIdLater, newIdLater, false),
	}

	latest, latestId, latestName, hasDM := selectLatestContentVersion(entries)

	assert.NotNil(t, latest)
	assert.Equal(t, newIdLater, latestId, "newer timestamp should win across formats")
	assert.Equal(t, "v-new-later."+newIdLater, latestName)
	assert.False(t, hasDM)
}
