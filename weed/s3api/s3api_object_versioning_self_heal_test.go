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

// TestSelectLatestVersion_MixedFormats ensures the chronological comparator
// is used when the directory contains both old- and new-format version ids
// created across a format upgrade.
func TestSelectLatestVersion_MixedFormats(t *testing.T) {
	baseTs := int64(1700000000000000000)
	oldId := createOldFormatVersionId(baseTs)
	newIdLater := createNewFormatVersionId(baseTs + int64(time.Minute)) // chronologically newer

	entries := []*filer_pb.Entry{
		newVersionEntry("v-old."+oldId, oldId, false),
		newVersionEntry("v-new-later."+newIdLater, newIdLater, false),
	}

	latest, latestId, latestName, isDM := selectLatestVersion(entries)

	assert.NotNil(t, latest)
	assert.Equal(t, newIdLater, latestId, "newer timestamp should win across formats")
	assert.Equal(t, "v-new-later."+newIdLater, latestName)
	assert.False(t, isDM)
}

// TestSelectLatestVersion_PromotesNewestDeleteMarker verifies the
// selector promotes a delete marker when it is the chronologically newest
// entry. Returning the older content version would "undelete" the object.
func TestSelectLatestVersion_PromotesNewestDeleteMarker(t *testing.T) {
	baseTs := int64(1700000000000000000)
	olderContentId := createOldFormatVersionId(baseTs)
	newerDmId := createOldFormatVersionId(baseTs + int64(time.Minute))

	entries := []*filer_pb.Entry{
		newVersionEntry("v-content."+olderContentId, olderContentId, false),
		newVersionEntry("dm-newer."+newerDmId, newerDmId, true),
	}

	latest, latestId, latestName, isDM := selectLatestVersion(entries)

	assert.NotNil(t, latest)
	assert.Equal(t, newerDmId, latestId, "newest delete marker must win")
	assert.Equal(t, "dm-newer."+newerDmId, latestName)
	assert.True(t, isDM, "selected entry is a delete marker")
}

// TestSelectLatestVersion_ContentWinsWhenNewer verifies that when a content
// version is chronologically newest, it is selected and isDeleteMarker=false.
func TestSelectLatestVersion_ContentWinsWhenNewer(t *testing.T) {
	baseTs := int64(1700000000000000000)
	olderDmId := createOldFormatVersionId(baseTs)
	newerContentId := createOldFormatVersionId(baseTs + int64(time.Minute))

	entries := []*filer_pb.Entry{
		newVersionEntry("dm-older."+olderDmId, olderDmId, true),
		newVersionEntry("v-newer."+newerContentId, newerContentId, false),
	}

	latest, latestId, latestName, isDM := selectLatestVersion(entries)

	assert.NotNil(t, latest)
	assert.Equal(t, newerContentId, latestId)
	assert.Equal(t, "v-newer."+newerContentId, latestName)
	assert.False(t, isDM)
}

// TestSelectLatestVersion_OnlyDeleteMarkers verifies that when only delete
// markers are present, the self-heal selector still returns the newest one
// so the pointer can be repaired and the caller correctly renders 404.
func TestSelectLatestVersion_OnlyDeleteMarkers(t *testing.T) {
	baseTs := int64(1700000000000000000)
	dmOlder := createOldFormatVersionId(baseTs)
	dmNewer := createOldFormatVersionId(baseTs + int64(time.Minute))

	entries := []*filer_pb.Entry{
		newVersionEntry("dm-older."+dmOlder, dmOlder, true),
		newVersionEntry("dm-newer."+dmNewer, dmNewer, true),
	}

	latest, latestId, latestName, isDM := selectLatestVersion(entries)

	assert.NotNil(t, latest, "self-heal must still promote the newest delete marker when that is all that remains")
	assert.Equal(t, dmNewer, latestId)
	assert.Equal(t, "dm-newer."+dmNewer, latestName)
	assert.True(t, isDM)
}

// TestSelectLatestVersion_EmptyOrUntagged verifies nil latestEntry when there
// is no version-id-tagged entry at all.
func TestSelectLatestVersion_EmptyOrUntagged(t *testing.T) {
	entries := []*filer_pb.Entry{
		nil,
		{Name: "no-extended"},
		{Name: "empty-extended", Extended: map[string][]byte{}},
		{Name: "no-version-id", Extended: map[string][]byte{"some-other-key": []byte("x")}},
	}

	latest, latestId, latestName, isDM := selectLatestVersion(entries)

	assert.Nil(t, latest)
	assert.Empty(t, latestId)
	assert.Empty(t, latestName)
	assert.False(t, isDM)
}
