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
// is no version entry at all. Names that are neither tagged with a version id
// nor shaped like a v_<versionId> file are ignored.
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

// newUntaggedVersionFile builds a v_<versionId> entry missing the
// Seaweed-X-Amz-Version-Id attribute, like ones the filename fallback recovers.
func newUntaggedVersionFile(versionId string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:       "v_" + versionId,
		Attributes: &filer_pb.FuseAttributes{},
		Extended:   map[string][]byte{"X-Amz-Storage-Class": []byte("STANDARD")},
	}
}

// TestVersionIdFromEntry covers the attribute-first, filename-fallback contract
// that lets selectLatestVersion recover version files written outside the
// normal versioned-PUT path.
func TestVersionIdFromEntry(t *testing.T) {
	id := "6775adb0d7b0d2e303e0fced6989bb57"

	// Attribute present: used verbatim.
	assert.Equal(t, id, versionIdFromEntry(newVersionEntry("v_"+id, id, false)))

	// Attribute absent but named v_<id>: derived from the filename.
	assert.Equal(t, id, versionIdFromEntry(newUntaggedVersionFile(id)))

	// Empty attribute value falls back to the filename.
	emptyAttr := &filer_pb.Entry{Name: "v_" + id, Extended: map[string][]byte{s3_constants.ExtVersionIdKey: []byte("")}}
	assert.Equal(t, id, versionIdFromEntry(emptyAttr))

	// Not a version file and not tagged: no id.
	assert.Empty(t, versionIdFromEntry(&filer_pb.Entry{Name: "not-a-version", Extended: map[string][]byte{"x": []byte("y")}}))

	// Nil and directory entries are ignored even if named like a version file.
	assert.Empty(t, versionIdFromEntry(nil))
	assert.Empty(t, versionIdFromEntry(&filer_pb.Entry{Name: "v_" + id, IsDirectory: true}))
}

// TestSelectLatestVersion_FilenameFallback verifies untagged version files are
// still selected by deriving the id from the v_<versionId> filename.
func TestSelectLatestVersion_FilenameFallback(t *testing.T) {
	baseTs := int64(1700000000000000000)
	olderId := createNewFormatVersionId(baseTs)
	newerId := createNewFormatVersionId(baseTs + int64(time.Minute))

	entries := []*filer_pb.Entry{
		newUntaggedVersionFile(olderId),
		newUntaggedVersionFile(newerId),
	}

	latest, latestId, latestName, isDM := selectLatestVersion(entries)

	assert.NotNil(t, latest, "untagged version files must still be selectable by filename")
	assert.Equal(t, newerId, latestId, "newest version wins via filename-derived id")
	assert.Equal(t, "v_"+newerId, latestName)
	assert.False(t, isDM)
}

// TestSelectLatestVersion_FilenameFallbackMixedWithTagged ensures filename-only
// entries compete correctly against properly tagged ones.
func TestSelectLatestVersion_FilenameFallbackMixedWithTagged(t *testing.T) {
	baseTs := int64(1700000000000000000)
	taggedOlderId := createNewFormatVersionId(baseTs)
	untaggedNewerId := createNewFormatVersionId(baseTs + int64(time.Minute))

	entries := []*filer_pb.Entry{
		newVersionEntry("v_"+taggedOlderId, taggedOlderId, false),
		newUntaggedVersionFile(untaggedNewerId),
	}

	latest, latestId, latestName, isDM := selectLatestVersion(entries)

	assert.NotNil(t, latest)
	assert.Equal(t, untaggedNewerId, latestId, "newer untagged version must win over older tagged one")
	assert.Equal(t, "v_"+untaggedNewerId, latestName)
	assert.False(t, isDM)
}

// TestSelectLatestVersion_FilenameFallbackDeleteMarker verifies a delete marker
// whose version-id attribute is missing is still recognized (id from filename)
// and still reported as a delete marker so the caller renders NoSuchKey.
func TestSelectLatestVersion_FilenameFallbackDeleteMarker(t *testing.T) {
	id := createNewFormatVersionId(1700000000000000000)
	entry := &filer_pb.Entry{
		Name:       "v_" + id,
		Attributes: &filer_pb.FuseAttributes{},
		Extended:   map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}

	latest, latestId, _, isDM := selectLatestVersion([]*filer_pb.Entry{entry})

	assert.NotNil(t, latest)
	assert.Equal(t, id, latestId)
	assert.True(t, isDM, "delete marker recognized by filename must still report isDeleteMarker")
}
