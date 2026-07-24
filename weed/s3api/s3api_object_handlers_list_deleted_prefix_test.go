package s3api

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// deleteMarkedVersionsDir builds the .versions directory left behind when the current
// version of a versioned object is a delete marker.
func deleteMarkedVersionsDir(object string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        object + s3_constants.VersionsFolder,
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended: map[string][]byte{
			s3_constants.ExtLatestVersionIdKey:          []byte("v-deleted"),
			s3_constants.ExtLatestVersionIsDeleteMarker: []byte("true"),
		},
	}
}

// liveVersionsDir builds the .versions directory of a versioned object whose current
// version is a real object.
func liveVersionsDir(object string) *filer_pb.Entry {
	now := time.Now().Unix()
	return &filer_pb.Entry{
		Name:        object + s3_constants.VersionsFolder,
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mtime: now},
		Extended: map[string][]byte{
			s3_constants.ExtLatestVersionIdKey:          []byte("v-live"),
			s3_constants.ExtLatestVersionSizeKey:        []byte("6"),
			s3_constants.ExtLatestVersionMtimeKey:       []byte(strconv.FormatInt(now, 10)),
			s3_constants.ExtLatestVersionETagKey:        []byte(`"b1946ac92492d2347c6235b4d2611184"`),
			s3_constants.ExtLatestVersionIsDeleteMarker: []byte("false"),
		},
	}
}

func newDir(name string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
}

func listedNames(t *testing.T, client filer_pb.SeaweedFilerClient, req listDirectoryRequest, cursor *ListingCursor) []string {
	t.Helper()
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	var seen []string
	_, err := s3a.doListFilerEntries(context.Background(), client, req, cursor, func(dir string, entry *filer_pb.Entry) {
		seen = append(seen, entry.Name)
	})
	assert.NoError(t, err)
	return seen
}

// TestDeletedPrefixIsNotACommonPrefix covers the versioned bucket case: deleting the
// only object under a prefix writes a delete marker, so no current version remains
// under it. The filer keeps the directory and the version history, but AWS derives
// CommonPrefixes from the keys a listing returns, so the prefix must be gone.
func TestDeletedPrefixIsNotACommonPrefix(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":                 {newDir("backup")},
			"/buckets/test/backup":          {newDir("20260101")},
			"/buckets/test/backup/20260101": {deleteMarkedVersionsDir("manifest")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Empty(t, seen, "a prefix whose only object is delete-marked must not be listed")

	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test/backup", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Empty(t, seen, "the parent listing must not report the deleted date prefix either")
}

// TestLivePrefixIsStillACommonPrefix guards the other half: a prefix whose versioned
// object still has a current version keeps being reported.
func TestLivePrefixIsStillACommonPrefix(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":                 {newDir("backup")},
			"/buckets/test/backup":          {newDir("20260101"), newDir("20260102")},
			"/buckets/test/backup/20260101": {deleteMarkedVersionsDir("manifest")},
			"/buckets/test/backup/20260102": {liveVersionsDir("manifest")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"backup"}, seen)

	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test/backup", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"20260102"}, seen, "only the date prefix with a current version is listed")
}

// TestDeletedPrefixGetsNoDirectoryMarker checks the trailing-slash probe. The empty
// directory marker exists for directories created out of band; a directory that only
// holds version history names nothing, so the probe answers empty like AWS does.
func TestDeletedPrefixGetsNoDirectoryMarker(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":        {newDir("backup")},
			"/buckets/test/backup": {deleteMarkedVersionsDir("manifest")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", prefix: "backup", delimiter: "/", bucket: "test"},
		&ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: true, hideDeletedPrefixes: true})
	assert.Empty(t, seen, "prefix=backup/ must not answer with a backup/ key")
}

// TestEmptyDirectoryStaysACommonPrefix pins the boundary of the change: an empty
// directory keeps the meaning it has today, since mount and mkdir create them and
// empty-folder cleanup owns their lifetime.
func TestEmptyDirectoryStaysACommonPrefix(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":      {newDir("logs")},
			"/buckets/test/logs": {},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"logs"}, seen)
}

// TestDirectoryKeyObjectListedDespiteDeletedChildren covers a directory created with
// PutObject on a trailing-slash key: that key exists in its own right, so it is
// reported no matter what happened to the objects below it.
func TestDirectoryKeyObjectListedDespiteDeletedChildren(t *testing.T) {
	keyObject := newDir("backup")
	keyObject.Attributes.Mime = s3_constants.FolderMimeType

	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":        {keyObject},
			"/buckets/test/backup": {deleteMarkedVersionsDir("manifest")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"backup"}, seen)
}

// TestUnstampedVersionKeepsThePrefix covers a .versions entry carrying no
// current-version stamp, which happens while the pointer written on the key's owner
// filer has not reached the filer serving the list. An object whose current version is
// unknown keeps its prefix instead of disappearing on a guess.
func TestUnstampedVersionKeepsThePrefix(t *testing.T) {
	unstamped := &filer_pb.Entry{
		Name:        "obj" + s3_constants.VersionsFolder,
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{},
	}
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":   {newDir("p")},
			"/buckets/test/p": {unstamped},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"p"}, seen)
}

// TestLiveVersionStampKeepsThePrefix pins the other stamp value: a current version that
// is not a delete marker is a key, so its prefix is reported.
func TestLiveVersionStampKeepsThePrefix(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":   {newDir("p")},
			"/buckets/test/p": {liveVersionsDir("obj")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"p"}, seen)
}

// TestBucketWithoutVersioningSkipsTheProbe pins the gate. A bucket that never had
// versioning cannot grow a directory full of delete markers, so its listings keep
// reporting every directory without paying for a probe per prefix.
func TestBucketWithoutVersioningSkipsTheProbe(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":        {newDir("backup")},
			"/buckets/test/backup": {deleteMarkedVersionsDir("manifest")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000})
	assert.Equal(t, []string{"backup"}, seen)
}

// TestDeletedPrefixesDoNotConsumeMaxKeys makes sure paging steps over the deleted
// prefixes instead of returning short pages of nothing.
func TestDeletedPrefixesDoNotConsumeMaxKeys(t *testing.T) {
	root := []*filer_pb.Entry{newDir("p1"), newDir("p2"), newDir("p3"), newDir("p4")}
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    root,
			"/buckets/test/p1": {deleteMarkedVersionsDir("obj")},
			"/buckets/test/p2": {liveVersionsDir("obj")},
			"/buckets/test/p3": {deleteMarkedVersionsDir("obj")},
			"/buckets/test/p4": {liveVersionsDir("obj")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"p2", "p4"}, seen)
}
