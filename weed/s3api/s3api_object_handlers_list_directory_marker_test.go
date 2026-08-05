package s3api

import (
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// directoryMarker builds the filer directory a PutObject on a trailing-slash key
// leaves behind.
func directoryMarker(name string) *filer_pb.Entry {
	entry := newDir(name)
	entry.Attributes.Mime = s3_constants.FolderMimeType
	entry.Extended = map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")}
	return entry
}

// ownVersionsDir builds the history of the key "<dir>/", which lives inside the
// directory it names rather than beside it.
func ownVersionsDir(deleted bool) *filer_pb.Entry {
	now := time.Now().Unix()
	extended := map[string][]byte{
		s3_constants.ExtLatestVersionIdKey:          []byte("v-dir"),
		s3_constants.ExtLatestVersionMtimeKey:       []byte(strconv.FormatInt(now, 10)),
		s3_constants.ExtLatestVersionSizeKey:        []byte("0"),
		s3_constants.ExtLatestVersionETagKey:        []byte(`"d41d8cd98f00b204e9800998ecf8427e"`),
		s3_constants.ExtLatestVersionIsDeleteMarker: []byte(strconv.FormatBool(deleted)),
	}
	return &filer_pb.Entry{
		Name:        s3_constants.VersionsFolder,
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mtime: now},
		Extended:    extended,
	}
}

// TestDeletedDirectoryMarkerIsNotListed covers the reported flow: PutObject on "m2/",
// DELETE on "m2/" writes a delete marker into m2/.versions, and the key must stop
// being reported even though the filer directory that carries it survives.
func TestDeletedDirectoryMarkerIsNotListed(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {directoryMarker("m2")},
			"/buckets/test/m2": {ownVersionsDir(true)},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Empty(t, seen, "a delete-marked directory marker is not a key and names no prefix")

	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Empty(t, seen, "the flat listing must not report it either")
}

// TestLiveDirectoryMarkerIsListed pins the other half: a marker whose current version
// is real is a key, and the listing reports it from the directory that carries it.
func TestLiveDirectoryMarkerIsListed(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {directoryMarker("m2")},
			"/buckets/test/m2": {ownVersionsDir(false)},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"m2"}, seen)

	// A live history must not also surface as a phantom key named after its container.
	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"m2"}, seen)
}

// TestDeletedDirectoryMarkerKeepsLiveChildren guards the boundary between the two
// questions a directory answers: deleting the key "m2/" says nothing about the objects
// below it, which keep the prefix alive.
func TestDeletedDirectoryMarkerKeepsLiveChildren(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {directoryMarker("m2")},
			"/buckets/test/m2": {ownVersionsDir(true), liveVersionsDir("keep.txt")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"m2"}, seen, "the directory still holds a live key, so it stays a prefix")

	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"keep.txt"}, seen, "only the surviving child is a key")
}

// TestDeletedDirectoryMarkerIsNotADirectoryProbe checks the trailing-slash probe: the
// key was deleted, so prefix=m2/ answers empty instead of resurfacing the marker.
func TestDeletedDirectoryMarkerIsNotADirectoryProbe(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {directoryMarker("m2")},
			"/buckets/test/m2": {ownVersionsDir(true)},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", prefix: "m2", delimiter: "/", bucket: "test"},
		&ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: true, hideDeletedPrefixes: true})
	assert.Empty(t, seen)
}

// TestUnversionedBucketKeepsItsDirectoryMarkers pins the gate: without versioning there
// is no history to consult and no lookup to pay for.
func TestUnversionedBucketKeepsItsDirectoryMarkers(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {directoryMarker("m2")},
			"/buckets/test/m2": {ownVersionsDir(true)},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000})
	assert.Equal(t, []string{"m2"}, seen)
}
