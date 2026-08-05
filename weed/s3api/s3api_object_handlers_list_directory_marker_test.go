package s3api

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	grpc "google.golang.org/grpc"
)

// directoryMarker builds the filer directory a PutObject on a trailing-slash key
// leaves behind. Deleting the key strips this metadata back off, so a deleted marker
// is just newDir.
func directoryMarker(name string) *filer_pb.Entry {
	entry := newDir(name)
	entry.Attributes.Mime = s3_constants.FolderMimeType
	entry.Extended = map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")}
	return entry
}

// TestDeletedDirectoryMarkerIsNotListed covers the reported flow. The delete demotes the
// directory that carried the key, so what is left lists as the plain directory it is.
func TestDeletedDirectoryMarkerIsNotListed(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {newDir("m2")},
			"/buckets/test/m2": {deleteMarkedVersionsDir("f.txt")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Empty(t, seen, "nothing under it is a key, so it names no prefix either")

	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Empty(t, seen, "the flat listing must not report it either")
}

// TestLiveDirectoryMarkerIsListed pins the other half: the directory entry carries the
// key, so the listing reports it straight off that entry.
func TestLiveDirectoryMarkerIsListed(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {directoryMarker("m2")},
			"/buckets/test/m2": {},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"m2"}, seen)

	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"m2"}, seen)
}

// TestDeletedDirectoryMarkerKeepsLiveChildren guards the boundary: deleting the key
// "m2/" says nothing about the objects below it, which keep the prefix alive.
func TestDeletedDirectoryMarkerKeepsLiveChildren(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {newDir("m2")},
			"/buckets/test/m2": {liveVersionsDir("keep.txt")},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"m2"}, seen, "the directory still holds a live key, so it stays a prefix")

	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"keep.txt"}, seen, "only the surviving child is a key")
}

// ownVersionsDir builds a history recorded inside a directory by an older build, which
// described the key "<dir>/" rather than any child of it.
func ownVersionsDir() *filer_pb.Entry {
	entry := deleteMarkedVersionsDir("")
	entry.Name = s3_constants.VersionsFolder
	return entry
}

// TestStaleOwnVersionsIsNotAKey pins the guard for buckets written before directory
// markers stopped being versioned: a history left inside a directory describes that
// directory, and must not surface as a key named after it.
func TestStaleOwnVersionsIsNotAKey(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {directoryMarker("m2")},
			"/buckets/test/m2": {ownVersionsDir()},
		},
	}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"m2"}, seen, "the marker is the only key; no phantom m2/m2")
}

// countingFilerClient records how many ListEntries round trips a listing spends.
type countingFilerClient struct {
	*testFilerClient
	calls int
}

func (c *countingFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.calls++
	return c.testFilerClient.ListEntries(ctx, in, opts...)
}

// TestDirectoryMarkersCostNoLookups is why the delete is recorded on the directory
// entry rather than in a history a listing has to go and read. Buckets written by tools
// that keep a marker per directory are made of these, so a round trip per marker is the
// whole listing cost.
func TestDirectoryMarkersCostNoLookups(t *testing.T) {
	client := &countingFilerClient{testFilerClient: &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":    {directoryMarker("d1"), directoryMarker("d2"), directoryMarker("d3")},
			"/buckets/test/d1": {},
			"/buckets/test/d2": {},
			"/buckets/test/d3": {},
		},
	}}

	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", delimiter: "/", bucket: "test"}, &ListingCursor{maxKeys: 1000, hideDeletedPrefixes: true})
	assert.Equal(t, []string{"d1", "d2", "d3"}, seen)
	assert.Equal(t, 1, client.calls, "listing directory markers must not cost a round trip per marker")
}
