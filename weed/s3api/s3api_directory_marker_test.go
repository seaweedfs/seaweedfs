package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// markerFiler deletes and updates its own entries, refusing to delete a
// directory that still has children, as a filer does.
type markerFiler struct {
	anonymousReadFiler
	deletes int
}

func (f *markerFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	target := path.Join(req.Directory, req.Name)
	for key := range f.entries {
		if strings.HasPrefix(key, target+"/") {
			return &filer_pb.DeleteEntryResponse{Error: filer.MsgFailDelNonEmptyFolder + " " + target}, nil
		}
	}
	delete(f.entries, target)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *markerFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	target := path.Join(req.Directory, req.Entry.Name)
	if f.entries[target] == nil {
		return nil, status.Error(codes.NotFound, target)
	}
	f.entries[target] = proto.Clone(req.Entry).(*filer_pb.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

// A nonempty "dir/" marker that so far exists only on its write owner: the
// delete and the demotion that follows its refusal both have to run there, or
// the owner keeps listing the key after a reported success.
func TestDeleteNonEmptyMarkerDemotesOnTheOwner(t *testing.T) {
	bucketEntry := &filer_pb.Entry{Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
	connected := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b": bucketEntry,
	}}}
	owner := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b":                 bucketEntry,
		"/buckets/b/album":           {Name: "album", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream"}},
		"/buckets/b/album/image.jpg": anonymousReadEntry(nil),
	}}}
	connectedAddr := startFakeFiler(t, connected)
	ownerAddr := startFakeFiler(t, owner)
	s3a := newPutTestServer(t, connectedAddr, ownerAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)
	require.Equal(t, connectedAddr, s3a.filerClient.GetCurrentFiler())

	r := httptest.NewRequest(http.MethodDelete, "/b/album/", nil)
	require.Equal(t, s3err.ErrNone, s3a.deleteDirectoryMarker(r, "b", "album/"))

	owner.mu.Lock()
	marker := owner.entries["/buckets/b/album"]
	owner.mu.Unlock()
	require.NotNil(t, marker, "the owner lost the directory its child lives in")
	require.False(t, marker.IsDirectoryKeyObject(), "the owner still holds album/ as an object")
	require.Contains(t, owner.entries, "/buckets/b/album/image.jpg")
	require.Zero(t, connected.deletes, "the delete went to the connected filer, not the owner")
}

// While a ring change is in its cooling-off window, the marker the read finds
// on the prior owner may not have reached the new owner yet.
func TestDeleteNonEmptyMarkerDemotesOnThePriorOwner(t *testing.T) {
	bucketEntry := &filer_pb.Entry{Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
	next := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b": bucketEntry,
	}}}
	prior := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b":                 bucketEntry,
		"/buckets/b/album":           {Name: "album", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream"}},
		"/buckets/b/album/image.jpg": anonymousReadEntry(nil),
	}}}
	nextAddr := startFakeFiler(t, next)
	priorAddr := startFakeFiler(t, prior)
	s3a := newPutTestServer(t, nextAddr, priorAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, priorAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{priorAddr}, 1)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{nextAddr}, 2)
	require.Equal(t, priorAddr, s3a.priorWriteOwner("b", "album"))

	r := httptest.NewRequest(http.MethodDelete, "/b/album/", nil)
	require.Equal(t, s3err.ErrNone, s3a.deleteDirectoryMarker(r, "b", "album/"))

	prior.mu.Lock()
	marker := prior.entries["/buckets/b/album"]
	prior.mu.Unlock()
	require.NotNil(t, marker, "the prior owner lost the directory its child lives in")
	require.False(t, marker.IsDirectoryKeyObject(), "the prior owner still holds album/ as an object")
}

// The new owner's empty marker is the key "album/", but the prior owner may
// already hold "album" with uploaded data the new owner has not received.
func TestDeleteMarkerLeavesDataOnThePriorOwner(t *testing.T) {
	bucketEntry := &filer_pb.Entry{Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
	next := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b":       bucketEntry,
		"/buckets/b/album": {Name: "album", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream"}},
	}}}
	prior := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b": bucketEntry,
		"/buckets/b/album": {Name: "album", Attributes: &filer_pb.FuseAttributes{FileSize: 11},
			Chunks: []*filer_pb.FileChunk{{FileId: "1,0123456789", Size: 11}}},
	}}}
	nextAddr := startFakeFiler(t, next)
	priorAddr := startFakeFiler(t, prior)
	s3a := newPutTestServer(t, nextAddr, priorAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, priorAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{priorAddr}, 1)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{nextAddr}, 2)
	require.Equal(t, priorAddr, s3a.priorWriteOwner("b", "album"))

	r := httptest.NewRequest(http.MethodDelete, "/b/album/", nil)
	require.Equal(t, s3err.ErrNone, s3a.deleteDirectoryMarker(r, "b", "album/"))

	next.mu.Lock()
	require.NotContains(t, next.entries, "/buckets/b/album", "the new owner kept the marker")
	next.mu.Unlock()
	prior.mu.Lock()
	defer prior.mu.Unlock()
	require.Contains(t, prior.entries, "/buckets/b/album", "album/ removed the object album on the prior owner")
	require.Zero(t, prior.deletes)
}

// A prior owner that cannot be reached may still hold the marker, whether or not
// the new owner has it yet, and another filer reporting the delete done would
// not remove it there.
func TestDeleteMarkerFailsWithoutThePriorOwner(t *testing.T) {
	for _, ownerHasMarker := range []bool{true, false} {
		bucketEntry := &filer_pb.Entry{Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
		marker := &filer_pb.Entry{Name: "album", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream"}}
		connected := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
			"/buckets/b": bucketEntry, "/buckets/b/album": marker,
		}}}
		next := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
			"/buckets/b": bucketEntry,
		}}}
		if ownerHasMarker {
			next.entries["/buckets/b/album"] = marker
		}
		connectedAddr := startFakeFiler(t, connected)
		nextAddr := startFakeFiler(t, next)
		priorAddr := closedFilerAddress(t)
		s3a := newPutTestServer(t, connectedAddr, nextAddr, priorAddr)
		s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
		s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
		s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, nextAddr)
		s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{priorAddr}, 1)
		s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{nextAddr}, 2)
		require.Equal(t, priorAddr, s3a.priorWriteOwner("b", "album"))

		r := httptest.NewRequest(http.MethodDelete, "/b/album/", nil)
		require.Equal(t, s3err.ErrInternalError, s3a.deleteDirectoryMarker(r, "b", "album/"), "owner has marker: %v", ownerHasMarker)
		require.Zero(t, connected.deletes, "the prior owner's delete was reported done by another filer")
		require.Zero(t, next.deletes, "the delete half finished before the prior owner was known")
	}
}

// The filer a delete reaches may hold another entry than the marker the read
// found, such as "album" with uploaded data, and "album/" does not name it.
func TestDeleteMarkerRechecksTheEntryItDeletes(t *testing.T) {
	bucketEntry := &filer_pb.Entry{Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
	owner := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b":       bucketEntry,
		"/buckets/b/album": {Name: "album", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream"}},
	}}}
	lookups := 0
	owner.onLookup = func() {
		if lookups++; lookups > 1 {
			owner.entries["/buckets/b/album"] = &filer_pb.Entry{Name: "album", Attributes: &filer_pb.FuseAttributes{FileSize: 11},
				Chunks: []*filer_pb.FileChunk{{FileId: "1,0123456789", Size: 11}}}
		}
	}
	ownerAddr := startFakeFiler(t, owner)
	s3a := newPutTestServer(t, ownerAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

	r := httptest.NewRequest(http.MethodDelete, "/b/album/", nil)
	require.Equal(t, s3err.ErrNone, s3a.deleteDirectoryMarker(r, "b", "album/"))

	owner.mu.Lock()
	defer owner.mu.Unlock()
	require.Contains(t, owner.entries, "/buckets/b/album", "album/ removed the object album")
	require.Zero(t, owner.deletes)
}

// A delete the failover sends to a filer without the marker the read found
// would be passed on to the key's write owner, which applies it unchecked and
// may hold "album" with uploaded data.
func TestDeleteMarkerSendsNoDeleteToAFilerThatMisses(t *testing.T) {
	bucketEntry := &filer_pb.Entry{Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
	connected := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b":       bucketEntry,
		"/buckets/b/album": {Name: "album", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream"}},
	}}}
	lookups := 0
	connected.onLookup = func() {
		if lookups++; lookups > 1 {
			delete(connected.entries, "/buckets/b/album")
		}
	}
	connectedAddr := startFakeFiler(t, connected)
	ownerAddr := closedFilerAddress(t)
	s3a := newPutTestServer(t, connectedAddr, ownerAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, connectedAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

	r := httptest.NewRequest(http.MethodDelete, "/b/album/", nil)
	require.Equal(t, s3err.ErrInternalError, s3a.deleteDirectoryMarker(r, "b", "album/"))
	require.Zero(t, connected.deletes, "a filer that missed the marker was sent the delete")
}

// "album/" does not name the object "album", whose data may be inline rather
// than in chunks, or which may be empty and known only by the promotion mark.
func TestDeleteMarkerLeavesTheObjectWithoutTheSlash(t *testing.T) {
	promoted := func(content []byte) *filer_pb.Entry {
		e := &filer_pb.Entry{Name: "album", Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(content))}, Content: content}
		e.MarkPrefixObject()
		return e
	}
	for name, entry := range map[string]*filer_pb.Entry{
		"inline file":          {Name: "album", Attributes: &filer_pb.FuseAttributes{FileSize: 5}, Content: []byte("hello")},
		"empty file":           {Name: "album", Attributes: &filer_pb.FuseAttributes{}},
		"inline prefix object": promoted([]byte("hello")),
		"empty prefix object":  promoted(nil),
	} {
		t.Run(name, func(t *testing.T) {
			owner := &markerFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
				"/buckets/b":       {Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
				"/buckets/b/album": entry,
			}}}
			ownerAddr := startFakeFiler(t, owner)
			s3a := newPutTestServer(t, ownerAddr)
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
			s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
			s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

			r := httptest.NewRequest(http.MethodDelete, "/b/album/", nil)
			require.Equal(t, s3err.ErrNone, s3a.deleteDirectoryMarker(r, "b", "album/"))
			owner.mu.Lock()
			defer owner.mu.Unlock()
			require.Contains(t, owner.entries, "/buckets/b/album", "album/ removed the object album")
			require.Zero(t, owner.deletes)
		})
	}
}
