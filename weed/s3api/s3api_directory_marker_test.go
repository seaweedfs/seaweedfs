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
