package meta_cache

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

func listResponses(n int) []*filer_pb.ListEntriesResponse {
	responses := make([]*filer_pb.ListEntriesResponse, 0, n)
	for i := 0; i < n; i++ {
		responses = append(responses, &filer_pb.ListEntriesResponse{
			Entry: &filer_pb.Entry{
				Name: fmt.Sprintf("f%05d", i),
				Attributes: &filer_pb.FuseAttributes{
					Crtime: 1, Mtime: 1, FileMode: 0o644, FileSize: 3,
				},
			},
		})
	}
	return responses
}

// TestEnsureVisitedRefusesOversizedDirectory checks that a directory past the
// limit is not cached, that the refusal is remembered, and that the partial
// build leaves nothing behind in the local store.
func TestEnsureVisitedRefusesOversizedDirectory(t *testing.T) {
	mc, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()

	accessor := &buildFilerAccessor{client: &buildListClient{responses: listResponses(10)}}

	err := EnsureVisited(mc, accessor, util.FullPath("/dir"), 5)
	var tooLarge *DirectoryTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("EnsureVisited = %v, want DirectoryTooLargeError", err)
	}
	if tooLarge.Path != util.FullPath("/dir") {
		t.Fatalf("oversized path = %s, want /dir", tooLarge.Path)
	}
	if mc.IsDirectoryCached(util.FullPath("/dir")) {
		t.Error("oversized directory reported cached")
	}
	// The aborted build must leave no partial children to be served later.
	count := 0
	if _, err := mc.ListDirectoryEntries(context.Background(), util.FullPath("/dir"), "", false, 100, func(e *filer.Entry) (bool, error) {
		count++
		return true, nil
	}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if count != 0 {
		t.Errorf("local store still holds %d children of the aborted build", count)
	}

	// A second visit fails fast without streaming to the limit again.
	if err := EnsureVisited(mc, accessor, util.FullPath("/dir"), 5); !errors.As(err, &tooLarge) {
		t.Fatalf("second EnsureVisited = %v, want DirectoryTooLargeError", err)
	}
}

// TestEnsureVisitedStepsOverOversizedAncestor checks a huge ancestor does not
// wedge the caching of its subdirectories.
func TestEnsureVisitedStepsOverOversizedAncestor(t *testing.T) {
	mc, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	mc.markOversized(util.FullPath("/huge"))

	accessor := &buildFilerAccessor{client: &buildListClient{responses: listResponses(3)}}
	if err := EnsureVisited(mc, accessor, util.FullPath("/huge/sub"), 5); err != nil {
		t.Fatalf("EnsureVisited under an oversized ancestor: %v", err)
	}
	if !mc.IsDirectoryCached(util.FullPath("/huge/sub")) {
		t.Error("subdirectory of an oversized ancestor was not cached")
	}
	if mc.IsDirectoryCached(util.FullPath("/huge")) {
		t.Error("oversized ancestor became cached")
	}
}

// TestEnsureVisitedUnderTheLimitStillCaches pins that the gate does not change
// behaviour for ordinary directories.
func TestEnsureVisitedUnderTheLimitStillCaches(t *testing.T) {
	mc, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()

	accessor := &buildFilerAccessor{client: &buildListClient{responses: listResponses(5)}}
	if err := EnsureVisited(mc, accessor, util.FullPath("/dir"), 5); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}
	if !mc.IsDirectoryCached(util.FullPath("/dir")) {
		t.Error("directory at the limit was not cached")
	}
}

// pathListClient serves canned listings per directory, so one visit can see
// directories of different sizes.
type pathListClient struct {
	filer_pb.SeaweedFilerClient
	perDir map[string][]*filer_pb.ListEntriesResponse
}

func (c *pathListClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	return &buildListStream{responses: c.perDir[in.Directory]}, nil
}

// TestEnsureVisitedAncestorFoundOversizedMidVisit covers the first discovery:
// the ancestor's refusal must neither cancel the descendant's build nor be
// reported as the descendant's own.
func TestEnsureVisitedAncestorFoundOversizedMidVisit(t *testing.T) {
	mc, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()

	accessor := &buildFilerAccessor{client: &pathListClient{perDir: map[string][]*filer_pb.ListEntriesResponse{
		"/huge":     listResponses(10),
		"/huge/sub": listResponses(3),
	}}}

	if err := EnsureVisited(mc, accessor, util.FullPath("/huge/sub"), 5); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}
	if !mc.IsDirectoryCached(util.FullPath("/huge/sub")) {
		t.Error("descendant of a just-discovered oversized ancestor was not cached")
	}
	if mc.IsDirectoryCached(util.FullPath("/huge")) {
		t.Error("oversized ancestor became cached")
	}
	if !mc.isOversized(util.FullPath("/huge")) {
		t.Error("oversized ancestor was not remembered")
	}
}
