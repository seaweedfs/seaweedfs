package weed_server

import (
	"context"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

// ScanEntries makes the rename test store an EntryScanner: every entry at and
// below dirPath, straight from the map, like a store reading one snapshot.
func (s *renameTestStore) ScanEntries(ctx context.Context, dirPath util.FullPath, fn func(ctx context.Context, entry *filer.Entry) error) error {
	s.mu.Lock()
	var paths []string
	for p := range s.entries {
		if p == "/" {
			continue
		}
		if dirPath == "/" || p == string(dirPath) || len(p) > len(dirPath) && p[:len(dirPath)+1] == string(dirPath)+"/" {
			paths = append(paths, p)
		}
	}
	s.mu.Unlock()
	// Deliberately not top-down: children before their parents.
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	for _, p := range paths {
		s.mu.Lock()
		e := s.entries[p]
		s.mu.Unlock()
		if err := fn(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

var _ filer.EntryScanner = (*renameTestStore)(nil)

// headerTraverseStream records the response header along with the entries.
type headerTraverseStream struct {
	captureTraverseStream
	headers []metadata.MD
}

func (s *headerTraverseStream) SendHeader(md metadata.MD) error {
	s.headers = append(s.headers, md)
	return nil
}

func snapshotRequestCtx() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(filer_pb.TraverseModeHeaderKey, filer_pb.TraverseModeSnapshot))
}

func newTraverseTestStore() *renameTestStore {
	store := newRenameTestStore()
	for _, dir := range []string{"/", "/a", "/a/sub", "/b", "/.system"} {
		store.entries[dir] = newDirectoryEntry(dir, 1)
	}
	for _, file := range []string{"/a/f1", "/a/sub/f2", "/b/f3", "/.system/secret"} {
		store.entries[file] = newFileEntry(file, 2)
	}
	return store
}

func TestTraverseBfsMetadataSnapshotMode(t *testing.T) {
	store := newTraverseTestStore()
	server := &FilerServer{filer: newRenameTestFiler(t, store)}
	stream := &headerTraverseStream{captureTraverseStream: captureTraverseStream{ctx: snapshotRequestCtx()}}

	err := server.TraverseBfsMetadata(&filer_pb.TraverseBfsMetadataRequest{
		Directory:        "/",
		ExcludedPrefixes: []string{"/.system"},
	}, stream)
	assert.NoError(t, err)

	if assert.Len(t, stream.headers, 1) {
		assert.Equal(t, []string{filer_pb.TraverseModeSnapshot}, stream.headers[0].Get(filer_pb.TraverseModeHeaderKey))
	}
	// The root comes first so a consumer knows what it asked for; everything
	// else is complete but in the store's order, with the exclusion honored.
	assert.Equal(t, "/", stream.visited[0])
	assert.ElementsMatch(t, []string{"/", "/a", "/a/f1", "/a/sub", "/a/sub/f2", "/b", "/b/f3"}, stream.visited)

	// No per-entry commit timestamp lookup in snapshot mode: entry_ts_ns is
	// left "unrecorded" (0), which consumers already treat as "use the mtime".
	for path, tsNs := range stream.entryTsNs {
		assert.Zerof(t, tsNs, "entry_ts_ns for %s", path)
	}
}

// Without the request metadata the walk is unchanged: no header, parents first.
func TestTraverseBfsMetadataDefaultStillWalks(t *testing.T) {
	store := newTraverseTestStore()
	server := &FilerServer{filer: newRenameTestFiler(t, store)}
	stream := &headerTraverseStream{captureTraverseStream: captureTraverseStream{ctx: context.Background()}}

	err := server.TraverseBfsMetadata(&filer_pb.TraverseBfsMetadataRequest{Directory: "/"}, stream)
	assert.NoError(t, err)
	assert.Empty(t, stream.headers)
	seen := make(map[string]bool)
	for _, path := range stream.visited {
		if path != "/" {
			parent, _ := util.FullPath(path).DirAndName()
			assert.Truef(t, seen[parent], "parent %s of %s not streamed first", parent, path)
		}
		seen[path] = true
	}
}

// A store that cannot scan (here: path-specific stores are configured, so one
// snapshot per store is not one snapshot) makes the filer walk and say so.
func TestTraverseBfsMetadataSnapshotRequestedButUnsupported(t *testing.T) {
	store := newTraverseTestStore()
	f := newRenameTestFiler(t, store)
	f.Store.AddPathSpecificStore("/elsewhere", "elsewhere", newRenameTestStore())
	server := &FilerServer{filer: f}
	stream := &headerTraverseStream{captureTraverseStream: captureTraverseStream{ctx: snapshotRequestCtx()}}

	err := server.TraverseBfsMetadata(&filer_pb.TraverseBfsMetadataRequest{Directory: "/a"}, stream)
	assert.NoError(t, err)
	if assert.Len(t, stream.headers, 1) {
		assert.Equal(t, []string{filer_pb.TraverseModeBfs}, stream.headers[0].Get(filer_pb.TraverseModeHeaderKey))
	}
	assert.Equal(t, []string{"/a", "/a/f1", "/a/sub", "/a/sub/f2"}, stream.visited)
}

type fakeScanner struct {
	entries []*filer.Entry
	err     error // returned after the entries
}

func (s *fakeScanner) ScanEntries(ctx context.Context, dirPath util.FullPath, fn func(ctx context.Context, entry *filer.Entry) error) error {
	for _, e := range s.entries {
		if err := fn(ctx, e); err != nil {
			return err
		}
	}
	return s.err
}

type traverseRecorder struct {
	headers []metadata.MD
	sent    []string
}

func (r *traverseRecorder) sendHeader(md metadata.MD) error {
	r.headers = append(r.headers, md)
	return nil
}
func (r *traverseRecorder) sendEntry(e *filer.Entry) error {
	r.sent = append(r.sent, string(e.FullPath))
	return nil
}

func noExclusion(util.FullPath) bool { return false }

// A store that only decides it cannot scan after yielding rows must not make
// the stream fall back to a walk: entries are already out under the snapshot header.
func TestTraverseFromSnapshotLateUnsupportedIsAnError(t *testing.T) {
	rec := &traverseRecorder{}
	scanner := &fakeScanner{entries: []*filer.Entry{newFileEntry("/ckpt/x", 2)}, err: filer.ErrUnsupportedEntryScan}
	served, err := traverseFromSnapshot(context.Background(), scanner, newDirectoryEntry("/ckpt", 1), noExclusion, rec.sendHeader, rec.sendEntry)
	assert.Error(t, err)
	assert.True(t, served)
}
