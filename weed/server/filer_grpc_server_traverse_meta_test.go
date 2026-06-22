package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/viant/ptrie"
	"google.golang.org/grpc"
)

func TestPtrie(t *testing.T) {
	b := []byte("/topics/abc/dev")
	excludedTrie := ptrie.New[bool]()
	excludedTrie.Put([]byte("/topics/abc/d"), true)
	excludedTrie.Put([]byte("/topics/abc"), true)

	assert.True(t, excludedTrie.MatchPrefix(b, func(key []byte, value bool) bool {
		println("matched1", string(key))
		return true
	}))

	assert.True(t, excludedTrie.MatchAll(b, func(key []byte, value bool) bool {
		println("matched2", string(key))
		return true
	}))

	assert.False(t, excludedTrie.MatchAll([]byte("/topics/ab"), func(key []byte, value bool) bool {
		println("matched3", string(key))
		return true
	}))

	assert.False(t, excludedTrie.Has(b))
}

type captureTraverseStream struct {
	grpc.ServerStream
	ctx     context.Context
	visited []string
}

func (s *captureTraverseStream) Context() context.Context { return s.ctx }

func (s *captureTraverseStream) Send(resp *filer_pb.TraverseBfsMetadataResponse) error {
	dir := resp.Directory
	if dir != "/" {
		dir += "/"
	}
	s.visited = append(s.visited, dir+resp.Entry.Name)
	return nil
}

func TestTraverseBfsMetadata(t *testing.T) {
	store := newRenameTestStore()
	// "/.system-data" shares the "/.system" byte prefix but is a different
	// path component, so it must not be excluded.
	for _, dir := range []string{"/", "/a", "/a/sub", "/b", "/.system", "/.system-data"} {
		store.entries[dir] = newDirectoryEntry(dir, 1)
	}
	for _, file := range []string{"/a/f1", "/a/sub/f2", "/b/f3", "/.system/secret", "/.system-data/keep"} {
		store.entries[file] = newFileEntry(file, 2)
	}

	server := &FilerServer{filer: newRenameTestFiler(store)}
	stream := &captureTraverseStream{ctx: context.Background()}

	err := server.TraverseBfsMetadata(&filer_pb.TraverseBfsMetadataRequest{
		Directory:        "/",
		ExcludedPrefixes: []string{"/.system"},
	}, stream)
	assert.NoError(t, err)

	// The excluded subtree is skipped; everything else (including the
	// /.system-data sibling) is visited exactly once.
	assert.ElementsMatch(t, []string{
		"/", "/a", "/a/f1", "/a/sub", "/a/sub/f2", "/b", "/b/f3",
		"/.system-data", "/.system-data/keep",
	}, stream.visited)

	// Each entry's parent must be streamed before the entry itself, so a
	// consumer can apply the tree top-down.
	seen := make(map[string]bool)
	for _, path := range stream.visited {
		if path != "/" {
			parent, _ := util.FullPath(path).DirAndName()
			assert.Truef(t, seen[parent], "parent %s of %s not streamed first", parent, path)
		}
		seen[path] = true
	}
}

var _ filer_pb.SeaweedFiler_TraverseBfsMetadataServer = (*captureTraverseStream)(nil)
