package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
)

// chunkFold is what one flush's MaybeManifestize packed into manifests: the
// manifests it saved and the chunks they hold. Until the filer acknowledges
// the flush, those manifests only wrap chunks the handle already had. A merge
// onto a peer's entry must see the chunks instead: it treats a chunk that is
// not in the base as the handle's own write and a base chunk missing from the
// handle as one it dropped, so a manifest of base chunks would put back chunks
// the peer dropped (a truncate, or a rewrite of the whole file), and a peer
// that folded the same chunks would end up with both manifests.
type chunkFold struct {
	manifests map[string]bool // contentChunkKey of each manifest the fold saved
	members   []*filer_pb.FileChunk
}

// newChunkFold compares the chunks a flush passed to MaybeManifestize with
// the chunks it got back. nil when nothing was folded.
func newChunkFold(before, after []*filer_pb.FileChunk) *chunkFold {
	inBefore := make(map[string]bool, len(before))
	for _, c := range before {
		inBefore[contentChunkKey(c)] = true
	}
	fold := &chunkFold{manifests: make(map[string]bool)}
	inAfter := make(map[string]bool, len(after))
	for _, c := range after {
		key := contentChunkKey(c)
		inAfter[key] = true
		if c.IsChunkManifest && !inBefore[key] {
			fold.manifests[key] = true
		}
	}
	if len(fold.manifests) == 0 {
		return nil
	}
	for _, c := range before {
		if !inAfter[contentChunkKey(c)] {
			fold.members = append(fold.members, c)
		}
	}
	return fold
}

// unfoldChunks returns chunks with the fold's manifests replaced by the chunks
// they hold. A list that lacks any of those manifests is returned as it is:
// it was not built from this fold.
func (f *chunkFold) unfoldChunks(chunks []*filer_pb.FileChunk) []*filer_pb.FileChunk {
	if f == nil {
		return chunks
	}
	unfolded := make([]*filer_pb.FileChunk, 0, len(chunks)-len(f.manifests)+len(f.members))
	found := 0
	for _, c := range chunks {
		if c.IsChunkManifest && f.manifests[contentChunkKey(c)] {
			found++
			continue
		}
		unfolded = append(unfolded, c)
	}
	if found != len(f.manifests) {
		return chunks
	}
	return append(unfolded, f.members...)
}

// unfold returns a copy of entry with the fold undone (see unfoldChunks).
func (f *chunkFold) unfold(entry *filer_pb.Entry) *filer_pb.Entry {
	if f == nil || entry == nil {
		return entry
	}
	unfolded := proto.Clone(entry).(*filer_pb.Entry)
	unfolded.Chunks = f.unfoldChunks(entry.Chunks)
	return unfolded
}

// revert puts the folded chunks back into the handle after the flush failed:
// the handle's base still has them, and a later merge or flush must not take
// the manifests for writes of its own.
func (f *chunkFold) revert(fh *FileHandle) {
	if f == nil {
		return
	}
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = f.unfoldChunks(e.Chunks)
	})
}
