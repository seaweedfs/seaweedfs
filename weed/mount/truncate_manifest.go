package mount

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// maxTruncateManifestDepth bounds how far manifests in manifests are opened.
const maxTruncateManifestDepth = 4

// truncateManifestWait bounds all the manifest reads of one truncate, which
// hold the handle's locks.
const truncateManifestWait = time.Minute

// truncateManifestResolver reads the chunks of one manifest for a truncate.
// Tests replace it.
var truncateManifestResolver = func(ctx context.Context, wfs *WFS, manifest *filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
	return filer.ResolveOneChunkManifest(ctx, wfs.LookupFn(), manifest, wfs.CacheInvalidator())
}

// cutThroughManifests returns chunks with each manifest a cut at size goes
// through replaced by the chunks it holds (expandManifestsAt), and whether it
// replaced one. The manifest reads share one deadline. The chunks are first
// registered for filer-issued read tokens under path: a truncate by path, with
// no open handle, has not registered them, and a mount without a signing key
// reads a manifest only with such a token.
func (wfs *WFS) cutThroughManifests(ctx context.Context, path util.FullPath, chunks []*filer_pb.FileChunk, size int64) ([]*filer_pb.FileChunk, bool, error) {
	if !slices.ContainsFunc(chunks, func(chunk *filer_pb.FileChunk) bool { return cutGoesThrough(chunk, size) }) {
		return chunks, false, nil
	}
	wfs.readAuth.Track(path, chunks)
	ctx, cancel := context.WithTimeout(ctx, truncateManifestWait)
	defer cancel()
	return expandManifestsAt(chunks, size, func(manifest *filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
		return truncateManifestResolver(ctx, wfs, manifest)
	})
}

// expandManifestsAt returns chunks with every manifest that a cut at size goes
// through replaced by the chunks it holds, nested ones too. Cutting a manifest
// only shortens its span: reads resolve its chunks at their own sizes, so what
// lay past the cut would come back as data once the file grows over it again
// without writing it (a hole, an extending truncate). The chunks are cut
// instead. A manifest's chunks are first cut to its own span, since one
// shortened before this was in place still holds them whole. Manifests that
// lie wholly before or after the cut are left as they are. expanded reports
// whether a manifest was replaced.
func expandManifestsAt(chunks []*filer_pb.FileChunk, size int64, resolve func(*filer_pb.FileChunk) ([]*filer_pb.FileChunk, error)) (cut []*filer_pb.FileChunk, expanded bool, err error) {
	for i, chunk := range chunks {
		if !cutGoesThrough(chunk, size) {
			if cut != nil {
				cut = append(cut, chunk)
			}
			continue
		}
		if cut == nil {
			cut = append(make([]*filer_pb.FileChunk, 0, len(chunks)), chunks[:i]...)
		}
		held, err := expandManifestAt(chunk, size, resolve, 1)
		if err != nil {
			return nil, false, err
		}
		cut = append(cut, held...)
	}
	if cut == nil {
		return chunks, false, nil
	}
	return cut, true, nil
}

func expandManifestAt(manifest *filer_pb.FileChunk, size int64, resolve func(*filer_pb.FileChunk) ([]*filer_pb.FileChunk, error), depth int) ([]*filer_pb.FileChunk, error) {
	if depth > maxTruncateManifestDepth {
		return nil, fmt.Errorf("manifest %s: nested deeper than %d", manifest.GetFileIdString(), maxTruncateManifestDepth)
	}
	held, err := resolve(manifest)
	if err != nil {
		return nil, fmt.Errorf("manifest %s: %w", manifest.GetFileIdString(), err)
	}
	end := chunkSpanEnd(manifest)
	var chunks []*filer_pb.FileChunk
	for _, chunk := range held {
		if chunk.Offset >= end {
			continue
		}
		if chunkSpanEnd(chunk) > end {
			chunk = proto.Clone(chunk).(*filer_pb.FileChunk)
			chunk.Size = uint64(end - chunk.Offset)
		}
		if cutGoesThrough(chunk, size) {
			nested, err := expandManifestAt(chunk, size, resolve, depth+1)
			if err != nil {
				return nil, err
			}
			chunks = append(chunks, nested...)
			continue
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// cutGoesThrough reports whether chunk is a manifest a cut at size divides.
func cutGoesThrough(chunk *filer_pb.FileChunk, size int64) bool {
	return chunk.IsChunkManifest && chunk.Offset < size && chunkSpanEnd(chunk) > size
}

func chunkSpanEnd(chunk *filer_pb.FileChunk) int64 {
	return chunk.Offset + int64(chunk.Size)
}
