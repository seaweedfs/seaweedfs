package filer

import (
	"context"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/protobuf/proto"
)

// ReuseStoredManifests keeps the manifests of the entry a commit replaces when
// the commit lists their chunks flat. A client that keeps its own chunk list,
// such as the Hadoop client, sends every chunk it wrote on each flush. Once the
// filer has folded some of them into a manifest, the next commit lists them
// flat again; folding them anew would write the same manifest under a new file
// id and delete the stored one, on every commit.
//
// chunks are the commit's data chunks and listed the manifests it lists
// itself. A stored manifest that the commit does not list is kept when it
// spans exactly the chunks it holds and every one of them is among chunks,
// unchanged. Its chunks then leave the list. reused are the kept manifests,
// rest the chunks left over, in their order. A manifest that cannot be read
// is not kept.
func ReuseStoredManifests(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, stored, listed, chunks []*filer_pb.FileChunk) (reused, rest []*filer_pb.FileChunk) {
	// No manifest the filer folds holds fewer chunks.
	if len(chunks) < SmallChunkManifestBatch {
		return nil, chunks
	}
	isListed := make(map[string]bool, len(listed))
	for _, manifest := range listed {
		isListed[ChunkIdentity(manifest.GetFileIdString())] = true
	}
	var dropped []*filer_pb.FileChunk
	for _, manifest := range stored {
		if manifest.IsChunkManifest && !isListed[ChunkIdentity(manifest.GetFileIdString())] {
			dropped = append(dropped, manifest)
		}
	}
	if len(dropped) == 0 {
		return nil, chunks
	}

	starts := make(map[int64]bool, len(chunks))
	stops := make(map[int64]bool, len(chunks))
	position := make(map[string]int, len(chunks))
	stamped := make(map[int64][]int, len(chunks))
	for i, chunk := range chunks {
		starts[chunk.Offset] = true
		stops[chunk.Offset+int64(chunk.Size)] = true
		position[ChunkIdentity(chunk.GetFileIdString())] = i
		stamped[chunk.ModifiedTsNs] = append(stamped[chunk.ModifiedTsNs], i)
	}

	taken := make([]bool, len(chunks))
	for _, manifest := range dropped {
		// Only a manifest whose span the list starts and stops at can match.
		if !starts[manifest.Offset] || !stops[manifest.Offset+int64(manifest.Size)] {
			continue
		}
		held, err := ResolveOneChunkManifest(ctx, lookupFileIdFn, manifest, nil)
		if err != nil {
			glog.V(1).InfofCtx(ctx, "reuse manifest %s: %v", manifest.GetFileIdString(), err)
			continue
		}
		if positions, ok := heldUnchanged(manifest, held, chunks, position, stamped, taken); ok {
			for _, i := range positions {
				taken[i] = true
			}
			reused = append(reused, manifest)
		}
	}
	if len(reused) == 0 {
		return nil, chunks
	}

	rest = make([]*filer_pb.FileChunk, 0, len(chunks))
	for i, chunk := range chunks {
		if !taken[i] {
			rest = append(rest, chunk)
		}
	}
	return reused, rest
}

// heldUnchanged returns where in chunks the chunks manifest holds are, if each
// is there as the manifest holds it, in its order and not taken by another
// manifest, and the manifest spans exactly them. Order matters: where two
// chunks with the same ModifiedTsNs overlap, the later one wins.
func heldUnchanged(manifest *filer_pb.FileChunk, held, chunks []*filer_pb.FileChunk, position map[string]int, stamped map[int64][]int, taken []bool) ([]int, bool) {
	if len(held) == 0 {
		return nil, false
	}
	positions := make([]int, 0, len(held))
	start, stop := held[0].Offset, held[0].Offset+int64(held[0].Size)
	for _, chunk := range held {
		if chunk.IsChunkManifest {
			return nil, false
		}
		i, found := position[ChunkIdentity(chunk.GetFileIdString())]
		if !found || taken[i] || !sameChunk(chunks[i], chunk) {
			return nil, false
		}
		if len(positions) > 0 && i <= positions[len(positions)-1] {
			return nil, false
		}
		positions = append(positions, i)
		start = min(start, chunk.Offset)
		stop = max(stop, chunk.Offset+int64(chunk.Size))
	}
	if manifest.Offset != start || manifest.Offset+int64(manifest.Size) != stop {
		return nil, false
	}
	// Kept, the manifest's chunks are read before every other chunk, so none
	// may overlap one of them with the same ModifiedTsNs.
	for _, h := range held {
		for _, i := range stamped[h.ModifiedTsNs] {
			if _, own := slices.BinarySearch(positions, i); own {
				continue
			}
			if chunk := chunks[i]; chunk.Offset < h.Offset+int64(h.Size) && h.Offset < chunk.Offset+int64(chunk.Size) {
				return nil, false
			}
		}
	}
	return positions, true
}

// sameChunk reports whether a and b are the same chunk, whichever way each
// spells its file ids.
func sameChunk(a, b *filer_pb.FileChunk) bool {
	return proto.Equal(comparableChunk(a), comparableChunk(b))
}

func comparableChunk(chunk *filer_pb.FileChunk) *filer_pb.FileChunk {
	c := proto.Clone(chunk).(*filer_pb.FileChunk)
	filer_pb.AfterEntryDeserialization([]*filer_pb.FileChunk{c})
	c.FileId = ChunkIdentity(c.FileId)
	c.SourceFileId = ChunkIdentity(c.SourceFileId)
	c.Fid, c.SourceFid = nil, nil
	return c
}
