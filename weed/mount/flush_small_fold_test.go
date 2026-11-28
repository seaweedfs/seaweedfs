package mount

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// fakeManifestSaver stands in for wfs.saveDataAsChunk when a test folds chunks:
// each saved manifest gets a fresh file id.
type fakeManifestSaver struct{ saved uint64 }

func (s *fakeManifestSaver) save(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (*filer_pb.FileChunk, error) {
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return nil, err
	}
	s.saved++
	return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 9, FileKey: s.saved, Cookie: 1}, ModifiedTsNs: tsNs}, nil
}

// A writer that appends records and rewrites a header at the front after each
// one, as formats with an index at the start of the file do. Each flush
// compacts, checks for bloat, then folds, as flushMetadataToFiler does. A
// header chunk folded into a manifest is never compacted away, and a manifest
// over the header and the appends spans the whole file, so folding them
// together makes the file look bloated after a few folds and the flush reads
// the whole file back. Only the appends may fold.
func TestSmallChunkFoldLeavesHeaderRewritesOutOfBloatCheck(t *testing.T) {
	const header = 4096
	saver := &fakeManifestSaver{}
	var key uint64
	next := func(offset int64, size uint64) *filer_pb.FileChunk {
		key++
		return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: key, Cookie: 1}, Offset: offset, Size: size, ModifiedTsNs: int64(key)}
	}

	chunks := []*filer_pb.FileChunk{next(0, header)}
	end := int64(header)
	for i := 0; i < 4*filer.SmallChunkManifestBatch; i++ {
		chunks = append(chunks, next(end, 300), next(0, header))
		end += 300

		manifests, flat := filer.SeparateManifestChunks(chunks)
		compacted, _, err := filer.CompactFileChunks(context.Background(), nil, flat)
		require.NoError(t, err)
		total, size, bloated := shouldMergeChunks(compacted, manifests)
		require.False(t, bloated, "flush %d: %d stored for %d bytes", i, total, size)
		folded, err := filer.MaybeManifestize(saver.save, nil, compacted)
		require.NoError(t, err)
		chunks = append(folded, manifests...)
	}

	manifests, flat := filer.SeparateManifestChunks(chunks)
	require.Len(t, manifests, 4, "the appends fold")
	require.Less(t, len(flat), filer.SmallChunkManifestBatch+1)
	require.Equal(t, uint64(end), filer.TotalSize(chunks))
}
