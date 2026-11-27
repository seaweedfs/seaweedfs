package filer

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// shortenedManifestFixture serves a manifest of three 100-byte chunks whose
// chunk in the entry an older truncate shortened to 150 bytes.
func shortenedManifestFixture(t *testing.T) (*manifestReadFixture, *filer_pb.FileChunk) {
	fixture := newManifestReadFixture(t, map[string][]*filer_pb.FileChunk{
		"short": {
			{FileId: "1,01", Offset: 0, Size: 100, ModifiedTsNs: 1},
			{FileId: "1,02", Offset: 100, Size: 100, ModifiedTsNs: 2},
			{FileId: "1,03", Offset: 200, Size: 100, ModifiedTsNs: 3},
		},
	}, nil)
	return fixture, &filer_pb.FileChunk{FileId: "short", IsChunkManifest: true, Offset: 0, Size: 150}
}

func chunkExtents(chunks []*filer_pb.FileChunk) map[string][2]int64 {
	extents := make(map[string][2]int64, len(chunks))
	for _, c := range chunks {
		extents[c.GetFileIdString()] = [2]int64{c.Offset, c.Offset + int64(c.Size)}
	}
	return extents
}

// Reads see the file as the truncate left it; deletions, which resolve with
// ResolveChunkManifest, still see every chunk the manifest references.
func TestResolveChunkManifestForReadCutsAShortenedManifest(t *testing.T) {
	fixture, shortened := shortenedManifestFixture(t)
	ctx := context.Background()

	data, manifests, err := ResolveChunkManifestForRead(ctx, fixture.lookup, []*filer_pb.FileChunk{shortened}, 0, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Len(t, manifests, 1)
	require.Equal(t, map[string][2]int64{"1,01": {0, 100}, "1,02": {100, 150}}, chunkExtents(data))

	all, _, err := ResolveChunkManifest(ctx, fixture.lookup, []*filer_pb.FileChunk{shortened}, 0, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Equal(t, map[string][2]int64{"1,01": {0, 100}, "1,02": {100, 200}, "1,03": {200, 300}}, chunkExtents(all))

	visibles, err := NonOverlappingVisibleIntervals(ctx, fixture.lookup, []*filer_pb.FileChunk{shortened}, 0, math.MaxInt64)
	require.NoError(t, err)
	var end int64
	for v := visibles.Front(); v != nil; v = v.Next {
		end = max(end, v.StopOffset)
	}
	require.Equal(t, int64(150), end, "nothing past the shortened manifest is visible")
}

// A mount reads through a ChunkGroup, which resolves manifests on its own.
func TestChunkGroupCutsAShortenedManifest(t *testing.T) {
	fixture, shortened := shortenedManifestFixture(t)
	group, err := NewChunkGroup(fixture.lookup, nil, []*filer_pb.FileChunk{shortened}, 1, nil, nil)
	require.NoError(t, err)

	held := make(map[string][2]int64)
	for _, section := range group.sections {
		for id, extent := range chunkExtents(section.chunks) {
			held[id] = extent
		}
	}
	require.Equal(t, map[string][2]int64{"1,01": {0, 100}, "1,02": {100, 150}}, held)
}

// A manifest that spans its chunks, as every fold makes one, keeps them as
// they are, slice and all.
func TestCutToManifestSpanLeavesAWholeManifestAlone(t *testing.T) {
	held := []*filer_pb.FileChunk{
		{FileId: "1,01", Offset: 0, Size: 100},
		{FileId: "1,02", Offset: 100, Size: 100},
	}
	whole := &filer_pb.FileChunk{FileId: "m", IsChunkManifest: true, Offset: 0, Size: 200}
	cut := cutToManifestSpan(whole, held)
	require.Equal(t, held, cut)
	require.Same(t, &held[0], &cut[0])

	short := &filer_pb.FileChunk{FileId: "m", IsChunkManifest: true, Offset: 0, Size: 150}
	cut = cutToManifestSpan(short, held)
	require.Equal(t, map[string][2]int64{"1,01": {0, 100}, "1,02": {100, 150}}, chunkExtents(cut))
	require.Equal(t, uint64(100), held[1].Size, "the resolved chunk is not changed in place")
}
