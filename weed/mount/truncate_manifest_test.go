package mount

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// manifestFixture keeps what each fake manifest holds, keyed by its file key.
type manifestFixture map[uint64][]*filer_pb.FileChunk

func (m manifestFixture) resolve(manifest *filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
	held, ok := m[manifest.Fid.FileKey]
	if !ok {
		return nil, errors.New("no such manifest")
	}
	return held, nil
}

func dataChunk(key uint64, offset int64, size uint64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: key, Cookie: 1}, Offset: offset, Size: size, ModifiedTsNs: int64(key)}
}

func manifestChunk(key uint64, offset int64, size uint64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 9, FileKey: key, Cookie: 1}, Offset: offset, Size: size, IsChunkManifest: true}
}

func spans(chunks []*filer_pb.FileChunk) [][3]int64 {
	var s [][3]int64
	for _, c := range chunks {
		manifest := int64(0)
		if c.IsChunkManifest {
			manifest = 1
		}
		s = append(s, [3]int64{c.Offset, int64(c.Size), manifest})
	}
	return s
}

// A cut through a manifest yields its chunks; manifests before and after the
// cut, and data chunks, stay as they are.
func TestExpandManifestsAtOpensOnlyTheManifestTheCutDivides(t *testing.T) {
	fixture := manifestFixture{}
	var first, second []*filer_pb.FileChunk
	for i := 0; i < 4; i++ {
		first = append(first, dataChunk(uint64(100+i), int64(i)*256, 256))
		second = append(second, dataChunk(uint64(200+i), 1024+int64(i)*256, 256))
	}
	fixture[1] = first
	fixture[2] = second
	before := manifestChunk(1, 0, 1024)
	divided := manifestChunk(2, 1024, 1024)
	after := manifestChunk(3, 2048, 1024) // not in the fixture: must not be read
	tail := dataChunk(300, 3072, 100)
	chunks := []*filer_pb.FileChunk{before, divided, after, tail}

	got, err := expandManifestsAt(chunks, 1500, fixture.resolve)
	require.NoError(t, err)
	require.Equal(t, [][3]int64{{0, 1024, 1}, {1024, 256, 0}, {1280, 256, 0}, {1536, 256, 0}, {1792, 256, 0}, {2048, 1024, 1}, {3072, 100, 0}}, spans(got))

	same, err := expandManifestsAt(chunks, 1024, fixture.resolve)
	require.NoError(t, err)
	require.Equal(t, chunks, same, "a cut at a manifest's edge opens nothing")
}

// Nested manifests the cut divides are opened too; a manifest shortened
// earlier gives up the chunks past its span and cuts the one it ends in.
func TestExpandManifestsAtOpensNestedAndShortenedManifests(t *testing.T) {
	fixture := manifestFixture{
		10: {dataChunk(1, 0, 400), dataChunk(2, 400, 400)},
		11: {dataChunk(3, 800, 400), dataChunk(4, 1200, 400)},
		12: {manifestChunk(10, 0, 800), manifestChunk(11, 800, 800)},
	}
	got, err := expandManifestsAt([]*filer_pb.FileChunk{manifestChunk(12, 0, 1600)}, 1000, fixture.resolve)
	require.NoError(t, err)
	require.Equal(t, [][3]int64{{0, 800, 1}, {800, 400, 0}, {1200, 400, 0}}, spans(got))

	// shortened to 1000 by an older truncate: chunk 4 is past it, chunk 3 ends at it
	got, err = expandManifestsAt([]*filer_pb.FileChunk{manifestChunk(11, 800, 200)}, 900, fixture.resolve)
	require.NoError(t, err)
	require.Equal(t, [][3]int64{{800, 200, 0}}, spans(got))
	require.Equal(t, uint64(400), fixture[11][0].Size, "the resolved chunks are not changed in place")
}

func TestExpandManifestsAtFailsWhenAManifestCannotBeRead(t *testing.T) {
	_, err := expandManifestsAt([]*filer_pb.FileChunk{manifestChunk(5, 0, 1000)}, 500, manifestFixture{}.resolve)
	require.Error(t, err)

	loop := manifestFixture{6: {manifestChunk(6, 0, 1000)}}
	_, err = expandManifestsAt([]*filer_pb.FileChunk{manifestChunk(6, 0, 1000)}, 500, loop.resolve)
	require.ErrorContains(t, err, "nested deeper")
}
