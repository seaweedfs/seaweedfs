//go:build !freebsd

package mount

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

	got, expanded, err := expandManifestsAt(chunks, 1500, fixture.resolve)
	require.NoError(t, err)
	require.True(t, expanded)
	require.Equal(t, [][3]int64{{0, 1024, 1}, {1024, 256, 0}, {1280, 256, 0}, {1536, 256, 0}, {1792, 256, 0}, {2048, 1024, 1}, {3072, 100, 0}}, spans(got))

	same, expanded, err := expandManifestsAt(chunks, 1024, fixture.resolve)
	require.NoError(t, err)
	require.False(t, expanded)
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
	got, _, err := expandManifestsAt([]*filer_pb.FileChunk{manifestChunk(12, 0, 1600)}, 1000, fixture.resolve)
	require.NoError(t, err)
	require.Equal(t, [][3]int64{{0, 800, 1}, {800, 400, 0}, {1200, 400, 0}}, spans(got))

	// shortened to 1000 by an older truncate: chunk 4 is past it, chunk 3 ends at it
	got, _, err = expandManifestsAt([]*filer_pb.FileChunk{manifestChunk(11, 800, 200)}, 900, fixture.resolve)
	require.NoError(t, err)
	require.Equal(t, [][3]int64{{800, 200, 0}}, spans(got))
	require.Equal(t, uint64(400), fixture[11][0].Size, "the resolved chunks are not changed in place")
}

func TestExpandManifestsAtFailsWhenAManifestCannotBeRead(t *testing.T) {
	_, _, err := expandManifestsAt([]*filer_pb.FileChunk{manifestChunk(5, 0, 1000)}, 500, manifestFixture{}.resolve)
	require.Error(t, err)

	loop := manifestFixture{6: {manifestChunk(6, 0, 1000)}}
	_, _, err = expandManifestsAt([]*filer_pb.FileChunk{manifestChunk(6, 0, 1000)}, 500, loop.resolve)
	require.ErrorContains(t, err, "nested deeper")
}

// manifestFileEntry is a 1200-byte file whose four 300-byte chunks (keys 1-4)
// sit in one manifest (key 9 on volume 9).
func manifestFileEntry() (*filer_pb.Entry, manifestFixture) {
	var held []*filer_pb.FileChunk
	for i := 0; i < 4; i++ {
		held = append(held, dataChunk(uint64(i+1), int64(i)*300, 300))
	}
	entry := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{Inode: 42, FileMode: 0644, Uid: 2000, Gid: 20, FileSize: 1200},
		Chunks: []*filer_pb.FileChunk{manifestChunk(1, 0, 1200)}}
	return entry, manifestFixture{1: held}
}

// A truncate through a manifest, with and without an open handle: the
// manifest's chunks are cut, its file id is registered for read tokens before
// it is read, all reads run under one deadline, and an open handle is marked
// as having rewritten its chunks.
func TestTruncateThroughManifestCutsItsChunks(t *testing.T) {
	for _, open := range []bool{false, true} {
		t.Run(fmt.Sprintf("open=%t", open), func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			wfs.readAuth = newReadAuth(wfs)
			current, fixture := manifestFileEntry()
			fake := &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
			startFakeFiler(t, wfs, fake)
			path := util.FullPath("/dir/file")
			wfs.inodeToPath.Lookup(util.FullPath("/dir"), 1, true, false, 0, false)
			wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
			wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
			require.NoError(t, wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", current), 1))

			manifestFid := current.Chunks[0].GetFileIdString()
			saved := truncateManifestResolver
			t.Cleanup(func() { truncateManifestResolver = saved })
			var reads int
			truncateManifestResolver = func(ctx context.Context, wfs *WFS, manifest *filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
				reads++
				deadline, ok := ctx.Deadline()
				require.True(t, ok, "manifest read without a deadline")
				require.LessOrEqual(t, time.Until(deadline), truncateManifestWait)
				wfs.readAuth.mu.Lock()
				tracked := wfs.readAuth.paths[manifestFid]
				wfs.readAuth.mu.Unlock()
				require.Equal(t, path, tracked, "manifest read before its id was registered for a read token")
				return fixture.resolve(manifest)
			}

			var fh *FileHandle
			var base *filer_pb.Entry
			if open {
				local := proto.Clone(current).(*filer_pb.Entry)
				wfs.mapPbIdFromFilerToLocal(local)
				fh = newFileHandle(wfs, 1, 42, local)
				base = proto.Clone(local).(*filer_pb.Entry)
				fh.baseEntry.Store(base)
				fh.RememberPath(path)
				wfs.fhMap.inode2fh[42], wfs.fhMap.fh2inode[1] = fh, 42
				t.Cleanup(func() { fh.dirtyPages.Destroy() })
			}

			in := &fuse.SetAttrIn{}
			in.NodeId, in.Valid, in.Size = 42, fuse.FATTR_SIZE, 450
			require.Equal(t, fuse.OK, wfs.SetAttr(nil, in, &fuse.AttrOut{}))
			require.Equal(t, 1, reads)

			want := [][3]int64{{0, 300, 0}, {300, 150, 0}}
			if open {
				require.Equal(t, want, spans(fh.GetEntry().GetEntry().Chunks))
				mark := fh.markSince(base)
				require.True(t, mark.rewrote, "the expanded chunks must not be merged over a peer's chunk change")
				require.Equal(t, int64(450), mark.truncatedTo)
				return
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			require.Equal(t, want, spans(fake.entry.Chunks))
			require.Equal(t, uint64(450), fake.entry.Attributes.FileSize)
		})
	}
}

// Mount A cut a manifest's chunks to 200 bytes and has not published; mount B
// cut the file to 100 bytes and did, and the filer queued the rest for
// deletion. A's merge must not publish its chunks past B's cut: they are not
// writes of A's. The rewrite mark set when the truncate expanded the manifest
// refuses the merge, as a manifest across the cut did before.
func TestMergeRefusesTruncateExpansionOverPeerTruncate(t *testing.T) {
	var held []*filer_pb.FileChunk
	for i := 0; i < 4; i++ {
		held = append(held, dataChunk(uint64(i+1), int64(i)*100, 100))
	}
	attrs := &filer_pb.FuseAttributes{Inode: 42, FileMode: 0644, FileSize: 400}
	base := &filer_pb.Entry{Name: "file", Attributes: attrs, Chunks: []*filer_pb.FileChunk{manifestChunk(1, 0, 400)}}

	local := proto.Clone(base).(*filer_pb.Entry)
	local.Attributes.FileSize = 200
	cut, expanded, err := expandManifestsAt(base.Chunks, 200, manifestFixture{1: held}.resolve)
	require.NoError(t, err)
	require.True(t, expanded)
	local.Chunks = cut[:2] // the chunks past 200 bytes are dropped by the truncate

	stored := proto.Clone(base).(*filer_pb.Entry)
	stored.Attributes.FileSize = 100
	stored.Chunks = []*filer_pb.FileChunk{proto.Clone(held[0]).(*filer_pb.FileChunk)}

	_, ok := mergeContentEntry(stored, base, local, local, 200, true)
	require.False(t, ok, "a truncate that expanded a manifest merged over a peer's truncate")

	// without the mark, the merge publishes chunk 2, which B's truncate dropped
	merged, ok := mergeContentEntry(stored, base, local, local, 200, false)
	require.True(t, ok)
	require.Contains(t, spans(merged.Chunks), [3]int64{100, 100, 0})
}

// An interrupt while the truncate reads a manifest answers EINTR, which the
// caller retries, not EIO.
func TestTruncateInterruptedReadingAManifestAnswersEINTR(t *testing.T) {
	w := newInvalidateTestWFS(t)
	entry := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{Inode: 42, FileMode: 0644, FileSize: 1000},
		Chunks:     []*filer_pb.FileChunk{manifestChunk(7, 0, 1000)},
	}
	w.inodeToPath.Lookup("/dir", 1, true, false, 0, false)
	w.inodeToPath.Lookup("/dir/file", 1, false, false, 42, true)
	w.inodeToPath.MarkChildrenCached("/dir")
	require.NoError(t, w.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", entry), 1))

	cancel := make(chan struct{})
	saved := truncateManifestResolver
	t.Cleanup(func() { truncateManifestResolver = saved })
	truncateManifestResolver = func(ctx context.Context, _ *WFS, _ *filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
		close(cancel)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	in := &fuse.SetAttrIn{}
	in.NodeId, in.Valid, in.Size = 42, fuse.FATTR_SIZE, 500
	require.Equal(t, fuse.EINTR, w.SetAttr(cancel, in, &fuse.AttrOut{}))
}
