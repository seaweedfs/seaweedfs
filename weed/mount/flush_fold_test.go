package mount

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// foldMergeSaver stands in for wfs.saveDataAsChunk when a test folds chunks:
// each saved manifest gets a fresh file id.
type foldMergeSaver struct{ saved uint64 }

func (s *foldMergeSaver) save(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (*filer_pb.FileChunk, error) {
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return nil, err
	}
	s.saved++
	return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 9, FileKey: s.saved, Cookie: 1}, ModifiedTsNs: tsNs}, nil
}

// mergeFoldFile is a file of n small appended chunks (base) whose handle
// appended one more chunk and folded on flush, as flushMetadataToFiler does
// (local, fold).
func mergeFoldFile(t *testing.T, n int) (base, local *filer_pb.Entry, fold *chunkFold) {
	t.Helper()
	var chunks []*filer_pb.FileChunk
	for i := 0; i < n; i++ {
		chunks = append(chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(i + 1), Cookie: 1}, Offset: int64(i) * 300, Size: 300, ModifiedTsNs: int64(i + 1)})
	}
	attrs := &filer_pb.FuseAttributes{Inode: 7, FileSize: uint64(n) * 300, FileMode: 0644}
	base = &filer_pb.Entry{Name: "progress.jsonl", Attributes: attrs, Chunks: chunks}

	appended := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(n + 1), Cookie: 1}, Offset: int64(n) * 300, Size: 300, ModifiedTsNs: int64(n + 1)}
	before := append(append([]*filer_pb.FileChunk(nil), chunks...), appended)
	saver := &foldMergeSaver{}
	after, err := filer.MaybeManifestize(saver.save, nil, before)
	require.NoError(t, err)
	fold = newChunkFold(before, after)
	require.NotNil(t, fold)
	localAttrs := proto.Clone(attrs).(*filer_pb.FuseAttributes)
	localAttrs.FileSize = uint64(n+1) * 300
	local = &filer_pb.Entry{Name: "progress.jsonl", Attributes: localAttrs, Chunks: after}
	return base, local, fold
}

func referenceCounts(chunks []*filer_pb.FileChunk) map[string]int {
	keys := make(map[string]int, len(chunks))
	for _, c := range chunks {
		keys[contentChunkKey(c)]++
	}
	return keys
}

func TestChunkFoldUnfoldsToTheChunksItHolds(t *testing.T) {
	base, local, fold := mergeFoldFile(t, filer.SmallChunkManifestBatch+44)
	manifests, _ := filer.SeparateManifestChunks(local.Chunks)
	require.Len(t, manifests, 1)
	require.Len(t, fold.members, filer.SmallChunkManifestBatch)

	unfolded := fold.unfoldChunks(local.Chunks)
	want := referenceCounts(base.Chunks)
	got := referenceCounts(unfolded)
	require.Len(t, got, len(want)+1, "the base chunks and the appended one")
	for key := range want {
		require.Equal(t, 1, got[key], "chunk %s", key)
	}

	// a list that is missing one of the fold's manifests is left alone
	require.Equal(t, base.Chunks, fold.unfoldChunks(base.Chunks))
	var nilFold *chunkFold
	require.Equal(t, local.Chunks, nilFold.unfoldChunks(local.Chunks))
	require.Nil(t, newChunkFold(base.Chunks, base.Chunks))
}

// A peer that folded the same chunks: the merge keeps the peer's manifest and
// does not add the handle's, so each chunk is referenced once.
func TestMergeAfterFoldKeepsThePeersFoldOnly(t *testing.T) {
	base, local, fold := mergeFoldFile(t, filer.SmallChunkManifestBatch+44)
	peerSaver := &foldMergeSaver{saved: 1000}
	peerChunks, err := filer.MaybeManifestize(peerSaver.save, nil, append([]*filer_pb.FileChunk(nil), base.Chunks...))
	require.NoError(t, err)
	peerManifests, _ := filer.SeparateManifestChunks(peerChunks)
	require.Len(t, peerManifests, 1)
	stored := proto.Clone(base).(*filer_pb.Entry)
	stored.Chunks = peerChunks

	merged, ok := mergeContentEntry(stored, base, fold.unfold(local), fold.unfold(local), -1, false)
	require.True(t, ok)
	manifests, _ := filer.SeparateManifestChunks(merged.Chunks)
	require.Len(t, manifests, 1)
	require.Equal(t, contentChunkKey(peerManifests[0]), contentChunkKey(manifests[0]))

	// without the unfold the handle's manifest of the same chunks is added too
	merged, ok = mergeContentEntry(stored, base, local, local, -1, false)
	require.True(t, ok)
	manifests, _ = filer.SeparateManifestChunks(merged.Chunks)
	require.Len(t, manifests, 2)
}

// A peer that rewrote the whole file: the merge must not bring the base
// chunks back inside the handle's manifest, where they would cover the peer's
// rewritten bytes wherever their stamps are newer.
func TestMergeAfterFoldDoesNotRestoreChunksThePeerDropped(t *testing.T) {
	base, local, fold := mergeFoldFile(t, filer.SmallChunkManifestBatch+44)
	stored := proto.Clone(base).(*filer_pb.Entry)
	stored.Chunks = []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 2, FileKey: 1, Cookie: 1}, Offset: 0, Size: base.Attributes.FileSize, ModifiedTsNs: 1}}

	merged, ok := mergeContentEntry(stored, base, fold.unfold(local), fold.unfold(local), -1, false)
	require.True(t, ok)
	keys := referenceCounts(merged.Chunks)
	require.Len(t, keys, 2, "the peer's rewrite and the handle's appended chunk")
	for _, c := range merged.Chunks {
		require.False(t, c.IsChunkManifest, "no manifest of dropped chunks")
	}
	for _, c := range base.Chunks {
		require.Zero(t, keys[contentChunkKey(c)], "base chunk %s came back", contentChunkKey(c))
	}

	// without the unfold the manifest of base chunks is added as a local write
	merged, ok = mergeContentEntry(stored, base, local, local, -1, false)
	require.True(t, ok)
	manifests, _ := filer.SeparateManifestChunks(merged.Chunks)
	require.Len(t, manifests, 1)
}

// foldMergeFiler keeps the stored entry and its IF_ENTRY_EQUAL condition like
// contentFlushConditionFiler, and points AssignVolume at a volume stub, so a
// flush can store the manifests it folds.
type foldMergeFiler struct {
	contentFlushConditionFiler
	volume  string
	assigns atomic.Int32
}

func (s *foldMergeFiler) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	n := s.assigns.Add(1)
	return &filer_pb.AssignVolumeResponse{
		FileId:   fmt.Sprintf("3,%02x637037d6", n),
		Count:    1,
		Location: &filer_pb.Location{Url: s.volume, PublicUrl: s.volume},
	}, nil
}

// A flush that folds the handle's chunks finds the entry rewritten by a peer
// (a bloat merge on another mount: one chunk, stamped older than the bytes it
// replaced). The merged commit carries the peer's rewrite and the handle's own
// append, not a manifest of the base chunks the peer dropped.
func TestFlushMergeAfterFoldLeavesThePeersRewrite(t *testing.T) {
	const n = filer.SmallChunkManifestBatch + 44
	wfs := newInvalidateTestWFS(t)
	wfs.option.ConcurrentWriters = 4
	wfs.option.uniqueCacheDirForWrite = t.TempDir()
	wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(4)
	fake := &foldMergeFiler{volume: newVolumeStub(t)}
	startFakeFiler(t, wfs, fake)
	wfs.option.VolumeServerAccess = "direct"
	wfs.filerClient = wdclient.NewFilerClient(wfs.option.FilerAddresses, wfs.option.GrpcDialOption, "")

	attrs := &filer_pb.FuseAttributes{Inode: 42, FileMode: 0644, Uid: 2000, Gid: 20, FileSize: n * 300}
	base := &filer_pb.Entry{Name: "file", Attributes: attrs}
	for i := 0; i < n; i++ {
		base.Chunks = append(base.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(i + 1), Cookie: 1}, Offset: int64(i) * 300, Size: 300, ModifiedTsNs: int64(100 + i)})
	}
	rewrite := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 2, FileKey: 999, Cookie: 1}, Offset: 0, Size: n * 300, ModifiedTsNs: 1}
	peer := proto.Clone(base).(*filer_pb.Entry)
	peer.Chunks = []*filer_pb.FileChunk{rewrite}
	fake.entry = peer

	appended := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: n + 1, Cookie: 1}, Offset: n * 300, Size: 300, ModifiedTsNs: 100 + n}
	live := proto.Clone(base).(*filer_pb.Entry)
	live.Chunks = append(live.Chunks, appended)
	live.Attributes.FileSize = (n + 1) * 300
	observed := proto.Clone(base).(*filer_pb.Entry)
	wfs.mapPbIdFromFilerToLocal(observed)
	wfs.mapPbIdFromFilerToLocal(live)
	fh := newFileHandle(wfs, 1, 42, live)
	t.Cleanup(func() { fh.dirtyPages.Destroy() })
	fh.baseEntry.Store(observed)
	fh.RememberPath(util.FullPath("/dir/file"))
	fh.dirtyMetadata = true

	require.Equal(t, fuse.OK, wfs.doFlush(context.Background(), fh, 1000, 20, false, 0))
	require.NotZero(t, fake.assigns.Load(), "the flush folded and stored a manifest")

	fake.mu.Lock()
	defer fake.mu.Unlock()
	keys := referenceCounts(fake.entry.Chunks)
	require.Len(t, keys, 2, "the peer's rewrite and the handle's append: %v", keys)
	require.Equal(t, 1, keys[contentChunkKey(rewrite)])
	require.Equal(t, 1, keys[contentChunkKey(appended)])
}

// refoldingFiler folds the small chunks of a create before it stores and
// acknowledges it, as the filer's cleanupChunks does.
type refoldingFiler struct {
	foldMergeFiler
	saver foldMergeSaver
}

func (s *refoldingFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	r = proto.Clone(r).(*filer_pb.CreateEntryRequest)
	folded, err := filer.MaybeManifestize(s.saver.save, nil, r.Entry.Chunks)
	if err != nil {
		return nil, err
	}
	r.Entry.Chunks = folded
	return s.foldMergeFiler.CreateEntry(ctx, r)
}

// A peer's chmod refuses the flush's first commit; the merged retry sends the
// handle's 256 small chunks unfolded and the filer folds them before storing.
// The handle must hold what the filer stored, which is also its base: holding
// the flat chunks against a base that lists a manifest, a later merge would
// take them for writes of its own, after a peer truncated the file too.
func TestFlushMergeInstallsTheEntryTheFilerStored(t *testing.T) {
	const n = filer.SmallChunkManifestBatch - 1
	wfs := newInvalidateTestWFS(t)
	wfs.option.ConcurrentWriters = 4
	wfs.option.uniqueCacheDirForWrite = t.TempDir()
	wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(4)
	fake := &refoldingFiler{foldMergeFiler: foldMergeFiler{volume: newVolumeStub(t)}, saver: foldMergeSaver{saved: 500}}
	startFakeFiler(t, wfs, fake)
	wfs.option.VolumeServerAccess = "direct"
	wfs.filerClient = wdclient.NewFilerClient(wfs.option.FilerAddresses, wfs.option.GrpcDialOption, "")

	attrs := &filer_pb.FuseAttributes{Inode: 42, FileMode: 0644, Uid: 2000, Gid: 20, FileSize: n * 300}
	base := &filer_pb.Entry{Name: "file", Attributes: attrs}
	for i := 0; i < n; i++ {
		base.Chunks = append(base.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(i + 1), Cookie: 1}, Offset: int64(i) * 300, Size: 300, ModifiedTsNs: int64(100 + i)})
	}
	peer := proto.Clone(base).(*filer_pb.Entry)
	peer.Attributes.FileMode = 0600 // a chmod on another mount
	fake.entry = peer

	live := proto.Clone(base).(*filer_pb.Entry)
	live.Chunks = append(live.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: n + 1, Cookie: 1}, Offset: n * 300, Size: 300, ModifiedTsNs: 100 + n})
	live.Attributes.FileSize = (n + 1) * 300
	observed := proto.Clone(base).(*filer_pb.Entry)
	wfs.mapPbIdFromFilerToLocal(observed)
	wfs.mapPbIdFromFilerToLocal(live)
	fh := newFileHandle(wfs, 1, 42, live)
	t.Cleanup(func() { fh.dirtyPages.Destroy() })
	fh.baseEntry.Store(observed)
	fh.RememberPath(util.FullPath("/dir/file"))
	fh.dirtyMetadata = true

	require.Equal(t, fuse.OK, wfs.doFlush(context.Background(), fh, 1000, 20, false, 0))

	fake.mu.Lock()
	stored := referenceCounts(fake.entry.Chunks)
	fake.mu.Unlock()
	manifests, _ := filer.SeparateManifestChunks(fake.entry.Chunks)
	require.Len(t, manifests, 1, "the filer folded the merged chunks")
	require.Equal(t, stored, referenceCounts(fh.GetEntry().GetEntry().Chunks), "the handle holds what the filer stored")
	require.Equal(t, stored, referenceCounts(fh.baseEntry.Load().Chunks), "the base is what the filer stored")
}
