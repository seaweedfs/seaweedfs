package mount

import (
	"context"
	"slices"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// compactingFiler compacts a create's chunks like the filer does and records
// the chunks the commit would delete as covered.
type compactingFiler struct {
	contentFlushConditionFiler
	sent    [][]uint64
	deleted []uint64
}

func (s *compactingFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	compacted, covered, err := filer.CompactFileChunks(ctx, nil, r.Entry.GetChunks())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.sent = append(s.sent, chunkFileKeys(r.Entry))
	s.mu.Unlock()
	r = proto.Clone(r).(*filer_pb.CreateEntryRequest)
	r.Entry.Chunks = compacted
	resp, err := s.contentFlushConditionFiler.CreateEntry(ctx, r)
	if err == nil && resp.GetErrorCode() == filer_pb.FilerError_OK {
		s.mu.Lock()
		for _, c := range covered {
			s.deleted = append(s.deleted, c.GetFid().GetFileKey())
		}
		s.mu.Unlock()
	}
	return resp, err
}

func coveredChunksEntry(chunks ...[3]int64) *filer_pb.Entry {
	e := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{Inode: 42, FileMode: 0644, Uid: 2000, Gid: 20, FileSize: 2}}
	for _, c := range chunks {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(c[0]), Cookie: 1}, Offset: c[1], Size: 1, ModifiedTsNs: c[0]})
	}
	return e
}

// A chunk overwritten before any publication named it is dropped by the
// mount's compaction; the request must still carry it so the filer deletes it.
// A covered chunk the stored entry already names is left to the filer's diff.
func TestFlushSendsUnpublishedCoveredChunks(t *testing.T) {
	for _, name := range []string{"close", "periodic"} {
		t.Run(name, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			stored := coveredChunksEntry([3]int64{1, 0})
			fake := &compactingFiler{contentFlushConditionFiler: contentFlushConditionFiler{entry: proto.Clone(stored).(*filer_pb.Entry)}}
			startFakeFiler(t, wfs, fake)
			observed := proto.Clone(stored).(*filer_pb.Entry)
			wfs.mapPbIdFromFilerToLocal(observed)
			live := coveredChunksEntry([3]int64{1, 0}, [3]int64{2, 0}, [3]int64{3, 1}, [3]int64{4, 1})
			wfs.mapPbIdFromFilerToLocal(live)
			fh := newFileHandle(wfs, 1, 42, live)
			t.Cleanup(func() { fh.dirtyPages.Destroy() })
			fh.baseEntry.Store(observed)
			fh.dirtyMetadata = true
			fh.RememberPath(util.FullPath("/dir/file"))
			if name == "close" {
				if code := wfs.doFlush(context.Background(), fh, 1000, 20, false, 0); code != fuse.OK {
					t.Fatal(code)
				}
			} else if err := wfs.flushFileMetadata(fh); err != nil {
				t.Fatal(err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.sent) != 1 || !slices.Equal(fake.sent[0], []uint64{2, 4, 3}) {
				t.Fatalf("sent chunks %v, want [[2 4 3]]", fake.sent)
			}
			if !slices.Equal(fake.deleted, []uint64{3}) {
				t.Fatalf("filer deletes %v, want [3]", fake.deleted)
			}
			if got := chunkFileKeys(fake.entry); !slices.Equal(got, []uint64{2, 4}) {
				t.Fatalf("stored chunks %v", got)
			}
			if got := chunkFileKeys(fh.GetEntry().GetEntry()); !slices.Equal(got, []uint64{2, 4}) {
				t.Fatalf("handle chunks %v", got)
			}
		})
	}
}

// A dropped chunk goes out only while newer plain chunks of the request hide
// all of it: an unstamped chunk hides nothing, the filer does not look into a
// manifest, and another dropped chunk is not sent to hide it either.
func TestUnpublishedChunksStayCovered(t *testing.T) {
	chunk := func(key uint64, offset, size, ts int64) *filer_pb.FileChunk {
		return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: key, Cookie: 1}, Offset: offset, Size: uint64(size), ModifiedTsNs: ts}
	}
	published := chunk(1, 0, 2, 1)
	dropped := []*filer_pb.FileChunk{published, chunk(2, 0, 2, 2), chunk(3, 0, 2, 3)}
	for _, tt := range []struct {
		name   string
		chunks []*filer_pb.FileChunk
		want   []uint64
	}{
		{"covered", []*filer_pb.FileChunk{chunk(4, 0, 1, 4), chunk(5, 1, 1, 5)}, []uint64{2, 3}},
		{"between", []*filer_pb.FileChunk{chunk(4, 0, 2, 3)}, []uint64{2}},
		{"partly", []*filer_pb.FileChunk{chunk(4, 0, 1, 4)}, nil},
		{"unstamped", []*filer_pb.FileChunk{chunk(4, 0, 2, 0)}, nil},
		{"manifest", []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 4, Cookie: 1}, Size: 2, ModifiedTsNs: 4, IsChunkManifest: true}}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fh := &FileHandle{}
			fh.baseEntry.Store(&filer_pb.Entry{Chunks: []*filer_pb.FileChunk{proto.Clone(published).(*filer_pb.FileChunk)}})
			got := chunkFileKeys(&filer_pb.Entry{Chunks: fh.unpublishedChunks(dropped, tt.chunks)})
			if !slices.Equal(got, tt.want) {
				t.Fatalf("unpublished %v, want %v", got, tt.want)
			}
		})
	}
}

// A merged chunk yields to a write newer than any byte it holds, as a peer's
// pending write over bytes the merge did not change must still show.
func TestOldestVisibleTsNs(t *testing.T) {
	chunk := func(key uint64, offset, size, ts int64) *filer_pb.FileChunk {
		return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: key, Cookie: 1}, Offset: offset, Size: uint64(size), ModifiedTsNs: ts}
	}
	list, err := filer.NonOverlappingVisibleIntervals(context.Background(), nil, []*filer_pb.FileChunk{chunk(1, 0, 8, 1), chunk(2, 4, 4, 5), chunk(3, 12, 4, 3)}, 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	var visibles []*filer.Interval[*filer.VisibleInterval]
	for x := list.Front(); x != nil; x = x.Next {
		visibles = append(visibles, x)
	}
	for _, tt := range []struct {
		start, stop, want int64
	}{
		{0, 8, 1},
		{4, 8, 5},
		{6, 7, 5},
		{12, 16, 3},
		{13, 15, 3},
		{8, 12, 0},
		{4, 16, 0},
		{0, 16, 0},
	} {
		if got := oldestVisibleTsNs(visibles, tt.start, tt.stop); got != tt.want {
			t.Errorf("[%d,%d): %d, want %d", tt.start, tt.stop, got, tt.want)
		}
	}
}
