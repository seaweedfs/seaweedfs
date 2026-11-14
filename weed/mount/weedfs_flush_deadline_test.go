package mount

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// slowDataFiler answers AssignVolume only after assignDelay, as a filer does
// while its master is slow or no volume slot is free, and points every
// assignment at a volume stub. CreateEntry, the metadata commit, answers at
// once; with stuckCommit it holds the request until the caller gives up, like
// a filer wedged on its store.
type slowDataFiler struct {
	fakeFilerServer
	volume             string
	assignDelay        time.Duration
	stuckCommit        bool
	assigns            atomic.Int32
	commits            atomic.Int32
	committedChunks    atomic.Int32
	committedManifests atomic.Int32
}

func (s *slowDataFiler) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	n := s.assigns.Add(1)
	select {
	case <-time.After(s.assignDelay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &filer_pb.AssignVolumeResponse{
		FileId:   fmt.Sprintf("3,%02x637037d6", n),
		Count:    1,
		Location: &filer_pb.Location{Url: s.volume, PublicUrl: s.volume},
	}, nil
}

func (s *slowDataFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.commits.Add(1)
	chunks := req.GetEntry().GetChunks()
	manifests, _ := filer.SeparateManifestChunks(chunks)
	s.committedChunks.Store(int32(len(chunks)))
	s.committedManifests.Store(int32(len(manifests)))
	if s.stuckCommit {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.fakeFilerServer.CreateEntry(ctx, req)
}

// newVolumeStub stores chunk uploads the way a volume server answers them.
func newVolumeStub(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reader, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		part, err := reader.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data, err := io.ReadAll(part)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-MD5", part.Header.Get("Content-MD5"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"size":%d}`, len(data))
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

// newSlowDataFlush opens /dir/file with dirty data not yet uploaded, written
// after the chunks the file already holds (stored), on a mount whose filer
// takes assignDelay to assign a chunk, and shortens the commit deadline to
// deadline.
func newSlowDataFlush(t *testing.T, assignDelay, deadline time.Duration, stuckCommit bool, stored ...*filer_pb.FileChunk) (*WFS, *FileHandle, *slowDataFiler) {
	t.Helper()

	saved := metadataFlushTimeout
	metadataFlushTimeout = deadline
	t.Cleanup(func() { metadataFlushTimeout = saved })

	wfs := newInvalidateTestWFS(t)
	wfs.option.ConcurrentWriters = 4
	wfs.option.uniqueCacheDirForWrite = t.TempDir()
	wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(4)

	fake := &slowDataFiler{volume: newVolumeStub(t), assignDelay: assignDelay, stuckCommit: stuckCommit}
	startFakeFiler(t, wfs, fake)
	// Upload straight to the assigned volume server, as mounts do by default.
	wfs.option.VolumeServerAccess = "direct"
	wfs.filerClient = wdclient.NewFilerClient(wfs.option.FilerAddresses, wfs.option.GrpcDialOption, "")

	data := []byte("the last shard of a checkpoint, written just before close()")
	offset := int64(filer.TotalSize(stored))
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: uint64(offset) + uint64(len(data)), FileMode: 0100644},
		Chunks:     stored,
	}, 0, 0)
	t.Cleanup(func() { wfs.ReleaseHandle(fh.fh) })
	if err := fh.dirtyPages.AddPage(offset, data, true, time.Now().UnixNano()); err != nil {
		t.Fatalf("AddPage: %v", err)
	}
	fh.dirtyMetadata = true
	return wfs, fh, fake
}

// storedSmallChunks returns n chunks a file already holds, back to back from
// offset 0, each small enough to count toward a manifest of small chunks.
func storedSmallChunks(n int) []*filer_pb.FileChunk {
	const size = 1024
	chunks := make([]*filer_pb.FileChunk, n)
	for i := range chunks {
		chunks[i] = &filer_pb.FileChunk{
			FileId:       fmt.Sprintf("4,%02x637037d6", i+1),
			Offset:       int64(i) * size,
			Size:         size,
			ModifiedTsNs: int64(i + 1),
		}
	}
	return chunks
}

// checkCommitted asserts the flush made the given number of uploads, then
// committed once, an entry of one chunk.
func checkCommitted(t *testing.T, fh *FileHandle, fake *slowDataFiler, uploads int32) {
	t.Helper()
	if got := fake.assigns.Load(); got != uploads {
		t.Fatalf("AssignVolume calls = %d, want %d", got, uploads)
	}
	if got := fake.commits.Load(); got != 1 {
		t.Fatalf("CreateEntry calls = %d, want 1", got)
	}
	if got := fake.committedChunks.Load(); got != 1 {
		t.Fatalf("committed chunks = %d, want 1", got)
	}
	if fh.dirtyMetadata {
		t.Fatal("handle still dirty after the commit")
	}
}

// close() after an upload that outlasts the commit deadline still commits:
// the deadline bounds CreateEntry, which the filer answers at once. Started at
// the top of Flush, the deadline was spent by the slow assign, and close()
// returned EIO for data that was already stored.
func TestFlushCommitsAfterSlowUpload(t *testing.T) {
	wfs, fh, fake := newSlowDataFlush(t, time.Second, 200*time.Millisecond, false)

	status := wfs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fh.inode}, Fh: uint64(fh.fh)})
	if status != fuse.OK {
		t.Fatalf("Flush = %v, want OK: the data is stored and the filer answers the commit at once", status)
	}
	checkCommitted(t, fh, fake, 1)
}

// fsync() gets the same commit-only deadline as close().
func TestFsyncCommitsAfterSlowUpload(t *testing.T) {
	wfs, fh, fake := newSlowDataFlush(t, time.Second, 200*time.Millisecond, false)

	status := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fh.inode}, Fh: uint64(fh.fh)})
	if status != fuse.OK {
		t.Fatalf("Fsync = %v, want OK", status)
	}
	checkCommitted(t, fh, fake, 1)
}

// A truncate below dirty data flushes the data first (see SetAttr); that
// flush gets the commit-only deadline too.
func TestTruncateFlushCommitsAfterSlowUpload(t *testing.T) {
	wfs, fh, fake := newSlowDataFlush(t, time.Second, 200*time.Millisecond, false)

	in := &fuse.SetAttrIn{}
	in.NodeId = fh.inode
	in.Valid = fuse.FATTR_SIZE
	in.Size = 8
	var out fuse.AttrOut
	if status := wfs.SetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("SetAttr size down = %v, want OK", status)
	}
	if got := fake.commits.Load(); got != 1 {
		t.Fatalf("CreateEntry calls = %d, want 1", got)
	}
	if out.Size != 8 {
		t.Fatalf("size after truncate = %d, want 8", out.Size)
	}
}

// The data upload is not the only one a flush makes. One that completes a
// batch of small chunks folds them into a manifest and uploads that too,
// while it prepares the entry it is about to commit. That upload must not
// spend the commit deadline either.
func TestFlushCommitsAfterSlowManifestUpload(t *testing.T) {
	// One chunk short of a batch: the chunk this flush uploads completes it.
	stored := storedSmallChunks(filer.SmallChunkManifestBatch - 1)
	wfs, fh, fake := newSlowDataFlush(t, time.Second, 200*time.Millisecond, false, stored...)

	status := wfs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fh.inode}, Fh: uint64(fh.fh)})
	if status != fuse.OK {
		t.Fatalf("Flush = %v, want OK: the data and the manifest are stored and the filer answers the commit at once", status)
	}
	checkCommitted(t, fh, fake, 2) // the data chunk, then the manifest
	if got := fake.committedManifests.Load(); got != 1 {
		t.Fatalf("committed manifests = %d, want 1: this flush no longer uploads a manifest", got)
	}
}

// The deadline still bounds the commit itself: against a filer that never
// answers CreateEntry, close() fails once the deadline passes instead of
// hanging, and the handle stays dirty for Release to try again.
func TestFlushCommitDeadlineStillBoundsAStuckFiler(t *testing.T) {
	const deadline = 200 * time.Millisecond
	wfs, fh, fake := newSlowDataFlush(t, 0, deadline, true)

	start := time.Now()
	status := wfs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fh.inode}, Fh: uint64(fh.fh)})
	elapsed := time.Since(start)
	if status == fuse.OK {
		t.Fatal("Flush = OK against a filer that never committed")
	}
	if elapsed < deadline || elapsed > deadline+5*time.Second {
		t.Fatalf("Flush returned after %v, want about the %v commit deadline", elapsed, deadline)
	}
	if got := fake.commits.Load(); got != 1 {
		t.Fatalf("CreateEntry calls = %d, want 1: no retry once the deadline passed", got)
	}
	if !fh.dirtyMetadata {
		t.Fatal("handle marked clean after a failed commit")
	}
}

// The attempts of one flush share the budget: what one filer call used is gone
// for the next. A nil budget bounds nothing.
func TestCommitBudgetIsSharedByItsCalls(t *testing.T) {
	budget := &commitBudget{left: 50 * time.Millisecond}
	ctx, end := budget.begin(context.Background())
	<-ctx.Done() // a filer that never answers
	end()
	if !budget.spent() {
		t.Fatalf("%v left after a call used the whole budget", budget.left)
	}
	ctx, end = budget.begin(context.Background())
	if ctx.Err() == nil {
		t.Fatal("a call started with time left after the budget ran out")
	}
	end()

	var none *commitBudget
	ctx, end = none.begin(context.Background())
	if _, bounded := ctx.Deadline(); bounded || none.spent() {
		t.Fatal("a nil budget bounded the call")
	}
	end()
}
