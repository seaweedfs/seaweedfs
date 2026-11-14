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
	volume          string
	assignDelay     time.Duration
	stuckCommit     bool
	assigns         atomic.Int32
	commits         atomic.Int32
	committedChunks atomic.Int32
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
	s.committedChunks.Store(int32(len(req.GetEntry().GetChunks())))
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

// newSlowDataFlush opens /dir/file with dirty data not yet uploaded, on a
// mount whose filer takes assignDelay to assign the chunk, and shortens the
// commit deadline to deadline.
func newSlowDataFlush(t *testing.T, assignDelay, deadline time.Duration, stuckCommit bool) (*WFS, *FileHandle, *slowDataFiler) {
	t.Helper()

	saved := metadataFlushTimeout
	metadataFlushTimeout = deadline
	t.Cleanup(func() { metadataFlushTimeout = saved })

	wfs := newInvalidateTestWFS(t)
	wfs.option.ConcurrentWriters = 4
	wfs.option.uniqueCacheDirForWrite = t.TempDir()
	wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(4)

	filer := &slowDataFiler{volume: newVolumeStub(t), assignDelay: assignDelay, stuckCommit: stuckCommit}
	startFakeFiler(t, wfs, filer)
	// Upload straight to the assigned volume server, as mounts do by default.
	wfs.option.VolumeServerAccess = "direct"
	wfs.filerClient = wdclient.NewFilerClient(wfs.option.FilerAddresses, wfs.option.GrpcDialOption, "")

	data := []byte("the last shard of a checkpoint, written just before close()")
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(data)), FileMode: 0100644},
	}, 0, 0)
	t.Cleanup(func() { wfs.ReleaseHandle(fh.fh) })
	if err := fh.dirtyPages.AddPage(0, data, true, time.Now().UnixNano()); err != nil {
		t.Fatalf("AddPage: %v", err)
	}
	fh.dirtyMetadata = true
	return wfs, fh, filer
}

// checkCommitted asserts the flush stored the chunk and committed it once.
func checkCommitted(t *testing.T, fh *FileHandle, filer *slowDataFiler) {
	t.Helper()
	if got := filer.assigns.Load(); got != 1 {
		t.Fatalf("AssignVolume calls = %d, want 1", got)
	}
	if got := filer.commits.Load(); got != 1 {
		t.Fatalf("CreateEntry calls = %d, want 1", got)
	}
	if got := filer.committedChunks.Load(); got != 1 {
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
	wfs, fh, filer := newSlowDataFlush(t, time.Second, 200*time.Millisecond, false)

	status := wfs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fh.inode}, Fh: uint64(fh.fh)})
	if status != fuse.OK {
		t.Fatalf("Flush = %v, want OK: the data is stored and the filer answers the commit at once", status)
	}
	checkCommitted(t, fh, filer)
}

// fsync() gets the same commit-only deadline as close().
func TestFsyncCommitsAfterSlowUpload(t *testing.T) {
	wfs, fh, filer := newSlowDataFlush(t, time.Second, 200*time.Millisecond, false)

	status := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fh.inode}, Fh: uint64(fh.fh)})
	if status != fuse.OK {
		t.Fatalf("Fsync = %v, want OK", status)
	}
	checkCommitted(t, fh, filer)
}

// A truncate below dirty data flushes the data first (see SetAttr); that
// flush gets the commit-only deadline too.
func TestTruncateFlushCommitsAfterSlowUpload(t *testing.T) {
	wfs, fh, filer := newSlowDataFlush(t, time.Second, 200*time.Millisecond, false)

	in := &fuse.SetAttrIn{}
	in.NodeId = fh.inode
	in.Valid = fuse.FATTR_SIZE
	in.Size = 8
	var out fuse.AttrOut
	if status := wfs.SetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("SetAttr size down = %v, want OK", status)
	}
	if got := filer.commits.Load(); got != 1 {
		t.Fatalf("CreateEntry calls = %d, want 1", got)
	}
	if out.Size != 8 {
		t.Fatalf("size after truncate = %d, want 8", out.Size)
	}
}

// The deadline still bounds the commit itself: against a filer that never
// answers CreateEntry, close() fails once the deadline passes instead of
// hanging, and the handle stays dirty for Release to try again.
func TestFlushCommitDeadlineStillBoundsAStuckFiler(t *testing.T) {
	const deadline = 200 * time.Millisecond
	wfs, fh, filer := newSlowDataFlush(t, 0, deadline, true)

	start := time.Now()
	status := wfs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fh.inode}, Fh: uint64(fh.fh)})
	elapsed := time.Since(start)
	if status == fuse.OK {
		t.Fatal("Flush = OK against a filer that never committed")
	}
	if elapsed < deadline || elapsed > deadline+5*time.Second {
		t.Fatalf("Flush returned after %v, want about the %v commit deadline", elapsed, deadline)
	}
	if got := filer.commits.Load(); got != 1 {
		t.Fatalf("CreateEntry calls = %d, want 1: no retry once the deadline passed", got)
	}
	if !fh.dirtyMetadata {
		t.Fatal("handle marked clean after a failed commit")
	}
}
