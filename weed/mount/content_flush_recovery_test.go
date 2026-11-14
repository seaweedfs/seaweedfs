//go:build !freebsd

package mount

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// Match the actual CreateEntry wire contract: the server stamps CommitTsNs,
// returns its committed entry in MetadataEvent, and refuses in-band.
type committedContentFiler struct{ contentFlushConditionFiler }

func (s *committedContentFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	for _, c := range r.GetCondition().GetClauses() {
		match := c.Kind == filer_pb.WriteCondition_IF_NOT_EXISTS && s.entry == nil || c.Kind == filer_pb.WriteCondition_IF_ENTRY_EQUAL && proto.Equal(c.ExpectedEntry, s.entry)
		if !match {
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	s.entry = proto.Clone(r.Entry).(*filer_pb.Entry)
	s.entry.CommitTsNs = int64(2000 + s.calls)
	return &filer_pb.CreateEntryResponse{LogTsNs: s.entry.CommitTsNs, MetadataEvent: &filer_pb.SubscribeMetadataResponse{Directory: r.Directory, TsNs: s.entry.CommitTsNs, EventNotification: &filer_pb.EventNotification{NewEntry: proto.Clone(s.entry).(*filer_pb.Entry)}}}, nil
}
func committedContentHandle(t *testing.T, peer bool) (*WFS, *FileHandle, *committedContentFiler) {
	t.Helper()
	w := newInvalidateTestWFS(t)
	base := flushConditionEntry(1)
	base.CommitTsNs = 100
	current := proto.Clone(base).(*filer_pb.Entry)
	if peer {
		// A peer replaced the file: the flush cannot merge into it.
		current = flushConditionEntry(4)
		current.Attributes.Inode = 99
		current.CommitTsNs = 101
	}
	s := &committedContentFiler{contentFlushConditionFiler: contentFlushConditionFiler{entry: current}}
	startFakeFiler(t, w, s)
	w.mapPbIdFromFilerToLocal(base)
	fh := newFileHandle(w, 1, 42, base)
	fh.RememberPath(util.FullPath("/dir/file"))
	fh.dirtyMetadata = true
	t.Cleanup(fh.dirtyPages.Destroy)
	return w, fh, s
}
func TestContentAsyncSuccessReleasesHandle(t *testing.T) {
	w, fh, s := committedContentHandle(t, false)
	w.option.CacheDirForWrite = t.TempDir()
	w.inodeToPath.Lookup("/dir/file", 0, false, false, fh.inode, true)
	w.fhMap.inode2fh[fh.inode] = fh
	done := make(chan struct{})
	w.pendingAsyncFlush = map[uint64]chan struct{}{fh.inode: done}
	w.asyncFlushWg.Add(1)
	w.processAsyncFlushItem(&asyncFlushItem{fh: fh, done: done})
	select {
	case <-done:
	default:
		t.Fatal("successful drain did not complete")
	}
	if _, kept := w.fhMap.FindFileHandle(fh.inode); kept {
		t.Fatal("successful drain retained handle")
	}
	files, err := globFailedContent(t, w)
	if err != nil || len(files) != 0 || s.calls != 1 {
		t.Fatalf("successful drain: recovery=%v error=%v calls=%d", files, err, s.calls)
	}
}

func TestContentAsyncConflictKeepsRecoveryRecord(t *testing.T) {
	w, fh, s := committedContentHandle(t, true)
	w.option.CacheDirForWrite = t.TempDir()
	pending := flushConditionEntry(1, 3).Chunks[1]
	pending.Offset = 2
	fh.UpdateEntry(func(e *filer_pb.Entry) { e.Chunks = append(e.Chunks, pending) })
	w.inodeToPath.Lookup("/dir/file", 0, false, false, fh.inode, true)
	w.fhMap.inode2fh[fh.inode] = fh
	fh.asyncFlushPending = true
	done := make(chan struct{})
	w.pendingAsyncFlush = map[uint64]chan struct{}{fh.inode: done}
	w.asyncFlushWg.Add(1)
	oldSleep := metadataFlushSleep
	metadataFlushSleep = func(context.Context, time.Duration) {}
	defer func() { metadataFlushSleep = oldSleep }()
	peer := proto.Clone(s.entry).(*filer_pb.Entry)
	w.processAsyncFlushItem(&asyncFlushItem{fh: fh, done: done})
	select {
	case <-done:
	default:
		t.Fatal("async completion not signalled")
	}
	if !proto.Equal(peer, s.entry) {
		t.Fatal("refused async flush changed the peer entry")
	}
	files, err := globFailedContent(t, w)
	if err != nil || len(files) != 1 {
		t.Fatalf("failed close discarded pending refs: recovery files %v, error %v", files, err)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Path    string
		Error   string
		Request *filer_pb.CreateEntryRequest
	}
	if err = json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	if r.Path != "/dir/file" || r.Error == "" || r.Request == nil {
		t.Fatalf("missing recovery context: %s", raw)
	}
	expected := flushConditionEntry(1)
	expected.CommitTsNs = 100
	if !proto.Equal(r.Request.Condition.Clauses[0].ExpectedEntry, expected) {
		t.Fatal("recovery lost observed CAS base")
	}
	if len(r.Request.Entry.Chunks) != 2 || !proto.Equal(r.Request.Entry.Chunks[1], pending) {
		t.Fatal("recovery lost pending chunks")
	}
	info, err := os.Stat(files[0])
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private recovery record: %v %v", info, err)
	}
	if s.calls != metadataFlushRetries+1 {
		t.Fatalf("publication attempts = %d, want existing retry bound %d", s.calls, metadataFlushRetries+1)
	}
}

func TestContentRecoveryUsesFailedRequestNotLaterHandle(t *testing.T) {
	w, fh, _ := committedContentHandle(t, true)
	w.option.CacheDirForWrite = t.TempDir()
	err := w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 1000, 20, nil)
	if err == nil {
		t.Fatal("expected refusal")
	}
	fh.UpdateEntry(func(e *filer_pb.Entry) { e.Chunks = nil; e.CommitTsNs = 9000 })
	path, saveErr := w.retainFailedContent(fh, "/dir", "file", err)
	if saveErr != nil {
		t.Fatal(saveErr)
	}
	raw, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	var r struct{ Request *filer_pb.CreateEntryRequest }
	if readErr = json.Unmarshal(raw, &r); readErr != nil {
		t.Fatal(readErr)
	}
	if len(r.Request.Entry.Chunks) != 1 || r.Request.Entry.CommitTsNs != 100 {
		t.Fatal("recovery used a later handle, not the failed publication")
	}
}

func TestContentAsyncRetentionWriteFailureKeepsDrainPending(t *testing.T) {
	w, fh, s := committedContentHandle(t, true)
	cache := filepath.Join(t.TempDir(), "cache")
	if err := os.WriteFile(cache, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	w.option.CacheDirForWrite = cache
	w.inodeToPath.Lookup("/dir/file", 0, false, false, fh.inode, true)
	w.fhMap.inode2fh[fh.inode] = fh
	done := make(chan struct{})
	w.pendingAsyncFlush = map[uint64]chan struct{}{fh.inode: done}
	w.asyncFlushWg.Add(1)
	oldSleep := metadataFlushSleep
	defer func() { metadataFlushSleep = oldSleep }()
	failures := 0
	metadataFlushSleep = func(context.Context, time.Duration) {
		if s.calls < metadataFlushRetries+1 {
			return // Existing publication retry backoff, before retention.
		}
		failures++
		if failures > 1 {
			t.Fatal("retention did not recover after writable cache restored")
		}
		select {
		case <-done:
			t.Fatal("failed retention signalled completion")
		default:
		}
		if _, ok := w.fhMap.FindFileHandle(fh.inode); !ok {
			t.Fatal("failed retention removed pending handle")
		}
		if err := os.Remove(cache); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(cache, 0700); err != nil {
			t.Fatal(err)
		}
	}
	w.processAsyncFlushItem(&asyncFlushItem{fh: fh, done: done})
	files, err := globFailedContent(t, w)
	if failures != 1 || err != nil || len(files) != 1 {
		t.Fatalf("expected one retention failure then saved record: %d %v %v", failures, files, err)
	}
	if s.calls != metadataFlushRetries+1 {
		t.Fatalf("retention retry added publication attempts: %d", s.calls)
	}
}

func globFailedContent(t *testing.T, w *WFS) ([]string, error) {
	t.Helper()
	root, err := w.failedContentDir()
	if err != nil {
		return nil, err
	}
	return filepath.Glob(filepath.Join(root, "*.json"))
}

func TestContentRecoverySkipsAnotherUsersDirectory(t *testing.T) {
	if os.Getuid() <= 0 {
		t.Skip("needs a non-root uid to model another user's private directory")
	}
	w, fh, _ := committedContentHandle(t, true)
	w.option.CacheDirForWrite = t.TempDir()
	if err := os.Mkdir(filepath.Join(w.option.CacheDirForWrite, "failed-metadata"), 0500); err != nil {
		t.Fatal(err)
	}
	err := w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 1000, 20, nil)
	if err == nil {
		t.Fatal("expected refusal")
	}
	if _, saveErr := w.retainFailedContent(fh, "/dir", "file", err); saveErr != nil {
		t.Fatalf("retention blocked by a directory this user cannot write: %v", saveErr)
	}
	if files, globErr := globFailedContent(t, w); globErr != nil || len(files) != 1 {
		t.Fatalf("recovery records %v, error %v", files, globErr)
	}
}
