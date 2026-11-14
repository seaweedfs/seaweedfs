//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func g6AppendHandle(t *testing.T, peer *g6ConditionalPeer, base *filer_pb.Entry, key uint64, writeback ...bool) (*WFS, uint64) {
	t.Helper()
	w := newInvalidateTestWFS(t)
	w.option.CacheDirForWrite = t.TempDir()
	w.option.DefaultPermissions = true
	w.option.WritebackCache = len(writeback) > 0 && writeback[0]
	w.posixLocks = NewPosixLockTable()
	w.option.ConcurrentWriters = 1
	w.concurrentWriters = util.NewLimitedConcurrentExecutor(1)
	startFakeFiler(t, w, peer)
	w.inodeToPath.Lookup("/dir", 1, true, false, 0, false)
	w.inodeToPath.Lookup("/dir/file", 1, false, false, 42, true)
	w.inodeToPath.MarkChildrenCached("/dir")
	if err := w.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", proto.Clone(base).(*filer_pb.Entry)), 1); err != nil {
		t.Fatal(err)
	}
	out := &fuse.OpenOut{}
	if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY | syscall.O_APPEND}, out); code != fuse.OK {
		t.Fatal(code)
	}
	fh := w.GetHandle(FileHandleId(out.Fh))
	peer.mu.Lock()
	if peer.blobs == nil {
		peer.blobs = make(map[string][]byte)
	}
	peer.blobs[base.Chunks[0].GetFileIdString()] = bytes.Repeat([]byte{'O'}, 256)
	peer.mu.Unlock()
	g6AppendUploader(t, fh, key, peer)
	return w, out.Fh
}

func g6AppendUploader(t *testing.T, fh *FileHandle, key uint64, peer *g6ConditionalPeer) {
	t.Helper()
	t.Cleanup(fh.dirtyPages.Destroy)
	fh.dirtyPages.randomWriter.(*ChunkedDirtyPages).saveDataAsChunk = func(util.FullPath) filer.SaveDataAsChunkFunctionType {
		return func(r io.Reader, _ string, offset, ts int64, _ uint64) (*filer_pb.FileChunk, error) {
			body, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			key++
			chunk := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: key, Cookie: 1}, Offset: offset, Size: uint64(len(body)), ModifiedTsNs: ts}
			peer.mu.Lock()
			peer.blobs[chunk.GetFileIdString()] = append([]byte(nil), body...)
			peer.mu.Unlock()
			return chunk, nil
		}
	}
}

func TestAppendAfterRefusedFlushHasNoHole(t *testing.T) {
	base := attributeAppendEntry(1)
	base.CommitTsNs = 100
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 100)
	write := func(w *WFS, handle, offset uint64, value byte) {
		t.Helper()
		data := bytes.Repeat([]byte{value}, 256)
		n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: offset, Size: 256}, data)
		if n != 256 || code != fuse.OK {
			t.Fatalf("write: %d %v", n, code)
		}
	}
	write(w, handle, 256, 'A')
	// A peer chmod changes only attributes after this handle observed its base.
	peer.mu.Lock()
	peer.entry.Attributes.FileMode = 0600
	peer.entry.CommitTsNs++
	peer.mu.Unlock()
	first := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle})
	t.Logf("first fsync=%v", first)
	w.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, ReleaseFlags: fuse.FUSE_RELEASE_FLOCK_UNLOCK})
	peer.mu.Lock()
	latest := proto.Clone(peer.entry).(*filer_pb.Entry)
	peer.mu.Unlock()
	if err := w.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", latest), 3000); err != nil {
		t.Fatal(err)
	}
	out := &fuse.OpenOut{}
	if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY | syscall.O_APPEND}, out); code != fuse.OK {
		t.Fatal(code)
	}
	next, nextHandle := w, out.Fh
	g6AppendUploader(t, w.GetHandle(FileHandleId(nextHandle)), 200, peer)
	// FUSE WRITE offsets are supplied by the kernel; its cached EOF advanced
	// on the first successful WRITE even when that handle's fsync failed.
	write(next, nextHandle, 512, 'B')
	if code := next.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: nextHandle}); code != fuse.OK {
		t.Fatal(code)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := append(bytes.Repeat([]byte{'O'}, 256), bytes.Repeat([]byte{'A'}, 256)...)
	want = append(want, bytes.Repeat([]byte{'B'}, 256)...)
	if peer.entry.Attributes.FileSize != 768 || peer.entry.Attributes.FileMode != 0600 || !bytes.Equal(g6StoredBytes(t, peer), want) {
		t.Fatal("original/A/B bytes, size or peer mode changed")
	}
	for _, offset := range []int64{0, 256, 512} {
		covered := false
		for _, c := range peer.entry.Chunks {
			if c.Offset <= offset && c.Offset+int64(c.Size) >= offset+256 {
				covered = true
			}
		}
		if !covered {
			t.Fatalf("fsync acknowledged a hole [%d,%d): first=%v chunks=%v", offset, offset+256, first, peer.entry.Chunks)
		}
	}
}

type g6ConditionalPeer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu           sync.Mutex
	entry        *filer_pb.Entry
	rejected     int
	refuse       bool
	blobs        map[string][]byte
	clock        int64
	beforeCreate func(context.Context)
}

func g6EntryEqual(current, expected *filer_pb.Entry) bool {
	actual := filer.FromPbEntry("", proto.Clone(current).(*filer_pb.Entry)).ToProtoEntry()
	want := proto.Clone(expected).(*filer_pb.Entry)
	filer_pb.BeforeEntrySerialization(actual.Chunks)
	filer_pb.BeforeEntrySerialization(want.Chunks)
	return proto.Equal(actual, filer.FromPbEntry("", want).ToProtoEntry())
}

func g6StoredBytes(t *testing.T, s *g6ConditionalPeer) []byte {
	t.Helper()
	result := make([]byte, s.entry.Attributes.FileSize)
	for _, c := range s.entry.Chunks {
		body, ok := s.blobs[c.GetFileIdString()]
		if !ok || uint64(len(body)) != c.Size || c.Offset < 0 || uint64(c.Offset)+c.Size > uint64(len(result)) {
			t.Fatalf("invalid stored reference %v", c)
		}
		copy(result[c.Offset:], body)
	}
	return result
}

func (s *g6ConditionalPeer) LookupDirectoryEntry(context.Context, *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(s.entry).(*filer_pb.Entry)}, nil
}

func (s *g6ConditionalPeer) UpdateEntry(_ context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range r.GetCondition().GetClauses() {
		if c.Kind != filer_pb.WriteCondition_IF_ENTRY_EQUAL || !g6EntryEqual(s.entry, c.ExpectedEntry) {
			return nil, status.Error(codes.FailedPrecondition, "entry changed")
		}
	}
	s.entry = proto.Clone(r.Entry).(*filer_pb.Entry)
	s.clock = max(max(s.clock, s.entry.CommitTsNs), 10000) + 1
	s.entry.CommitTsNs = s.clock
	return &filer_pb.UpdateEntryResponse{LogTsNs: s.clock, MetadataEvent: &filer_pb.SubscribeMetadataResponse{Directory: r.Directory, TsNs: s.clock, EventNotification: &filer_pb.EventNotification{NewEntry: proto.Clone(s.entry).(*filer_pb.Entry)}}}, nil
}

func (s *g6ConditionalPeer) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if s.beforeCreate != nil {
		s.beforeCreate(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refuse {
		s.rejected++
		return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
	}
	for _, c := range r.GetCondition().GetClauses() {
		match := c.Kind == filer_pb.WriteCondition_IF_NOT_EXISTS && s.entry == nil || c.Kind == filer_pb.WriteCondition_IF_ENTRY_EQUAL && s.entry != nil && c.ExpectedEntry != nil && g6EntryEqual(s.entry, c.ExpectedEntry)
		if !match {
			s.rejected++
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	s.entry = proto.Clone(r.Entry).(*filer_pb.Entry)
	s.clock = max(max(s.clock, s.entry.CommitTsNs), 10000) + 1
	s.entry.CommitTsNs = s.clock
	return &filer_pb.CreateEntryResponse{LogTsNs: s.entry.CommitTsNs, MetadataEvent: &filer_pb.SubscribeMetadataResponse{Directory: r.Directory, TsNs: s.entry.CommitTsNs, EventNotification: &filer_pb.EventNotification{NewEntry: proto.Clone(s.entry).(*filer_pb.Entry)}}}, nil
}

func TestG6AppendFsyncAfterPeerAttribute(t *testing.T) {
	for _, op := range []string{"none", "chmod", "chown", "utimes", "setxattr"} {
		t.Run(op, func(t *testing.T) {
			writer := newInvalidateTestWFS(t)
			attr := newInvalidateTestWFS(t)
			attr.atimeMap = make(map[uint64]time.Time)
			base := attributeAppendEntry(1)
			base.CommitTsNs = 100
			s := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
			s.blobs = map[string][]byte{base.Chunks[0].GetFileIdString(): bytes.Repeat([]byte{'O'}, 256), attributeAppendEntry(2).Chunks[1].GetFileIdString(): bytes.Repeat([]byte{'A'}, 256)}
			startFakeFiler(t, writer, s)
			startFakeFiler(t, attr, s)
			path := util.FullPath("/dir/file")
			local := proto.Clone(base).(*filer_pb.Entry)
			writer.mapPbIdFromFilerToLocal(local)
			fh := newFileHandle(writer, 1, 42, local)
			fh.RememberPath(path)
			t.Cleanup(fh.dirtyPages.Destroy)
			writer.fhMap.inode2fh[42], writer.fhMap.fh2inode[1] = fh, 42
			pending := attributeAppendEntry(2).Chunks[1]
			fh.UpdateEntry(func(e *filer_pb.Entry) { e.Chunks = append(e.Chunks, pending); e.Attributes.FileSize = 512 })
			fh.dirtyMetadata = true
			attr.inodeToPath.Lookup("/dir", 1, true, false, 0, false)
			attr.inodeToPath.Lookup(path, 1, false, false, 42, true)
			attr.inodeToPath.MarkChildrenCached("/dir")
			if err := attr.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", base), 1); err != nil {
				t.Fatal(err)
			}
			code := fuse.OK
			if op == "setxattr" {
				code = attr.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: 42}}, "user.test", []byte("peer acknowledged"))
			} else if op != "none" {
				in := &fuse.SetAttrIn{}
				in.NodeId = 42
				switch op {
				case "chmod":
					in.Valid, in.Mode = fuse.FATTR_MODE, 0600
				case "chown":
					in.Valid, in.Owner.Uid, in.Owner.Gid = fuse.FATTR_UID|fuse.FATTR_GID, 1001, 21
				case "utimes":
					in.Valid, in.Mtime, in.Mtimensec = fuse.FATTR_MTIME, 123, 456
				}
				code = attr.SetAttr(nil, in, &fuse.AttrOut{})
			}
			if code != fuse.OK {
				t.Fatalf("peer attribute: %v", code)
			}
			s.mu.Lock()
			wantChunk, gotChunk := proto.Clone(base.Chunks[0]).(*filer_pb.FileChunk), proto.Clone(s.entry.Chunks[0]).(*filer_pb.FileChunk)
			filer_pb.BeforeEntrySerialization([]*filer_pb.FileChunk{wantChunk, gotChunk})
			contentUnchanged := proto.Equal(wantChunk, gotChunk) && len(s.entry.Chunks) == 1
			ackedPeer := proto.Clone(s.entry).(*filer_pb.Entry)
			s.mu.Unlock()
			if !contentUnchanged {
				t.Fatal("peer attribute changed content before append publication")
			}
			if code = writer.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: 1}); code != fuse.OK {
				t.Fatalf("append after acknowledged peer %s returned %v; pending=%d base=%d rejected=%d", op, code, len(fh.entry.Chunks), len(fh.baseEntry.Load().Chunks), s.rejected)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			want := append(bytes.Repeat([]byte{'O'}, 256), bytes.Repeat([]byte{'A'}, 256)...)
			if s.entry.Attributes.FileSize != 512 || !bytes.Equal(g6StoredBytes(t, s), want) {
				t.Fatal("append lost original/pending bytes or size")
			}
			a, b := s.entry.Attributes, ackedPeer.Attributes
			if op == "chmod" && a.FileMode != b.FileMode || op == "chown" && (a.Uid != b.Uid || a.Gid != b.Gid) || op == "utimes" && (a.Mtime != b.Mtime || a.MtimeNs != b.MtimeNs) || op == "setxattr" && !bytes.Equal(s.entry.Extended[XATTR_PREFIX+"user.test"], ackedPeer.Extended[XATTR_PREFIX+"user.test"]) {
				t.Fatal("append rolled back acknowledged peer attributes")
			}
		})
	}
}

func TestAppendCompatibleCommittedPrefix(t *testing.T) {
	w := newInvalidateTestWFS(t)
	base, current, pending := attributeAppendEntry(1), attributeAppendEntry(2), attributeAppendEntry(3)
	current.Attributes.FileMode = 0600
	peer := &g6ConditionalPeer{entry: current, blobs: make(map[string][]byte)}
	for i, c := range pending.Chunks {
		peer.blobs[c.GetFileIdString()] = bytes.Repeat([]byte{[]byte{'O', 'A', 'B'}[i]}, 256)
	}
	startFakeFiler(t, w, peer)
	w.mapPbIdFromFilerToLocal(base)
	w.mapPbIdFromFilerToLocal(pending)
	fh := newFileHandle(w, 1, 42, pending)
	t.Cleanup(fh.dirtyPages.Destroy)
	fh.RememberPath("/dir/file")
	fh.baseEntry.Store(base)
	fh.dirtyMetadata = true
	if code := w.doFlush(context.Background(), fh, 1000, 20, false, 0); code != fuse.OK {
		t.Fatal(code)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := append(bytes.Repeat([]byte{'O'}, 256), bytes.Repeat([]byte{'A'}, 256)...)
	want = append(want, bytes.Repeat([]byte{'B'}, 256)...)
	if peer.rejected != 1 || peer.entry.Attributes.FileMode != 0600 || peer.entry.Attributes.FileSize != 768 || !bytes.Equal(g6StoredBytes(t, peer), want) {
		t.Fatal("prefix rebase lost bytes, size or peer attributes")
	}
}

func TestAsyncRefusalAfterForgetDoesNotPoisonRelookup(t *testing.T) {
	testAsyncRefusalAfterForget(t, false)
}

func TestAsyncPublicationBlockedBeforeCaptureDoesNotPoisonRelookup(t *testing.T) {
	testAsyncRefusalAfterForget(t, true)
}

func testAsyncRefusalAfterForget(t *testing.T, publicationBlocked bool) {
	base := attributeAppendEntry(1)
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry), refuse: true}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once, resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	peer.beforeCreate = func(ctx context.Context) {
		once.Do(func() {
			close(entered)
			select {
			case <-resume:
			case <-ctx.Done():
			}
		})
	}
	w, handle := g6AppendHandle(t, peer, base, 1000, true)
	w.pendingAsyncFlush = make(map[uint64]chan struct{})
	w.startAsyncFlushWorkers(1)
	t.Cleanup(w.WaitForAsyncFlush)
	t.Cleanup(release)
	unlockPublication := func() {}
	if publicationBlocked {
		unlock := w.lockMetadataPublication("/dir/file")
		var unlockOnce sync.Once
		unlockPublication = func() { unlockOnce.Do(unlock) }
		t.Cleanup(unlockPublication)
	}
	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: 256, Size: 256}, bytes.Repeat([]byte{'A'}, 256)); n != 256 || code != fuse.OK {
		t.Fatal(n, code)
	}
	w.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle})
	w.pendingAsyncFlushMu.Lock()
	done := w.pendingAsyncFlush[42]
	w.pendingAsyncFlushMu.Unlock()
	if done == nil {
		t.Fatal("Release did not queue an async publication")
	}
	waitEntered := func() {
		t.Helper()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("async publication did not reach the filer")
		}
	}
	if publicationBlocked {
		if blocked, _ := waitFlushPublication(t, make(chan fuse.Status)); !blocked {
			t.Fatal("worker was not blocked before publication capture")
		}
	} else {
		waitEntered()
	}
	w.Forget(42, 1)
	w.inodeToPath.Lookup("/dir/file", 1, false, false, 42, true)
	if publicationBlocked {
		unlockPublication()
		waitEntered()
	}
	release()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("async refusal did not finish")
	}
	peer.mu.Lock()
	if peer.rejected != 4*(maxContentFlushMerges+1) {
		t.Errorf("refusals = %d, want sixteen (four outer attempts, four merge attempts each)", peer.rejected)
	}
	peer.refuse = false
	peer.mu.Unlock()
	if _, found := w.fhMap.FindFileHandle(42); found {
		t.Fatal("async close retained a handle")
	}
	out := &fuse.OpenOut{}
	if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY | syscall.O_APPEND}, out); code != fuse.OK {
		t.Fatal(code)
	}
	g6AppendUploader(t, w.GetHandle(FileHandleId(out.Fh)), 1100, peer)
	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh, Offset: 256, Size: 256}, bytes.Repeat([]byte{'B'}, 256)); n != 256 || code != fuse.OK {
		t.Fatal(n, code)
	}
	if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh}); code != fuse.OK {
		t.Fatalf("new inode lifetime inherited stale EIO: %v", code)
	}
	w.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh, ReleaseFlags: fuse.FUSE_RELEASE_FLOCK_UNLOCK})
	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := append(bytes.Repeat([]byte{'O'}, 256), bytes.Repeat([]byte{'B'}, 256)...)
	if peer.entry.Attributes.FileSize != 512 || !bytes.Equal(g6StoredBytes(t, peer), want) {
		t.Fatal("relookup publication changed original/new bytes or size")
	}
}

func TestAppendAfterPendingPositionalExtensionPublishes(t *testing.T) {
	base := attributeAppendEntry(1)
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, appendHandle := g6AppendHandle(t, peer, base, 1000)
	out := &fuse.OpenOut{}
	if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY}, out); code != fuse.OK {
		t.Fatal(code)
	}
	write := func(h, offset uint64, value byte) {
		t.Helper()
		n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: h, Offset: offset, Size: 256}, bytes.Repeat([]byte{value}, 256))
		if n != 256 || code != fuse.OK {
			t.Fatal("write", n, code)
		}
	}
	write(out.Fh, 256, 'A')
	write(appendHandle, 512, 'B') // the kernel appends at its EOF, which includes A
	if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: appendHandle}); code != fuse.OK {
		t.Fatal("append over a pending extension", code)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := append(bytes.Repeat([]byte{'O'}, 256), bytes.Repeat([]byte{'A'}, 256)...)
	want = append(want, bytes.Repeat([]byte{'B'}, 256)...)
	if !bytes.Equal(g6StoredBytes(t, peer), want) {
		t.Fatal("pending extension or append bytes changed")
	}
}

func TestPositionalSparseWriteStillPublishes(t *testing.T) {
	base := attributeAppendEntry(1)
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, _ := g6AppendHandle(t, peer, base, 900)
	out := &fuse.OpenOut{}
	if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY}, out); code != fuse.OK {
		t.Fatal(code)
	}
	n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh, Offset: 512, Size: 256}, bytes.Repeat([]byte{'S'}, 256))
	if n != 256 || code != fuse.OK {
		t.Fatal(n, code)
	}
	if code = w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh}); code != fuse.OK {
		t.Fatal("positional sparse write", code)
	}
	assertSparseWrite(t, peer)
}

func TestSparseWriteAfterAppendFlagClearedPublishes(t *testing.T) {
	base := attributeAppendEntry(1)
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 950)
	n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: 512, Size: 256, Flags: syscall.O_WRONLY}, bytes.Repeat([]byte{'S'}, 256))
	if n != 256 || code != fuse.OK {
		t.Fatal(n, code)
	}
	if code = w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle}); code != fuse.OK {
		t.Fatal("sparse write after clearing O_APPEND", code)
	}
	assertSparseWrite(t, peer)
}

func assertSparseWrite(t *testing.T, peer *g6ConditionalPeer) {
	t.Helper()
	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := append(bytes.Repeat([]byte{'O'}, 256), make([]byte, 256)...)
	want = append(want, bytes.Repeat([]byte{'S'}, 256)...)
	if !bytes.Equal(g6StoredBytes(t, peer), want) {
		t.Fatal("sparse positional data changed")
	}
}

func TestTruncatingOpenDiscardsRefusedAppend(t *testing.T) {
	base := attributeAppendEntry(1)
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 1700)
	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: 512, Size: 256}, bytes.Repeat([]byte{'A'}, 256)); n != 256 || code != fuse.OK {
		t.Fatal(n, code)
	}
	if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle}); code == fuse.OK {
		t.Fatal("append past the committed EOF published")
	}
	dir, _ := w.inodeToPath.GetInode("/dir")
	out := &fuse.CreateOut{}
	if code := w.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dir}, Flags: syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC, Mode: 0644}, "file", out); code != fuse.OK {
		t.Fatal("O_TRUNC create", code)
	}
	g6AppendUploader(t, w.GetHandle(FileHandleId(out.Fh)), 1800, peer)
	payload := bytes.Repeat([]byte{'Z'}, 256)
	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh, Size: 256}, payload); n != 256 || code != fuse.OK {
		t.Fatal(n, code)
	}
	if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh}); code != fuse.OK {
		t.Fatal("rewrite after an O_TRUNC open", code)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if !bytes.Equal(g6StoredBytes(t, peer), payload) {
		t.Fatal("truncate and rewrite did not publish the new bytes")
	}
}

type g6TtlRuleFiler struct{ *g6ConditionalPeer }

func (s *g6TtlRuleFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if r.Entry.IsDirectory {
		return &filer_pb.CreateEntryResponse{}, nil
	}
	r = proto.Clone(r).(*filer_pb.CreateEntryRequest)
	if r.Entry.Attributes.TtlSec == 0 {
		r.Entry.Attributes.TtlSec = 3600
	}
	return s.g6ConditionalPeer.CreateEntry(ctx, r)
}

func (s *g6TtlRuleFiler) UpdateEntry(ctx context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	if r.Entry.IsDirectory {
		return &filer_pb.UpdateEntryResponse{}, nil
	}
	return s.g6ConditionalPeer.UpdateEntry(ctx, r)
}

func TestEagerCreateWriteKeepsFilerDefaults(t *testing.T) {
	peer := &g6ConditionalPeer{blobs: make(map[string][]byte)}
	w := newInvalidateTestWFS(t)
	w.option.EagerFilerCreate = true
	w.option.ConcurrentWriters = 1
	w.concurrentWriters = util.NewLimitedConcurrentExecutor(1)
	startFakeFiler(t, w, &g6TtlRuleFiler{peer})
	dir := w.inodeToPath.Lookup("/dir", 1, true, false, 0, false)
	w.inodeToPath.MarkChildrenCached("/")
	w.inodeToPath.MarkChildrenCached("/dir")
	dirEntry := &filer_pb.Entry{Name: "dir", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0777), Inode: dir}}
	if err := w.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", dirEntry), 1); err != nil {
		t.Fatal(err)
	}
	out := &fuse.CreateOut{}
	if code := w.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dir, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 20}}}, Flags: syscall.O_WRONLY, Mode: 0644}, "file", out); code != fuse.OK {
		t.Fatal("create", code)
	}
	g6AppendUploader(t, w.GetHandle(FileHandleId(out.Fh)), 2100, peer)
	payload := bytes.Repeat([]byte{'E'}, 256)
	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh, Size: 256}, payload); n != 256 || code != fuse.OK {
		t.Fatal(n, code)
	}
	if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}); code != fuse.OK {
		t.Fatal("write after an eager create", code)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.entry.Attributes.TtlSec != 3600 || !bytes.Equal(g6StoredBytes(t, peer), payload) {
		t.Fatal("eager create lost the stored defaults or the written bytes")
	}
}

type g6LostAnswerFiler struct {
	*g6ConditionalPeer
	lost atomic.Bool
}

func (s *g6LostAnswerFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	resp, err := s.g6ConditionalPeer.CreateEntry(ctx, r)
	if err == nil && resp.Error == "" && s.lost.CompareAndSwap(false, true) {
		return nil, status.Error(codes.Unavailable, "answer lost after commit")
	}
	return resp, err
}

func TestLostAnswerToOwnShrinkingFlushIsNotAConflict(t *testing.T) {
	base := attributeAppendEntry(2)
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 2200)
	fake := &g6LostAnswerFiler{g6ConditionalPeer: peer}
	startFakeFiler(t, w, fake)
	if code := w.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: 42}, Valid: fuse.FATTR_SIZE | fuse.FATTR_FH, Fh: handle}}, &fuse.AttrOut{}); code != fuse.OK {
		t.Fatal("ftruncate", code)
	}
	payload := bytes.Repeat([]byte{'N'}, 128)
	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Size: 128}, payload); n != 128 || code != fuse.OK {
		t.Fatal(n, code)
	}
	if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle}); code != fuse.OK {
		t.Fatal("own committed overwrite reported as a conflict", code)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if !fake.lost.Load() || !bytes.Equal(g6StoredBytes(t, peer), payload) {
		t.Fatal("lost answer was not exercised or the overwrite did not land")
	}
}
