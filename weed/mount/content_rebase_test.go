//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"io"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func g6AppendHandle(t *testing.T, peer *g6ConditionalPeer, base *filer_pb.Entry, key uint64) (*WFS, uint64) {
	t.Helper()
	w := newInvalidateTestWFS(t)
	w.option.DefaultPermissions = true
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

// Reproduce the kernel's retained append position after a failed fsync. The
// next open must not acknowledge content beyond a silently discarded chunk.
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

// Unlike the inherited attribute fixture, this peer checks CreateEntry's
// write condition and returns the server's committed entry/stamp.
type g6ConditionalPeer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu       sync.Mutex
	entry    *filer_pb.Entry
	rejected int
	refuse   bool
	blobs    map[string][]byte
	clock    int64
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
func (s *g6ConditionalPeer) CreateEntry(_ context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
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
	if code := w.doFlush(context.Background(), fh, 1000, 20, false); code != fuse.OK {
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

func TestRealConflictCannotPublishAppendGap(t *testing.T) {
	for _, mode := range []struct {
		name      string
		writeback bool
		coalesced bool
	}{{"normal", false, false}, {"writeback", true, false}, {"writeback-coalesced", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			base := attributeAppendEntry(1)
			peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
			w, handle := g6AppendHandle(t, peer, base, 600)
			w.option.WritebackCache = mode.writeback
			write := func(h, offset uint64, value byte) {
				t.Helper()
				n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: h, Offset: offset, Size: 256}, bytes.Repeat([]byte{value}, 256))
				if n != 256 || code != fuse.OK {
					t.Fatal("write", n, code)
				}
			}
			write(handle, 256, 'A')
			replacement := attributeAppendEntry(1)
			replacement.CommitTsNs = 20000
			replacement.Chunks[0].Fid.FileKey = 900
			replacement.Attributes.FileMode = 0600
			peer.mu.Lock()
			peer.entry = proto.Clone(replacement).(*filer_pb.Entry)
			peer.clock = replacement.CommitTsNs
			peer.blobs[replacement.Chunks[0].GetFileIdString()] = bytes.Repeat([]byte{'P'}, 256)
			peer.mu.Unlock()
			if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle}); code != fuse.EIO {
				t.Fatal("real conflict must return EIO", code)
			}
			w.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, ReleaseFlags: fuse.FUSE_RELEASE_FLOCK_UNLOCK})
			if _, found := w.fhMap.FindFileHandle(42); found {
				t.Fatal("last close retained a conflicted handle")
			}
			if err := w.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", replacement), 20000); err != nil {
				t.Fatal(err)
			}
			open := func(key uint64) uint64 {
				t.Helper()
				out := &fuse.OpenOut{}
				if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY | syscall.O_APPEND}, out); code != fuse.OK {
					t.Fatal(code)
				}
				g6AppendUploader(t, w.GetHandle(FileHandleId(out.Fh)), key, peer)
				return out.Fh
			}
			next := open(700)
			if mode.coalesced {
				// Model a writeback page that contains the abandoned interval.
				payload := append(bytes.Repeat([]byte{'P'}, 256), make([]byte, 256)...)
				payload = append(payload, bytes.Repeat([]byte{'B'}, 256)...)
				if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: next, Offset: 0, Size: uint32(len(payload))}, payload); n != uint32(len(payload)) || code != fuse.OK {
					t.Fatal(n, code)
				}
			} else {
				write(next, 512, 'B') // stale kernel EOF after the refused A
			}
			for i := 0; i < 2; i++ {
				if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: next}); code != fuse.EIO {
					t.Fatalf("gap fsync falsely succeeded: %v", code)
				}
				peer.mu.Lock()
				unchanged := g6EntryEqual(peer.entry, replacement)
				peer.mu.Unlock()
				if !unchanged {
					t.Fatal("gap/conflict changed stored content")
				}
			}
			w.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: next, ReleaseFlags: fuse.FUSE_RELEASE_FLOCK_UNLOCK})
			if mode.writeback {
				if _, failed := w.writebackContentErrors.Load(uint64(42)); !failed {
					t.Fatal("close cleared the writeback content error")
				}
				w.inodeToPath.Lookup("/dir/file", 1, false, false, 42, true)
				w.Forget(42, 1)
				if _, failed := w.writebackContentErrors.Load(uint64(42)); !failed {
					t.Fatal("partial Forget cleared the writeback content error")
				}
				w.Forget(42, 1)
				if _, failed := w.writebackContentErrors.Load(uint64(42)); failed {
					t.Fatal("final Forget did not clear the writeback content error")
				}
				w.inodeToPath.Lookup("/dir/file", 1, false, false, 42, true)
			}
			correct := open(800)
			write(correct, 256, 'C') // caller has now re-read the actual EOF
			if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: correct}); code != fuse.OK {
				t.Fatal(code)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			want := append(bytes.Repeat([]byte{'P'}, 256), bytes.Repeat([]byte{'C'}, 256)...)
			if peer.entry.Attributes.FileSize != 512 || peer.entry.Attributes.FileMode != 0600 || !bytes.Equal(g6StoredBytes(t, peer), want) {
				t.Fatal("correct-EOF recovery changed bytes, size or attrs")
			}
		})
	}
}

// The append invariant must not outlaw an intentional sparse positional write.
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
	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := append(bytes.Repeat([]byte{'O'}, 256), make([]byte, 256)...)
	want = append(want, bytes.Repeat([]byte{'S'}, 256)...)
	if !bytes.Equal(g6StoredBytes(t, peer), want) {
		t.Fatal("sparse positional data changed")
	}
}
