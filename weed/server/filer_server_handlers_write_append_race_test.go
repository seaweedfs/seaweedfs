package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// appendRaceFindHookStore runs a hook after each FindEntry returns, so a test
// can interleave another writer between a caller's read and its write.
type appendRaceFindHookStore struct {
	*renameTestStore
	hookMu sync.Mutex
	hook   func(p util.FullPath)
}

func (s *appendRaceFindHookStore) FindEntry(ctx context.Context, p util.FullPath) (*filer.Entry, error) {
	entry, err := s.renameTestStore.FindEntry(ctx, p)
	s.hookMu.Lock()
	hook := s.hook
	s.hookMu.Unlock()
	if hook != nil {
		hook(p)
	}
	return entry, err
}

func (s *appendRaceFindHookStore) setHook(hook func(p util.FullPath)) {
	s.hookMu.Lock()
	s.hook = hook
	s.hookMu.Unlock()
}

// appendRaceSyncStore records, per path, whether each entry write was asked
// to be durable before returning.
type appendRaceSyncStore struct {
	*renameTestStore
	mu     sync.Mutex
	synced map[util.FullPath][]bool
}

func (s *appendRaceSyncStore) record(ctx context.Context, p util.FullPath) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.synced[p] = append(s.synced[p], filer.SyncWrite(ctx))
}

func (s *appendRaceSyncStore) writes(p util.FullPath) []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.synced[p]...)
}

func (s *appendRaceSyncStore) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	s.record(ctx, entry.FullPath)
	return s.renameTestStore.InsertEntry(ctx, entry)
}

func (s *appendRaceSyncStore) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	s.record(ctx, entry.FullPath)
	return s.renameTestStore.UpdateEntry(ctx, entry)
}

// newAppendRaceFilers starts two gRPC-serving filers on one store with the same
// two-member ring. Only the second filer's reads run the hook; ownerWrites
// records the first filer's entry writes.
func newAppendRaceFilers(t *testing.T) (owner, other *FilerServer, store *appendRaceFindHookStore, ownerWrites *appendRaceSyncStore) {
	t.Helper()
	inner := newRenameTestStore()
	inner.entries["/"] = newDirectoryEntry("/", 1)
	store = &appendRaceFindHookStore{renameTestStore: inner}
	ownerWrites = &appendRaceSyncStore{renameTestStore: inner, synced: map[util.FullPath][]bool{}}

	newServer := func(hooked bool) (*FilerServer, net.Listener) {
		f := newRenameTestFiler(t, inner)
		if hooked {
			f.Store = filer.NewFilerStoreWrapper(store)
		} else {
			f.Store = filer.NewFilerStoreWrapper(ownerWrites)
		}
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		return &FilerServer{
			filer:          f,
			option:         &FilerOption{SaveToFilerLimit: 64, MaxMB: 1},
			entryLockTable: util.NewLockTable[util.FullPath](),
			grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, lis
	}
	owner, ownerLis := newServer(false)
	other, otherLis := newServer(true)
	addr := func(lis net.Listener) pb.ServerAddress {
		return pb.NewServerAddressWithGrpcPort(lis.Addr().String(), lis.Addr().(*net.TCPAddr).Port)
	}
	ring := []pb.ServerAddress{addr(ownerLis), addr(otherLis)}
	for _, s := range []struct {
		fs  *FilerServer
		lis net.Listener
	}{{owner, ownerLis}, {other, otherLis}} {
		self := addr(s.lis)
		s.fs.option.Host = self
		s.fs.filer.Dlm = lock_manager.NewDistributedLockManager(self)
		s.fs.filer.Dlm.LockRing.SetSnapshot(ring, 1)
		srv := grpc.NewServer()
		filer_pb.RegisterSeaweedFilerServer(srv, s.fs)
		go srv.Serve(s.lis)
		t.Cleanup(srv.Stop)
		pb.InvalidateGrpcConnection(self.ToGrpcAddress())
	}
	return owner, other, store, ownerWrites
}

// ownerPath returns a file path whose writes the ring gives to owner.
func ownerPath(t *testing.T, owner, other *FilerServer) string {
	t.Helper()
	for i := 0; i < 4000; i++ {
		p := fmt.Sprintf("/append-%d.txt", i)
		if other.writeOwner(entryRouteKey(util.FullPath(p))) == owner.option.Host {
			return p
		}
	}
	t.Skip("no path owned by the first filer")
	return ""
}

// An HTTP append on a filer that does not own the entry must not overwrite an
// append another filer commits after this one read the entry: either both
// acknowledged appends are in the final entry, or this append is refused.
// The other append is injected at the store right after one of this filer's
// reads of the entry, each read in turn, with and without fsync.
func TestHttpAppendDoesNotOverwriteConcurrentAppend(t *testing.T) {
	for _, fsync := range []bool{false, true} {
		injected := 0
		for read := 1; read <= 8; read++ {
			t.Run(fmt.Sprintf("fsync_%v/after_read_%d", fsync, read), func(t *testing.T) {
				if httpAppendWithConcurrentAppend(t, read, fsync) {
					injected++
				}
			})
		}
		if injected == 0 {
			t.Fatalf("fsync=%v: the other filer's append was never injected: the race was not exercised", fsync)
		}
	}
}

func httpAppendWithConcurrentAppend(t *testing.T, atRead int, fsync bool) bool {
	ctx := context.Background()
	if fsync {
		ctx = filer.WithSyncWrite(ctx)
	}
	oldContent := []byte("old-")
	appendA := []byte("A-")
	appendB := []byte("B-")

	owner, fs, store, _ := newAppendRaceFilers(t)
	filePath := ownerPath(t, owner, fs)
	fileName := util.FullPath(filePath).Name()
	if err := store.renameTestStore.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Atime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			Mode:     0640,
			FileSize: uint64(len(oldContent)),
		},
		Content: append([]byte(nil), oldContent...),
	}); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}

	so := &operation.StorageOption{}
	r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", bytes.NewReader(appendA))
	r.ContentLength = -1

	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(
		ctx, httptest.NewRecorder(), r, r.Body, 4, fileName, "", -1, so,
	)
	if err != nil {
		t.Fatalf("prepare append: %v", err)
	}

	var mu sync.Mutex
	reads := 0
	otherWrote := false
	store.setHook(func(p util.FullPath) {
		if p != util.FullPath(filePath) {
			return
		}
		mu.Lock()
		reads++
		hit := reads == atRead
		mu.Unlock()
		if !hit {
			return
		}
		content := append(append([]byte(nil), oldContent...), appendB...)
		if err := store.renameTestStore.UpdateEntry(ctx, &filer.Entry{
			FullPath: util.FullPath(filePath),
			Attr: filer.Attr{
				Mtime:    time.Unix(2, 0),
				Atime:    time.Unix(2, 0),
				Crtime:   time.Unix(1, 0),
				Mode:     0640,
				FileSize: uint64(len(content)),
			},
			Content: content,
		}); err != nil {
			t.Errorf("the other filer's append: %v", err)
			return
		}
		mu.Lock()
		otherWrote = true
		mu.Unlock()
	})

	_, saveErr, _ := fs.saveMetaData(ctx, r, fileName, "", so, 4, md5Hash.Sum(nil), fileChunks, chunkOffset, smallContent)
	store.setHook(nil)

	mu.Lock()
	wrote, sawReads := otherWrote, reads
	mu.Unlock()
	if !wrote {
		t.Skipf("the append reads the entry %d times before its commit; no read %d", sawReads, atRead)
	}

	final, err := owner.filer.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find final entry: %v", err)
	}
	got := string(final.Content)
	if saveErr != nil {
		if got != string(oldContent)+string(appendB) {
			t.Errorf("append refused (%v) but the entry is %q, want %q", saveErr, got, string(oldContent)+string(appendB))
		}
		return true
	}
	if !strings.Contains(got, string(appendB)) || !strings.Contains(got, string(appendA)) {
		t.Errorf("both appends were acknowledged but the entry is %q: the other filer's %q was overwritten", got, appendB)
	}
	return true
}

// Without a concurrent writer, an append on a non-owner commits through the
// owner, keeping its fsync, and lands in full.
func TestHttpAppendOnNonOwnerCommitsThroughOwner(t *testing.T) {
	for _, fsync := range []bool{false, true} {
		t.Run(fmt.Sprintf("fsync_%v", fsync), func(t *testing.T) {
			httpAppendOnNonOwner(t, fsync)
		})
	}
}

func httpAppendOnNonOwner(t *testing.T, fsync bool) {
	ctx := context.Background()
	if fsync {
		ctx = filer.WithSyncWrite(ctx)
	}
	owner, fs, store, ownerWrites := newAppendRaceFilers(t)
	filePath := ownerPath(t, owner, fs)
	fileName := util.FullPath(filePath).Name()
	if err := store.renameTestStore.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr:     filer.Attr{Mtime: time.Unix(1, 0), Atime: time.Unix(1, 0), Crtime: time.Unix(1, 0), Mode: 0640, FileSize: 4},
		Content:  []byte("old-"),
	}); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}

	so := &operation.StorageOption{}
	r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", bytes.NewReader([]byte("A-")))
	r.ContentLength = -1
	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(
		ctx, httptest.NewRecorder(), r, r.Body, 4, fileName, "", -1, so,
	)
	if err != nil {
		t.Fatalf("prepare append: %v", err)
	}
	if _, saveErr, _ := fs.saveMetaData(ctx, r, fileName, "", so, 4, md5Hash.Sum(nil), fileChunks, chunkOffset, smallContent); saveErr != nil {
		t.Fatalf("append: %v", saveErr)
	}
	final, err := owner.filer.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find final entry: %v", err)
	}
	if got := string(final.Content); got != "old-A-" {
		t.Fatalf("entry is %q, want %q", got, "old-A-")
	}
	if got := ownerWrites.writes(util.FullPath(filePath)); len(got) != 1 || got[0] != fsync {
		t.Errorf("the owner's writes asked for sync %v, want [%v]", got, fsync)
	}
}

// A create that asks for fsync reaches the store as a sync write.
func TestCreateEntryFsyncReachesStore(t *testing.T) {
	owner, _, _, ownerWrites := newAppendRaceFilers(t)
	for _, fsync := range []bool{false, true} {
		p := util.FullPath(fmt.Sprintf("/fsync-%v.txt", fsync))
		resp, err := owner.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: "/",
			Entry:     &filer_pb.Entry{Name: p.Name(), Attributes: &filer_pb.FuseAttributes{Mtime: 1, FileMode: 0640}},
			Fsync:     fsync,
		})
		if err != nil || resp.Error != "" {
			t.Fatalf("create %s: %v %s", p, err, resp.GetError())
		}
		if got := ownerWrites.writes(p); len(got) != 1 || got[0] != fsync {
			t.Errorf("fsync=%v: the store writes asked for sync %v, want [%v]", fsync, got, fsync)
		}
	}
}

// A store that keeps entries as given returns FileSize zero beside chunks; an
// uncontended append or offset write on a non-owner must still commit through
// the owner and land after the existing bytes.
func TestHttpWriteOnNonOwnerWithUnnormalizedEntry(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		offset      int64
	}{
		{"offset write", "", 4},
		{"append", "?op=append", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			owner, fs, store, _ := newAppendRaceFilers(t)
			filePath := ownerPath(t, owner, fs)
			fileName := util.FullPath(filePath).Name()
			if err := store.renameTestStore.InsertEntry(ctx, &filer.Entry{
				FullPath: util.FullPath(filePath),
				Attr:     filer.Attr{Mtime: time.Unix(1, 0), Atime: time.Unix(1, 0), Crtime: time.Unix(1, 0), Mode: 0640},
				Chunks:   []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 4, ModifiedTsNs: 1}},
			}); err != nil {
				t.Fatalf("insert chunked entry: %v", err)
			}

			r := httptest.NewRequest(http.MethodPut, filePath+tc.query, nil)
			fileChunks := []*filer_pb.FileChunk{{FileId: "3,02637037d6", Offset: tc.offset, Size: 2, ModifiedTsNs: 2}}
			if _, saveErr, _ := fs.saveMetaData(ctx, r, fileName, "", &operation.StorageOption{}, 4, nil, fileChunks, tc.offset+2, nil); saveErr != nil {
				t.Fatalf("write: %v", saveErr)
			}
			final, err := owner.filer.FindEntry(ctx, util.FullPath(filePath))
			if err != nil {
				t.Fatalf("find final entry: %v", err)
			}
			if got := filer.TotalSize(final.GetChunks()); got != 6 {
				t.Fatalf("entry covers %d bytes, want 6", got)
			}
			for _, chunk := range final.GetChunks() {
				if chunk.FileId == "3,02637037d6" && chunk.Offset != 4 {
					t.Fatalf("new chunk starts at %d, want 4", chunk.Offset)
				}
			}
			if final.FileSize != 6 {
				t.Fatalf("entry size is %d, want 6", final.FileSize)
			}
		})
	}
}
