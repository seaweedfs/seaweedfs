//go:build !freebsd

package mount

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The writer's acknowledged entry and a second mount's cached entry diverge
// before the attribute syscall. The filer implements the existing atomic
// IF_ENTRY_EQUAL contract; no sleeps or FUSE device are needed for this order.
type attributeCASFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu                 sync.Mutex
	entry              *filer_pb.Entry
	updates, conflicts int
	beforeUpdate       func(int, *filer_pb.Entry)
	afterCommit        func()
	updateErr          error
	missing            bool
}

func (s *attributeCASFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.missing {
		return nil, status.Error(codes.NotFound, "deleted")
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(s.entry).(*filer_pb.Entry)}, nil
}

func (s *attributeCASFiler) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates++
	if s.updateErr != nil {
		return nil, s.updateErr
	}
	if s.beforeUpdate != nil {
		s.beforeUpdate(s.updates, s.entry)
	}
	if clauses := req.GetCondition().GetClauses(); len(clauses) > 0 && !proto.Equal(clauses[0].GetExpectedEntry(), s.entry) {
		s.conflicts++
		return nil, status.Error(codes.FailedPrecondition, "entry changed")
	}
	s.entry = proto.Clone(req.Entry).(*filer_pb.Entry)
	if s.afterCommit != nil {
		// Release the owner lock: concurrent CreateEntry must be able to publish
		// while the committed UpdateEntry response is held at this barrier.
		s.mu.Unlock()
		s.afterCommit()
		s.mu.Lock()
	}
	return &filer_pb.UpdateEntryResponse{LogTsNs: 2000}, nil
}

func TestAttributeOnlyConflictOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flags   uint32
		prepare func(*attributeCASFiler)
		want    fuse.Status
		calls   int
	}{
		{"bounded", 0, func(s *attributeCASFiler) {
			s.beforeUpdate = func(n int, e *filer_pb.Entry) { e.Extended["concurrent"] = []byte(fmt.Sprint(n)) }
		}, fuse.Status(syscall.EAGAIN), 3},
		{"replacement", 0, func(s *attributeCASFiler) { s.entry.Attributes.Inode = 99 }, fuse.Status(syscall.ESTALE), 1},
		{"deleted", 0, func(s *attributeCASFiler) { s.missing = true }, fuse.ENOENT, 1},
		{"create-existing", xattr_CREATE, func(s *attributeCASFiler) { s.entry.Extended["xattr-user.test"] = []byte("other mount") }, fuse.Status(syscall.EEXIST), 1},
		{"replace-removed", xattr_REPLACE, func(s *attributeCASFiler) { delete(s.entry.Extended, "xattr-user.test") }, fuse.ENODATA, 1},
		{"permission", 0, func(s *attributeCASFiler) { s.updateErr = status.Error(codes.PermissionDenied, "refused") }, fuse.Status(syscall.EACCES), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			cached := attributeAppendEntry(1)
			if tc.flags == xattr_REPLACE {
				cached.Extended["xattr-user.test"] = []byte("cached")
			}
			fake := &attributeCASFiler{entry: attributeAppendEntry(300)}
			tc.prepare(fake)
			startFakeFiler(t, wfs, fake)
			path := util.FullPath("/dir/file")
			wfs.inodeToPath.Lookup(util.FullPath("/dir"), 1, true, false, 0, false)
			wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
			wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
			if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", cached), 1); err != nil {
				t.Fatal(err)
			}
			in := &fuse.SetXAttrIn{Flags: tc.flags}
			in.NodeId = 42
			if code := wfs.SetXAttr(nil, in, "user.test", []byte("changed")); code != tc.want {
				t.Fatalf("got %v, want %v", code, tc.want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.updates != tc.calls {
				t.Fatalf("updates=%d want %d", fake.updates, tc.calls)
			}
			if len(fake.entry.Chunks) != 300 || fake.entry.Attributes.FileSize != 76800 {
				t.Fatal("failed attribute op changed content")
			}
		})
	}
}

func (s *attributeCASFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entry = proto.Clone(req.Entry).(*filer_pb.Entry)
	return &filer_pb.CreateEntryResponse{LogTsNs: 2000}, nil
}

func attributeAppendEntry(records int) *filer_pb.Entry {
	e := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{
		Inode: 42, Uid: 2000, Gid: 20, FileMode: 0644, FileSize: uint64(records * 256), Mtime: 10,
	}, Extended: map[string][]byte{"xattr-user.other": []byte("keep"), "xattr-user.remove": []byte("remove")}}
	for i := 0; i < records; i++ {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(i + 1), Cookie: 1}, Offset: int64(i * 256), Size: 256, ModifiedTsNs: int64(i + 1)})
	}
	return e
}

// A peer's acknowledged attribute change leaves the content alone, so our
// append's conditional flush must still publish, keeping the peer's change.
func TestAppendFlushAfterPeerAttributeChange(t *testing.T) {
	for _, op := range []string{"none", "chmod", "chown", "utimes", "setxattr"} {
		t.Run(op, func(t *testing.T) {
			writer, peer := newInvalidateTestWFS(t), newInvalidateTestWFS(t)
			peer.atimeMap = make(map[uint64]time.Time)
			base := attributeAppendEntry(1)
			base.CommitTsNs = 100
			fake := &contentFlushConditionFiler{entry: proto.Clone(base).(*filer_pb.Entry)}
			startFakeFiler(t, writer, fake)
			startFakeFiler(t, peer, fake)
			path := util.FullPath("/dir/file")
			local := proto.Clone(base).(*filer_pb.Entry)
			writer.mapPbIdFromFilerToLocal(local)
			fh := newFileHandle(writer, 1, 42, local)
			fh.RememberPath(path)
			t.Cleanup(fh.dirtyPages.Destroy)
			writer.fhMap.inode2fh[42], writer.fhMap.fh2inode[1] = fh, 42
			appended := attributeAppendEntry(2).Chunks[1]
			fh.UpdateEntry(func(e *filer_pb.Entry) {
				e.Chunks = append(e.Chunks, appended)
				e.Attributes.FileSize = 512
			})
			fh.dirtyMetadata = true

			peer.inodeToPath.Lookup("/dir", 1, true, false, 0, false)
			peer.inodeToPath.Lookup(path, 1, false, false, 42, true)
			peer.inodeToPath.MarkChildrenCached("/dir")
			if err := peer.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", base), 1); err != nil {
				t.Fatal(err)
			}
			code := fuse.OK
			in := &fuse.SetAttrIn{}
			in.NodeId = 42
			switch op {
			case "chmod":
				in.Valid, in.Mode = fuse.FATTR_MODE, 0600
			case "chown":
				in.Valid, in.Owner.Uid, in.Owner.Gid = fuse.FATTR_UID|fuse.FATTR_GID, 1001, 21
			case "utimes":
				in.Valid, in.Mtime = fuse.FATTR_MTIME, 123
			case "setxattr":
				code = peer.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: 42}}, "user.test", []byte("peer"))
			}
			if in.Valid != 0 {
				code = peer.SetAttr(nil, in, &fuse.AttrOut{})
			}
			if code != fuse.OK {
				t.Fatalf("peer %s: %v", op, code)
			}

			if code = writer.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: 1}); code != fuse.OK {
				t.Fatalf("append fsync after peer %s: %v", op, code)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			got := fake.entry
			if len(got.Chunks) != 2 || got.Attributes.FileSize != 512 {
				t.Fatalf("append not published after peer %s: %v", op, got)
			}
			a := got.Attributes
			kept := map[string]bool{
				"none":     true,
				"chmod":    a.FileMode&0777 == 0600,
				"chown":    a.Uid == 1001 && a.Gid == 21,
				"utimes":   a.Mtime == 123,
				"setxattr": string(got.Extended["xattr-user.test"]) == "peer",
			}[op]
			if !kept {
				t.Fatalf("append flush dropped the peer's %s: %v", op, got)
			}
		})
	}
}

func TestAttributeOnlyOpsPreserveAcknowledgedAppends(t *testing.T) {
	for _, op := range []string{"chmod", "chown", "utimes", "setxattr", "removexattr"} {
		for _, stale := range []bool{false, true} {
			for _, open := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stale=%t/open=%t", op, stale, open), func(t *testing.T) {
					wfs := newInvalidateTestWFS(t)
					wfs.atimeMap = make(map[uint64]time.Time)
					current := attributeAppendEntry(300)
					fake := &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
					startFakeFiler(t, wfs, fake)
					cached := proto.Clone(current).(*filer_pb.Entry)
					if stale {
						cached = attributeAppendEntry(1)
						cached.Extended["xattr-user.other"] = []byte("obsolete")
						cached.Attributes.Gid = 17
					}
					path := util.FullPath("/dir/file")
					wfs.inodeToPath.Lookup(util.FullPath("/dir"), 1, true, false, 0, false)
					wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
					wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
					if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", cached), 1); err != nil {
						t.Fatal(err)
					}
					var fh *FileHandle
					if open {
						local := proto.Clone(cached).(*filer_pb.Entry)
						wfs.mapPbIdFromFilerToLocal(local)
						fh = &FileHandle{fh: 1, inode: 42, wfs: wfs, entry: &LockedEntry{Entry: local}}
						fh.dirtyPages = newPageWriter(fh, 1024)
						fh.RememberPath(path)
						wfs.fhMap.inode2fh[42], wfs.fhMap.fh2inode[1] = fh, 42
						t.Cleanup(func() { fh.dirtyPages.Destroy() })
					}
					var code fuse.Status
					out := &fuse.AttrOut{}
					if op == "removexattr" {
						code = wfs.RemoveXAttr(nil, &fuse.InHeader{NodeId: 42}, "user.remove")
					} else if op == "setxattr" {
						in := &fuse.SetXAttrIn{}
						in.NodeId = 42
						code = wfs.SetXAttr(nil, in, "user.test", []byte("new"))
					} else {
						in := &fuse.SetAttrIn{}
						in.NodeId = 42
						switch op {
						case "chmod":
							in.Valid, in.Mode = fuse.FATTR_MODE, 0600
						case "chown":
							in.Valid, in.Owner.Uid, in.Owner.Gid = fuse.FATTR_UID|fuse.FATTR_GID, 1001, 21
						case "utimes":
							in.Valid, in.Mtime, in.Mtimensec = fuse.FATTR_MTIME|fuse.FATTR_ATIME, 123, 456
						}
						code = wfs.SetAttr(nil, in, out)
					}
					if code != fuse.OK {
						t.Fatalf("attribute syscall: %v", code)
					}
					if (op == "chmod" || op == "utimes") && out.Attr.Uid != 1000 {
						t.Fatalf("reply uid %d, want the local id 1000", out.Attr.Uid)
					}
					if open {
						if code := wfs.doFlush(context.Background(), fh, 1000, 20, false); code != fuse.OK {
							t.Fatalf("flush after attribute syscall: %v", code)
						}
					}
					fake.mu.Lock()
					defer fake.mu.Unlock()
					if len(fake.entry.Chunks) != 300 || fake.entry.Attributes.FileSize != 76800 {
						t.Fatalf("lost fsync-acknowledged appends: chunks=%d size=%d, want 300/76800", len(fake.entry.Chunks), fake.entry.Attributes.FileSize)
					}
					for i, chunk := range fake.entry.Chunks {
						if chunk.GetFileIdString() != current.Chunks[i].GetFileIdString() || chunk.Offset != int64(i*256) || chunk.Size != 256 {
							t.Fatalf("acknowledged record %d no longer references its original bytes: %v", i, chunk)
						}
					}
					switch op {
					case "chmod":
						if fake.entry.Attributes.FileMode != 0600 {
							t.Fatal("mode not saved")
						}
					case "chown":
						if fake.entry.Attributes.Uid != 1001 || fake.entry.Attributes.Gid != 21 {
							t.Fatal("owner not saved")
						}
					case "utimes":
						if fake.entry.Attributes.Mtime != 123 || fake.entry.Attributes.MtimeNs != 456 {
							t.Fatal("mtime not saved")
						}
					case "setxattr":
						if string(fake.entry.Extended["xattr-user.test"]) != "new" {
							t.Fatal("xattr not saved")
						}
					case "removexattr":
						if _, exists := fake.entry.Extended["xattr-user.remove"]; exists {
							t.Fatal("xattr not removed")
						}
					}
					if string(fake.entry.Extended["xattr-user.other"]) != "keep" {
						t.Fatal("unrelated xattr lost")
					}
					if op != "chown" && (fake.entry.Attributes.Uid != 2000 || fake.entry.Attributes.Gid != 20) {
						t.Fatal("unrelated owner or uid mapping lost")
					}
				})
			}
		}
	}
}

// The filer holds the attribute RPC while the real WRITE handler dirties the
// same clean handle. Its subsequent flush must keep both the acknowledged
// attribute and the buffered bytes. The upload adapter stores bytes in memory.
func TestAcknowledgedAttributeSurvivesWriteDuringRPC(t *testing.T) {
	for _, op := range []string{"chmod", "chown", "utimes", "setxattr", "removexattr"} {
		t.Run(op, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			wfs.atimeMap = make(map[uint64]time.Time)
			wfs.option.ConcurrentWriters = 1
			wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(1)
			current := attributeAppendEntry(1)
			fake := &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(resume) }) })
			fake.beforeUpdate = func(n int, e *filer_pb.Entry) {
				if n == 1 {
					close(entered)
					<-resume
				}
			}
			startFakeFiler(t, wfs, fake)
			path := util.FullPath("/dir/file")
			wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
			local := proto.Clone(current).(*filer_pb.Entry)
			wfs.mapPbIdFromFilerToLocal(local)
			fh := newFileHandle(wfs, 1, 42, local)
			fh.RememberPath(path)
			wfs.fhMap.inode2fh[42], wfs.fhMap.fh2inode[1] = fh, 42
			save := &flakySave{}
			fh.dirtyPages.randomWriter.(*ChunkedDirtyPages).saveDataAsChunk = save.saveDataAsChunk
			t.Cleanup(fh.dirtyPages.Destroy)
			done := make(chan fuse.Status, 1)
			go func() {
				if op == "setxattr" {
					done <- wfs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: 42}}, "user.test", []byte("acknowledged"))
				} else if op == "removexattr" {
					done <- wfs.RemoveXAttr(nil, &fuse.InHeader{NodeId: 42}, "user.remove")
				} else {
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
					done <- wfs.SetAttr(nil, in, &fuse.AttrOut{})
				}
			}()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("attribute RPC never arrived")
			}
			payload := []byte("background write while attribute RPC waits")
			in := &fuse.WriteIn{}
			in.NodeId, in.Fh, in.Offset, in.Size = 42, 1, 256, uint32(len(payload))
			if n, code := wfs.Write(nil, in, payload); code != fuse.OK || n != uint32(len(payload)) {
				t.Fatalf("WRITE: %d %v", n, code)
			}
			once.Do(func() { close(resume) })
			select {
			case code := <-done:
				if code != fuse.OK {
					t.Fatalf("attribute: %v", code)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("attribute RPC did not finish")
			}
			if code := wfs.doFlush(context.Background(), fh, 1000, 20, false); code != fuse.OK {
				t.Fatalf("flush: %v", code)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			switch op {
			case "chmod":
				if fake.entry.Attributes.FileMode != 0600 {
					t.Fatal("flush undid acknowledged chmod")
				}
			case "chown":
				if fake.entry.Attributes.Uid != 1001 || fake.entry.Attributes.Gid != 21 {
					t.Fatal("flush undid acknowledged chown")
				}
			case "utimes":
				if fake.entry.Attributes.Mtime != 123 || fake.entry.Attributes.MtimeNs != 456 {
					t.Fatal("flush undid acknowledged utimes")
				}
			case "setxattr":
				if string(fake.entry.Extended["xattr-user.test"]) != "acknowledged" {
					t.Fatal("flush undid acknowledged setxattr")
				}
			case "removexattr":
				if _, exists := fake.entry.Extended["xattr-user.remove"]; exists {
					t.Fatal("flush undid acknowledged removexattr")
				}
			}
			if len(save.bodies) != 1 || save.bodies[0] != string(payload) {
				t.Fatalf("pending bytes changed: %q", save.bodies)
			}
			if fake.entry.Attributes.FileSize != 256+uint64(len(payload)) || len(fake.entry.Chunks) != 2 {
				t.Fatalf("pending content lost: %v", fake.entry)
			}
			original, appended := false, false
			for _, chunk := range fake.entry.Chunks {
				switch chunk.Offset {
				case 0:
					original = chunk.GetFileIdString() == current.Chunks[0].GetFileIdString() && chunk.Size == 256
				case 256:
					appended = chunk.GetFileIdString() == "1,01" && chunk.Size == uint64(len(payload))
				}
			}
			if !original || !appended {
				t.Fatalf("chunk identity/range changed: %v", fake.entry.Chunks)
			}

		})
	}
}

// Force owner-commit -> concurrent WRITE -> whole-entry flush -> RPC response.
// The corrected caller must serialize publication across the entire interval.
func TestAttributePublicationBeforeResponse(t *testing.T) {
	for _, contention := range []string{"response", "installation", "retention"} {
		for _, op := range []string{"chmod", "chown", "utimes", "setxattr", "removexattr"} {
			t.Run(contention+"/"+op, func(t *testing.T) {
				wfs := newInvalidateTestWFS(t)
				wfs.atimeMap = make(map[uint64]time.Time)
				wfs.option.ConcurrentWriters = 1
				wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(1)
				current := attributeAppendEntry(1)
				fake := &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
				entered, resume := make(chan struct{}), make(chan struct{})
				var once sync.Once
				t.Cleanup(func() { once.Do(func() { close(resume) }) })
				fake.afterCommit = func() { close(entered); <-resume }

				startFakeFiler(t, wfs, fake)
				path := util.FullPath("/dir/file")
				wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
				local := proto.Clone(current).(*filer_pb.Entry)
				wfs.mapPbIdFromFilerToLocal(local)
				fh := newFileHandle(wfs, 1, 42, local)
				fh.RememberPath(path)
				wfs.fhMap.inode2fh[42], wfs.fhMap.fh2inode[1] = fh, 42
				save := &flakySave{}
				fh.dirtyPages.randomWriter.(*ChunkedDirtyPages).saveDataAsChunk = save.saveDataAsChunk
				t.Cleanup(fh.dirtyPages.Destroy)
				done := make(chan fuse.Status, 1)
				go func() {
					if op == "setxattr" {
						done <- wfs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: 42}}, "user.test", []byte("acknowledged"))
					} else if op == "removexattr" {
						done <- wfs.RemoveXAttr(nil, &fuse.InHeader{NodeId: 42}, "user.remove")
					} else {
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
						done <- wfs.SetAttr(nil, in, &fuse.AttrOut{})
					}
				}()
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("attribute RPC never arrived")
				}
				payload := []byte("background write while attribute RPC waits")
				in := &fuse.WriteIn{}
				in.NodeId, in.Fh, in.Offset, in.Size = 42, 1, 256, uint32(len(payload))
				if n, code := wfs.Write(nil, in, payload); code != fuse.OK || n != uint32(len(payload)) {
					t.Fatalf("WRITE: %d %v", n, code)
				}
				flushDone := make(chan fuse.Status, 1)
				flushSettled, publicationEscaped := false, false
				flush := func() { flushDone <- wfs.doFlush(context.Background(), fh, 1000, 20, false) }
				// Probe ownership while the committed response is held. On the old caller
				// no publication boundary exists, so force its whole-entry flush to commit
				// BEFORE releasing the response. With coordination, queue that same flush
				// behind the attribute and force contention at installation/retention.
				table := wfs.metadataPublicationTable()
				probe, unguarded := table.TryAcquireLock("test publication witness", path, util.ExclusiveLock)
				if unguarded {
					table.ReleaseLock(path, probe)
					flush()
					if code := <-flushDone; code != fuse.OK {
						t.Fatalf("pre-response flush: %v", code)
					}
					once.Do(func() { close(resume) })
				} else {
					release := func() {}
					if contention == "installation" {
						active := wfs.fhLockTable.AcquireLock("test installation contention", fh.fh, util.ExclusiveLock)
						release = func() { wfs.fhLockTable.ReleaseLock(fh.fh, active) }
					} else if contention == "retention" {
						fh.entryLock.Lock()
						release = fh.entryLock.Unlock
					}
					go flush()
					// Do not open the RPC response gate merely because flush was started:
					// witness this caller actually asleep on the publication condition.
					if blocked, code := waitFlushPublication(t, flushDone); !blocked {
						flushSettled, publicationEscaped = true, true
						if code != fuse.OK {
							t.Errorf("flush before response gate: %v", code)
						}
					}
					once.Do(func() { close(resume) })
					if contention != "response" {
						frame := "installAckedEntry"
						if contention == "retention" {
							frame = "retainAcknowledgedAttributes"
						}
						// A stack witness proves the real caller reached the held lock. Time
						// only bounds a broken test; no latency threshold decides correctness.
						blocked := waitAttributeFrame(frame)
						if blocked && !flushSettled {
							// Check again before releasing installation/retention. A prematurely
							// unlocked publication lets the real flush finish while this gate is
							// held: join that flush BEFORE retention, forcing the lost-attr order.
							if waiting, code := waitFlushPublication(t, flushDone); !waiting {
								flushSettled, publicationEscaped = true, true
								if code != fuse.OK {
									t.Errorf("flush before %s gate: %v", contention, code)
								}
							}
						}
						release()
						if !blocked {
							t.Fatalf("caller never contended at %s", frame)
						}
					}
				}
				select {
				case code := <-done:
					if code != fuse.OK {
						t.Fatalf("attribute: %v", code)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("attribute did not finish")
				}
				if !unguarded && !flushSettled {
					select {
					case code := <-flushDone:
						if code != fuse.OK {
							t.Fatalf("flush: %v", code)
						}
					case <-time.After(10 * time.Second):
						t.Fatal("flush did not finish")
					}
				}

				fake.mu.Lock()
				defer fake.mu.Unlock()
				switch op {
				case "chmod":
					if fake.entry.Attributes.FileMode != 0600 {
						t.Fatal("flush undid acknowledged chmod")
					}
				case "chown":
					if fake.entry.Attributes.Uid != 1001 || fake.entry.Attributes.Gid != 21 {
						t.Fatal("flush undid acknowledged chown")
					}
				case "utimes":
					if fake.entry.Attributes.Mtime != 123 || fake.entry.Attributes.MtimeNs != 456 {
						t.Fatal("flush undid acknowledged utimes")
					}
				case "setxattr":
					if string(fake.entry.Extended["xattr-user.test"]) != "acknowledged" {
						t.Fatal("flush undid acknowledged setxattr")
					}
				case "removexattr":
					if _, exists := fake.entry.Extended["xattr-user.remove"]; exists {
						t.Fatal("flush undid acknowledged removexattr")
					}
				}
				if len(save.bodies) != 1 || save.bodies[0] != string(payload) {
					t.Fatalf("pending bytes changed: %q", save.bodies)
				}
				if fake.entry.Attributes.FileSize != 256+uint64(len(payload)) || len(fake.entry.Chunks) != 2 {
					t.Fatalf("pending content lost: %v", fake.entry)
				}
				original, appended := false, false
				for _, chunk := range fake.entry.Chunks {
					switch chunk.Offset {
					case 0:
						original = chunk.GetFileIdString() == current.Chunks[0].GetFileIdString() && chunk.Size == 256
					case 256:
						appended = chunk.GetFileIdString() == "1,01" && chunk.Size == uint64(len(payload))
					}
				}
				if !original || !appended {
					t.Fatalf("chunk identity/range changed: %v", fake.entry.Chunks)
				}
				if publicationEscaped {
					t.Fatal("flush escaped publication before its attribute gate opened")
				}

			})
		}
	}
}

func waitAttributeFrame(frame string) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		if strings.Contains(string(buf[:n]), "."+frame+"(") {
			return true
		}
		runtime.Gosched()
	}
	return false
}

// Require the flush goroutine's waiting state AND both publication frames.
// Seeing a frame alone also admits a runnable waiter awakened by an early
// unlock. Broadcast makes that goroutine runnable before retention is reached,
// so the early-unlock mutant must progress to its real CreateEntry instead.
func waitFlushPublication(t *testing.T, done <-chan fuse.Status) (bool, fuse.Status) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-done:
			return false, code
		default:
		}
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			header, _, _ := strings.Cut(stack, "\n")
			if strings.Contains(header, "[sync.Cond.Wait]") &&
				strings.Contains(stack, ".flushMetadataToFiler(") && strings.Contains(stack, ".lockMetadataPublication(") {
				t.Log("flush arrived and is blocked at publication before opening gate")
				return true, fuse.OK
			}
		}
		runtime.Gosched()
	}
	t.Error("flush neither blocked at publication nor settled")
	return false, fuse.EIO
}

// The filer applies the update even though the caller's syscall was
// interrupted while the RPC was in flight.
type interruptingAttributeFiler struct {
	*attributeCASFiler
	once      sync.Once
	interrupt chan struct{}
}

func (s *interruptingAttributeFiler) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.once.Do(func() { close(s.interrupt) })
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
	}
	return s.attributeCASFiler.UpdateEntry(context.Background(), req)
}

func TestAttributeSaveSurvivesInterrupt(t *testing.T) {
	for _, op := range []string{"chmod", "setxattr"} {
		t.Run(op, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			wfs.atimeMap = make(map[uint64]time.Time)
			current := attributeAppendEntry(300)
			fake := &interruptingAttributeFiler{attributeCASFiler: &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}, interrupt: make(chan struct{})}
			startFakeFiler(t, wfs, fake)
			path := util.FullPath("/dir/file")
			wfs.inodeToPath.Lookup(util.FullPath("/dir"), 1, true, false, 0, false)
			wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
			wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
			if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", current), 1); err != nil {
				t.Fatal(err)
			}
			var code fuse.Status
			if op == "setxattr" {
				in := &fuse.SetXAttrIn{}
				in.NodeId = 42
				code = wfs.SetXAttr(fake.interrupt, in, "user.test", []byte("new"))
			} else {
				in := &fuse.SetAttrIn{}
				in.NodeId = 42
				in.Valid, in.Mode = fuse.FATTR_MODE, 0600
				code = wfs.SetAttr(fake.interrupt, in, &fuse.AttrOut{})
			}
			if code != fuse.OK {
				t.Fatalf("interrupted %s reported %v for an update the filer applied", op, code)
			}
			cached, _, err := wfs.metaCache.FindEntry(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if op == "chmod" && cached.Attr.Mode.Perm() != 0600 {
				t.Fatalf("local cache mode %v, want 0600", cached.Attr.Mode.Perm())
			}
			if op == "setxattr" && string(cached.Extended["xattr-user.test"]) != "new" {
				t.Fatal("local cache missing the saved xattr")
			}
		})
	}
}

// The test mount maps local uid 1000 to filer uid 2000, so a file the filer
// records as uid 1000 reads back as local 1000 and maps forward to 2000.
func TestAttributeSaveKeepsCollidingFilerIds(t *testing.T) {
	for _, op := range []string{"chmod", "setxattr"} {
		t.Run(op, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			wfs.atimeMap = make(map[uint64]time.Time)
			current := attributeAppendEntry(3)
			current.Attributes.Uid = 1000
			fake := &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
			startFakeFiler(t, wfs, fake)
			path := util.FullPath("/dir/file")
			wfs.inodeToPath.Lookup(util.FullPath("/dir"), 1, true, false, 0, false)
			wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
			wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
			if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", current), 1); err != nil {
				t.Fatal(err)
			}
			var code fuse.Status
			if op == "setxattr" {
				in := &fuse.SetXAttrIn{}
				in.NodeId = 42
				code = wfs.SetXAttr(nil, in, "user.test", []byte("new"))
			} else {
				in := &fuse.SetAttrIn{}
				in.NodeId = 42
				in.Valid, in.Mode = fuse.FATTR_MODE, 0600
				code = wfs.SetAttr(nil, in, &fuse.AttrOut{})
			}
			if code != fuse.OK {
				t.Fatalf("%s on a colliding uid: %v", op, code)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.entry.Attributes.Uid != 1000 {
				t.Fatalf("%s rewrote the filer uid to %d", op, fake.entry.Attributes.Uid)
			}
			cached, _, err := wfs.metaCache.FindEntry(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if cached.Attr.Uid != 1000 {
				t.Fatalf("local cache uid %d, want 1000", cached.Attr.Uid)
			}
		})
	}
}

// The seed filer has not applied the owner's latest update yet and forwards
// conditional writes to the owner, as filers do under the lock ring.
type laggingSeedFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	stale *filer_pb.Entry
	owner *attributeCASFiler
}

func (s *laggingSeedFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(s.stale).(*filer_pb.Entry)}, nil
}

func (s *laggingSeedFiler) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	return s.owner.UpdateEntry(ctx, req)
}

func TestAttributeRetryReadsOwnerFiler(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.atimeMap = make(map[uint64]time.Time)
	stale := attributeAppendEntry(1)
	owner := &attributeCASFiler{entry: attributeAppendEntry(300)}
	startFakeFiler(t, wfs, owner)
	ownerAddress := wfs.option.FilerAddresses[0]
	startFakeFiler(t, wfs, &laggingSeedFiler{stale: stale, owner: owner})
	wfs.lockClient = cluster.NewLockClient(nil, wfs.option.FilerAddresses[0])
	wfs.lockClient.SetRing([]pb.ServerAddress{ownerAddress}, 1)

	path := util.FullPath("/dir/file")
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), 1, true, false, 0, false)
	wfs.inodeToPath.Lookup(path, 1, false, false, 42, true)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", stale), 1); err != nil {
		t.Fatal(err)
	}
	in := &fuse.SetAttrIn{}
	in.NodeId = 42
	in.Valid, in.Mode = fuse.FATTR_MODE, 0600
	if code := wfs.SetAttr(nil, in, &fuse.AttrOut{}); code != fuse.OK {
		t.Fatalf("chmod behind a lagging seed filer: %v", code)
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if len(owner.entry.Chunks) != 300 || owner.entry.Attributes.FileMode&0777 != 0600 {
		t.Fatalf("owner entry: %d chunks, mode %o", len(owner.entry.Chunks), owner.entry.Attributes.FileMode)
	}
}
