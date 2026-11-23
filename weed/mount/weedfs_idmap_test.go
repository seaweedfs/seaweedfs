package mount

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// On a mount with -idmap the kernel sends unknownCallerId for every request
// that creates no inode. The caller-dependent rules must then neither treat
// the caller as an unprivileged stranger nor record its id on an entry.

func callerHeader(nodeId uint64, uid, gid uint32) fuse.InHeader {
	return fuse.InHeader{NodeId: nodeId, Caller: fuse.Caller{Owner: fuse.Owner{Uid: uid, Gid: gid}}}
}

// stickyDirFiler adds deletes to renameRaceFiler.
type stickyDirFiler struct {
	*renameRaceFiler
}

func (s stickyDirFiler) DeleteEntry(_ context.Context, r *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, util.NewFullPath(r.Directory, r.Name))
	return &filer_pb.DeleteEntryResponse{}, nil
}

// newStickyDirWFS mounts /sticky, a sticky directory of uid 123 that holds
// the files "unlinked" and "renamed" and the directory "dir", each of uid
// 1000, and the plain directory /final.
func newStickyDirWFS(t *testing.T) (wfs *WFS, sticky, final uint64) {
	t.Helper()
	wfs, _ = newCreateTestWFS(t)
	fake := &renameRaceFiler{entries: map[util.FullPath]*filer_pb.Entry{}}
	startFakeFiler(t, wfs, stickyDirFiler{fake})
	entry := func(p util.FullPath, mode uint32, uid uint32) {
		fake.entries[p] = &filer_pb.Entry{
			Name:        p.Name(),
			IsDirectory: mode&(1<<31) != 0,
			Attributes:  &filer_pb.FuseAttributes{FileMode: mode, Uid: uid, Gid: uid, Mtime: 1, Crtime: 1},
		}
	}
	entry("/sticky", 0o1777|1<<31, 123)
	entry("/sticky/unlinked", 0o644, 1000)
	entry("/sticky/renamed", 0o644, 1000)
	entry("/sticky/dir", 0o755|1<<31, 1000)
	entry("/final", 0o777|1<<31, 123)
	now := time.Now().Unix()
	sticky = wfs.inodeToPath.Lookup("/sticky", now, true, false, 0, true)
	final = wfs.inodeToPath.Lookup("/final", now, true, false, 0, true)
	return wfs, sticky, final
}

// The kernel enforces the sticky bit before it sends unlink, rmdir or
// rename; with -idmap these requests carry the unknown caller.
func TestStickyDirectoryUnknownCaller(t *testing.T) {
	for _, tc := range []struct {
		name   string
		caller uint32
		want   fuse.Status
	}{
		{"unknown caller", unknownCallerId, fuse.OK},
		{"stranger", 2000, fuse.EPERM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs, sticky, final := newStickyDirWFS(t)
			header := callerHeader(sticky, tc.caller, tc.caller)
			if code := wfs.Unlink(make(chan struct{}), &header, "unlinked"); code != tc.want {
				t.Errorf("Unlink = %v, want %v", code, tc.want)
			}
			if code := wfs.Rmdir(make(chan struct{}), &header, "dir"); code != tc.want {
				t.Errorf("Rmdir = %v, want %v", code, tc.want)
			}
			in := &fuse.RenameIn{InHeader: header, Newdir: final}
			if code := wfs.Rename(make(chan struct{}), in, "renamed", "renamed"); code != tc.want {
				t.Errorf("Rename = %v, want %v", code, tc.want)
			}
		})
	}
}

func TestWriteByUnknownCallerClearsSetIdBitsOnlyWhenFlagged(t *testing.T) {
	for _, tc := range []struct {
		name       string
		caller     uint32
		writeFlags uint32
		wantMode   uint32
	}{
		{"unknown caller", unknownCallerId, 0, 0o6755},
		{"unknown caller without CAP_FSETID", unknownCallerId, fuse.WRITE_KILL_SUIDGID, 0o755},
		{"root", 0, 0, 0o6755},
		{"non-root", 1000, 0, 0o755},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs, inode, fh := newUnlinkedOpenFile(t)
			fh.GetEntry().GetEntry().Attributes.FileMode = 0o6755

			in := &fuse.WriteIn{InHeader: callerHeader(inode, tc.caller, tc.caller), Fh: uint64(fh.fh), Size: 4, WriteFlags: tc.writeFlags}
			if _, status := wfs.Write(nil, in, []byte("data")); status != fuse.OK {
				t.Fatalf("Write: %v", status)
			}
			if got := fh.GetEntry().GetEntry().Attributes.FileMode; got != tc.wantMode {
				t.Fatalf("mode after write = %o, want %o", got, tc.wantMode)
			}
		})
	}
}

// A flush stamps its caller on an entry without an owner; an unknown caller
// leaves the entry as it is.
func TestFlushByUnknownCallerKeepsOwner(t *testing.T) {
	for _, tc := range []struct {
		name    string
		caller  uint32
		wantUid uint32
	}{
		{"unknown caller", unknownCallerId, 0},
		{"known caller", 7, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs, fh, _ := newSlowDataFlush(t, 0, time.Second, false)

			in := &fuse.FlushIn{InHeader: callerHeader(fh.inode, tc.caller, tc.caller), Fh: uint64(fh.fh)}
			if status := wfs.Flush(nil, in); status != fuse.OK {
				t.Fatalf("Flush: %v", status)
			}
			attr := fh.GetEntry().GetEntry().Attributes
			if attr.Uid != tc.wantUid || attr.Gid != tc.wantUid {
				t.Fatalf("owner after flush = %d:%d, want %d:%d", attr.Uid, attr.Gid, tc.wantUid, tc.wantUid)
			}
		})
	}
}
