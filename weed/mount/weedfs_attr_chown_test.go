package mount

import (
	"runtime"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestChownKillsSetId(t *testing.T) {
	for _, tc := range []struct {
		goos        string
		callerUid   uint32
		isDirectory bool
		want        bool
	}{
		{"linux", 1000, false, false},
		{"linux", 0, false, false},
		{"darwin", 1000, false, true},
		{"darwin", 0, false, false},
		{"darwin", 1000, true, false},
		{"freebsd", 1000, false, true},
	} {
		if got := chownKillsSetId(tc.goos, tc.callerUid, tc.isDirectory); got != tc.want {
			t.Errorf("chownKillsSetId(%q, %d, %v) = %v, want %v", tc.goos, tc.callerUid, tc.isDirectory, got, tc.want)
		}
	}
}

// On Linux the kernel sends the cleared mode with a chown that must drop the
// set-id bits; an ownership-only request keeps them.
func TestChownKeepsSetIdBitsTheKernelKeeps(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the kernel clears the set-id bits on Linux")
	}
	for _, tc := range []struct {
		name      string
		caller    uint32
		valid     uint32
		mode      uint32
		directory bool
	}{
		{"root gives a file away", 0, fuse.FATTR_UID | fuse.FATTR_GID, 0o6644, false},
		{"user changes the group of a file without group execute", 1000, fuse.FATTR_GID, 0o2644, false},
		{"root gives a directory away", 0, fuse.FATTR_UID | fuse.FATTR_GID, 0o6755, true},
		{"user changes a directory's group", 1000, fuse.FATTR_GID, 0o2775, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs := &WFS{option: &Option{}}
			entry := &filer_pb.Entry{Name: "x", IsDirectory: tc.directory, Attributes: &filer_pb.FuseAttributes{FileMode: tc.mode, Uid: 1000, Gid: 1000}}
			in := &fuse.SetAttrIn{}
			in.NodeId = 2
			in.Caller.Uid = tc.caller
			in.Valid = tc.valid
			in.Owner = fuse.Owner{Uid: 2000, Gid: 2000}
			if status := wfs.applySetAttr("/x", in, entry); status != fuse.OK {
				t.Fatalf("applySetAttr: %v", status)
			}
			if got := entry.Attributes.FileMode; got != tc.mode {
				t.Fatalf("mode after chown = %o, want %o", got, tc.mode)
			}

			live := &filer_pb.Entry{Name: "x", IsDirectory: tc.directory, Attributes: &filer_pb.FuseAttributes{FileMode: tc.mode}}
			retainSetAttr(in, live, entry)
			if got := live.Attributes.FileMode; got != tc.mode {
				t.Fatalf("retained mode after chown = %o, want %o", got, tc.mode)
			}
		})
	}
}
