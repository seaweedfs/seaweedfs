package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestChownSetIdBitsFollowCallerAndKeepDirectories(t *testing.T) {
	for _, tc := range []struct {
		name      string
		caller    uint32
		newOwner  uint32
		directory bool
		wantMode  uint32
	}{
		{"root gives a file away", 0, 1000, false, 0o6755},
		{"user gives a file to root", 1000, 0, false, 0o755},
		{"user changes a file's group", 1000, 1000, false, 0o755},
		{"root gives a directory away", 0, 1000, true, 0o6755},
		{"user changes a directory's group", 1000, 1000, true, 0o6755},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs := &WFS{option: &Option{}}
			entry := &filer_pb.Entry{Name: "x", IsDirectory: tc.directory, Attributes: &filer_pb.FuseAttributes{FileMode: 0o6755, Uid: 1000, Gid: 1000}}
			in := &fuse.SetAttrIn{}
			in.NodeId = 2
			in.Caller.Uid = tc.caller
			in.Valid = fuse.FATTR_UID | fuse.FATTR_GID
			in.Owner = fuse.Owner{Uid: tc.newOwner, Gid: 2000}
			if status := wfs.applySetAttr("/x", in, entry); status != fuse.OK {
				t.Fatalf("applySetAttr: %v", status)
			}
			if got := entry.Attributes.FileMode; got != tc.wantMode {
				t.Fatalf("mode after chown = %o, want %o", got, tc.wantMode)
			}

			live := &filer_pb.Entry{Name: "x", IsDirectory: tc.directory, Attributes: &filer_pb.FuseAttributes{FileMode: 0o6755}}
			retainSetAttr(in, live, entry)
			if got := live.Attributes.FileMode; got != tc.wantMode {
				t.Fatalf("retained mode after chown = %o, want %o", got, tc.wantMode)
			}
		})
	}
}
