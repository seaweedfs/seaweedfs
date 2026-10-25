package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestOpenDirEnablesKernelListingCache pins the reply flags: without them the
// kernel calls back for every enumeration, and losing them would silently
// revert repeat listings to full walks of the mount.
func TestOpenDirEnablesKernelListingCache(t *testing.T) {
	dir := util.FullPath("/images")
	wfs := newBenchWFS(t, dir, 4)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)

	var out fuse.OpenOut
	if status := wfs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dirInode}}, &out); status != fuse.OK {
		t.Fatalf("OpenDir: %v", status)
	}
	defer wfs.ReleaseDir(&fuse.ReleaseIn{Fh: out.Fh})

	for _, want := range []struct {
		name string
		flag uint32
	}{{"FOPEN_CACHE_DIR", fuse.FOPEN_CACHE_DIR}, {"FOPEN_KEEP_CACHE", fuse.FOPEN_KEEP_CACHE}} {
		if out.OpenFlags&want.flag == 0 {
			t.Errorf("OpenDir reply lacks %s", want.name)
		}
	}
}
