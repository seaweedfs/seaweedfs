package mount

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Past the cap the listing is still complete, but takes no more references.
func TestReadDirPlusStopsAtInodeCap(t *testing.T) {
	dir := util.FullPath("/images")
	names := make([]string, 40)
	for i := range names {
		names[i] = fmt.Sprintf("f%03d.jpg", i)
	}
	wfs := newPagingWFS(t, dir, names, 0)
	wfs.option.MaxInodeEntries = 10
	dirInode, _ := wfs.inodeToPath.GetInode(dir)

	sink := &benchSink{plus: true, takesRef: true, sinkLimit: 8}
	if got := walkOnce(t, wfs, dirInode, sink, false); got != len(names)+2 {
		t.Fatalf("listed %d entries, want %d", got, len(names)+2)
	}
	if got := wfs.inodeToPath.Len(); got >= len(names) {
		t.Fatalf("inode table holds %d, want it capped well under %d", got, len(names))
	}

	// Starting at the cap, nothing gets attributes.
	dhid, _ := wfs.AcquireDirectoryHandle()
	defer wfs.ReleaseDirectoryHandle(dhid)
	sink.reset()
	status := wfs.doReadDirectory(&fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: dirInode},
		Fh:       uint64(dhid),
		Size:     1 << 20,
	}, sink, true)
	if status != fuse.OK {
		t.Fatalf("readdir: %v", status)
	}
	if sink.count == 0 {
		t.Fatal("listed nothing")
	}
	for i := range sink.attrs {
		if sink.attrs[i].NodeId != 0 {
			t.Fatalf("entry %d took a reference past the cap", i)
		}
	}
}
