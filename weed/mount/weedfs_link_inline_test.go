package mount

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A small file the filer keeps inside its entry (-saveToFilerLimit, or
// ?saveInside=true on the upload) has content and no chunks. The filer writes
// a hard link's shared record from each write of one of its names, so the
// create of the new name has to carry the content as the update of the source
// does. Left out, the record lost it, and both names read zeros from then on.
func TestLinkCarriesInlineContentToTheNewName(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	content := []byte("kept inside the entry\n")
	source := &filer_pb.Entry{
		Name:    "inline.txt",
		Content: content,
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o644,
			FileSize: uint64(len(content)),
			Crtime:   1,
			Mtime:    1,
			Uid:      99,
			Gid:      100,
		},
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", source), 0); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	sourcePath := util.FullPath("/inline.txt")
	sourceInode := wfs.inodeToPath.Lookup(sourcePath, source.Attributes.Crtime, false, false, 0, true)

	doLink(t, wfs, sourceInode, "hardlink.txt")

	// The test filer returns no event, so the mount caches each name as it
	// sent it, and its cache keeps one record for a hard link's names as the
	// filer does: a create without the content empties it for both.
	for _, path := range []util.FullPath{sourcePath, "/hardlink.txt"} {
		if got := cachedLink(t, wfs, path).Content; !bytes.Equal(got, content) {
			t.Fatalf("%s has content %q after the link, want %q", path, got, content)
		}
	}
}
