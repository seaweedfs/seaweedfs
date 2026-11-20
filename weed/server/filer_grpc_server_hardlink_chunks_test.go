package weed_server

import (
	"context"
	"slices"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A write that replaces one name of a hard-linked file unlinks that name and
// nothing else: the file's other names keep reading its chunks. These tests
// write over /dir/dst.txt, one of the links set up by hardLinkedTarget, whose
// file's only chunk is 3,0c03; with two links /dir/other.txt is the second.

const replacedChunk, writtenChunk = "3,0c03", "4,0d04"

func fileEntryWithChunk(path string, inode uint64, fileId string) *filer.Entry {
	entry := newFileEntry(path, inode)
	entry.Chunks = []*filer_pb.FileChunk{{FileId: fileId, Offset: 0, Size: 4, ModifiedTsNs: 1}}
	return entry
}

func queuedDeletions(f *filer.Filer) []string {
	var ids []string
	for i := 0; i < 2; i++ {
		f.FileIdDeletionQueue.Consume(func(batch []string) { ids = append(ids, batch...) })
	}
	sort.Strings(ids)
	return ids
}

// hardLinkedTarget puts /dir/dst.txt in the store as one of links hard links
// to a file whose only chunk is 3,0c03; with two links /dir/other.txt is the
// second.
func hardLinkedTarget(t *testing.T, store *renameTestStore, links int32) filer.HardLinkId {
	t.Helper()
	id := filer.HardLinkId("hl-chunks-test-01")
	shared := fileEntryWithChunk("/dir/dst.txt", 303, replacedChunk)
	shared.HardLinkId, shared.HardLinkCounter = id, links
	blob, err := shared.EncodeAttributesAndChunks()
	if err != nil {
		t.Fatalf("encode hard link: %v", err)
	}
	store.kv = map[string][]byte{string(id): blob}
	for _, p := range []string{"/dir/dst.txt", "/dir/other.txt"}[:min(int(links), 2)] {
		link := newFileEntry(p, 303)
		link.HardLinkId, link.HardLinkCounter = id, links
		store.entries[p] = link
	}
	return id
}

func hardLinkChunkServer(t *testing.T, links int32) (*FilerServer, *filer.Filer, *renameTestStore, filer.HardLinkId) {
	t.Helper()
	store := newRenameTestStore()
	store.entries["/dir"] = newDirectoryEntry("/dir", 100)
	id := hardLinkedTarget(t, store, links)
	swapNotificationQueue(t, &captureQueue{})
	f := newRenameTestFiler(t, store)
	return &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}, f, store, id
}

func written(fileId string, tsNs int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fileId, Offset: 0, Size: 4, ModifiedTsNs: tsNs}
}

func dstEntry(hardLinkId filer.HardLinkId, links int32, chunks ...*filer_pb.FileChunk) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:            "dst.txt",
		Attributes:      &filer_pb.FuseAttributes{Mtime: 1700000100, FileMode: 0644, Inode: 404, FileSize: 4},
		Chunks:          chunks,
		HardLinkId:      hardLinkId,
		HardLinkCounter: links,
	}
}

// checkDeletions compares the distinct chunks queued for deletion: a chunk
// both the old entry's diff and the request's covered chunks name is queued
// twice, which deletes it once.
func checkDeletions(t *testing.T, f *filer.Filer, want ...string) {
	t.Helper()
	got := slices.Compact(queuedDeletions(f))
	if len(got) != len(want) {
		t.Fatalf("queued chunk deletions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("queued chunk deletions = %v, want %v", got, want)
		}
	}
}

// checkOtherLink checks that /dir/other.txt, the surviving link, reads
// wantChunk with wantLinks links.
func checkOtherLink(t *testing.T, f *filer.Filer, wantChunk string, wantLinks int32) {
	t.Helper()
	other, err := f.FindEntry(context.Background(), "/dir/other.txt")
	if err != nil {
		t.Fatalf("find the other link: %v", err)
	}
	if other.HardLinkCounter != wantLinks {
		t.Fatalf("other link count = %d, want %d", other.HardLinkCounter, wantLinks)
	}
	if chunks := other.GetChunks(); len(chunks) != 1 || chunks[0].GetFileIdString() != wantChunk {
		t.Fatalf("other link chunks = %v, want %s", chunks, wantChunk)
	}
}

func createDst(t *testing.T, server *FilerServer, entry *filer_pb.Entry) {
	t.Helper()
	resp, err := server.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/dir", Entry: entry, SkipCheckParentDirectory: true,
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("CreateEntry: %v %q", err, resp.GetError())
	}
}

func updateDst(t *testing.T, server *FilerServer, entry *filer_pb.Entry) {
	t.Helper()
	if _, err := server.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{Directory: "/dir", Entry: entry}); err != nil {
		t.Fatalf("UpdateEntry: %v", err)
	}
}

// A create over the path (an S3 PUT, a client that does not know the path is
// a hard link) replaces dst.txt with a new file.
func TestCreateEntryOverHardLinkKeepsSharedChunks(t *testing.T) {
	server, f, store, id := hardLinkChunkServer(t, 2)
	createDst(t, server, dstEntry(nil, 0, written(writtenChunk, 2)))

	checkDeletions(t, f)
	checkOtherLink(t, f, replacedChunk, 1)
	if _, found := store.kv[string(id)]; !found {
		t.Fatal("hard link record deleted while a link remains")
	}
	dst, err := f.FindEntry(context.Background(), "/dir/dst.txt")
	if err != nil {
		t.Fatalf("find dst: %v", err)
	}
	if len(dst.HardLinkId) != 0 || len(dst.GetChunks()) != 1 || dst.GetChunks()[0].GetFileIdString() != writtenChunk {
		t.Fatalf("dst = link %x chunks %v, want a plain file with %s", dst.HardLinkId, dst.GetChunks(), writtenChunk)
	}
}

func TestCreateEntryOverLastHardLinkDeletesItsChunks(t *testing.T) {
	server, f, store, id := hardLinkChunkServer(t, 1)
	createDst(t, server, dstEntry(nil, 0, written(writtenChunk, 2)))

	checkDeletions(t, f, replacedChunk)
	if _, found := store.kv[string(id)]; found {
		t.Fatal("hard link record kept after its last link was replaced")
	}
}

// A create whose chunk list still carries the file's chunk under a newer one
// (a client writing over the range from a stale view): the covered chunk is
// the other link's data.
func TestCreateEntryOverHardLinkKeepsSharedChunkItCovers(t *testing.T) {
	server, f, _, _ := hardLinkChunkServer(t, 2)
	createDst(t, server, dstEntry(nil, 0, written(replacedChunk, 1), written(writtenChunk, 2)))

	checkDeletions(t, f)
	checkOtherLink(t, f, replacedChunk, 1)
}

// The lookup of dst.txt fails to read the shared record, so the replaced
// entry is the name's own row, which has no chunks.
func TestCreateEntryOverHardLinkKeepsSharedChunkItCoversAfterFailedLookup(t *testing.T) {
	server, f, store, _ := hardLinkChunkServer(t, 2)
	store.kvGetErrs = 1
	createDst(t, server, dstEntry(nil, 0, written(replacedChunk, 1), written(writtenChunk, 2)))

	checkDeletions(t, f)
	checkOtherLink(t, f, replacedChunk, 1)
}

func TestCreateEntryOverLastHardLinkDeletesChunkItCovers(t *testing.T) {
	server, f, _, _ := hardLinkChunkServer(t, 1)
	createDst(t, server, dstEntry(nil, 0, written(replacedChunk, 1), written(writtenChunk, 2)))

	checkDeletions(t, f, replacedChunk)
}

// An update that no longer carries the hard link (a client with a stale view
// of dst.txt) drops this name's link like a create over it.
func TestUpdateEntryDroppingHardLinkKeepsSharedChunks(t *testing.T) {
	server, f, store, id := hardLinkChunkServer(t, 2)
	updateDst(t, server, dstEntry(nil, 0, written(writtenChunk, 2)))

	checkDeletions(t, f)
	checkOtherLink(t, f, replacedChunk, 1)
	if _, found := store.kv[string(id)]; !found {
		t.Fatal("hard link record deleted while a link remains")
	}
}

func TestUpdateEntryDroppingLastHardLinkDeletesItsChunks(t *testing.T) {
	server, f, _, _ := hardLinkChunkServer(t, 1)
	updateDst(t, server, dstEntry(nil, 0, written(writtenChunk, 2)))

	checkDeletions(t, f, replacedChunk)
}

// Writing the file through one of its names changes the file for every name,
// so the chunk the write replaced is garbage.
func TestUpdateEntryThroughHardLinkDeletesReplacedChunk(t *testing.T) {
	server, f, _, id := hardLinkChunkServer(t, 2)
	updateDst(t, server, dstEntry(id, 2, written(writtenChunk, 2)))

	checkDeletions(t, f, replacedChunk)
	checkOtherLink(t, f, writtenChunk, 2)
}

func renameSrcOverHardLinkedDst(t *testing.T, server *FilerServer, store *renameTestStore) {
	t.Helper()
	store.entries["/dir/src.txt"] = fileEntryWithChunk("/dir/src.txt", 101, "1,0a01")
	if _, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/dir", OldName: "src.txt", NewDirectory: "/dir", NewName: "dst.txt",
	}); err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}
	dst, err := server.filer.FindEntry(context.Background(), "/dir/dst.txt")
	if err != nil {
		t.Fatalf("find dst: %v", err)
	}
	if chunks := dst.GetChunks(); len(chunks) != 1 || chunks[0].GetFileIdString() != "1,0a01" {
		t.Fatalf("dst chunks = %v, want the renamed file's 1,0a01", chunks)
	}
}

// Saving a file by writing a temporary one and renaming it over the old
// name (Python's .pyc writes, checkpoint savers) must not take the data of
// another hard link to the old file with it.
func TestAtomicRenameEntryOverHardLinkKeepsSharedChunks(t *testing.T) {
	server, f, store, id := hardLinkChunkServer(t, 2)
	renameSrcOverHardLinkedDst(t, server, store)

	checkDeletions(t, f)
	checkOtherLink(t, f, replacedChunk, 1)
	if _, found := store.kv[string(id)]; !found {
		t.Fatal("hard link record deleted while a link remains")
	}
}

func TestAtomicRenameEntryOverLastHardLinkDeletesItsChunks(t *testing.T) {
	server, f, store, id := hardLinkChunkServer(t, 1)
	renameSrcOverHardLinkedDst(t, server, store)

	checkDeletions(t, f, replacedChunk)
	if _, found := store.kv[string(id)]; found {
		t.Fatal("hard link record kept after its last link was replaced")
	}
}
