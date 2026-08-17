package leveldb

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// listHookStore runs a hook once, right after a directory listing returns, so a
// test can act inside the window between a delete's emptiness check and the
// removal of the folder.
type listHookStore struct {
	filer.FilerStore
	hook  func()
	fired bool
}

func (s *listHookStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	lastFileName, err := s.FilerStore.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix, eachEntryFunc)
	if s.hook != nil && !s.fired {
		s.fired = true
		s.hook()
	}
	return lastFileName, err
}

// TestNonRecursiveFolderDeleteKeepsRacingChild covers the S3 empty-folder
// cleanup path: the delete lists a folder, finds it empty, and must not then
// bulk-delete its children, because an object written in between would be
// destroyed after the write was already acknowledged.
func TestNonRecursiveFolderDeleteKeepsRacingChild(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	store := &LevelDB2Store{}
	if err := store.initialize(t.TempDir(), 2); err != nil {
		t.Fatal(err)
	}
	hooked := &listHookStore{FilerStore: store}
	testFiler.SetStore(hooked)

	// the test has no metadata log consumer
	ctx := filer.WithSuppressedMetadataEvents(context.Background())
	dir := util.FullPath("/buckets/testbucket/data/abc")
	child := dir.Child("obj")

	dirEntry := &filer.Entry{FullPath: dir, Attr: filer.Attr{Mode: os.ModeDir | 0755}}
	if err := testFiler.CreateEntry(ctx, dirEntry, nil, false, false, nil, false, testFiler.MaxFilenameLength); err != nil {
		t.Fatalf("create folder: %v", err)
	}

	hooked.hook = func() {
		entry := &filer.Entry{FullPath: child, Attr: filer.Attr{Mode: 0640}}
		if err := testFiler.CreateEntry(ctx, entry, nil, false, false, nil, false, testFiler.MaxFilenameLength); err != nil {
			t.Errorf("create entry racing the folder delete: %v", err)
		}
	}

	if err := testFiler.DeleteEntryMetaAndData(ctx, dir, false, false, false, false, nil, 0); err != nil {
		t.Fatalf("delete empty folder: %v", err)
	}

	if _, err := testFiler.FindEntry(ctx, child); err != nil {
		t.Errorf("entry created during the folder delete was removed: %v", err)
	}

	// The folder entry itself still goes, so the surviving entry is reachable by
	// path but absent from listings until the folder comes back. Pinning that here
	// keeps the remaining exposure visible; tighten it if the two steps ever become
	// atomic.
	if _, err := testFiler.FindEntry(ctx, dir); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Errorf("folder entry should still be removed, got %v", err)
	}
}

// TestEnsureDirectoryEntryRestoresRacingParent covers the leftover of the same
// race: the entry survives the folder delete but its directory does not, so the
// entry drops out of listings until the directory is put back.
func TestEnsureDirectoryEntryRestoresRacingParent(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	store := &LevelDB2Store{}
	if err := store.initialize(t.TempDir(), 2); err != nil {
		t.Fatal(err)
	}
	hooked := &listHookStore{FilerStore: store}
	testFiler.SetStore(hooked)

	ctx := filer.WithSuppressedMetadataEvents(context.Background())
	parent := util.FullPath("/buckets/testbucket/data")
	dir := parent.Child("abc")
	child := dir.Child("obj")

	dirEntry := &filer.Entry{FullPath: dir, Attr: filer.Attr{Mode: os.ModeDir | 0755}}
	if err := testFiler.CreateEntry(ctx, dirEntry, nil, false, false, nil, false, testFiler.MaxFilenameLength); err != nil {
		t.Fatalf("create folder: %v", err)
	}

	// the cleaner reads these before deleting, so a restore does not have to guess
	attrs, err := testFiler.DirectoryAttributes(ctx, dir)
	if err != nil {
		t.Fatalf("read folder attributes: %v", err)
	}

	hooked.hook = func() {
		entry := &filer.Entry{FullPath: child, Attr: filer.Attr{Mode: 0640}}
		if err := testFiler.CreateEntry(ctx, entry, nil, false, false, nil, false, testFiler.MaxFilenameLength); err != nil {
			t.Errorf("create entry racing the folder delete: %v", err)
		}
	}

	if err := testFiler.DeleteEntryMetaAndData(ctx, dir, false, false, false, false, nil, 0); err != nil {
		t.Fatalf("delete empty folder: %v", err)
	}

	if names := listNames(ctx, t, testFiler, parent); len(names) != 0 {
		t.Fatalf("folder should be gone from its parent before the restore, got %v", names)
	}

	if err := testFiler.EnsureDirectoryEntry(ctx, dir, attrs); err != nil {
		t.Fatalf("restore folder: %v", err)
	}

	restored, err := testFiler.FindEntry(ctx, dir)
	if err != nil {
		t.Fatalf("find restored folder: %v", err)
	}
	if !restored.IsDirectory() {
		t.Errorf("restored %s is not a directory", dir)
	}
	if names := listNames(ctx, t, testFiler, parent); len(names) != 1 || names[0] != "abc" {
		t.Errorf("restored folder should be listed under its parent, got %v", names)
	}
}

// TestEnsureDirectoryEntryRestoresExactAttributes checks that a restored directory
// comes back exactly as it was. Guessing from an ancestor, or forcing traversal bits,
// would hand back a directory that grants access the original one denied.
func TestEnsureDirectoryEntryRestoresExactAttributes(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	store := &LevelDB2Store{}
	if err := store.initialize(t.TempDir(), 2); err != nil {
		t.Fatal(err)
	}
	testFiler.SetStore(store)

	ctx := filer.WithSuppressedMetadataEvents(context.Background())
	// a private directory under a world-traversable parent
	dir := util.FullPath("/buckets/testbucket/data").Child("private")
	dirEntry := &filer.Entry{FullPath: dir, Attr: filer.Attr{Mode: os.ModeDir | 0700, Uid: 4242, Gid: 4343}}
	if err := testFiler.CreateEntry(ctx, dirEntry, nil, false, false, nil, false, testFiler.MaxFilenameLength); err != nil {
		t.Fatalf("create folder: %v", err)
	}

	attrs, err := testFiler.DirectoryAttributes(ctx, dir)
	if err != nil {
		t.Fatalf("read folder attributes: %v", err)
	}
	if err := testFiler.DeleteEntryMetaAndData(ctx, dir, false, false, false, false, nil, 0); err != nil {
		t.Fatalf("delete folder: %v", err)
	}

	if err := testFiler.EnsureDirectoryEntry(ctx, dir, attrs); err != nil {
		t.Fatalf("restore folder: %v", err)
	}

	restored, err := testFiler.FindEntry(ctx, dir)
	if err != nil {
		t.Fatalf("find restored folder: %v", err)
	}
	if !restored.IsDirectory() {
		t.Errorf("restored %s is not a directory", dir)
	}
	if restored.Mode.Perm() != 0700 {
		t.Errorf("restored folder should keep mode 0700, got %o", restored.Mode.Perm())
	}
	if restored.Uid != 4242 || restored.Gid != 4343 {
		t.Errorf("restored folder should keep its owner, got uid=%d gid=%d", restored.Uid, restored.Gid)
	}
}

func listNames(ctx context.Context, t *testing.T, f *filer.Filer, dir util.FullPath) []string {
	t.Helper()
	entries, _, err := f.ListDirectoryEntries(ctx, dir, "", false, 100, "", "", "")
	if err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
